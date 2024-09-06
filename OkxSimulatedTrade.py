import asyncio
import json
import hmac
import hashlib
import base64
import time
import random
import string
import websockets
import logging
import StatusChecker as sc


class OkxSimulatedTrader:
    def __init__(self, api_key: str, secret_key: str, passphrase: str, user_id: str = 'cuicui',
                 order_timeout: int = 10, ordtype: str = 'post_only', log_file: str = 'trader_info.log'):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.base_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        self.user_id = user_id
        self.order_timeout = order_timeout
        self.ordType = ordtype
        self.private_websocket = None
        self.orders = {}  # 存储订单信息
        self.order_tasks = {}  # 存储订单任务
        self.message_list = []
        self.order_checker = sc.OrderStatusChecker(api_key, secret_key, passphrase)
        self._initialize_logging(log_file)  # 初始化日志记录

    def _initialize_logging(self, log_file: str) -> None:
        """初始化日志记录"""
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logging initialized")

    async def connect_and_login(self) -> None:
        """建立WebSocket连接并登录"""
        try:
            self.private_websocket = await websockets.connect(self.base_url)
            await self.login(self.private_websocket)
            await self.subscribe_to_orders_channel()
            asyncio.create_task(self.consume_messages())  # 启动消息处理任务
        except Exception as e:
            self.logger.error(f"连接失败: {e}")

    def generate_signature(self, timestamp: str, method: str, request_path: str, body: str = '') -> str:
        """生成登录签名"""
        message = f'{timestamp}{method.upper()}{request_path}{body}'
        mac = hmac.new(self.secret_key.encode(), message.encode(), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode()

    async def login(self, websocket: websockets.WebSocketClientProtocol) -> None:
        """发送登录请求"""
        try:
            timestamp = str(int(time.time()))
            sign = self.generate_signature(timestamp, 'GET', '/users/self/verify')
            login_param = {
                "op": "login",
                "args": [{
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": sign
                }]
            }
            await websocket.send(json.dumps(login_param))
            response = await websocket.recv()
            response_data = json.loads(response)
            self.logger.info(f'login, {response_data}')
            if response_data.get("code") == "0":
                self.logger.info("OKXSimulatedTrader 连接并登录成功！")
            else:
                self.logger.error(f"OKXSimulatedTrader登录失败: {response_data.get('msg', '未知错误')}")
        except Exception as e:
            self.logger.error(f"登录过程中发生错误: {e}")

    async def subscribe_to_orders_channel(self) -> None:
        """订阅订单频道"""
        try:
            subscribe_param = {
                "op": "subscribe",
                "args": [{"channel": "orders", "instType": "SPOT"}]  # 订阅具体的市场和产品
            }
            await self.private_websocket.send(json.dumps(subscribe_param))
            self.logger.info("Sent subscription request to orders channel.")

            # 处理订阅确认信息
            response = await self.private_websocket.recv()
            data = json.loads(response)
            self.logger.info(f'subscribe_to_orders_channel: {data}')

            if data.get("event") == "subscribe" and data.get("arg", {}).get("channel") == "orders":
                self.logger.info("Subscription to orders channel successful!")
            else:
                self.logger.error(f"Failed to subscribe to orders channel: {data.get('msg', 'Unknown error')}")
                return

            # 处理连接数量信息
            response = await self.private_websocket.recv()
            data = json.loads(response)
            self.logger.info(f'channel-conn-count: {data}')

            if data.get("event") == "channel-conn-count":
                conn_count = data.get("connCount", "Unknown")
                self.logger.info(f"Current connection count for orders channel: {conn_count}")

        except Exception as e:
            self.logger.error(f"Error while subscribing to orders channel: {e}")

    @staticmethod
    def generate_clordid() -> str:
        """生成随机的clOrdId"""
        length = random.randint(1, 32)
        characters = string.ascii_letters + string.digits
        return ''.join(random.choice(characters) for _ in range(length))

    def check_order_status(self, inst_id: str, cl_ord_id: str) -> str:
        """
        调用 OrderStatusChecker 类的方法，查询订单状态
        """
        return self.order_checker.get_order_status(inst_id, cl_ord_id)

    async def place_limit_order(self, symbol: str, side: str, price: float, size: float) -> None:
        """下限价单"""
        ordtype = self.ordType
        clordid = self.generate_clordid()
        message_queue = asyncio.Queue()  # 为每个订单创建独立的消息队列
        self.orders[clordid] = {
            'symbol': symbol,
            'side': side,
            'price': price,
            'size': size,
            'status': 'pending',
            'message_queue': message_queue  # 存储消息队列
        }

        # 为每个订单创建一个独立的任务
        task = asyncio.create_task(self.process_order(clordid, symbol, side, price, size, ordtype))
        self.order_tasks[clordid] = task
        self.logger.info(f"Created task for order {clordid}")

    async def process_order(self, clordid: str, symbol: str, side: str, price: float, size: float, ordtype: str) -> None:
        """处理单个订单的提交、监控、取消等操作"""
        try:
            # 1. 提交订单
            success = await self.submit_order(clordid, symbol, side, price, size, ordtype)
            if not success:
                self.logger.error(f"Order {clordid} failed to place. Skipping further processing.")
                return  # 订单提交失败，跳过后续处理

            # 2. 等待超时后尝试取消订单
            await asyncio.sleep(self.order_timeout)
            order_status = self.order_checker.get_order_status(symbol, clordid)

            if order_status in ("filled", "canceled"):
                self.logger.info(f"Order {clordid} is already {order_status}. Skipping cancellation.")
                self.orders[clordid]['status'] = order_status  # 更新订单状态
                return  # 跳过取消操作
            await self.cancel_order(clordid)

        finally:
            self.order_tasks.pop(clordid, None)
            self.logger.info(f"Task for order {clordid} completed")

    async def submit_order(self, clordid: str, symbol: str, side: str, price: float, size: float, ordtype: str) -> bool:
        """模拟提交订单的逻辑"""
        try:
            order_param = {
                "id": self.user_id,
                "op": "order",
                "args": [{
                    "side": side,
                    "instId": symbol,
                    "tdMode": "isolated",
                    "ordType": ordtype,
                    "px": price,
                    "sz": size,
                    "clOrdId": clordid
                }]
            }
            await self.private_websocket.send(json.dumps(order_param))
            self.logger.info(f"Sent {side} limit order for {size} {symbol} at {price}.")

            # 等待消息并处理
            data = await self.orders[clordid]['message_queue'].get()
            self.logger.info(f"submit_order, {data}")
            if data.get("code") == "0":
                self.logger.info(f"Order placed successfully with ID: {clordid}")
                return True
            else:
                self.logger.error(f"Failed to place order: {data.get('msg', '未知错误')}")
                return False
        except Exception as e:
            self.logger.error(f"Error while submitting order: {e}")
            return False

    async def cancel_order(self, clordid: str) -> None:
        """取消订单"""
        if not clordid:
            self.logger.warning("No order to cancel.")
            return
        order = self.orders.get(clordid)
        inst_id = order.get('symbol') if order else None
        if not inst_id:
            self.logger.warning(f"Missing symbol for order with clOrdId: {clordid}")
            return
        try:
            cancel_param = {
                "id": self.user_id,
                "op": "cancel-order",
                "args": [{"instId": inst_id, "clOrdId": clordid}]
            }
            await self.private_websocket.send(json.dumps(cancel_param))
            self.logger.info(f"Sent cancel request for order {clordid}.")

            # 等待消息并处理
            data = await self.orders[clordid]['message_queue'].get()
            self.logger.info(f"cancel_order, {data}")
            s_msg = data.get('data', [{}])[0].get('sMsg', '')
            if data.get("code") == "0":
                self.logger.info(f"Order {clordid} canceled successfully.")
                self.orders[clordid]['status'] = 'canceled'
            elif 'filled' in s_msg:
                self.logger.info(f"Order {clordid} has been filled.")
                self.orders[clordid]['status'] = 'filled'
            else:
                self.logger.error(f"Failed to cancel order: {s_msg}")
        except Exception as e:
            self.logger.error(f"Error while canceling order: {e}")

    async def consume_messages(self) -> None:
        """消费来自WebSocket的消息"""
        async for message in self.private_websocket:
            try:
                data = json.loads(message)
                self.logger.info(f"Received message: {data}")

                if 'data' in data:
                    for order in data['data']:
                        clordid = order.get('clOrdId')
                        if clordid in self.order_tasks:
                            await self.orders[clordid]['message_queue'].put(data)
            except json.JSONDecodeError as e:
                self.logger.error(f"Error decoding JSON message: {e}")
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")


