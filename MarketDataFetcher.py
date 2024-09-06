import asyncio
import websockets
import json
import logging
from collections import deque


class MarketDataFetch:
    def __init__(self, symbol, log_file="trade_data.log", max_messages=50):
        self.instId = symbol
        self.base_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.data_deque = deque()  # 初始化一个有最大长度的 deque
        self.order_book_data = {}  # 存储订单簿数据
        self.max_messages = max_messages  # 最大消息数
        self.message_count = 0  # 已接收到的消息数
        self.stop_flag = asyncio.Event()  # 用于控制停止运行的标志
        self.public_websocket = None  # WebSocket 连接的引用
        self.trade_queue = asyncio.Queue()
        self.order_book_deque = deque()

        # 初始化日志记录
        self._initialize_logging(log_file)

    def _initialize_logging(self, log_file):
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logging initialized")

    async def subscribe_to_trades(self):
        try:
            # 发送交易频道订阅消息
            subscribe_message = {
                "op": "subscribe",
                "args": [{"channel": 'trades', "instId": self.instId}]
            }
            await self.public_websocket.send(json.dumps(subscribe_message))
            self.logger.info(f"Subscribed to trades channel for {self.instId}")
        except Exception as e:
            self.logger.error(f"Failed to subscribe: {e}")

    async def subscribe_to_orderbook(self):
        try:
            # 发送订单簿频道订阅消息
            subscribe_message = {
                "op": "subscribe",
                "args": [{"channel": "books", "instId": self.instId}]
            }
            await self.public_websocket.send(json.dumps(subscribe_message))
            self.logger.info(f"Subscribed to order book channel for {self.instId}")
        except Exception as e:
            self.logger.error(f"Failed to subscribe to order book: {e}")

    async def connect(self):
        while not self.stop_flag.is_set():
            try:
                self.public_websocket = await websockets.connect(self.base_url)
                self.logger.info("Public WebSocket connection established")

                # 订阅交易数据和订单簿数据
                await self.subscribe_to_trades()
                await self.subscribe_to_orderbook()

                while not self.stop_flag.is_set():
                    response = await self.public_websocket.recv()
                    await self.process_message(response)

                    # 如果达到最大消息数，则停止
                    if self.max_messages is not None and self.message_count >= self.max_messages:
                        self.logger.info("Reached maximum number of messages. Stopping...")
                        self.stop_flag.set()

            except websockets.exceptions.ConnectionClosed as e:
                self.logger.error(f"WebSocket connection closed with error: {e}")
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"Error in WebSocket connection: {e}")
                await asyncio.sleep(2)

    async def process_message(self, message):
        try:
            # 解析消息
            data = json.loads(message)
            if 'data' in data:
                if data['arg']['channel'] == 'trades':
                    await self.handle_trade_message(data['data'])
                elif data['arg']['channel'] == 'books':
                    self.handle_order_book_message(data['data'][0])  # 处理第一个返回值
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    async def handle_trade_message(self, trade_data):
        for trade in trade_data:
            trade_info = {
                'instId': trade.get('instId'),
                'tradeId': trade.get('tradeId'),
                'px': trade.get('px'),
                'sz': trade.get('sz'),
                'side': trade.get('side'),
                'ts': trade.get('ts'),
                'count': trade.get('count')
            }
            # 将交易信息添加到 deque 中
            self.data_deque.append(trade_info)
            self.message_count += 1
            self.logger.info(f"Added trade info: {trade_info}")

            # 将交易信息放入队列，作为广播
            await self.trade_queue.put(trade_info)

    def handle_order_book_message(self, order_book_data):
        # 更新订单簿数据
        self.order_book_data = order_book_data
        self.logger.info(f"Received order book data: {order_book_data}")

        self.order_book_deque.append(order_book_data)

    def get_latest_bid4_price(self):
        if self.order_book_deque:
            latest_order_book_data = self.order_book_deque[-1]
            if "bids" in latest_order_book_data and len(latest_order_book_data["bids"]) >= 5:
                return latest_order_book_data["bids"][5][0]
            return None

    def get_latest_bid2_price(self):
        if self.order_book_deque:
            latest_order_book_data = self.order_book_deque[-1]  # 获取 deque 中最新的订单簿数据
            if "bids" in latest_order_book_data and len(latest_order_book_data["bids"]) >= 3:
                return latest_order_book_data["bids"][2][0]
            return None

    def get_latest_bid0_price(self):
        if self.order_book_deque:
            latest_order_book_data = self.order_book_deque[-1]  # 获取 deque 中最新的订单簿数据
            if "bids" in latest_order_book_data and len(latest_order_book_data["bids"]) >= 1:
                return latest_order_book_data["bids"][0][0]  # 获取最新的 bid0 价格
            return None

    async def run(self):
        try:
            while not self.stop_flag.is_set():
                await self.connect()
        except KeyboardInterrupt:
            self.logger.info("Program terminated by user.")
            await self.on_stop()

    async def on_stop(self):
        self.stop_flag.set()  # 设置停止标志
        if self.public_websocket and not self.public_websocket.closed:
            try:
                # 发送关闭请求，确保优雅关闭
                await self.public_websocket.close()
                self.logger.info("WebSocket connection closed")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket connection: {e}")






