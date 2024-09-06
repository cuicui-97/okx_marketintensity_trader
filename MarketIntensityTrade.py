import asyncio
import MarketDataFetcher as fetcher
import OkxSimulatedTrade as trader
import CalculateMarkerIntensity as cmi


class MarketIntensityTradingManager:
    def __init__(self, api_key, secret_key, passphrase, symbol,
                 max_messages=1800, window_size=30, threshold=0.05):
        """
        初始化 MarketIntensityTradingManager 类

        :param api_key: OKX API 密钥
        :param secret_key: OKX Secret 密钥
        :param passphrase: OKX Passphrase
        :param symbol: 交易对符号，如 'BTC-USDT'
        :param max_messages: 最大处理的消息数
        :param window_size: 每 window_size条交易数据进行求和
        :param threshold: 判断flag的阈值

        """

        self.fetcher = fetcher.MarketDataFetch(symbol, max_messages=max_messages)
        self.trader = trader.OkxSimulatedTrader(api_key, secret_key, passphrase)
        self.market_intensity_calculator = cmi.MarketIntensityCalculator(threshold=threshold, window_size=window_size)

    async def run(self):
        # 连接并登录到交易平台
        await self.trader.connect_and_login()

        # 启动 fetcher.run() 的异步任务
        fetcher_task = asyncio.create_task(self.fetcher.run())

        try:
            while not self.fetcher.stop_flag.is_set():
                if self.fetcher.data_deque:
                    current_trade = self.fetcher.data_deque.popleft()
                    print(current_trade)
                    # 将当前交易数据传递给 TradeSizeCalculator 进行计算
                    self.market_intensity_calculator.add_trade(current_trade)

                    if self.market_intensity_calculator.get_flag():
                        # 获取最新的 bid0 价格
                        order_price = self.fetcher.get_latest_bid0_price()

                        if order_price is not None:
                            order_size = 0.001  # 示例下单数量

                            # 提交订单
                            asyncio.create_task(
                                self.trader.place_limit_order(
                                    symbol=self.fetcher.instId,
                                    side="buy",
                                    price=order_price,
                                    size=order_size
                                )
                            )
                            print(
                                f"订单已提交: symbol={self.fetcher.instId}, side=buy, price={order_price}, size={order_size}")
                            # 重置 flag
                            self.market_intensity_calculator.flag = False
                        else:
                            print("无法获取最新的价格")
                            self.market_intensity_calculator.flag = False
                    else:
                        print('未出现交易信号！')

                await asyncio.sleep(0)

        except Exception as e:
            print(f"发生错误: {e}")

        finally:

            await fetcher_task
            await asyncio.sleep(15)







