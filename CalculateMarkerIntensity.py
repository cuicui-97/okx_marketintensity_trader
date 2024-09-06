class MarketIntensityCalculator:
    def __init__(self, threshold, window_size=10):
        """
        初始化TradeSizeCalculator类

        :param threshold: 判断flag的阈值
        :param window_size: 每window_size条交易数据进行求和
        """
        self.threshold = threshold
        self.window_size = window_size
        self.size_list = []
        self.flag = False

    def add_trade(self, trade):
        """
        添加一笔交易数据，并更新size_list

        :param trade: 单条交易数据，字典格式，包含'side'（买卖方向）和'sz'（交易size）
        """
        size = float(trade['sz'])
        if trade['side'] == 'buy':
            self.size_list.append(size)
        elif trade['side'] == 'sell':
            self.size_list.append(-size)

        if len(self.size_list) >= self.window_size:
            self. calculate_market_intensity()

    def calculate_market_intensity(self):
        """
        计算size_list的和，并根据阈值更新flag
        """
        market_intensity = sum(self.size_list)/len(self.size_list)
        if market_intensity > self.threshold:
            self.flag = True
        else:
            self.flag = False
        self.size_list.clear()

    def get_flag(self):
        """
        返回当前flag的值
        """
        return self.flag


