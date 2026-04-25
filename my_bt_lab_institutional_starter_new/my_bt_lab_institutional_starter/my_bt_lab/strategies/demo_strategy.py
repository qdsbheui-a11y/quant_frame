"""
演示策略 - 展示平滑收益率曲线

这个策略展示了正确的权益曲线形态：
- 空仓时收益率为 0%
- 持仓期间收益率随价格连续变化
- 平仓后收益率锁定，直到下一次开仓
"""

import backtrader as bt
import numpy as np


class DemoStrategy(bt.Strategy):
    """简单移动平均线交叉策略"""
    
    params = dict(
        fast_period=10,
        slow_period=30,
        printlog=False,
        size=10,  # 每次开10手，放大收益波动
    )
    
    def __init__(self):
        # 均线
        self.fast_ma = bt.indicators.SMA(self.data.close, period=self.p.fast_period)
        self.slow_ma = bt.indicators.SMA(self.data.close, period=self.p.slow_period)
        
        # 交叉信号
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)
        
        # 初始权益
        self.initial_cash = None
        
        # 订单状态
        self.order = None
        
    def log(self, txt, dt=None):
        if self.p.printlog:
            dt = dt or self.data.datetime.datetime(0)
            print(f"[{dt}] {txt}")
    
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"BUY EXECUTED, Price: {order.executed.price:.2f}")
            else:
                self.log(f"SELL EXECUTED, Price: {order.executed.price:.2f}")
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"ORDER FAILED: {order.getstatusname()}")
        
        self.order = None
    
    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f"TRADE PNL: {trade.pnl:.2f}")
    
    def next(self):
        # 检查是否有待处理订单
        if self.order:
            return
        
        # 获取当前持仓
        pos = self.getposition()
        
        # 使用更灵敏的规则：短期均线从下方穿越或价格站上均线
        ma5 = self.fast_ma[0]
        ma30 = self.slow_ma[0]
        price = self.data.close[0]
        
        # 无持仓，满足条件买入
        if pos.size == 0:
            # 金叉 或者 价格站上均线
            if self.crossover > 0 or (price > ma5 and price > ma30):
                self.log(f"买入信号 -> 买入 {self.p.size}手 @ {price:.2f}")
                self.order = self.buy(size=self.p.size)
        
        # 有持仓
        else:
            # 死叉 或者 价格跌破均线
            if self.crossover < 0 or (price < ma5 and price < ma30):
                self.log(f"卖出信号 -> 卖出 {self.p.size}手 @ {price:.2f}")
                self.order = self.sell(size=self.p.size)


class SmoothEquityStrategy(bt.Strategy):
    """
    平滑收益率策略 - 展示连续变化的权益曲线
    
    特点：
    1. 每次开仓后计算持仓收益率
    2. 平仓时将收益率锁定
    3. 空仓时收益率为0
    """
    
    params = dict(
        printlog=False,
    )
    
    def __init__(self):
        self.order = None
        # 记录开仓价格
        self.entry_price = None
        self.entry_size = None
        # 开仓时的权益（用于计算收益率）
        self.entry_equity = None
        
    def log(self, txt, dt=None):
        if self.p.printlog:
            dt = dt or self.data.datetime.datetime(0)
            print(f"[{dt}] {txt}")
    
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        
        if order.status == order.Completed:
            if order.isbuy():
                self.entry_price = order.executed.price
                self.entry_size = order.executed.size
                self.entry_equity = self.broker.getvalue()
                self.log(f"BUY @ {self.entry_price:.2f}, 持仓: {self.entry_size}")
            else:
                pnl = order.executed.price - self.entry_price
                self.log(f"SELL @ {order.executed.price:.2f}, PNL: {pnl:.2f}")
                self.entry_price = None
                self.entry_size = None
                self.entry_equity = None
        
        self.order = None
    
    def next(self):
        if self.order:
            return
        
        pos = self.getposition()
        
        # 无持仓，按固定规则开仓
        if pos.size == 0:
            # 简单规则：收盘价 > 20日均线买入
            ma20 = self.data.close[-19] if len(self.data) >= 20 else self.data.close[0]
            if self.data.close[0] > ma20:
                self.order = self.buy()
        # 有持仓，跌破均线平仓
        else:
            ma20 = self.data.close[-19] if len(self.data) >= 20 else self.data.close[0]
            if self.data.close[0] < ma20:
                self.order = self.sell()
