import backtrader as bt


class BaseStrategy(bt.Strategy):
    params = dict(printlog=True)

    def log(self, txt, data=None, dt=None):
        if not self.p.printlog:
            return
        data = data or self.datas[0]
        dt = dt or data.datetime.datetime(0)
        dname = getattr(data, "_name", "DATA")
        print(f"[{dt}][{dname}] {txt}")

    def notify_order(self, order):
        data = order.data
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status == order.Completed:
            action = "BUY" if order.isbuy() else "SELL"
            self.log(
                f"ORDER {action} EXECUTED, "
                f"Price={order.executed.price:.2f}, "
                f"Size={order.executed.size}, "
                f"Value={order.executed.value:.2f}, "
                f"Comm={order.executed.comm:.2f}",
                data=data,
            )
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"ORDER FAILED: status={order.getstatusname()}", data=data)

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(
                f"TRADE CLOSED, GROSS={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}",
                data=trade.data,
            )
