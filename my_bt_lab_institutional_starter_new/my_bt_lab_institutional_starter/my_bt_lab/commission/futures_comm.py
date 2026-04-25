import backtrader as bt


class SimpleFuturesComm(bt.CommInfoBase):
    params = dict(
        commission=0.0005,
        mult=10.0,
        margin=10000.0,
        margin_rate=None,
        commtype=bt.CommInfoBase.COMM_PERC,
        stocklike=False,
    )

    def get_margin(self, price):
        if self.p.margin_rate is not None:
            return float(price) * float(self.p.mult) * float(self.p.margin_rate)
        return float(self.p.margin)

    def _getcommission(self, size, price, pseudoexec):
        qty = abs(float(size or 0.0))
        px = abs(float(price or 0.0))
        if qty <= 0 or px <= 0:
            return 0.0

        # fixed: 按“每手固定费用”收取
        if self.p.commtype == bt.CommInfoBase.COMM_FIXED:
            return qty * float(self.p.commission)

        # perc: 按“成交额比例”收取，成交额 = 手数 * 价格 * 合约乘数
        return qty * px * float(self.p.mult) * float(self.p.commission)
