from collections import defaultdict
import backtrader as bt


class TradeStatsAnalyzer(bt.Analyzer):
    def start(self):
        self.total_closed = 0
        self.total_net_pnl = 0.0
        self.by_symbol = defaultdict(lambda: {"closed_trades": 0, "net_pnl": 0.0})

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        dname = getattr(trade.data, "_name", "DATA")
        pnl = float(trade.pnlcomm)
        self.total_closed += 1
        self.total_net_pnl += pnl
        self.by_symbol[dname]["closed_trades"] += 1
        self.by_symbol[dname]["net_pnl"] += pnl

    def get_analysis(self):
        return {
            "closed_trades": self.total_closed,
            "net_pnl": round(self.total_net_pnl, 2),
            "by_symbol": {k: {"closed_trades": v["closed_trades"], "net_pnl": round(v["net_pnl"], 2)} for k, v in self.by_symbol.items()},
        }
