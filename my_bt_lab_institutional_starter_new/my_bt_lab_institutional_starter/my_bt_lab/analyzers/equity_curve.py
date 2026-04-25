from __future__ import annotations

from typing import List, Dict, Any
import backtrader as bt


class EquityCurveAnalyzer(bt.Analyzer):
    """Capture (datetime, broker_value, broker_cash, equity) each next().

    计算真正的权益曲线，包含持仓市值。
    - value: 原始broker.getvalue()（可能不准确）
    - cash: 可用资金
    - static_equity: cash + position_value（持仓市值）
    - dynamic_equity: 正确的动态权益
    """

    def start(self):
        self._rows: List[Dict[str, Any]] = []

    def _calc_position_value(self) -> float:
        """计算所有持仓的市值总和 = 持仓数量 × 当前价格"""
        total_value = 0.0
        for data in self.datas:
            try:
                position = self.strategy.broker.getposition(data)
                if position.size != 0 and len(data) > 0:
                    price = data.close[0]
                    total_value += position.size * price
            except Exception:
                # 如果无法获取，跳过
                pass
        return total_value

    def _calc_equity(self, cash: float) -> float:
        """计算正确的权益 = cash + 持仓市值
        
        直接计算，避免 backtrader broker.getvalue() 在 resample 模式下的 bug
        """
        return cash + self._calc_position_value()

    def next(self):
        try:
            dt = self.strategy.datetime.datetime(0)
        except Exception:
            # fallback to first data
            dt = self.datas[0].datetime.datetime(0)

        broker_value = float(self.strategy.broker.getvalue())
        cash = float(self.strategy.broker.getcash())
        # 直接计算正确的权益 = cash + 持仓市值
        equity = self._calc_equity(cash)

        self._rows.append(
            {
                "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "value": broker_value,
                "cash": cash,
                "static_equity": equity,
                "dynamic_equity": equity,
            }
        )

    def get_analysis(self):
        return self._rows
