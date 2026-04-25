from __future__ import annotations

from typing import Any, Dict, List

import backtrader as bt


class SnapshotLedgerAnalyzer(bt.Analyzer):
    params = dict(
        account_mode="cash",
        symbol_specs=None,
        slip_perc=0.0,
    )

    def start(self):
        self._rows: List[Dict[str, Any]] = []
        self._fee_cum: float = 0.0
        self._slip_cum: float = 0.0

    @staticmethod
    def _fmt_dt(dt) -> str:
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _mult(self, data) -> float:
        name = getattr(data, "_name", "")
        specs = self.p.symbol_specs or {}
        spec = specs.get(name, {}) if isinstance(specs, dict) else {}
        mult = spec.get("mult")
        if mult is not None:
            try:
                return float(mult)
            except Exception:
                pass
        try:
            ci = self.strategy.broker.getcommissioninfo(data)
            return float(getattr(ci.p, "mult", 1.0) or 1.0)
        except Exception:
            return 1.0

    def _estimate_slippage_loss(self, order) -> float:
        slip = float(self.p.slip_perc or 0.0)
        if slip <= 0:
            return 0.0
        try:
            qty = abs(float(order.executed.size or 0.0))
            price = float(order.executed.price or 0.0)
            if qty <= 0 or price <= 0:
                return 0.0
            mult = self._mult(order.data)
            if order.isbuy():
                raw_price = price / (1.0 + slip)
            else:
                denom = 1.0 - slip
                raw_price = price / denom if denom > 0 else price
            return abs(price - raw_price) * qty * mult
        except Exception:
            return 0.0

    def notify_order(self, order):
        if order.status != order.Completed:
            return
        try:
            self._fee_cum += abs(float(order.executed.comm or 0.0))
        except Exception:
            pass
        self._slip_cum += self._estimate_slippage_loss(order)

    def _calc_position_value(self) -> float:
        """计算所有持仓的市值总和 = 持仓数量 × 当前价格"""
        total_value = 0.0
        for data in self.strategy.datas:
            try:
                pos = self.strategy.getposition(data)
                size = float(getattr(pos, "size", 0.0) or 0.0)
                if size != 0 and len(data) > 0:
                    price = float(data.close[0])
                    mult = self._mult(data)
                    total_value += size * price * mult
            except Exception:
                pass
        return total_value

    def next(self):
        dt = self.strategy.datetime.datetime(0)
        broker = self.strategy.broker
        account_mode = str(self.p.account_mode or "cash").lower().strip()

        cash = float(broker.getcash())
        # 直接计算正确的权益 = cash + 持仓市值
        # 避免 backtrader broker.getvalue() 在 resample 模式下的 bug
        dynamic_equity = cash + self._calc_position_value()
        static_equity = float(cash)
        floating_pnl = 0.0
        l_margin = 0.0
        s_margin = 0.0
        pos_count = 0

        for data in self.strategy.datas:
            pos = self.strategy.getposition(data)
            size = float(getattr(pos, "size", 0.0) or 0.0)
            if size == 0:
                continue
            pos_count += 1

            cur_price = float(data.close[0])
            avg_price = float(getattr(pos, "price", cur_price) or cur_price)
            mult = self._mult(data)

            # 浮盈亏统一按当前价 - 持仓均价 来估算；size 为负时自然得到空头浮盈亏
            floating_pnl += (cur_price - avg_price) * size * mult

            if account_mode == "futures":
                try:
                    ci = broker.getcommissioninfo(data)
                    margin_per = float(ci.get_margin(cur_price) or 0.0)
                except Exception:
                    margin_per = 0.0
                if size > 0:
                    l_margin += abs(size) * margin_per
                else:
                    s_margin += abs(size) * margin_per
            else:
                # 现金账户下，静态权益 = 现金 + 持仓成本
                static_equity += size * avg_price * mult

        if account_mode == "futures":
            # 期货口径：静态权益 = 动态权益 - 浮动盈亏
            # 这样开仓只会占用保证金，不会让"静态权益"曲线平白暴跌
            static_equity = dynamic_equity - floating_pnl
            # 可用资金 = 动态权益 - 已占用保证金
            available = dynamic_equity - l_margin - s_margin
        else:
            available = cash

        self._rows.append(
            {
                "dt": self._fmt_dt(dt),
                "cash": float(cash),
                "static_equity": float(static_equity),
                "dynamic_equity": float(dynamic_equity),
                "floating_pnl": float(floating_pnl),
                "l_margin": float(l_margin),
                "s_margin": float(s_margin),
                "available": float(available),
                "fee_cum": float(self._fee_cum),
                "slip_cum": float(self._slip_cum),
                "pos_count": int(pos_count),
            }
        )

    def get_analysis(self):
        return self._rows
