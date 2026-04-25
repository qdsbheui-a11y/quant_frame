from __future__ import annotations

from typing import Any, Dict, List, Optional

import backtrader as bt


class OrderFillLedgerAnalyzer(bt.Analyzer):
    params = dict(
        slip_perc=0.0,
        symbol_specs=None,
    )

    def start(self):
        self._orders_by_id: Dict[int, Dict[str, Any]] = {}
        self._fills: List[Dict[str, Any]] = []
        self._next_fill_id: int = 1
        self._positions: Dict[str, Dict[str, float]] = {}
        self._filled_order_ids: set[int] = set()

    @staticmethod
    def _fmt_dt(dt: Any) -> Optional[str]:
        if dt is None:
            return None
        try:
            if isinstance(dt, (int, float)):
                if float(dt) == 0.0:
                    return None
                return bt.num2date(dt).strftime("%Y-%m-%d %H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                return str(dt)
            except Exception:
                return None

    def _symbol(self, order) -> str:
        return getattr(order.data, "_name", "DATA")

    def _side(self, order) -> str:
        try:
            if order.isbuy():
                return "buy"
            if order.issell():
                return "sell"
        except Exception:
            pass
        return "unknown"

    def _order_type(self, order) -> str:
        try:
            return str(order.getordername() or "market").lower()
        except Exception:
            return "market"

    def _submit_dt(self, order) -> Optional[str]:
        created = getattr(order, "created", None)
        created_dt = getattr(created, "dt", None)
        out = self._fmt_dt(created_dt)
        if out:
            return out
        try:
            return self.strategy.datetime.datetime(0).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _exec_dt(self, order) -> Optional[str]:
        executed = getattr(order, "executed", None)
        executed_dt = getattr(executed, "dt", None)
        return self._fmt_dt(executed_dt)

    def _order_qty(self, order) -> float:
        created = getattr(order, "created", None)
        size = getattr(created, "size", None)
        if size is None:
            size = getattr(order, "size", 0.0)
        try:
            return abs(float(size or 0.0))
        except Exception:
            return 0.0

    def _mult(self, order) -> float:
        sym = self._symbol(order)
        specs = self.p.symbol_specs or {}
        spec = specs.get(sym, {}) if isinstance(specs, dict) else {}
        mult = spec.get("mult")
        if mult is not None:
            try:
                return float(mult)
            except Exception:
                pass
        try:
            ci = self.strategy.broker.getcommissioninfo(order.data)
            return float(getattr(ci.p, "mult", 1.0) or 1.0)
        except Exception:
            return 1.0

    def _reason(self, order) -> Optional[str]:
        status = getattr(order, "status", None)
        if status == order.Margin:
            return "margin_rejected"
        if status == order.Rejected:
            return "rejected"
        if status == order.Canceled:
            return "canceled"
        return None

    def _ensure_order_row(self, order) -> Dict[str, Any]:
        oid = int(getattr(order, "ref", 0) or 0)
        row = self._orders_by_id.get(oid)
        if row is None:
            row = {
                "order_id": oid,
                "symbol": self._symbol(order),
                "side": self._side(order),
                "order_type": self._order_type(order),
                "submit_dt": self._submit_dt(order),
                "exec_dt": None,
                "order_qty": self._order_qty(order),
                "status": str(order.getstatusname()),
                "reason": None,
            }
            self._orders_by_id[oid] = row
        return row

    def _estimate_slippage_loss(self, side: str, exec_price: float, qty: float, mult: float) -> float:
        slip = float(self.p.slip_perc or 0.0)
        if slip <= 0 or exec_price <= 0 or qty <= 0 or mult <= 0:
            return 0.0
        try:
            if side == "buy":
                raw_price = exec_price / (1.0 + slip)
            else:
                denom = 1.0 - slip
                raw_price = exec_price / denom if denom > 0 else exec_price
            return abs(exec_price - raw_price) * abs(qty) * mult
        except Exception:
            return 0.0

    def _build_fill(self, order) -> Optional[Dict[str, Any]]:
        oid = int(getattr(order, "ref", 0) or 0)
        if oid in self._filled_order_ids:
            return None

        exec_info = getattr(order, "executed", None)
        qty = abs(float(getattr(exec_info, "size", 0.0) or 0.0))
        price = float(getattr(exec_info, "price", 0.0) or 0.0)
        if qty <= 0 or price <= 0:
            return None

        sym = self._symbol(order)
        side = self._side(order)
        mult = self._mult(order)
        turnover = qty * price * mult
        commission = abs(float(getattr(exec_info, "comm", 0.0) or 0.0))
        slippage_loss = self._estimate_slippage_loss(
            side=side, exec_price=price, qty=qty, mult=mult
        )

        # size: signed position
        #   > 0 : long
        #   < 0 : short
        # avg_price: average entry price of current side
        pos = self._positions.setdefault(sym, {"size": 0.0, "avg_price": 0.0})
        cur_size = float(pos.get("size", 0.0) or 0.0)
        cur_avg = float(pos.get("avg_price", 0.0) or 0.0)

        realized_pnl = 0.0
        trade_type = "UNKNOWN"

        if side == "buy":
            # 1) 当前是空头 -> 买平
            if cur_size < 0:
                short_qty = abs(cur_size)
                close_qty = min(short_qty, qty)
                realized_pnl = (cur_avg - price) * close_qty * mult
                remain_short = short_qty - close_qty

                if remain_short > 0:
                    # 仍保留空头
                    pos["size"] = -remain_short
                    pos["avg_price"] = cur_avg
                else:
                    # 这套策略通常不会一笔反手到多头；先按平空到 0 处理
                    pos["size"] = 0.0
                    pos["avg_price"] = 0.0

                trade_type = "BUY_CLOSE"

            # 2) 当前是空仓/多头 -> 买开
            else:
                new_size = cur_size + qty
                new_avg = (
                    ((cur_size * cur_avg) + (qty * price)) / new_size
                    if new_size > 0 else 0.0
                )
                pos["size"] = new_size
                pos["avg_price"] = new_avg
                trade_type = "BUY_OPEN"

        elif side == "sell":
            # 1) 当前是多头 -> 卖平
            if cur_size > 0:
                close_qty = min(cur_size, qty)
                realized_pnl = (price - cur_avg) * close_qty * mult
                remain_long = cur_size - close_qty

                if remain_long > 0:
                    pos["size"] = remain_long
                    pos["avg_price"] = cur_avg
                else:
                    # 这套策略通常不会一笔反手到空头；先按平多到 0 处理
                    pos["size"] = 0.0
                    pos["avg_price"] = 0.0

                trade_type = "SELL_CLOSE"

            # 2) 当前是空仓/空头 -> 卖开
            else:
                short_qty = abs(cur_size)
                new_short = short_qty + qty
                new_avg = (
                    ((short_qty * cur_avg) + (qty * price)) / new_short
                    if new_short > 0 else 0.0
                )
                pos["size"] = -new_short
                pos["avg_price"] = new_avg
                trade_type = "SELL_OPEN"

        else:
            return None

        row = {
            "fill_id": self._next_fill_id,
            "order_id": oid,
            "dt": self._exec_dt(order),
            "symbol": sym,
            "trade_type": trade_type,
            "order_type": self._order_type(order),
            "fill_qty": float(qty),
            "fill_price": float(price),
            "turnover": float(turnover),
            "order_qty": float(self._order_qty(order)),
            "realized_pnl": float(realized_pnl),
            "commission": float(commission),
            "slippage_loss": float(slippage_loss),
        }
        self._next_fill_id += 1
        self._filled_order_ids.add(oid)
        return row

    def notify_order(self, order):
        row = self._ensure_order_row(order)
        row["status"] = str(order.getstatusname())
        row["reason"] = self._reason(order)

        exec_dt = self._exec_dt(order)
        if exec_dt:
            row["exec_dt"] = exec_dt

        if order.status == order.Completed:
            fill = self._build_fill(order)
            if fill is not None:
                self._fills.append(fill)

    def get_analysis(self):
        orders = [self._orders_by_id[k] for k in sorted(self._orders_by_id)]
        fills = sorted(
            self._fills,
            key=lambda x: (
                str(x.get("dt") or ""),
                int(x.get("order_id") or 0),
                int(x.get("fill_id") or 0),
            ),
        )
        return {"orders": orders, "fills": fills}