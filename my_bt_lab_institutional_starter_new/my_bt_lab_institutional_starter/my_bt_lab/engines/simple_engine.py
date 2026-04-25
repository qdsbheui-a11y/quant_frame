from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import logging
import math

from my_bt_lab.data.loaders_df import load_data_item_to_df
from my_bt_lab.core.ledger import Ledger
from my_bt_lab.core.types import LedgerFill, LedgerOrder, LedgerSnapshot
from my_bt_lab.brokers.safe_backbroker import SafeBackBroker

logger = logging.getLogger(__name__)


@dataclass
class SimpleBacktestResult:
    start_value: float
    end_value: float
    trade_stats: Dict[str, Any]
    drawdown: Dict[str, Any]
    trades: List[Dict[str, Any]]
    equity_curve: List[Dict[str, Any]]
    time_return: Dict[str, Any]

    # M1 ledger outputs (single source of truth)
    orders: List[Dict[str, Any]]
    fills: List[Dict[str, Any]]
    snapshots: List[Dict[str, Any]]


@dataclass
class Order:
    """Strategy intent order (not a broker/orderbook object yet)."""

    symbol: str
    side: str          # 'buy' or 'sell'
    size: float        # positive
    submit_dt: pd.Timestamp
    order_type: str = "market"  # M1: market
    order_id: Optional[int] = None


class SimpleBroker:
    """A self-research broker/account model (M1).

    This is still a *starter* implementation, but it already produces a proper
    order/fill/snapshot ledger so reports/metrics no longer need to reverse
    engineer from 'trades'.

    Execution model (M1):
    - Market orders executed at *next bar open* (symbol-local next bar)
    - Long-only

    Account model:
    - account_mode='cash'    : stock-like, pay notional when buy, receive when sell
    - account_mode='futures' : futures-like margin, pay commission, PnL realized on close,
                               floating PnL shown in dynamic equity

    Notes:
    - No partial fill (M1)
    - Slippage is percent-of-price (M1)
    - Margin is computed for snapshots; strict margin rejection is best-effort (M1)
    """

    def __init__(
        self,
        starting_cash: float,
        sym_specs: Dict[str, Dict[str, Any]],
        default_comm: Dict[str, Any],
        slip_perc: float = 0.0,
        account_mode: str = "cash",
    ):
        self.starting_cash = float(starting_cash)
        self.cash = float(starting_cash)

        self.sym_specs = sym_specs or {}
        self.default_comm = default_comm or {}
        self.slip_perc = float(slip_perc or 0.0)
        self.account_mode = str(account_mode or "cash").lower().strip()
        if self.account_mode not in {"cash", "futures"}:
            self.account_mode = "cash"

        self.pos_size: Dict[str, float] = {}
        self.pos_avg: Dict[str, float] = {}
        self._open_trade: Dict[str, Dict[str, Any]] = {}

        # "closed trades" view (kept for backward compat)
        self.trades: List[Dict[str, Any]] = []

        # cumulative costs (for snapshots)
        self.fee_cum: float = 0.0
        self.slip_cum: float = 0.0

    def _spec(self, sym: str) -> Dict[str, Any]:
        return dict(self.default_comm, **(self.sym_specs.get(sym, {}) or {}))

    def _commission_rate(self, sym: str) -> float:
        s = self._spec(sym)
        return float(s.get("commission", 0.0) or 0.0)

    def _commission_type(self, sym: str) -> str:
        s = self._spec(sym)
        return str(s.get("commtype", "perc") or "perc").lower().strip()

    def _mult(self, sym: str) -> float:
        s = self._spec(sym)
        return float(s.get("mult", 1.0) or 1.0)

    def _margin_fixed(self, sym: str) -> float:
        s = self._spec(sym)
        return float(s.get("margin", 0.0) or 0.0)

    def _margin_rate(self, sym: str) -> Optional[float]:
        s = self._spec(sym)
        mr = s.get("margin_rate", None)
        if mr is None:
            return None
        try:
            mr = float(mr)
        except Exception:
            return None
        if not math.isfinite(mr) or mr <= 0:
            return None
        return mr

    def _margin_per_contract(self, sym: str, price: float) -> float:
        """Per-contract margin computed from spec.

        - If spec.margin > 0: fixed per contract
        - Else if margin_rate set: price * mult * margin_rate
        - Else: 0
        """
        fixed = self._margin_fixed(sym)
        if fixed > 0:
            return fixed
        mr = self._margin_rate(sym)
        if mr is not None:
            return abs(float(price)) * self._mult(sym) * mr
        return 0.0

    def _charge_comm(self, sym: str, turnover: float) -> float:
        ct = self._commission_type(sym)
        c = self._commission_rate(sym)
        if ct == "fixed":
            return abs(float(c))
        return abs(float(turnover)) * abs(float(c))

    def _round_size(self, sym: str, raw_size: float, fallback_min_size: float = 1.0) -> float:
        """Round size according to symbol spec (min_size / size_step)."""
        s = self._spec(sym)
        step = float(s.get("size_step", 1.0) or 1.0)
        if step <= 0:
            step = 1.0
        min_size = float(s.get("min_size", fallback_min_size) or fallback_min_size)
        if min_size <= 0:
            min_size = fallback_min_size

        sz = max(0.0, float(raw_size))
        sz = math.floor(sz / step) * step
        if sz > 0:
            sz = max(sz, min_size)
        return float(sz)

    def _margin_total(self, last_prices: Dict[str, float]) -> Tuple[float, float]:
        """(l_margin, s_margin) at current positions. M1: long-only."""
        if self.account_mode != "futures":
            return 0.0, 0.0

        l = 0.0
        for sym, sz in self.pos_size.items():
            if sz <= 0:
                continue
            px = float(last_prices.get(sym, self.pos_avg.get(sym, 0.0)))
            l += abs(sz) * self._margin_per_contract(sym, px)
        return float(l), 0.0

    def available_cash(self, last_prices: Dict[str, float]) -> float:
        l, s = self._margin_total(last_prices)
        return float(self.cash - l - s)

    def dynamic_equity(self, last_prices: Dict[str, float]) -> float:
        """Dynamic equity (动态权益)."""
        if self.account_mode == "futures":
            # cash (static) + floating pnl
            floating = 0.0
            for sym, sz in self.pos_size.items():
                if sz == 0:
                    continue
                last_px = float(last_prices.get(sym, self.pos_avg.get(sym, 0.0)))
                avg_px = float(self.pos_avg.get(sym, last_px))
                floating += sz * (last_px - avg_px) * self._mult(sym)
            return float(self.cash + floating)

        # cash-style: cash + market value
        v = self.cash
        for sym, sz in self.pos_size.items():
            if sz == 0:
                continue
            px = float(last_prices.get(sym, self.pos_avg.get(sym, 0.0)))
            v += sz * px * self._mult(sym)
        return float(v)

    def static_equity(self, last_prices: Dict[str, float]) -> float:
        """Static equity (静态权益).

        - futures: static equity is cash/balance (exclude floating pnl)
        - cash: cost-basis equity = cash + position valued at avg price
        """
        if self.account_mode == "futures":
            return float(self.cash)

        v = self.cash
        for sym, sz in self.pos_size.items():
            if sz == 0:
                continue
            avg_px = float(self.pos_avg.get(sym, 0.0))
            v += sz * avg_px * self._mult(sym)
        return float(v)

    # Backward-compat alias
    def value(self, last_prices: Dict[str, float]) -> float:
        return self.dynamic_equity(last_prices)

    def _max_affordable_size(self, sym: str, price: float, fallback_min_size: float = 1.0) -> float:
        """Max size that current account can take.

        - cash mode: uses cash/notional
        - futures mode: uses available cash / (margin per contract + est commission)
        """
        price = float(price)
        mult = self._mult(sym)
        ct = self._commission_type(sym)
        c = self._commission_rate(sym)

        if self.account_mode == "futures":
            # use margin requirement if available
            last_prices = {sym: price}
            avail = self.available_cash(last_prices)
            mpc = self._margin_per_contract(sym, price)
            if mpc <= 0:
                # fallback: behave like cash mode if margin not configured
                pass
            else:
                if ct == "fixed":
                    per_unit_cost = mpc + abs(float(c))
                else:
                    per_unit_cost = mpc + (price * mult * abs(float(c)))
                if per_unit_cost <= 0:
                    return 0.0
                raw = max(0.0, avail) / per_unit_cost
                return self._round_size(sym, raw, fallback_min_size=fallback_min_size)

        # cash mode
        if ct == "fixed":
            cash_for_notional = max(0.0, self.cash - abs(float(c)))
            denom = price * mult
        else:
            cash_for_notional = self.cash
            denom = price * mult * (1.0 + c)

        if denom <= 0:
            return 0.0
        raw = cash_for_notional / denom
        return self._round_size(sym, raw, fallback_min_size=fallback_min_size)

    def check_order(self, sym: str, side: str, size: float, est_price: float, last_prices: Dict[str, float]) -> Tuple[bool, Optional[str]]:
        """Best-effort pre-check at submit time (M1)."""
        size = float(size)
        if size <= 0:
            return False, "size<=0"

        pos = float(self.pos_size.get(sym, 0.0))
        if side == "sell" and pos <= 0:
            return False, "no_position"

        # buy constraints
        if side != "buy":
            return True, None

        est_price = float(est_price)
        mult = self._mult(sym)
        est_turnover = size * est_price * mult
        est_comm = self._charge_comm(sym, est_turnover)

        if self.account_mode == "futures":
            # additional margin check (best-effort)
            cur_lm, cur_sm = self._margin_total(last_prices)
            cur_margin = cur_lm + cur_sm
            mpc = self._margin_per_contract(sym, est_price)
            add_margin = 0.0
            if mpc > 0:
                add_margin = size * mpc
            avail = self.cash - cur_margin
            if avail < add_margin + est_comm:
                return False, "insufficient_available_cash_for_margin"
            return True, None

        # cash mode
        need = est_turnover + est_comm
        if self.cash < need:
            return False, "insufficient_cash"
        return True, None

    def execute(
        self,
        sym: str,
        side: str,
        size: float,
        raw_price: float,
        dt: pd.Timestamp,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Execute at given dt/price.

        Returns:
            (fill_dict, trade_dict_or_none)
        """
        size = float(size)
        raw_price = float(raw_price)

        # apply slippage
        exec_price = float(raw_price)
        slip = float(self.slip_perc or 0.0)
        if slip != 0.0:
            if side == "buy":
                exec_price *= (1.0 + slip)
            else:
                exec_price *= (1.0 - slip)

        mult = self._mult(sym)

        # M1 slippage loss definition: (exec - raw) * qty * mult
        slippage_loss = abs(exec_price - raw_price) * abs(size) * mult

        if side == "buy":
            turnover = size * exec_price * mult
            comm = self._charge_comm(sym, turnover)

            # account cash movement
            if self.account_mode == "cash":
                self.cash -= turnover + comm
            else:
                self.cash -= comm

            # update position avg
            prev_sz = float(self.pos_size.get(sym, 0.0))
            prev_avg = float(self.pos_avg.get(sym, 0.0))
            new_sz = prev_sz + size
            new_avg = (prev_sz * prev_avg + size * exec_price) / new_sz if prev_sz > 0 else exec_price
            self.pos_size[sym] = new_sz
            self.pos_avg[sym] = new_avg

            # track "open trade" for closed-trade view
            ot = self._open_trade.get(sym)
            if ot is None:
                self._open_trade[sym] = {"entry_dt": dt, "entry_price": exec_price, "size": new_sz, "comm": comm}
            else:
                ot["size"] = new_sz
                ot["comm"] = float(ot.get("comm", 0.0)) + comm
                self._open_trade[sym] = ot

            self.fee_cum += comm
            self.slip_cum += slippage_loss

            fill = {
                "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": sym,
                "trade_type": "BUY_OPEN",
                "order_type": "market",
                "fill_qty": float(size),
                "fill_price": float(exec_price),
                "turnover": float(turnover),
                "order_qty": float(size),
                "realized_pnl": 0.0,
                "commission": float(comm),
                "slippage_loss": float(slippage_loss),
            }
            return fill, None

        # sell
        prev_sz = float(self.pos_size.get(sym, 0.0))
        if prev_sz <= 0:
            return None, None

        sell_sz = min(prev_sz, size)
        turnover = sell_sz * exec_price * mult
        comm = self._charge_comm(sym, turnover)

        entry_price = float(self.pos_avg.get(sym, exec_price))
        pnl = (exec_price - entry_price) * sell_sz * mult

        # account cash movement
        if self.account_mode == "cash":
            self.cash += turnover - comm
        else:
            self.cash += pnl - comm

        ot = self._open_trade.get(sym, {})
        entry_dt = ot.get("entry_dt", dt)
        entry_comm = float(ot.get("comm", 0.0))

        remaining = prev_sz - sell_sz
        self.pos_size[sym] = remaining
        if remaining <= 0:
            self.pos_avg[sym] = 0.0
            self._open_trade.pop(sym, None)

        self.fee_cum += comm
        self.slip_cum += slippage_loss

        # keep old closed-trade view (one row per close)
        trade = {
            "symbol": sym,
            "direction": "long",
            "size": sell_sz,
            "entry_dt": pd.Timestamp(entry_dt).strftime("%Y-%m-%d %H:%M:%S"),
            "entry_price": entry_price,
            "exit_dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_price": exec_price,
            "pnl": float(pnl),
            "pnlcomm": float(pnl - entry_comm - comm),
        }
        self.trades.append(trade)

        fill = {
            "dt": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": sym,
            "trade_type": "SELL_CLOSE",
            "order_type": "market",
            "fill_qty": float(sell_sz),
            "fill_price": float(exec_price),
            "turnover": float(turnover),
            "order_qty": float(size),
            "realized_pnl": float(pnl),
            "commission": float(comm),
            "slippage_loss": float(slippage_loss),
        }
        return fill, trade

    def open_positions_count(self) -> int:
        return sum(1 for v in self.pos_size.values() if v and v > 0)


class SimpleStrategy:
    def __init__(self, params: Dict[str, Any] | None = None):
        self.p = params or {}

    def on_bar(self, dt: pd.Timestamp, bars: Dict[str, Dict[str, float]], broker: SimpleBroker) -> List[Order]:
        return []


class SimpleMaAtrTrend(SimpleStrategy):
    """MA cross + ATR stop, long-only. Coarse sizing."""

    def __init__(self, params: Dict[str, Any] | None = None):
        super().__init__(params)
        self.fast = int(self.p.get("fast", 10))
        self.slow = int(self.p.get("slow", 30))
        self.atr_period = int(self.p.get("atr_period", 14))
        self.atr_stop_mult = float(self.p.get("atr_stop_mult", 2.0))
        self.risk_per_trade = float(self.p.get("risk_per_trade", 0.01) or 0.01)
        self.max_positions = int(self.p.get("max_positions", 2))
        self.min_size = float(self.p.get("min_size", 1))

        self._hist: Dict[str, List[Dict[str, float]]] = {}
        self._stop: Dict[str, float] = {}

    def _atr(self, sym: str) -> Optional[float]:
        h = self._hist.get(sym, [])
        if len(h) < self.atr_period + 1:
            return None
        trs = []
        for i in range(1, self.atr_period + 1):
            cur = h[-i]
            prev = h[-i - 1]
            tr = max(
                cur["high"] - cur["low"],
                abs(cur["high"] - prev["close"]),
                abs(cur["low"] - prev["close"]),
            )
            trs.append(tr)
        return float(sum(trs) / len(trs)) if trs else None

    def _sma(self, sym: str, n: int) -> Optional[float]:
        h = self._hist.get(sym, [])
        if len(h) < n:
            return None
        return float(sum(x["close"] for x in h[-n:]) / n)

    def on_bar(self, dt: pd.Timestamp, bars: Dict[str, Dict[str, float]], broker: SimpleBroker) -> List[Order]:
        orders: List[Order] = []

        # update history
        for sym, bar in bars.items():
            self._hist.setdefault(sym, []).append(bar)

        open_positions = broker.open_positions_count()
        # current equity using latest close (coarse)
        last_prices = {s: float(b["close"]) for s, b in bars.items()}
        equity = broker.dynamic_equity(last_prices)

        for sym, bar in bars.items():
            close = float(bar["close"])
            pos = float(broker.pos_size.get(sym, 0.0))

            fast = self._sma(sym, self.fast)
            slow = self._sma(sym, self.slow)
            atr = self._atr(sym)

            if fast is None or slow is None or atr is None or atr <= 0:
                continue

            # manage stop
            if pos > 0:
                prev_stop = float(self._stop.get(sym, -1e18))
                stop = max(prev_stop, close - self.atr_stop_mult * atr)
                self._stop[sym] = stop
                if close < stop:
                    orders.append(Order(symbol=sym, side="sell", size=pos, submit_dt=dt))
                    continue

            # cross signals
            if pos == 0:
                if open_positions >= self.max_positions:
                    continue
                if fast > slow:
                    # Risk-based sizing:
                    # size ~= (equity * risk_per_trade) / (stop_dist * mult)
                    stop_dist = self.atr_stop_mult * atr
                    per_unit_risk = stop_dist * broker._mult(sym)
                    raw_size = (equity * self.risk_per_trade) / per_unit_risk if per_unit_risk > 0 else 0.0

                    size = broker._round_size(sym, raw_size, fallback_min_size=self.min_size)
                    affordable = broker._max_affordable_size(sym, price=close, fallback_min_size=self.min_size)
                    size = min(size, affordable)
                    if size <= 0:
                        continue
                    orders.append(Order(symbol=sym, side="buy", size=size, submit_dt=dt))
                    open_positions += 1
                    self._stop[sym] = close - self.atr_stop_mult * atr
            else:
                if fast < slow:
                    orders.append(Order(symbol=sym, side="sell", size=pos, submit_dt=dt))

        return orders


def _project_root_from_cfg_path(cfg_path: Path) -> Path:
    if cfg_path.parent.name == "configs":
        return cfg_path.parents[3]
    return cfg_path.parent


def _build_symbol_dfs(cfg: Dict[str, Any], project_root: Path) -> Dict[str, pd.DataFrame]:
    dfs: Dict[str, pd.DataFrame] = {}
    for item in cfg.get("data", []) or []:
        name = item["name"]
        df = load_data_item_to_df(item=item, project_root=project_root, cfg=cfg)
        logger.info("数据加载成功: name=%s rows=%d", name, len(df))
        dfs[name] = df
    return dfs


def run_simple(cfg: Dict[str, Any], cfg_path: Path) -> SimpleBacktestResult:
    project_root = _project_root_from_cfg_path(cfg_path)
    dfs = _build_symbol_dfs(cfg, project_root)

    broker_cfg = cfg.get("broker", {}) or {}
    starting_cash = float(broker_cfg.get("starting_cash", 100000.0))
    default_comm = cfg.get("commission_default", cfg.get("commission", {})) or {}
    sym_specs = cfg.get("symbols", {}) or {}

    slip_perc = float(broker_cfg.get("slip_perc", 0.0) or 0.0)
    account_mode = str(broker_cfg.get("account_mode", "cash") or "cash")

    broker = SimpleBroker(
        starting_cash=starting_cash,
        sym_specs=sym_specs,
        default_comm=default_comm,
        slip_perc=slip_perc,
        account_mode=account_mode,
    )

    s_cfg = cfg.get("strategy", {}) or {}
    params = dict(s_cfg.get("params") or {})
    if not s_cfg.get("name"):
        # backward compat
        for k, v in s_cfg.items():
            if k not in {"name", "params"}:
                params.setdefault(k, v)

    strat = SimpleMaAtrTrend(params=params)

    # Precompute per-symbol datetime index to find next bar open
    sym_times = {sym: pd.Index(pd.to_datetime(df["datetime"])) for sym, df in dfs.items()}

    # Build global timeline: union of all datetimes
    all_times = pd.Index([])
    for df in dfs.values():
        all_times = all_times.union(pd.to_datetime(df["datetime"]))
    all_times = all_times.sort_values()

    # Orders scheduled for execution at a specific datetime (symbol-local next open)
    scheduled: Dict[pd.Timestamp, List[Order]] = {}

    last_close: Dict[str, float] = {}

    ledger = Ledger()
    order_id_seq = 1
    fill_id_seq = 1
    order_idx: Dict[int, int] = {}

    start_value = broker.dynamic_equity(last_close)

    peak = start_value
    max_dd = 0.0
    max_moneydown = 0.0

    # Build lookup df row by datetime for each symbol
    sym_row_map = {}
    for sym, df in dfs.items():
        df2 = df.copy()
        df2["datetime"] = pd.to_datetime(df2["datetime"])
        df2 = df2.set_index("datetime")
        sym_row_map[sym] = df2

    for dt in all_times:
        dt = pd.Timestamp(dt)

        # 1) execute scheduled orders at this dt using open price
        if dt in scheduled:
            for od in scheduled.pop(dt):
                row = sym_row_map[od.symbol].loc[dt]
                raw_open = float(row["open"])

                fill_dict, _ = broker.execute(od.symbol, od.side, od.size, raw_open, dt)
                if fill_dict is None:
                    # should not happen often; still mark rejected
                    if od.order_id is not None and od.order_id in order_idx:
                        i = order_idx[od.order_id]
                        old = ledger.orders[i]
                        ledger.orders[i] = LedgerOrder(
                            order_id=old.order_id,
                            symbol=old.symbol,
                            side=old.side,
                            order_type=old.order_type,
                            submit_dt=old.submit_dt,
                            exec_dt=old.exec_dt,
                            order_qty=old.order_qty,
                            status="Rejected",
                            reason="execute_failed_or_no_position",
                        )
                    continue

                oid = int(od.order_id or -1)
                ledger.add_fill(
                    LedgerFill(
                        fill_id=fill_id_seq,
                        order_id=oid,
                        dt=fill_dict["dt"],
                        symbol=fill_dict["symbol"],
                        trade_type=fill_dict["trade_type"],
                        order_type=fill_dict["order_type"],
                        fill_qty=float(fill_dict["fill_qty"]),
                        fill_price=float(fill_dict["fill_price"]),
                        turnover=float(fill_dict["turnover"]),
                        order_qty=float(fill_dict["order_qty"]),
                        realized_pnl=float(fill_dict["realized_pnl"]),
                        commission=float(fill_dict["commission"]),
                        slippage_loss=float(fill_dict["slippage_loss"]),
                    )
                )
                fill_id_seq += 1

                # update order status -> Filled
                if od.order_id is not None and od.order_id in order_idx:
                    i = order_idx[od.order_id]
                    old = ledger.orders[i]
                    ledger.orders[i] = LedgerOrder(
                        order_id=old.order_id,
                        symbol=old.symbol,
                        side=old.side,
                        order_type=old.order_type,
                        submit_dt=old.submit_dt,
                        exec_dt=old.exec_dt,
                        order_qty=old.order_qty,
                        status="Filled",
                        reason=None,
                    )

        # 2) build bars dict for symbols that have a bar at dt
        bars: Dict[str, Dict[str, float]] = {}
        for sym, dfidx in sym_row_map.items():
            if dt not in dfidx.index:
                continue
            row = dfidx.loc[dt]
            bar = {
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            }
            bars[sym] = bar
            last_close[sym] = float(bar["close"])

        if not bars:
            continue

        # 3) strategy generates orders at dt
        new_orders = strat.on_bar(dt, bars, broker)

        # 4) schedule orders to next bar open of that symbol + write OMS ledger
        for od in new_orders:
            sym = od.symbol
            times = sym_times.get(sym)
            if times is None:
                continue

            # find current dt position in that symbol's times
            try:
                cur_idx = times.get_loc(dt)
            except KeyError:
                continue
            nxt_idx = cur_idx + 1
            if nxt_idx >= len(times):
                continue
            exec_dt = pd.Timestamp(times[nxt_idx])

            # assign order_id
            if od.order_id is None:
                od.order_id = order_id_seq
                order_id_seq += 1

            # submit-time check (best-effort)
            est_price = float(bars.get(sym, {}).get("close", 0.0))
            ok, reason = broker.check_order(sym, od.side, od.size, est_price, last_prices=last_close)

            o = LedgerOrder(
                order_id=int(od.order_id),
                symbol=sym,
                side=od.side,
                order_type=str(od.order_type or "market"),
                submit_dt=dt.strftime("%Y-%m-%d %H:%M:%S"),
                exec_dt=exec_dt.strftime("%Y-%m-%d %H:%M:%S"),
                order_qty=float(od.size),
                status="Submitted" if ok else "Rejected",
                reason=reason,
            )
            order_idx[o.order_id] = len(ledger.orders)
            ledger.add_order(o)

            if not ok:
                continue

            scheduled.setdefault(exec_dt, []).append(od)

        # 5) record snapshot at dt (end of bar)
        dyn = broker.dynamic_equity(last_close)
        sta = broker.static_equity(last_close)
        lm, sm = broker._margin_total(last_close)
        avail = broker.available_cash(last_close)

        ledger.add_snapshot(
            LedgerSnapshot(
                dt=dt.strftime("%Y-%m-%d %H:%M:%S"),
                cash=float(broker.cash),
                static_equity=float(sta),
                dynamic_equity=float(dyn),
                l_margin=float(lm),
                s_margin=float(sm),
                available=float(avail),
                fee_cum=float(broker.fee_cum),
                slip_cum=float(broker.slip_cum),
                pos_count=int(broker.open_positions_count()),
            )
        )

        # dd on dynamic equity
        value = dyn
        if value > peak:
            peak = value
        dd = (peak - value) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            max_moneydown = peak - value

    end_value = broker.dynamic_equity(last_close)

    # trade stats (still from closed-trade view)
    closed = len(broker.trades)
    net_pnl = sum(float(t["pnlcomm"]) for t in broker.trades)
    by_symbol: Dict[str, Dict[str, Any]] = {}
    for t in broker.trades:
        sym = t["symbol"]
        by_symbol.setdefault(sym, {"closed_trades": 0, "net_pnl": 0.0})
        by_symbol[sym]["closed_trades"] += 1
        by_symbol[sym]["net_pnl"] += float(t["pnlcomm"])
    for sym in by_symbol:
        by_symbol[sym]["net_pnl"] = round(by_symbol[sym]["net_pnl"], 2)

    trade_stats = {"closed_trades": closed, "net_pnl": round(net_pnl, 2), "by_symbol": by_symbol}

    # equity curve: reuse snapshots but keep legacy keys: datetime/value/cash
    equity_curve: List[Dict[str, Any]] = []
    for s in ledger.snapshots:
        equity_curve.append(
            {
                "datetime": s.dt,
                "value": float(s.dynamic_equity),  # legacy 'value' = dynamic equity
                "cash": float(s.cash),
                "static_equity": float(s.static_equity),
                "dynamic_equity": float(s.dynamic_equity),
                "l_margin": float(s.l_margin),
                "s_margin": float(s.s_margin),
                "available": float(s.available),
                "fee_cum": float(s.fee_cum),
                "slip_cum": float(s.slip_cum),
                "pos_count": int(s.pos_count),
            }
        )

    # time_return (simple from dynamic equity)
    time_return: Dict[str, Any] = {}
    if len(equity_curve) >= 2:
        prev = float(equity_curve[0]["value"])
        for row in equity_curve[1:]:
            cur = float(row["value"])
            r = (cur / prev - 1.0) if prev else 0.0
            time_return[row["datetime"]] = float(r)
            prev = cur

    payload_ledger = ledger.to_payload()

    return SimpleBacktestResult(
        start_value=float(start_value),
        end_value=float(end_value),
        trade_stats=trade_stats,
        drawdown={"max_drawdown_pct": float(max_dd * 100.0), "max_moneydown": float(max_moneydown)},
        trades=broker.trades,
        equity_curve=equity_curve,
        time_return=time_return,
        orders=payload_ledger["orders"],
        fills=payload_ledger["fills"],
        snapshots=payload_ledger["snapshots"],
    )
