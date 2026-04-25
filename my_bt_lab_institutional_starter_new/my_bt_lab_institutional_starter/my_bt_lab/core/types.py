from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class LedgerOrder:
    order_id: int
    symbol: str
    side: str                # buy / sell
    order_type: str          # market / limit / stop ...
    submit_dt: str           # '%Y-%m-%d %H:%M:%S'
    exec_dt: Optional[str]   # planned exec time (best-effort)
    order_qty: float
    status: str              # Submitted / Rejected / Filled
    reason: Optional[str] = None


@dataclass
class LedgerFill:
    fill_id: int
    order_id: int
    dt: str                  # '%Y-%m-%d %H:%M:%S'
    symbol: str
    trade_type: str          # BUY_OPEN / SELL_CLOSE (M1: long-only)
    order_type: str
    fill_qty: float
    fill_price: float
    turnover: float
    order_qty: float
    realized_pnl: float
    commission: float
    slippage_loss: float


@dataclass
class LedgerSnapshot:
    dt: str                  # '%Y-%m-%d %H:%M:%S'
    cash: float
    static_equity: float
    dynamic_equity: float
    l_margin: float
    s_margin: float
    available: float
    fee_cum: float
    slip_cum: float
    pos_count: int
