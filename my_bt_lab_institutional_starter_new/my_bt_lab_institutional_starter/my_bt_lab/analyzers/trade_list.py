from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import backtrader as bt


@dataclass
class TradeRecord:
    symbol: str
    direction: str          # long / short / unknown
    size: float
    entry_dt: Optional[str]
    entry_price: Optional[float]
    exit_dt: Optional[str]
    exit_price: Optional[float]
    pnl: float
    pnlcomm: float
    barlen: int
    ref: int

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


class TradeListAnalyzer(bt.Analyzer):
    """Collect per-trade records.

    Notes:
    - Backtrader's Trade object varies slightly by version and broker settings.
    - We try best-effort extraction from trade.history; if unavailable we fall back to trade attributes.
    """

    def start(self):
        self._trades: List[TradeRecord] = []

    @staticmethod
    def _fmt_dt(dt) -> Optional[str]:
        if dt is None:
            return None
        try:
            # dt may be float (backtrader date num)
            if isinstance(dt, (int, float)):
                return bt.num2date(dt).strftime("%Y-%m-%d %H:%M:%S")
            # dt may be datetime
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                return str(dt)
            except Exception:
                return None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        d = trade.data
        symbol = getattr(d, "_name", "DATA")

        entry_dt = None
        exit_dt = None
        entry_price = None
        exit_price = None
        size = None
        direction = "unknown"

        # Prefer trade.history if present
        hist = getattr(trade, "history", None)
        if hist:
            try:
                open_evt = hist[0]
                close_evt = hist[-1]
                entry_dt = self._fmt_dt(getattr(open_evt, "dt", None))
                exit_dt = self._fmt_dt(getattr(close_evt, "dt", None))
                entry_price = float(getattr(open_evt, "price", None)) if getattr(open_evt, "price", None) is not None else None
                exit_price = float(getattr(close_evt, "price", None)) if getattr(close_evt, "price", None) is not None else None

                # size on open event is typically signed
                open_size = getattr(open_evt, "size", None)
                if open_size is not None:
                    try:
                        open_size = float(open_size)
                        direction = "long" if open_size > 0 else ("short" if open_size < 0 else "unknown")
                        size = abs(open_size)
                    except Exception:
                        pass
            except Exception:
                pass

        # Fallbacks
        if entry_dt is None:
            entry_dt = self._fmt_dt(getattr(trade, "dtopen", None))
        if exit_dt is None:
            exit_dt = self._fmt_dt(getattr(trade, "dtclose", None))

        if entry_price is None:
            try:
                entry_price = float(getattr(trade, "price", None))
            except Exception:
                entry_price = None

        # If exit_price missing but we have pnl and size, approximate.
        pnl = float(getattr(trade, "pnl", 0.0))
        pnlcomm = float(getattr(trade, "pnlcomm", pnl))
        barlen = int(getattr(trade, "barlen", 0) or 0)
        ref = int(getattr(trade, "ref", 0) or 0)

        if size is None:
            # trade.size may be 0 after close; try trade.history sizes, else unknown -> 0
            size = 0.0

        if exit_price is None and entry_price is not None and size:
            # Assume pnl = (exit-entry) * size  (stocklike)
            # For futures (mult != 1) this is approximate, but still useful for debugging.
            try:
                exit_price = entry_price + (pnl / float(size))
            except Exception:
                exit_price = None

        self._trades.append(
            TradeRecord(
                symbol=symbol,
                direction=direction,
                size=float(size),
                entry_dt=entry_dt,
                entry_price=entry_price,
                exit_dt=exit_dt,
                exit_price=exit_price,
                pnl=pnl,
                pnlcomm=pnlcomm,
                barlen=barlen,
                ref=ref,
            )
        )

    def get_analysis(self):
        return [t.to_dict() for t in self._trades]
