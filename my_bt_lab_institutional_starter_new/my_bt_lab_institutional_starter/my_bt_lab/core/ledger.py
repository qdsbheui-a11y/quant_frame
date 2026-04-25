from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List

from my_bt_lab.core.types import LedgerFill, LedgerOrder, LedgerSnapshot


class Ledger:
    """A tiny in-memory ledger (M1).

    This is the single source of truth for:
    - orders (委托)
    - fills (成交)
    - snapshots (账户快照)

    Report/metrics should derive from this, not from 'trades' reverse engineering.
    """

    def __init__(self):
        self.orders: List[LedgerOrder] = []
        self.fills: List[LedgerFill] = []
        self.snapshots: List[LedgerSnapshot] = []

    def add_order(self, o: LedgerOrder) -> None:
        self.orders.append(o)

    def add_fill(self, f: LedgerFill) -> None:
        self.fills.append(f)

    def add_snapshot(self, s: LedgerSnapshot) -> None:
        self.snapshots.append(s)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "orders": [asdict(x) for x in self.orders],
            "fills": [asdict(x) for x in self.fills],
            # keep naming aligned with existing pipeline (writer/html_report)
            "snapshots": [asdict(x) for x in self.snapshots],
        }
