from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path 
from typing import Any, Dict, List, Tuple

import backtrader as bt
import logging

from my_bt_lab.analyzers.trade_stats import TradeStatsAnalyzer
from my_bt_lab.analyzers.trade_list import TradeListAnalyzer
from my_bt_lab.analyzers.equity_curve import EquityCurveAnalyzer
from my_bt_lab.analyzers.order_fill_ledger import OrderFillLedgerAnalyzer
from my_bt_lab.analyzers.snapshot_ledger import SnapshotLedgerAnalyzer
from my_bt_lab.brokers.backtrader_setup import setup_broker
from my_bt_lab.brokers.safe_backbroker import SafeBackBroker
from my_bt_lab.data.loaders_bt import load_data_item
from my_bt_lab.registry.strategy_registry import get_strategy


@dataclass
class BacktestResult:
    start_value: float
    end_value: float
    trade_stats: Dict[str, Any]
    drawdown: Dict[str, Any]
    trades: List[Dict[str, Any]]
    equity_curve: List[Dict[str, Any]]
    time_return: Dict[str, Any]
    orders: List[Dict[str, Any]]
    fills: List[Dict[str, Any]]
    snapshots: List[Dict[str, Any]]
    realized_pnl: float
    floating_pnl: float
    open_positions: List[Dict[str, Any]]


def _project_root_from_cfg_path(cfg_path: Path) -> Path:
    if cfg_path.parent.name == "configs":
        return cfg_path.parents[3]
    return cfg_path.parent


def _parse_bt_timeframe(value: str) -> bt.TimeFrame:
    text = str(value or "days").strip().lower()
    mapping = {
        "m": bt.TimeFrame.Minutes,
        "min": bt.TimeFrame.Minutes,
        "mins": bt.TimeFrame.Minutes,
        "minute": bt.TimeFrame.Minutes,
        "minutes": bt.TimeFrame.Minutes,
        "h": bt.TimeFrame.Minutes,
        "hour": bt.TimeFrame.Minutes,
        "hours": bt.TimeFrame.Minutes,
        "d": bt.TimeFrame.Days,
        "day": bt.TimeFrame.Days,
        "days": bt.TimeFrame.Days,
        "w": bt.TimeFrame.Weeks,
        "week": bt.TimeFrame.Weeks,
        "weeks": bt.TimeFrame.Weeks,
        "mo": bt.TimeFrame.Months,
        "month": bt.TimeFrame.Months,
        "months": bt.TimeFrame.Months,
    }
    return mapping.get(text, bt.TimeFrame.Days)


def build_cerebro(cfg: Dict[str, Any], project_root: Path) -> bt.Cerebro:
    cerebro = bt.Cerebro(runonce=False)
    cerebro.setbroker(SafeBackBroker())
    setup_broker(cerebro, cfg)

    data_cfgs = cfg.get("data", []) or []
    if not data_cfgs:
        raise ValueError("配置文件里没有 data 列表")

    raw_data_map: Dict[str, bt.feed.DataBase] = {}
    data_roles: Dict[str, Dict[str, Any]] = {}

    for item in data_cfgs:
        name = str(item["name"])
        symbol = str(item.get("symbol") or name)
        role = str(item.get("role") or "exec").lower().strip()

        data = load_data_item(item=item, project_root=project_root, cfg=cfg)
        logging.info("数据加载成功: name=%s symbol=%s role=%s", name, symbol, role)

        cerebro.adddata(data, name=name)
        raw_data_map[name] = data
        data_roles[name] = {
            "symbol": symbol,
            "role": role,
            "timeframe": str(item.get("timeframe") or "days"),
            "compression": int(item.get("compression", 1) or 1),
            "source": name,
        }

    for item in (cfg.get("resample", []) or []):
        source = str(item["source"])
        if source not in raw_data_map:
            raise ValueError(f"resample.source={source} 未在 data 列表中找到")

        name = str(item["name"])
        source_data = raw_data_map[source]
        source_meta = data_roles[source]

        symbol = str(item.get("symbol") or source_meta["symbol"])
        role = str(item.get("role") or "signal").lower().strip()
        timeframe = str(item.get("timeframe") or "minutes")
        compression = int(item.get("compression", 60) or 60)

        logging.info(
            "重采样数据: source=%s -> name=%s symbol=%s role=%s timeframe=%s compression=%s",
            source,
            name,
            symbol,
            role,
            timeframe,
            compression,
        )

        cerebro.resampledata(
            source_data,
            timeframe=_parse_bt_timeframe(timeframe),
            compression=compression,
            name=name,
        )

        data_roles[name] = {
            "symbol": symbol,
            "role": role,
            "timeframe": timeframe,
            "compression": compression,
            "source": source,
        }

    s_cfg = cfg.get("strategy", {}) or {}
    name = s_cfg.get("name")
    params = dict(s_cfg.get("params") or {})
    if not name:
        name = "cta_trend"
        for k, v in s_cfg.items():
            if k not in {"name", "params"}:
                params.setdefault(k, v)

    strat_cls = get_strategy(str(name))
    params.setdefault("symbol_specs", cfg.get("symbols", {}) or {})
    params.setdefault("data_roles", data_roles)
    cerebro.addstrategy(strat_cls, **params)

    broker_cfg = cfg.get("broker", {}) or {}
    symbol_specs = cfg.get("symbols", {}) or {}
    slip_perc = float(broker_cfg.get("slip_perc", 0.0) or 0.0)
    account_mode = str(broker_cfg.get("account_mode", "cash") or "cash")

    cerebro.addanalyzer(TradeStatsAnalyzer, _name="trade_stats")
    cerebro.addanalyzer(TradeListAnalyzer, _name="trade_list")
    cerebro.addanalyzer(EquityCurveAnalyzer, _name="equity_curve")
    cerebro.addanalyzer(
        OrderFillLedgerAnalyzer,
        _name="order_fill_ledger",
        slip_perc=slip_perc,
        symbol_specs=symbol_specs,
    )
    cerebro.addanalyzer(
        SnapshotLedgerAnalyzer,
        _name="snapshot_ledger",
        account_mode=account_mode,
        symbol_specs=symbol_specs,
        slip_perc=slip_perc,
    )
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="time_return")
    return cerebro


def _collect_open_positions(strat, broker) -> Tuple[List[Dict[str, Any]], float]:
    data_roles = getattr(strat.p, "data_roles", {}) or {}

    rows: List[Dict[str, Any]] = []
    floating_pnl = 0.0

    for d in strat.datas:
        name = getattr(d, "_name", "") or ""
        meta = data_roles.get(name, {})
        role = str(meta.get("role") or "both").lower().strip()

        if role not in {"exec", "both"}:
            continue

        pos = strat.getposition(d)
        size = float(getattr(pos, "size", 0.0) or 0.0)
        if size == 0:
            continue

        try:
            last_price = float(d.close[0])
        except Exception:
            continue
        avg_price = float(getattr(pos, "price", 0.0) or 0.0)

        comminfo = broker.getcommissioninfo(d)
        mult = float(getattr(comminfo.p, "mult", 1.0) or 1.0)

        pnl = (last_price - avg_price) * size * mult
        floating_pnl += pnl

        rows.append(
            {
                "symbol": str(meta.get("symbol") or name),
                "data_name": name,
                "direction": "long" if size > 0 else "short",
                "size": size,
                "avg_price": avg_price,
                "last_price": last_price,
                "floating_pnl": pnl,
            }
        )

    return rows, float(floating_pnl)


def run_backtest(cfg: Dict[str, Any], cfg_path: Path) -> BacktestResult:
    project_root = _project_root_from_cfg_path(cfg_path)
    cerebro = build_cerebro(cfg=cfg, project_root=project_root)

    start_value = float(cerebro.broker.getvalue())
    # Enable Backtrader Trade.history so analyzers can extract entry side,
    # entry/exit timestamps, and prices more reliably.
    results = cerebro.run(tradehistory=True)
    strat = results[0]
    end_value = float(cerebro.broker.getvalue())

    trade_stats = strat.analyzers.trade_stats.get_analysis()
    dd = strat.analyzers.drawdown.get_analysis()
    drawdown = {
        "max_drawdown_pct": float(getattr(dd.max, "drawdown", 0.0)),
        "max_moneydown": float(getattr(dd.max, "moneydown", 0.0)),
    }

    trades = strat.analyzers.trade_list.get_analysis()

    ledger = strat.analyzers.order_fill_ledger.get_analysis()
    orders = ledger.get("orders", []) if isinstance(ledger, dict) else []
    fills = ledger.get("fills", []) if isinstance(ledger, dict) else []
    snapshots = strat.analyzers.snapshot_ledger.get_analysis()

    if snapshots:
        equity_curve = [
            {
                "datetime": row.get("dt"),
                "value": float(row.get("dynamic_equity", 0.0) or 0.0),
                "cash": float(row.get("cash", 0.0) or 0.0),
                "static_equity": float(row.get("static_equity", 0.0) or 0.0),
                "dynamic_equity": float(row.get("dynamic_equity", 0.0) or 0.0),
                "l_margin": float(row.get("l_margin", 0.0) or 0.0),
                "s_margin": float(row.get("s_margin", 0.0) or 0.0),
                "available": float(row.get("available", 0.0) or 0.0),
                "fee_cum": float(row.get("fee_cum", 0.0) or 0.0),
                "slip_cum": float(row.get("slip_cum", 0.0) or 0.0),
                "pos_count": int(row.get("pos_count", 0) or 0),
            }
            for row in snapshots
        ]
    else:
        equity_curve = strat.analyzers.equity_curve.get_analysis()

    time_return = strat.analyzers.time_return.get_analysis()

    open_positions, floating_pnl = _collect_open_positions(strat, cerebro.broker)
    realized_pnl = float(trade_stats.get("net_pnl", 0.0) or 0.0)

    return BacktestResult(
        start_value=start_value,
        end_value=end_value,
        trade_stats=trade_stats,
        drawdown=drawdown,
        trades=trades,
        equity_curve=equity_curve,
        time_return=time_return,
        orders=orders,
        fills=fills,
        snapshots=snapshots,
        realized_pnl=realized_pnl,
        floating_pnl=floating_pnl,
        open_positions=open_positions,
    )
