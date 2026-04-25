from __future__ import annotations

import backtrader as bt

from my_bt_lab.commission.futures_comm import SimpleFuturesComm


def _build_comminfo_from_cfg(comm_cfg: dict, account_mode: str = "cash") -> SimpleFuturesComm:
    account_mode = str(account_mode or "cash").lower().strip()
    is_cash = account_mode == "cash"
    return SimpleFuturesComm(
        commission=float(comm_cfg.get("commission", 0.0005)),
        mult=float(comm_cfg.get("mult", 1.0)),
        margin=(0.0 if is_cash else float(comm_cfg.get("margin", 0.0)) if comm_cfg.get("margin") is not None else 0.0),
        margin_rate=(None if is_cash else comm_cfg.get("margin_rate", None)),
        commtype=(
            bt.CommInfoBase.COMM_FIXED
            if str(comm_cfg.get("commtype", "perc")).lower() == "fixed"
            else bt.CommInfoBase.COMM_PERC
        ),
        stocklike=is_cash,
    )


def setup_broker(cerebro: bt.Cerebro, cfg: dict) -> None:
    broker_cfg = cfg.get("broker", {}) or {}
    default_comm_cfg = cfg.get("commission_default", cfg.get("commission", {})) or {}
    symbol_cfgs = cfg.get("symbols", {}) or {}
    account_mode = str(broker_cfg.get("account_mode", "cash") or "cash")

    starting_cash = float(broker_cfg.get("starting_cash", 100000.0))
    cerebro.broker.setcash(starting_cash)

    if bool(broker_cfg.get("coc", False)):
        cerebro.broker.set_coc(True)

    slip_perc = float(broker_cfg.get("slip_perc", 0.0) or 0.0)
    if slip_perc > 0:
        try:
            cerebro.broker.set_slippage_perc(perc=slip_perc)
        except TypeError:
            cerebro.broker.set_slippage_perc(slip_perc)

    default_ci = _build_comminfo_from_cfg(default_comm_cfg, account_mode=account_mode)
    cerebro.broker.addcommissioninfo(default_ci)

    for sym, scfg in symbol_cfgs.items():
        if not isinstance(scfg, dict):
            continue
        merged = dict(default_comm_cfg)
        merged.update({k: v for k, v in scfg.items() if k in {"commission", "mult", "margin", "margin_rate", "commtype"}})
        ci = _build_comminfo_from_cfg(merged, account_mode=account_mode)
        cerebro.broker.addcommissioninfo(ci, name=str(sym))