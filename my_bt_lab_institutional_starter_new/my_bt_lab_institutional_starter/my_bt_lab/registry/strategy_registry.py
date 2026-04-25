from __future__ import annotations

from typing import Dict, Type, Any

# Backtrader strategies
from my_bt_lab.strategies.cta_trend import MultiDataAtrCtaStrategy
from my_bt_lab.strategies.donchian_daily_mtf import DonchianDailyMtfStrategy

STRATEGY_REGISTRY: Dict[str, Type[Any]] = {
    "cta_trend": MultiDataAtrCtaStrategy,
    "donchian_daily_mtf": DonchianDailyMtfStrategy,
}

def get_strategy(name: str):
    if not name:
        raise KeyError("strategy.name 不能为空")
    key = str(name).strip().lower()
    if key not in STRATEGY_REGISTRY:
        raise KeyError(f"未注册的策略: {name}. 已注册: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[key]
