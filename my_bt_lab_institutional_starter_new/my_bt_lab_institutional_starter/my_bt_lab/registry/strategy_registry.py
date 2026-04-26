from __future__ import annotations

import importlib
import inspect
import re
from pathlib import Path
from typing import Any, Dict, Type

# Built-in Backtrader strategies
from my_bt_lab.strategies.base_strategy import BaseStrategy
from my_bt_lab.strategies.cta_trend import MultiDataAtrCtaStrategy
from my_bt_lab.strategies.donchian_daily_mtf import DonchianDailyMtfStrategy


EXPLICIT_STRATEGY_REGISTRY: Dict[str, Type[Any]] = {
    "cta_trend": MultiDataAtrCtaStrategy,
    "donchian_daily_mtf": DonchianDailyMtfStrategy,
}

# Non-fatal import/discovery errors are stored here so UI/diagnostics can show them later.
STRATEGY_LOAD_ERRORS: Dict[str, str] = {}


def _camel_to_snake(name: str) -> str:
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", str(name))
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower().strip("_")


def _strategy_key(module_stem: str, class_name: str, existing: Dict[str, Type[Any]]) -> str:
    key = str(module_stem).strip().lower()
    if key not in existing:
        return key
    class_key = _camel_to_snake(class_name)
    if class_key not in existing:
        return class_key
    i = 2
    while f"{class_key}_{i}" in existing:
        i += 1
    return f"{class_key}_{i}"


def _discover_strategy_classes() -> Dict[str, Type[Any]]:
    registry: Dict[str, Type[Any]] = dict(EXPLICIT_STRATEGY_REGISTRY)
    STRATEGY_LOAD_ERRORS.clear()

    strategies_dir = Path(__file__).resolve().parents[1] / "strategies"
    if not strategies_dir.exists():
        return registry

    skip_modules = {
        "__init__",
        "base_strategy",
        "cta_trend",
        "donchian_daily_mtf",
    }

    for path in sorted(strategies_dir.glob("*.py"), key=lambda p: p.name.lower()):
        module_stem = path.stem
        if module_stem.startswith("_") or module_stem in skip_modules:
            continue

        module_name = f"my_bt_lab.strategies.{module_stem}"
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            # Keep one bad custom strategy from breaking the whole app.
            STRATEGY_LOAD_ERRORS[module_stem] = f"导入失败: {exc}"
            continue

        discovered = []
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module.__name__:
                continue
            try:
                is_strategy = issubclass(cls, BaseStrategy) and cls is not BaseStrategy
            except Exception:
                is_strategy = False
            if is_strategy:
                discovered.append(cls)

        for cls in discovered:
            explicit_name = getattr(cls, "strategy_name", None)
            key = str(explicit_name).strip().lower() if explicit_name else _strategy_key(module_stem, cls.__name__, registry)
            if not key:
                key = _strategy_key(module_stem, cls.__name__, registry)
            registry[key] = cls

    return registry


STRATEGY_REGISTRY: Dict[str, Type[Any]] = _discover_strategy_classes()


def refresh_strategy_registry() -> Dict[str, Type[Any]]:
    """Refresh strategy registry after adding a strategy module at runtime."""
    global STRATEGY_REGISTRY
    STRATEGY_REGISTRY = _discover_strategy_classes()
    return STRATEGY_REGISTRY


def register_strategy(name: str, strategy_cls: Type[Any]) -> None:
    key = str(name or "").strip().lower()
    if not key:
        raise KeyError("strategy.name 不能为空")
    if not inspect.isclass(strategy_cls):
        raise TypeError("strategy_cls 必须是策略类")
    STRATEGY_REGISTRY[key] = strategy_cls


def get_strategy(name: str):
    if not name:
        raise KeyError("strategy.name 不能为空")
    key = str(name).strip().lower()
    if key not in STRATEGY_REGISTRY:
        raise KeyError(f"未注册的策略: {name}. 已注册: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[key]
