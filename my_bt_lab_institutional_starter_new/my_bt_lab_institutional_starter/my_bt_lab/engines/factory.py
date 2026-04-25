from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def run(cfg: Dict[str, Any], cfg_path: Path):
    engine_cfg = cfg.get("engine", {}) or {}
    name = str(engine_cfg.get("name", "backtrader")).lower().strip()

    if name == "backtrader":
        from my_bt_lab.engines.backtrader_engine import run_backtest as run_backtrader
        return run_backtrader(cfg, cfg_path)

    if name == "simple":
        from my_bt_lab.engines.simple_engine import run_simple
        return run_simple(cfg, cfg_path)

    raise ValueError(f"未知 engine.name={name}. 当前仅支持: backtrader / simple")
