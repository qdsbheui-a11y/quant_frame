from __future__ import annotations

"""
简单参数优化脚本（命令行使用）。

功能：
- 读取一个 YAML 配置
- 在给定的参数网格上循环回测
- 收集每个参数组合的关键指标，写入 CSV 方便后续分析

使用示例（在项目根目录）：

    python -m my_bt_lab.app.param_sweep -c my_bt_lab/app/configs/cta.yaml -o runs/param_sweep_results.csv

参数网格来源：
- 优先从配置文件中的 optimize.strategy_params 读取，例如：

  optimize:
    strategy_params:
      fast: [5, 10, 20]
      slow: [30, 60]
      atr_stop_mult: [1.5, 2.0]

- 如果配置中没有该字段，则使用本文件内 DEFAULT_PARAM_GRID 作为示例（你可以按需修改）。
"""

import argparse
import itertools
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from my_bt_lab.config.load import load_yaml_config
from my_bt_lab.engines.factory import run as run_engine


# 若配置文件中未提供 optimize.strategy_params，则使用此兜底示例网格
DEFAULT_PARAM_GRID: Dict[str, List[Any]] = {
    "fast": [5, 10, 20],
    "slow": [30, 60],
    "atr_stop_mult": [1.5, 2.0],
}


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="my_bt_lab 简单参数优化工具")
    p.add_argument(
        "--config",
        "-c",
        type=str,
        required=False,
        default="my_bt_lab/app/configs/cta.yaml",
        help="基础 YAML 配置路径",
    )
    p.add_argument(
        "--output",
        "-o",
        type=str,
        required=False,
        default="runs/param_sweep_results.csv",
        help="优化结果输出 CSV 路径",
    )
    return p


def _get_param_grid_from_cfg(cfg: Dict[str, Any]) -> Dict[str, List[Any]]:
    opt = cfg.get("optimize", {}) or {}
    s_params = opt.get("strategy_params")
    if isinstance(s_params, dict) and s_params:
        grid: Dict[str, List[Any]] = {}
        for k, v in s_params.items():
            if isinstance(v, (list, tuple, set)):
                grid[k] = list(v)
            else:
                grid[k] = [v]
        return grid
    return DEFAULT_PARAM_GRID


def _iter_param_combinations(grid: Dict[str, List[Any]]):
    keys = sorted(grid.keys())
    for values in itertools.product(*(grid[k] for k in keys)):
        yield {k: v for k, v in zip(keys, values)}


def main():
    args = _build_argparser().parse_args()
    cfg, cfg_path = load_yaml_config(args.config)

    grid = _get_param_grid_from_cfg(cfg)
    combos = list(_iter_param_combinations(grid))
    if not combos:
        print("未检测到任何参数组合，退出。")
        return

    print(f"总共需要回测的参数组合个数: {len(combos)}")

    rows: List[Dict[str, Any]] = []

    for i, params_override in enumerate(combos, start=1):
        print(f"[{i}/{len(combos)}] 运行参数组合: {params_override}")

        # 每次都重新 load 基础配置，避免参数污染
        base_cfg, base_cfg_path = load_yaml_config(cfg_path)

        strat_cfg = base_cfg.setdefault("strategy", {})
        s_params = strat_cfg.setdefault("params", {})
        s_params.update(params_override)

        result = run_engine(base_cfg, base_cfg_path)

        start_value = float(getattr(result, "start_value", float("nan")))
        end_value = float(getattr(result, "end_value", float("nan")))
        total_ret = (
            (end_value / start_value - 1.0) if start_value else float("nan")
        )

        drawdown = getattr(result, "drawdown", {}) or {}
        max_dd_pct = float(drawdown.get("max_drawdown_pct", float("nan")))
        max_moneydown = float(drawdown.get("max_moneydown", float("nan")))

        trade_stats = getattr(result, "trade_stats", {}) or {}
        closed_trades = int(trade_stats.get("closed_trades", 0))
        net_pnl = float(trade_stats.get("net_pnl", 0.0))

        row: Dict[str, Any] = {}
        # 记录参数
        row.update(params_override)
        # 记录结果指标
        row.update(
            dict(
                start_value=start_value,
                end_value=end_value,
                total_return_pct=total_ret * 100.0,
                max_drawdown_pct=max_dd_pct,
                max_moneydown=max_moneydown,
                closed_trades=closed_trades,
                net_pnl=net_pnl,
            )
        )
        rows.append(row)

    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"参数优化结果已保存到: {out_path}")


if __name__ == "__main__":
    main()

