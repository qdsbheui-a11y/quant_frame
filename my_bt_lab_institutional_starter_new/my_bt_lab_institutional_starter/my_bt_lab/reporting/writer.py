from __future__ import annotations

import json
import platform
import subprocess
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from my_bt_lab.reporting.html_report import write_html_report


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _try_git_head(project_root: Path) -> Optional[str]:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(project_root), stderr=subprocess.DEVNULL)
        return out.decode("utf-8").strip()
    except Exception:
        return None


def _json_sanitize(obj: Any) -> Any:
    try:
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
    except Exception:
        pass

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(k, (datetime, date)):
                kk = k.isoformat()
            elif isinstance(k, (str, int, float, bool)) or k is None:
                kk = k
            else:
                kk = str(k)
            out[kk] = _json_sanitize(v)
        return out
    if isinstance(obj, (list, tuple, set)):
        return [_json_sanitize(x) for x in obj]
    return obj


def prepare_run_dir(output_root: Path, tag: str | None = None) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_tag = "".join(c for c in (tag or "run") if c.isalnum() or c in "-_")[:40] or "run"
    base_name = f"{ts}_{safe_tag}"

    for attempt in range(1000):
        suffix = "" if attempt == 0 else f"_{attempt:03d}"
        run_dir = output_root / f"{base_name}{suffix}"
        try:
            run_dir.mkdir(parents=True, exist_ok=False)
            return run_dir
        except FileExistsError:
            continue

    raise FileExistsError(f"无法为本次回测创建唯一运行目录: {output_root / base_name}")


def _write_table(run_dir: Path, filename: str, rows: Any, columns: Optional[List[str]] = None) -> None:
    if rows is None:
        return

    if isinstance(rows, dict):
        df = pd.DataFrame([{"key": k, "value": v} for k, v in rows.items()])
    else:
        df = pd.DataFrame(rows)

    if df.empty and columns:
        df = pd.DataFrame(columns=columns)

    df.to_csv(run_dir / filename, index=False, encoding="utf-8-sig")


def write_result(run_dir: Path, cfg: Dict[str, Any], cfg_path: Path, result: Any, project_root: Path):
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "config.yaml").write_text(cfg_path.read_text(encoding="utf-8"), encoding="utf-8")

    meta = {
        "utc_time": _utc_now_iso(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "git_head": _try_git_head(project_root),
        "cfg_path": str(cfg_path),
    }
    (run_dir / "run_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    try:
        payload = asdict(result)
    except Exception:
        payload = result if isinstance(result, dict) else {"result": str(result)}
    payload = _json_sanitize(payload)
    (run_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    _write_table(
        run_dir,
        "orders.csv",
        payload.get("orders"),
        columns=["order_id", "symbol", "side", "order_type", "submit_dt", "exec_dt", "order_qty", "status", "reason"],
    )
    _write_table(
        run_dir,
        "fills.csv",
        payload.get("fills"),
        columns=[
            "fill_id",
            "order_id",
            "dt",
            "symbol",
            "trade_type",
            "order_type",
            "fill_qty",
            "fill_price",
            "turnover",
            "order_qty",
            "realized_pnl",
            "commission",
            "slippage_loss",
        ],
    )
    _write_table(
        run_dir,
        "trades.csv",
        payload.get("trades"),
        columns=["symbol", "direction", "size", "entry_dt", "entry_price", "exit_dt", "exit_price", "pnl", "pnlcomm"],
    )
    _write_table(
        run_dir,
        "equity_curve.csv",
        payload.get("equity_curve"),
        columns=[
            "datetime",
            "value",
            "cash",
            "static_equity",
            "dynamic_equity",
            "l_margin",
            "s_margin",
            "available",
            "fee_cum",
            "slip_cum",
            "pos_count",
        ],
    )
    _write_table(
        run_dir,
        "snapshots.csv",
        payload.get("snapshots"),
        columns=["dt", "cash", "static_equity", "dynamic_equity", "l_margin", "s_margin", "available", "fee_cum", "slip_cum", "pos_count"],
    )

    open_positions = payload.get("open_positions") or []
    _write_table(
        run_dir,
        "open_positions.csv",
        open_positions,
        columns=["symbol", "data_name", "direction", "size", "avg_price", "last_price", "floating_pnl"],
    )

    time_ret = payload.get("time_return")
    if isinstance(time_ret, dict):
        _write_table(
            run_dir,
            "time_return.csv",
            [{"datetime": k, "return": v} for k, v in time_ret.items()],
            columns=["datetime", "return"],
        )

    rep_cfg = cfg.get("report") or {}
    if bool(rep_cfg.get("html", True)):
        title = str(rep_cfg.get("title") or (cfg.get("output") or {}).get("tag") or "回测报告")
        asset_dir = rep_cfg.get("asset_dir")
        out_folder = str(rep_cfg.get("out_folder") or "report_html")
        try:
            write_html_report(
                run_dir=run_dir,
                cfg=cfg,
                result=payload,
                title=title,
                asset_dir=asset_dir,
                out_folder=out_folder,
            )
        except Exception as e:
            (run_dir / "report_error.txt").write_text(str(e), encoding="utf-8")
