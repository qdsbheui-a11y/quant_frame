from __future__ import annotations

import copy
import json
import logging
import tempfile
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import yaml

from my_bt_lab.engines.factory import run as run_engine
from my_bt_lab.registry.strategy_registry import STRATEGY_REGISTRY
from my_bt_lab.reporting.writer import prepare_run_dir, write_result

TIMEFRAME_OPTIONS = ["minutes", "days", "weeks", "months"]
DATA_SOURCE_OPTIONS = ["csv", "tushare", "postgres"]
ROLE_OPTIONS = ["exec", "signal", "both"]
ACCOUNT_MODE_OPTIONS = ["cash", "futures"]
ENGINE_OPTIONS = ["backtrader", "simple"]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _configs_root() -> Path:
    return _project_root() / "my_bt_lab" / "app" / "configs"


def _list_config_files() -> List[Path]:
    cfg_root = _configs_root()
    if not cfg_root.exists():
        return []
    return sorted([p for p in cfg_root.glob("*.yaml")], key=lambda p: p.name.lower())


def _list_strategy_names() -> List[str]:
    return sorted(set(str(k) for k in STRATEGY_REGISTRY.keys()))


def _read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _coerce_text_value(text: str) -> Any:
    raw = str(text).strip()
    if raw == "":
        return ""
    lower = raw.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in {"none", "null"}:
        return None
    try:
        if raw.startswith("0") and raw not in {"0", "0.0"} and not raw.startswith("0."):
            raise ValueError
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def _parse_grid_text(text: str) -> Dict[str, List[Any]]:
    out: Dict[str, List[Any]] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        vals = [x.strip() for x in v.split(",") if x.strip()]
        parsed_vals = [_coerce_text_value(item) for item in vals]
        if key and parsed_vals:
            out[key] = parsed_vals
    return out


def _grid_text_from_cfg(cfg: Dict[str, Any]) -> str:
    lines: List[str] = []
    optimize = cfg.get("optimize", {}) or {}
    params = optimize.get("strategy_params", {}) or {}
    if not isinstance(params, dict):
        return ""
    for k in sorted(params.keys()):
        v = params[k]
        if isinstance(v, (list, tuple, set)):
            lines.append(f"{k}=" + ",".join(str(x) for x in v))
        else:
            lines.append(f"{k}={v}")
    return "\n".join(lines)


def _iter_param_combinations(grid: Dict[str, List[Any]]):
    keys = sorted(grid.keys())
    for vals in product(*(grid[k] for k in keys)):
        yield {k: v for k, v in zip(keys, vals)}


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "-"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):,.2f}%"
    except Exception:
        return "-"


def _collect_result_metrics(result: Any) -> Dict[str, Any]:
    start_value = float(getattr(result, "start_value", float("nan")))
    end_value = float(getattr(result, "end_value", float("nan")))
    total_ret_pct = (end_value / start_value - 1.0) * 100.0 if start_value else float("nan")
    drawdown = getattr(result, "drawdown", {}) or {}
    trade_stats = getattr(result, "trade_stats", {}) or {}
    return {
        "start_value": start_value,
        "end_value": end_value,
        "total_return_pct": total_ret_pct,
        "max_drawdown_pct": float(drawdown.get("max_drawdown_pct", float("nan"))),
        "max_moneydown": float(drawdown.get("max_moneydown", float("nan"))),
        "closed_trades": int(trade_stats.get("closed_trades", 0)),
        "net_pnl": float(trade_stats.get("net_pnl", 0.0)),
        "win_rate": float(trade_stats.get("win_rate", float("nan"))) if trade_stats.get("win_rate") is not None else float("nan"),
        "realized_pnl": float(getattr(result, "realized_pnl", trade_stats.get("net_pnl", 0.0) or 0.0)),
        "floating_pnl": float(getattr(result, "floating_pnl", 0.0) or 0.0),
    }


def _write_temp_cfg(cfg: Dict[str, Any]) -> Path:
    tmp = tempfile.NamedTemporaryFile(prefix="mt4_cfg_", suffix=".yaml", delete=False, mode="w", encoding="utf-8")
    try:
        yaml.safe_dump(cfg, tmp, allow_unicode=True, sort_keys=False)
        tmp.flush()
    finally:
        tmp.close()
    return Path(tmp.name).resolve()


def _read_text_tail(path: Path, max_chars: int = 8000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-max_chars:]


def _load_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _list_run_dirs(runs_root: Path, limit: int = 30) -> List[Path]:
    if not runs_root.exists():
        return []
    dirs = [p for p in runs_root.iterdir() if p.is_dir()]
    dirs.sort(key=lambda p: p.name, reverse=True)
    return dirs[:limit]


def _build_history_table(runs_root: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for run_dir in _list_run_dirs(runs_root, limit=50):
        result_payload = _load_json_if_exists(run_dir / "result.json")
        meta_payload = _load_json_if_exists(run_dir / "run_meta.json")
        trade_stats = result_payload.get("trade_stats", {}) or {}
        drawdown = result_payload.get("drawdown", {}) or {}
        rows.append(
            {
                "run_dir": run_dir.name,
                "config": Path(meta_payload.get("cfg_path", "")).name if meta_payload.get("cfg_path") else "-",
                "end_value": result_payload.get("end_value"),
                "net_pnl": trade_stats.get("net_pnl"),
                "closed_trades": trade_stats.get("closed_trades"),
                "max_drawdown_pct": drawdown.get("max_drawdown_pct"),
                "generated_at": meta_payload.get("utc_time", "-"),
            }
        )
    return pd.DataFrame(rows)


def _run_single_backtest(cfg: Dict[str, Any], runs_root: Path, tag: str) -> Tuple[Path, Any, Optional[str]]:
    cfg_path = _write_temp_cfg(cfg)
    run_dir = prepare_run_dir(runs_root, tag=tag)

    log_path = run_dir / "run.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")],
        force=True,
    )

    try:
        result = run_engine(cfg, cfg_path)
        write_result(run_dir, cfg, cfg_path, result, project_root=_project_root())
        return run_dir, result, None
    except Exception as e:  # pragma: no cover
        return run_dir, None, str(e)


def _render_header() -> None:
    st.set_page_config(page_title="MT4风格回测工作台", layout="wide")
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #0b1220 0%, #101826 100%);
        }
        .main-title {
            color: #e6edf7;
            font-size: 30px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        .sub-title {
            color: #94a3b8;
            font-size: 14px;
            margin-bottom: 16px;
        }
        .panel {
            background: rgba(15, 23, 42, 0.88);
            border: 1px solid rgba(148, 163, 184, 0.25);
            border-radius: 10px;
            padding: 14px 16px;
            margin-bottom: 12px;
        }
        .mt4-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            background: #1d4ed8;
            color: white;
            font-size: 12px;
            margin-right: 8px;
        }
        .hint {
            color: #cbd5e1;
            font-size: 13px;
        }
        .step-row {
            display: flex;
            gap: 10px;
            margin-bottom: 10px;
            flex-wrap: wrap;
        }
        .step-chip {
            background: rgba(30, 41, 59, 0.95);
            border: 1px solid rgba(59, 130, 246, 0.25);
            color: #dbeafe;
            border-radius: 8px;
            padding: 8px 10px;
            font-size: 13px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="main-title">MT4风格回测工作台</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-title">给非程序员使用：选模板 → 改参数 → 点“开始回测” → 直接查看资金曲线、交易、日志和历史任务。</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="step-row">'
        '<div class="step-chip">1. 选择策略模板</div>'
        '<div class="step-chip">2. 设置账户/数据/参数</div>'
        '<div class="step-chip">3. 开始回测或参数优化</div>'
        '<div class="step-chip">4. 在右侧查看结果与历史</div>'
        '</div>',
        unsafe_allow_html=True,
    )


def _render_top_status(cfg: Dict[str, Any], runs_root: Path, selected_template: Path) -> None:
    strategy_name = (cfg.get("strategy", {}) or {}).get("name", "-")
    data_count = len(cfg.get("data", []) or [])
    resample_count = len(cfg.get("resample", []) or [])
    engine_name = (cfg.get("engine", {}) or {}).get("name", "backtrader")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("模板", selected_template.name)
    c2.metric("策略", strategy_name)
    c3.metric("数据流", f"{data_count} + {resample_count}")
    c4.metric("引擎 / 输出", f"{engine_name}", str(runs_root))


def _render_strategy_editor(cfg: Dict[str, Any]) -> None:
    strategy_cfg = cfg.setdefault("strategy", {})
    strategy_options = _list_strategy_names()
    current_name = str(strategy_cfg.get("name", strategy_options[0] if strategy_options else "cta_trend"))
    if current_name not in strategy_options and current_name:
        strategy_options = strategy_options + [current_name]
    strategy_cfg["name"] = st.selectbox(
        "EA / 策略",
        options=strategy_options or [current_name],
        index=(strategy_options or [current_name]).index(current_name),
        help="类似 MT4 的 Expert Advisor 选择。",
    )

    params = strategy_cfg.setdefault("params", {})
    if not params:
        st.info("当前模板没有 strategy.params，可在高级 JSON 中手动补充。")
        return

    st.caption("策略参数")
    for key in sorted(params.keys()):
        val = params[key]
        if isinstance(val, bool):
            params[key] = st.checkbox(key, value=val)
        elif isinstance(val, int) and not isinstance(val, bool):
            params[key] = int(st.number_input(key, value=int(val), step=1))
        elif isinstance(val, float):
            params[key] = float(st.number_input(key, value=float(val), step=0.1, format="%.4f" if abs(val) < 1 else "%.2f"))
        else:
            params[key] = st.text_input(key, value=str(val))


def _render_broker_editor(cfg: Dict[str, Any]) -> None:
    engine_cfg = cfg.setdefault("engine", {})
    broker = cfg.setdefault("broker", {})
    commission_default = cfg.setdefault("commission_default", {})

    engine_name = str(engine_cfg.get("name", ENGINE_OPTIONS[0])).lower()
    if engine_name not in ENGINE_OPTIONS:
        engine_name = ENGINE_OPTIONS[0]
    engine_cfg["name"] = st.selectbox("回测引擎", options=ENGINE_OPTIONS, index=ENGINE_OPTIONS.index(engine_name))

    col1, col2 = st.columns(2)
    broker["starting_cash"] = float(
        col1.number_input("初始资金", min_value=1.0, value=float(broker.get("starting_cash", 100000.0)), step=10000.0)
    )
    broker["slip_perc"] = float(
        col2.number_input("滑点比例", min_value=0.0, value=float(broker.get("slip_perc", 0.0)), step=0.0001, format="%.4f")
    )

    col3, col4 = st.columns(2)
    account_mode = str(broker.get("account_mode", "cash")).lower()
    if account_mode not in ACCOUNT_MODE_OPTIONS:
        account_mode = "cash"
    broker["account_mode"] = col3.selectbox("账户模式", options=ACCOUNT_MODE_OPTIONS, index=ACCOUNT_MODE_OPTIONS.index(account_mode))
    broker["coc"] = col4.checkbox("收盘成交(coc)", value=bool(broker.get("coc", False)))

    with st.expander("手续费 / 保证金默认设置", expanded=False):
        c1, c2 = st.columns(2)
        commission_default["commission"] = float(
            c1.number_input("默认手续费", min_value=0.0, value=float(commission_default.get("commission", 0.0005) or 0.0), step=0.0001, format="%.4f")
        )
        commission_default["mult"] = float(
            c2.number_input("默认合约乘数", min_value=0.0, value=float(commission_default.get("mult", 1.0) or 1.0), step=1.0)
        )
        c3, c4 = st.columns(2)
        commission_default["margin"] = float(
            c3.number_input("默认保证金", min_value=0.0, value=float(commission_default.get("margin", 0.0) or 0.0), step=100.0)
        )
        margin_rate = commission_default.get("margin_rate", None)
        margin_rate_text = "" if margin_rate is None else str(margin_rate)
        commission_default["margin_rate"] = _coerce_text_value(c4.text_input("默认保证金率(可空)", value=margin_rate_text))
        commtype = str(commission_default.get("commtype", "perc") or "perc")
        commission_default["commtype"] = st.selectbox("手续费类型", options=["perc", "fixed"], index=0 if commtype == "perc" else 1)


def _render_data_editor(cfg: Dict[str, Any]) -> None:
    data_cfg = cfg.setdefault("data", [])
    if not data_cfg:
        data_cfg.append({"name": "data_1", "symbol": "data_1", "role": "exec", "source": "csv", "csv": "", "timeframe": "days", "compression": 1})

    st.caption("主数据源")
    for i, item in enumerate(data_cfg):
        with st.expander(f"数据源 #{i + 1}: {item.get('name', f'data_{i + 1}')}", expanded=(i == 0)):
            c1, c2 = st.columns(2)
            item["name"] = c1.text_input(f"名称 #{i + 1}", value=str(item.get("name", f"data_{i + 1}")), key=f"data_name_{i}")
            item["symbol"] = c2.text_input(f"交易品种 #{i + 1}", value=str(item.get("symbol", item.get("name", ""))), key=f"data_symbol_{i}")

            c3, c4, c5 = st.columns(3)
            source_value = str(item.get("source", "csv")).lower()
            if source_value not in DATA_SOURCE_OPTIONS:
                source_value = "csv"
            role_value = str(item.get("role", "exec")).lower()
            if role_value not in ROLE_OPTIONS:
                role_value = "exec"
            tf_value = str(item.get("timeframe", "days")).lower()
            if tf_value not in TIMEFRAME_OPTIONS:
                tf_value = "days"
            item["source"] = c3.selectbox("来源", options=DATA_SOURCE_OPTIONS, index=DATA_SOURCE_OPTIONS.index(source_value), key=f"data_source_{i}")
            item["role"] = c4.selectbox("角色", options=ROLE_OPTIONS, index=ROLE_OPTIONS.index(role_value), key=f"data_role_{i}")
            item["timeframe"] = c5.selectbox("周期", options=TIMEFRAME_OPTIONS, index=TIMEFRAME_OPTIONS.index(tf_value), key=f"data_tf_{i}")
            item["compression"] = int(st.number_input(f"周期压缩 #{i + 1}", min_value=1, value=int(item.get("compression", 1) or 1), step=1, key=f"data_comp_{i}"))

            if item["source"] == "csv":
                item["csv"] = st.text_input(f"CSV 路径 #{i + 1}", value=str(item.get("csv", "")), key=f"data_csv_{i}")
                if item.get("csv"):
                    csv_abs = (_project_root() / str(item.get("csv"))).resolve()
                    st.caption(f"实际路径: {csv_abs}")
            elif item["source"] == "tushare":
                c6, c7 = st.columns(2)
                item["ts_code"] = c6.text_input(f"ts_code #{i + 1}", value=str(item.get("ts_code", "")), key=f"data_ts_code_{i}")
                item["cache_csv"] = c7.text_input(f"缓存CSV #{i + 1}", value=str(item.get("cache_csv", item.get("csv", ""))), key=f"data_cache_csv_{i}")
            elif item["source"] == "postgres":
                c8, c9 = st.columns(2)
                item["code"] = c8.text_input(f"数据库 code #{i + 1}", value=str(item.get("code", "")), key=f"data_code_{i}")
                item["start"] = c9.text_input(f"开始时间 #{i + 1}", value=str(item.get("start", "")), key=f"data_start_{i}")
                item["end"] = st.text_input(f"结束时间 #{i + 1}", value=str(item.get("end", "")), key=f"data_end_{i}")


def _render_resample_editor(cfg: Dict[str, Any]) -> None:
    resample_cfg = cfg.setdefault("resample", [])
    if not resample_cfg:
        st.caption("当前模板没有重采样数据。若策略不需要多周期信号，可忽略。")
        return

    st.caption("重采样 / 信号周期")
    for i, item in enumerate(resample_cfg):
        with st.expander(f"重采样 #{i + 1}: {item.get('name', f'resample_{i + 1}')}", expanded=(i == 0)):
            c1, c2, c3 = st.columns(3)
            item["source"] = c1.text_input(f"来源数据 #{i + 1}", value=str(item.get("source", "")), key=f"res_source_{i}")
            item["name"] = c2.text_input(f"新名称 #{i + 1}", value=str(item.get("name", "")), key=f"res_name_{i}")
            item["symbol"] = c3.text_input(f"品种 #{i + 1}", value=str(item.get("symbol", "")), key=f"res_symbol_{i}")
            c4, c5, c6 = st.columns(3)
            role_value = str(item.get("role", "signal")).lower()
            if role_value not in ROLE_OPTIONS:
                role_value = "signal"
            tf_value = str(item.get("timeframe", "days")).lower()
            if tf_value not in TIMEFRAME_OPTIONS:
                tf_value = "days"
            item["role"] = c4.selectbox("角色", options=ROLE_OPTIONS, index=ROLE_OPTIONS.index(role_value), key=f"res_role_{i}")
            item["timeframe"] = c5.selectbox("周期", options=TIMEFRAME_OPTIONS, index=TIMEFRAME_OPTIONS.index(tf_value), key=f"res_tf_{i}")
            item["compression"] = int(c6.number_input("压缩", min_value=1, value=int(item.get("compression", 1) or 1), step=1, key=f"res_comp_{i}"))


def _render_symbol_specs_editor(cfg: Dict[str, Any]) -> None:
    symbols = cfg.setdefault("symbols", {})
    if not symbols:
        st.caption("当前模板没有 symbols 合约规则，可忽略。")
        return

    st.caption("品种规则 / 合约规格")
    for symbol_name in list(symbols.keys()):
        spec = symbols.setdefault(symbol_name, {})
        with st.expander(f"{symbol_name}", expanded=False):
            c1, c2 = st.columns(2)
            spec["tick_size"] = float(c1.number_input(f"tick_size::{symbol_name}", min_value=0.0, value=float(spec.get("tick_size", 0.0) or 0.0), step=0.1))
            spec["price_precision"] = int(c2.number_input(f"price_precision::{symbol_name}", min_value=0, value=int(spec.get("price_precision", 0) or 0), step=1))
            c3, c4 = st.columns(2)
            spec["size_step"] = float(c3.number_input(f"size_step::{symbol_name}", min_value=0.0, value=float(spec.get("size_step", 1.0) or 1.0), step=1.0))
            spec["min_size"] = float(c4.number_input(f"min_size::{symbol_name}", min_value=0.0, value=float(spec.get("min_size", 1.0) or 1.0), step=1.0))
            c5, c6 = st.columns(2)
            spec["mult"] = float(c5.number_input(f"mult::{symbol_name}", min_value=0.0, value=float(spec.get("mult", 1.0) or 1.0), step=1.0))
            spec["commission"] = float(c6.number_input(f"commission::{symbol_name}", min_value=0.0, value=float(spec.get("commission", 0.0) or 0.0), step=0.0001, format="%.4f"))
            c7, c8 = st.columns(2)
            spec["margin"] = float(c7.number_input(f"margin::{symbol_name}", min_value=0.0, value=float(spec.get("margin", 0.0) or 0.0), step=100.0))
            margin_rate_val = spec.get("margin_rate", None)
            spec["margin_rate"] = _coerce_text_value(c8.text_input(f"margin_rate::{symbol_name}", value="" if margin_rate_val is None else str(margin_rate_val)))
            commtype = str(spec.get("commtype", "perc") or "perc")
            spec["commtype"] = st.selectbox(f"commtype::{symbol_name}", options=["perc", "fixed"], index=0 if commtype == "perc" else 1)


def _render_output_editor(cfg: Dict[str, Any], default_runs_root: Path) -> Tuple[Path, str]:
    output_cfg = cfg.setdefault("output", {})
    report_cfg = cfg.setdefault("report", {})

    runs_root = Path(st.text_input("结果输出目录", value=str(default_runs_root))).expanduser().resolve()
    tag_default = str(output_cfg.get("tag") or f"mt4_{datetime.now().strftime('%H%M%S')}")
    tag = st.text_input("任务标签", value=tag_default)

    with st.expander("报告输出设置", expanded=False):
        report_cfg["html"] = st.checkbox("生成 HTML 报告", value=bool(report_cfg.get("html", True)))
        report_cfg["title"] = st.text_input("报告标题", value=str(report_cfg.get("title", "回测报告")))
        report_cfg["out_folder"] = st.text_input("报告目录名", value=str(report_cfg.get("out_folder", "report_html")))
        report_cfg["asset_dir"] = st.text_input("外部资源目录(可空)", value="" if report_cfg.get("asset_dir") is None else str(report_cfg.get("asset_dir")))
        if report_cfg.get("asset_dir") == "":
            report_cfg["asset_dir"] = None

    output_cfg["tag"] = tag
    return runs_root, tag


def _render_advanced_json_editor(cfg: Dict[str, Any]) -> Dict[str, Any]:
    with st.expander("高级配置(JSON) - 专业用户", expanded=False):
        raw_json = st.text_area(
            "可直接编辑完整配置（JSON）",
            value=json.dumps(cfg, ensure_ascii=False, indent=2),
            height=360,
        )
        try:
            return json.loads(raw_json)
        except Exception:
            st.warning("JSON 格式有误，将继续使用上方图形界面内容。")
    return cfg


def _render_metrics(result: Any) -> None:
    m = _collect_result_metrics(result)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("初始资金", _fmt_num(m["start_value"]))
    c2.metric("结束资金", _fmt_num(m["end_value"]), _fmt_pct(m["total_return_pct"]))
    c3.metric("最大回撤", _fmt_pct(m["max_drawdown_pct"]))
    c4.metric("已平仓笔数", str(m["closed_trades"]))
    c5.metric("已实现盈亏", _fmt_num(m["realized_pnl"]))
    c6.metric("浮动盈亏", _fmt_num(m["floating_pnl"]))
    st.caption(f"净收益: {_fmt_num(m['net_pnl'])} | 最大资金回撤: {_fmt_num(m['max_moneydown'])} | 胜率: {_fmt_pct(m['win_rate'])}")


def _render_equity_chart(eq: pd.DataFrame) -> None:
    if eq.empty or "datetime" not in eq.columns:
        st.info("暂无权益曲线数据")
        return
    eq2 = eq.copy()
    eq2["datetime"] = pd.to_datetime(eq2["datetime"])
    eq2 = eq2.sort_values("datetime").set_index("datetime")
    cols = [c for c in ["value", "cash", "available"] if c in eq2.columns]
    if cols:
        st.line_chart(eq2[cols], height=360)
    st.dataframe(eq.reset_index(drop=True), use_container_width=True, height=240)


def _render_result_tabs(run_dir: Optional[Path], result: Any, opt_df: Optional[pd.DataFrame] = None) -> None:
    eq = pd.DataFrame(getattr(result, "equity_curve", []) or []) if result is not None else pd.DataFrame()
    trades = pd.DataFrame(getattr(result, "trades", []) or []) if result is not None else pd.DataFrame()
    orders = pd.DataFrame(getattr(result, "orders", []) or []) if result is not None else pd.DataFrame()
    fills = pd.DataFrame(getattr(result, "fills", []) or []) if result is not None else pd.DataFrame()
    snapshots = pd.DataFrame(getattr(result, "snapshots", []) or []) if result is not None else pd.DataFrame()
    open_positions = pd.DataFrame(getattr(result, "open_positions", []) or []) if result is not None else pd.DataFrame()

    tab_names = ["概览", "权益图", "交易", "订单/成交", "账户快照", "持仓", "日志", "导出文件"]
    if opt_df is not None:
        tab_names.insert(1, "优化结果")

    tabs = st.tabs(tab_names)
    tab_map = dict(zip(tab_names, tabs))

    with tab_map["概览"]:
        if result is None:
            st.info("还没有运行结果。")
        else:
            _render_metrics(result)
            c1, c2 = st.columns(2)
            c1.markdown("**运行目录**")
            c1.code(str(run_dir) if run_dir else "-")
            report_dir = (run_dir / str((getattr(result, 'report', None) or ''))) if False else None
            html_report = run_dir / "report_html" / "index.html" if run_dir else None
            c2.markdown("**HTML 报告**")
            c2.code(str(html_report) if html_report and html_report.exists() else "未生成或路径不存在")

    if opt_df is not None and "优化结果" in tab_map:
        with tab_map["优化结果"]:
            if opt_df.empty:
                st.info("暂无优化结果")
            else:
                view = opt_df.copy()
                sort_col = "total_return_pct" if "total_return_pct" in view.columns else view.columns[-1]
                if sort_col in view.columns:
                    view = view.sort_values(sort_col, ascending=False, na_position="last")
                st.dataframe(view, use_container_width=True, height=520)

    with tab_map["权益图"]:
        _render_equity_chart(eq)

    with tab_map["交易"]:
        if trades.empty:
            st.info("暂无交易数据")
        else:
            st.dataframe(trades, use_container_width=True, height=460)

    with tab_map["订单/成交"]:
        sub1, sub2 = st.tabs(["订单", "成交"])
        with sub1:
            if orders.empty:
                st.info("暂无订单数据")
            else:
                st.dataframe(orders, use_container_width=True, height=420)
        with sub2:
            if fills.empty:
                st.info("暂无成交数据")
            else:
                st.dataframe(fills, use_container_width=True, height=420)

    with tab_map["账户快照"]:
        if snapshots.empty:
            st.info("暂无账户快照")
        else:
            st.dataframe(snapshots, use_container_width=True, height=460)

    with tab_map["持仓"]:
        if open_positions.empty:
            st.info("当前无未平仓持仓")
        else:
            st.dataframe(open_positions, use_container_width=True, height=320)

    with tab_map["日志"]:
        if run_dir is None:
            st.info("还没有日志")
        else:
            log_path = run_dir / "run.log"
            if log_path.exists():
                st.code(_read_text_tail(log_path, max_chars=12000))
            else:
                st.info("未找到日志文件")

    with tab_map["导出文件"]:
        if run_dir is None or not run_dir.exists():
            st.info("还没有导出文件")
        else:
            files = [p for p in sorted(run_dir.rglob("*")) if p.is_file()]
            rows = [{"文件": str(p.relative_to(run_dir)), "大小(KB)": round(p.stat().st_size / 1024, 2)} for p in files]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, height=420)
            result_json = run_dir / "result.json"
            config_yaml = run_dir / "config.yaml"
            if result_json.exists():
                st.download_button("下载 result.json", data=result_json.read_bytes(), file_name=result_json.name)
            if config_yaml.exists():
                st.download_button("下载 config.yaml", data=config_yaml.read_bytes(), file_name=config_yaml.name)


def _render_history_panel(runs_root: Path) -> None:
    st.markdown("### 历史任务")
    history_df = _build_history_table(runs_root)
    if history_df.empty:
        st.info("当前输出目录下还没有历史回测任务。")
        return

    st.dataframe(history_df, use_container_width=True, height=260)
    run_names = history_df["run_dir"].tolist()
    selected_run_name = st.selectbox("查看历史任务详情", options=run_names, index=0)
    run_dir = runs_root / selected_run_name

    col1, col2 = st.columns(2)
    col1.code(str(run_dir))
    log_path = run_dir / "run.log"
    col2.code(str(log_path) if log_path.exists() else "无日志")

    with st.expander("历史任务日志尾部", expanded=False):
        if log_path.exists():
            st.code(_read_text_tail(log_path))
        else:
            st.info("没有日志文件")


def _store_last_run(run_dir: Optional[Path], result: Any, opt_df: Optional[pd.DataFrame], cfg: Dict[str, Any]) -> None:
    st.session_state["last_run_dir"] = str(run_dir) if run_dir else None
    st.session_state["last_result_obj"] = result
    st.session_state["last_opt_df"] = opt_df
    st.session_state["last_cfg_json"] = json.dumps(cfg, ensure_ascii=False)


def _restore_last_run() -> Tuple[Optional[Path], Any, Optional[pd.DataFrame], Dict[str, Any]]:
    run_dir_str = st.session_state.get("last_run_dir")
    run_dir = Path(run_dir_str) if run_dir_str else None
    result = st.session_state.get("last_result_obj")
    opt_df = st.session_state.get("last_opt_df")
    cfg_json = st.session_state.get("last_cfg_json")
    cfg = json.loads(cfg_json) if cfg_json else {}
    return run_dir, result, opt_df, cfg


def main() -> None:
    _render_header()

    cfg_files = _list_config_files()
    if not cfg_files:
        st.error("未找到配置模板，请检查 my_bt_lab/app/configs 目录。")
        return

    left, right = st.columns([1.05, 1.45], gap="large")
    default_runs_root = _project_root() / "runs"

    with left:
        st.markdown("### 策略测试器设置")
        selected = st.selectbox("模板配置", options=cfg_files, format_func=lambda p: p.name)
        cfg_base = _read_yaml(selected)
        cfg = copy.deepcopy(cfg_base)
        _render_top_status(cfg, default_runs_root.resolve(), selected)

        with st.form("mt4_workbench_form", clear_on_submit=False):
            mode = st.radio("运行模式", options=["单次回测", "参数优化"], horizontal=True)

            with st.expander("1) 策略设置", expanded=True):
                _render_strategy_editor(cfg)

            with st.expander("2) 账户与回测设置", expanded=True):
                _render_broker_editor(cfg)

            with st.expander("3) 数据设置", expanded=True):
                _render_data_editor(cfg)
                _render_resample_editor(cfg)

            with st.expander("4) 品种规则", expanded=False):
                _render_symbol_specs_editor(cfg)

            grid_text = ""
            if mode == "参数优化":
                with st.expander("5) 参数优化网格", expanded=True):
                    grid_text = st.text_area(
                        "每行一个参数，格式: 参数名=值1,值2,...",
                        value=_grid_text_from_cfg(cfg) or "fast=5,10,20\nslow=20,30,60",
                        height=140,
                    )

            with st.expander("6) 输出设置", expanded=True):
                runs_root, tag = _render_output_editor(cfg, default_runs_root=default_runs_root.resolve())

            cfg = _render_advanced_json_editor(cfg)
            run_clicked = st.form_submit_button("开始回测", type="primary", use_container_width=True)

    with right:
        st.markdown("### 结果 / 历史 / 日志")
        last_run_dir, last_result, last_opt_df, _ = _restore_last_run()

        if run_clicked and mode == "单次回测":
            with st.spinner("正在运行回测，请稍候..."):
                run_dir, result, err = _run_single_backtest(cfg, runs_root, tag)
            if err:
                st.error(f"运行失败: {err}")
                st.caption(f"日志目录: {run_dir}")
                _store_last_run(run_dir, None, None, cfg)
            else:
                st.success(f"回测完成，结果目录: {run_dir}")
                _store_last_run(run_dir, result, None, cfg)
                last_run_dir, last_result, last_opt_df, _ = _restore_last_run()

        elif run_clicked and mode == "参数优化":
            grid = _parse_grid_text(grid_text)
            if not grid:
                st.error("参数网格为空或格式不正确。")
            else:
                combos = list(_iter_param_combinations(grid))
                st.info(f"参数组合数: {len(combos)}")
                rows: List[Dict[str, Any]] = []
                progress = st.progress(0.0)
                msg = st.empty()
                last_good_result = None
                last_good_run_dir = None
                for idx, combo in enumerate(combos, start=1):
                    msg.write(f"运行中 {idx}/{len(combos)}: {combo}")
                    cfg_i = copy.deepcopy(cfg)
                    cfg_i.setdefault("strategy", {}).setdefault("params", {}).update(combo)
                    run_dir_i, result_i, err = _run_single_backtest(cfg_i, runs_root, f"{tag}_{idx:03d}")
                    if err:
                        row = dict(combo)
                        row["error"] = err
                    else:
                        row = dict(combo)
                        row.update(_collect_result_metrics(result_i))
                        last_good_result = result_i
                        last_good_run_dir = run_dir_i
                    rows.append(row)
                    progress.progress(idx / len(combos))
                opt_df = pd.DataFrame(rows)
                out_csv = runs_root / f"opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{tag}.csv"
                out_csv.parent.mkdir(parents=True, exist_ok=True)
                opt_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
                st.success(f"参数优化完成，汇总文件已保存: {out_csv}")
                _store_last_run(last_good_run_dir, last_good_result, opt_df, cfg)
                last_run_dir, last_result, last_opt_df, _ = _restore_last_run()

        if last_result is not None or last_opt_df is not None:
            _render_result_tabs(last_run_dir, last_result, last_opt_df)
        else:
            st.info("请先在左侧设置参数，然后点击“开始回测”。")

        st.markdown("---")
        _render_history_panel(runs_root=runs_root)


if __name__ == "__main__":
    main()
