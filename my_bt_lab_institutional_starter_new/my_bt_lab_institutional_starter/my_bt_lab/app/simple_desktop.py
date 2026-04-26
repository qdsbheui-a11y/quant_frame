from __future__ import annotations

import copy
import logging
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

try:
    from PySide6.QtCore import QThread, Qt, Signal
    from PySide6.QtGui import QColor
    from PySide6.QtWidgets import (
        QApplication,
        QComboBox,
        QDateEdit,
        QDoubleSpinBox,
        QFormLayout,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QListView,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QPlainTextEdit,
        QTableWidget,
        QTableWidgetItem,
        QTabWidget,
        QVBoxLayout,
        QWidget,
    )

    try:
        from PySide6.QtCharts import QChart, QChartView, QLineSeries, QValueAxis

        QT_CHARTS_AVAILABLE = True
    except Exception:
        QT_CHARTS_AVAILABLE = False

    QT_AVAILABLE = True
    QT_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover
    QT_AVAILABLE = False
    QT_CHARTS_AVAILABLE = False
    QT_IMPORT_ERROR = exc


APP_TITLE = "量化回测助手 - 普通用户版"


FALLBACK_DATA_PRESETS: Dict[str, Dict[str, Any]] = {
    "BTCUSDT tick 数据库回测 - 1分钟K": {
        "preset_type": "db_tick",
        "code": "BTCUSDT",
        "period": "1分钟",
        "start": "2026-04-10",
        "end": "2026-04-10",
        "hint": "读取数据库 tick_data，并在数据库端聚合为 1分钟K。",
    },
    "BTCUSDT tick 数据库回测 - 5分钟K": {
        "preset_type": "db_tick",
        "code": "BTCUSDT",
        "period": "5分钟",
        "start": "2026-04-10",
        "end": "2026-04-10",
        "hint": "读取数据库 tick_data，并在数据库端聚合为 5分钟K。",
    },
    "自定义数据库 tick 回测": {
        "preset_type": "db_tick",
        "code": "BTCUSDT",
        "period": "1分钟",
        "start": "2026-04-10",
        "end": "2026-04-10",
        "hint": "自定义品种代码和日期；底层使用 PostgreSQL tick_data 预设。",
    },
}


RISK_PRESETS: Dict[str, Optional[float]] = {
    "保守 - 单笔风险 0.005%": 0.00005,
    "平衡 - 单笔风险 0.01%": 0.0001,
    "积极 - 单笔风险 0.05%": 0.0005,
    "自定义": None,
}


PERIOD_PRESETS: Dict[str, Tuple[str, int]] = {
    "Tick原始数据（自动转1分钟K）": ("minutes", 1),
    "1分钟": ("minutes", 1),
    "5分钟": ("minutes", 5),
    "15分钟": ("minutes", 15),
    "30分钟": ("minutes", 30),
    "60分钟": ("minutes", 60),
}


STRATEGY_LABELS = {
    "cta_trend": "CTA 趋势策略（均线 + ATR）",
    "donchian_daily_mtf": "Donchian 突破策略（高级）",
}


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def configs_root() -> Path:
    return project_root() / "my_bt_lab" / "app" / "configs"


def _date_only(value: Any, default: str = "2026-04-10") -> str:
    text = str(value or default).strip()
    if not text:
        return default
    return text.split()[0]


def _format_date(qdate) -> str:
    return qdate.toString("yyyy-MM-dd")


def _safe_read_text_tail(path: Path, lines: int = 200) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:])


def _set_date(widget: QDateEdit, value: Any) -> None:
    widget.setDate(datetime.strptime(_date_only(value), "%Y-%m-%d").date())


def _period_label(timeframe: Any, compression: Any) -> str:
    tf = str(timeframe or "minutes").strip().lower()
    try:
        comp = int(compression or 1)
    except Exception:
        comp = 1

    if tf in {"minutes", "minute", "min", "m"}:
        if comp == 1:
            return "1分钟"
        label = f"{comp}分钟"
        return label if label in PERIOD_PRESETS else "1分钟"

    return "1分钟"


def _norm_source(source: Any) -> str:
    text = str(source or "csv").strip().lower()
    if text in {"db", "postgresql"}:
        return "postgres"
    if text in {"xlsx", "xls"}:
        return "excel"
    return text or "csv"


def _resolve_project_path(path_value: Any) -> Optional[Path]:
    raw = str(path_value or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    return (project_root() / path).resolve()


def _infer_ts_code_from_path(path_value: Any) -> Optional[str]:
    import re

    stem = Path(str(path_value or "")).stem
    match = re.search(r"(\d{6})[_-]([A-Za-z]{2})", stem)
    if match:
        return f"{match.group(1)}.{match.group(2).upper()}"
    return None


def _csv_datetime_issues(csv_path: Path, item: Dict[str, Any]) -> List[str]:
    """Lightweight CSV schema/date-format check for the template dropdown.

    This intentionally reads only a small sample. It is not a full data load.
    """
    if not csv_path.exists():
        return []

    try:
        import pandas as pd
    except Exception:
        return []

    read_kwargs: Dict[str, Any] = {
        "sep": item.get("sep", ","),
        "encoding": item.get("encoding", "utf-8"),
        "nrows": 200,
    }
    if item.get("header_row") is not None:
        read_kwargs["header"] = item.get("header_row")
    if item.get("skiprows") is not None:
        read_kwargs["skiprows"] = item.get("skiprows")

    try:
        sample = pd.read_csv(csv_path, **read_kwargs)
    except Exception as exc:
        return [f"CSV读取失败：{exc}"]

    if sample.empty:
        return ["CSV文件为空或前200行无有效数据。"]

    sample.columns = [str(col).strip().lower() for col in sample.columns]
    columns = set(sample.columns)

    schema = item.get("schema") if isinstance(item.get("schema"), dict) else {}
    dt_col = (
        item.get("datetime_col")
        or schema.get("datetime")
        or item.get("date_col")
        or None
    )
    if dt_col:
        dt_col = str(dt_col).strip().lower()
    else:
        for candidate in ["datetime", "date", "trade_date", "trade_time", "time", "timestamp", "dt"]:
            if candidate in columns:
                dt_col = candidate
                break

    if not dt_col:
        return [f"找不到日期列。当前列={list(sample.columns)}。"]

    if dt_col not in columns:
        return [f"日期列配置为 {dt_col}，但CSV中不存在。当前列={list(sample.columns)}。"]

    datetime_format = item.get("datetime_format")
    raw_values = sample[dt_col].dropna().astype(str).str.strip()
    if raw_values.empty:
        return [f"日期列 {dt_col} 为空。"]

    try:
        if datetime_format:
            parsed = pd.to_datetime(raw_values, format=str(datetime_format), errors="coerce")
        else:
            parsed = pd.to_datetime(raw_values, errors="coerce")
    except Exception as exc:
        return [f"日期格式解析异常：{exc}"]

    bad_count = int(parsed.isna().sum())
    if bad_count == len(parsed):
        fmt = f"，datetime_format={datetime_format}" if datetime_format else ""
        return [f"日期解析全部失败：列={dt_col}{fmt}。请检查 YAML 的 schema/datetime_format。"]

    return []


def _template_precheck(cfg: Dict[str, Any]) -> Tuple[str, List[str], bool]:
    """Return (status, issues, can_run) for one YAML template.

    Status is meant to be shown directly in the dropdown.
    """
    data_items = cfg.get("data", []) or []
    if not data_items:
        return "不可运行", ["模板没有 data 配置。"], False

    issues: List[str] = []
    warnings: List[str] = []
    tcfg = cfg.get("tushare", {}) or {}
    token_env = str(tcfg.get("token_env") or "TUSHARE_TOKEN")

    for item in data_items:
        source = _norm_source(item.get("source"))
        name = str(item.get("name") or item.get("symbol") or item.get("code") or item.get("ts_code") or "数据源")

        if source == "csv":
            csv_path = _resolve_project_path(item.get("csv") or item.get("cache_csv"))
            if not csv_path:
                issues.append(f"{name}: 未配置CSV文件路径。")
                continue

            if not csv_path.exists():
                ts_code = item.get("ts_code") or _infer_ts_code_from_path(csv_path)
                can_auto_tushare = bool(tcfg and ts_code)
                if "mock" in str(csv_path).lower():
                    issues.append(f"{name}: 缺少mock文件 {csv_path}")
                elif can_auto_tushare:
                    if not os.environ.get(token_env):
                        issues.append(f"{name}: 缺少本地CSV，可尝试Tushare生成缓存，但未设置 {token_env}。")
                    else:
                        warnings.append(f"{name}: 缺少本地CSV，将尝试通过Tushare生成缓存。")
                else:
                    issues.append(f"{name}: 缺少本地CSV文件 {csv_path}")
                continue

            date_issues = _csv_datetime_issues(csv_path, item)
            for issue in date_issues:
                issues.append(f"{name}: {issue}")

        elif source == "excel":
            excel_path = _resolve_project_path(item.get("excel") or item.get("csv"))
            if not excel_path or not excel_path.exists():
                issues.append(f"{name}: 缺少Excel文件 {excel_path}")

        elif source == "tushare":
            cache_path = _resolve_project_path(item.get("cache_csv"))
            refresh = bool(item.get("refresh", False))
            if cache_path and cache_path.exists() and not refresh:
                continue
            if not os.environ.get(token_env):
                issues.append(f"{name}: 需要设置 {token_env} 才能从Tushare拉取数据。")
            else:
                warnings.append(f"{name}: 将尝试从Tushare拉取数据；请确认账号有对应数据权限。")

        elif source == "postgres":
            # PostgreSQL templates can be run from the UI because SSH password can be entered there.
            continue

        else:
            issues.append(f"{name}: 暂不支持的数据源 source={source}。")

    if issues:
        first = "需修复日期格式"
        text = " ".join(issues)
        if "Tushare" in text or "TUSHARE" in text:
            first = "需要TUSHARE_TOKEN"
        if "缺少mock" in text:
            first = "缺少mock文件"
        elif "缺少本地CSV" in text or "缺少CSV" in text:
            first = "缺少本地CSV"
        elif "日期" in text or "datetime" in text:
            first = "需修复日期格式"
        elif "暂不支持" in text:
            first = "需高级模式"
        return first, issues, False

    if warnings:
        return "可尝试", warnings, True

    return "可运行", [], True


def _friendly_exception_message(detail: str) -> str:
    text = str(detail or "")

    if "datetime 解析失败" in text:
        return "日期解析失败：请检查所选 YAML 的日期列映射 schema.datetime 和 datetime_format，或换用已标记为“可运行”的模板。"
    if "找不到数据文件" in text:
        return "缺少本地数据文件：该模板依赖本机CSV/mock文件。请补齐文件，或换用数据库/Tushare模板。"
    if "无法自动从Tushare生成缓存" in text:
        return "无法自动从Tushare生成缓存：请检查 TUSHARE_TOKEN、ts_code、数据权限和网络连接。"
    if "TUSHARE_TOKEN" in text or "token" in text.lower():
        return "Tushare配置不完整：请先设置 TUSHARE_TOKEN，并确认账号具备对应数据权限。"
    if "未找到数据" in text:
        return "数据库中没有查到符合条件的数据：请检查品种代码、开始/结束日期、表名和数据源。"
    if "No module named" in text:
        return "运行环境缺少依赖模块：请确认已在正确项目目录启动，并安装所需依赖。"
    if "unexpected keyword argument" in text:
        return "策略参数不匹配：该策略暂未适配普通版参数面板，请使用原始YAML或高级界面调整参数。"

    return "回测失败。请查看日志页中的技术细节。"


def _write_runtime_cfg(cfg: Dict[str, Any]) -> Path:
    """Write runtime config under app/configs so engines infer project_root correctly."""
    path = configs_root() / "__simple_runtime.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def _load_yaml_presets() -> Dict[str, Dict[str, Any]]:
    presets: Dict[str, Dict[str, Any]] = {}
    root = configs_root()
    if not root.exists():
        return presets

    for path in sorted(root.glob("*.yaml"), key=lambda p: p.name.lower()):
        try:
            cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            label = f"不可读取：{path.name}"
            presets[label] = {
                "preset_type": "yaml_template",
                "template_path": str(path),
                "code": "BTCUSDT",
                "period": "1分钟",
                "start": "2026-04-10",
                "end": "2026-04-10",
                "strategy_name": "cta_trend",
                "base_cfg": {},
                "status": "不可读取",
                "can_run": False,
                "issues": [f"YAML读取失败：{exc}"],
                "hint": f"YAML读取失败：{exc}",
            }
            continue

        data_items = cfg.get("data", []) or []
        first = data_items[0] if data_items else {}
        strategy = cfg.get("strategy", {}) or {}

        source = str(first.get("source") or "").strip().lower()
        table_name = str(first.get("table_name") or "").strip().lower()
        code = str(first.get("code") or first.get("ts_code") or first.get("symbol") or first.get("name") or "BTCUSDT")

        status, issues, can_run = _template_precheck(cfg)
        label = f"{status}：{path.name}"
        hint_parts = [f"模板文件：{path.name}", f"状态：{status}"]
        if issues:
            hint_parts.append("检查结果：" + "；".join(issues[:3]))
        else:
            hint_parts.append("检查结果：未发现阻断性问题。")

        presets[label] = {
            "preset_type": "yaml_template",
            "template_path": str(path),
            "source": source,
            "table_name": table_name,
            "code": code,
            "period": _period_label(first.get("timeframe"), first.get("compression", 1)),
            "start": _date_only(first.get("start") or first.get("start_date")),
            "end": _date_only(first.get("end") or first.get("end_date")),
            "strategy_name": str(strategy.get("name") or "cta_trend"),
            "base_cfg": cfg,
            "status": status,
            "can_run": can_run,
            "issues": issues,
            "hint": "\n".join(hint_parts),
        }

    return presets

def _load_strategy_labels() -> Dict[str, str]:
    try:
        from my_bt_lab.registry.strategy_registry import STRATEGY_REGISTRY

        names = sorted(str(name) for name in STRATEGY_REGISTRY.keys())
    except Exception:
        names = ["cta_trend", "donchian_daily_mtf"]

    labels: Dict[str, str] = {}
    for name in names:
        labels[STRATEGY_LABELS.get(name, name)] = name
    return labels


def _metric_rows(result) -> List[Dict[str, Any]]:
    trade_stats = getattr(result, "trade_stats", {}) or {}
    drawdown = getattr(result, "drawdown", {}) or {}

    start_value = float(getattr(result, "start_value", 0.0) or 0.0)
    end_value = float(getattr(result, "end_value", 0.0) or 0.0)
    net_pnl = float(trade_stats.get("net_pnl", end_value - start_value) or 0.0)
    ret = (end_value / start_value - 1.0) if start_value else 0.0

    return [
        {"指标": "初始资金", "数值": f"{start_value:,.2f}"},
        {"指标": "结束资金", "数值": f"{end_value:,.2f}"},
        {"指标": "净利润", "数值": f"{net_pnl:,.2f}"},
        {"指标": "收益率", "数值": f"{ret:.4%}"},
        {"指标": "已平仓交易", "数值": str(int(trade_stats.get("closed_trades", 0) or 0))},
        {"指标": "最大回撤", "数值": f"{float(drawdown.get('max_drawdown_pct', 0.0) or 0.0):.4f}%"},
    ]


def _strategy_params(
    strategy_name: str,
    initial_cash: float,
    risk_per_trade: float,
    base_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = dict(base_params or {})

    if strategy_name == "donchian_daily_mtf":
        params.setdefault("entry_lookback_days", 20)
        params.setdefault("exit_lookback_days", 10)
        params.setdefault("breakout_add_ticks", 1)
        params.setdefault("entry_mode", "range")
        params.setdefault("atr_period", 20)
        params.setdefault("atr_mult", 2.0)
        params.setdefault("trail_lv1_atr", 2.0)
        params.setdefault("trail_lv2_atr", 5.0)
        params.setdefault("trail_lock_atr", 2.0)
        params["risk_cash"] = float(initial_cash) * float(risk_per_trade)
        params.setdefault("max_positions", 99)
        params.setdefault("min_size", 1)
        return params

    if strategy_name == "cta_trend":
        params.setdefault("fast", 10)
        params.setdefault("slow", 30)
        params.setdefault("atr_period", 14)
        params.setdefault("atr_stop_mult", 2.0)
        params["risk_per_trade"] = risk_per_trade
        params.setdefault("max_positions", 2)
        params.setdefault("min_size", 1)
        return params

    # 对未知策略不要乱塞 fast/slow/risk_per_trade，避免 unexpected keyword argument。
    # 如果 YAML 模板里带了该策略的 params，就保留；否则让策略使用自身默认参数。
    return params


def _default_postgres_cfg() -> Dict[str, Any]:
    return {
        "host": "8.148.188.209",
        "port": 5432,
        "dbname": "quant_lab",
        "user": "postgres",
        "password": "postgres",
        "password_env": "PGPASSWORD",
        "sslmode": "disable",
        "search_path": "public",
        "ssh": {
            "enabled": True,
            "host": "8.148.188.209",
            "port": 22,
            "user": "Administrator",
            "password_env": "SSH_PASSWORD",
            "remote_bind_host": "127.0.0.1",
            "remote_bind_port": 5432,
        },
    }


def _build_db_tick_data_item(code: str, start: str, end: str, timeframe: str, compression: int) -> Dict[str, Any]:
    data_name = f"{code}_tick"
    return {
        "name": data_name,
        "symbol": data_name,
        "source": "postgres",
        "role": "exec",
        "code": code,
        "code_col": "instrument_id",
        "data_type": "tick",
        "table_schema": "public",
        "table_name": "tick_data",
        "timeframe": timeframe,
        "compression": compression,
        "start": start,
        "end": end,
    }


def _patch_data_items(
    data_items: List[Dict[str, Any]],
    *,
    code: str,
    start: str,
    end: str,
    timeframe: str,
    compression: int,
) -> List[Dict[str, Any]]:
    patched: List[Dict[str, Any]] = []

    for item in data_items or []:
        new_item = dict(item)
        source = str(new_item.get("source") or "").strip().lower()
        data_type = str(new_item.get("data_type") or "").strip().lower()
        table_name = str(new_item.get("table_name") or "").strip().lower()

        if source == "postgres" and (data_type == "tick" or table_name == "tick_data"):
            # 数据库 tick 模板按普通版输入覆盖。
            new_item.update(_build_db_tick_data_item(code, start, end, timeframe, compression))
        else:
            # CSV/Tushare/普通 PG 表尽量保留原配置，只覆盖用户能理解的通用字段。
            if "code" in new_item:
                new_item["code"] = code
            if "ts_code" in new_item:
                new_item["ts_code"] = code
            if "symbol" in new_item:
                new_item["symbol"] = code
            if "name" in new_item and not str(new_item.get("name") or "").strip():
                new_item["name"] = code

            new_item["start"] = start
            new_item["end"] = end
            new_item["timeframe"] = timeframe
            new_item["compression"] = compression

        patched.append(new_item)

    return patched


def _build_runtime_config(
    *,
    code: str,
    start: str,
    end: str,
    timeframe: str,
    compression: int,
    initial_cash: float,
    risk_per_trade: float,
    strategy_name: str,
    preset: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    preset = preset or {}
    base_cfg = preset.get("base_cfg") if isinstance(preset.get("base_cfg"), dict) else None
    cfg = copy.deepcopy(base_cfg or {})

    preset_type = str(preset.get("preset_type") or "").strip()

    if preset_type == "db_tick" or not cfg:
        cfg.setdefault("postgres", _default_postgres_cfg())
        cfg["data"] = [_build_db_tick_data_item(code, start, end, timeframe, compression)]
    else:
        # YAML 模板：保留数据源细节，只覆盖普通用户输入。
        data_items = cfg.get("data", []) or []
        cfg["data"] = _patch_data_items(
            data_items,
            code=code,
            start=start,
            end=end,
            timeframe=timeframe,
            compression=compression,
        )
        if "postgres" not in cfg and any((item.get("source") == "postgres") for item in cfg["data"]):
            cfg["postgres"] = _default_postgres_cfg()

    base_strategy = cfg.get("strategy", {}) or {}
    base_params = base_strategy.get("params") if isinstance(base_strategy.get("params"), dict) else {}

    cfg["strategy"] = {
        "name": strategy_name,
        "params": _strategy_params(strategy_name, initial_cash, risk_per_trade, base_params),
    }

    cfg["broker"] = {
        **(cfg.get("broker", {}) or {}),
        "starting_cash": float(initial_cash),
        "account_mode": (cfg.get("broker", {}) or {}).get("account_mode", "cash"),
    }

    cfg["engine"] = {
        **(cfg.get("engine", {}) or {}),
        "name": "backtrader",
        "cash": float(initial_cash),
    }

    cfg.setdefault(
        "commission_default",
        {"commission": 0.0003, "mult": 1, "margin": 0, "commtype": "perc"},
    )

    # 数据库 tick 场景需要 symbol_specs；其他模板保留原 symbols。
    if preset_type == "db_tick" or not cfg.get("symbols"):
        data_name = f"{code}_tick"
        cfg["symbols"] = {
            data_name: {
                "mult": 1,
                "commission": 0.0003,
                "margin": 0,
                "commtype": "perc",
            }
        }

    cfg["output"] = {"tag": "simple_db_backtest"}
    cfg["report"] = {"html": False}
    return cfg


def _chart_points_from_equity(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, float]]:
    points: List[Dict[str, float]] = []
    for idx, row in enumerate(rows or []):
        value = row.get(key)
        if value is None and key == "value":
            value = row.get("dynamic_equity")
        try:
            points.append({"x": float(idx), "y": float(value or 0.0)})
        except Exception:
            continue
    return points


def _drawdown_points(rows: List[Dict[str, Any]]) -> List[Dict[str, float]]:
    points: List[Dict[str, float]] = []
    peak: Optional[float] = None

    for idx, row in enumerate(rows or []):
        value = row.get("value", row.get("dynamic_equity"))
        try:
            equity = float(value or 0.0)
        except Exception:
            continue

        if peak is None or equity > peak:
            peak = equity

        dd = 0.0 if not peak else (equity / peak - 1.0) * 100.0
        points.append({"x": float(idx), "y": dd})

    return points


if QT_AVAILABLE:
    class SimpleBacktestWorker(QThread):
        completed = Signal(dict)
        failed = Signal(dict)
        status = Signal(str)

        def __init__(self, cfg: Dict[str, Any], runs_root: Path, parent=None):
            super().__init__(parent)
            self.cfg = copy.deepcopy(cfg)
            self.runs_root = Path(runs_root)

        def run(self) -> None:
            try:
                from my_bt_lab.app.desktop_support import collect_result_metrics
                from my_bt_lab.engines.factory import run as run_engine
                from my_bt_lab.reporting.writer import prepare_run_dir, write_result

                cfg_path = _write_runtime_cfg(self.cfg)
                run_dir = prepare_run_dir(self.runs_root, tag=self.cfg.get("output", {}).get("tag") or "simple")
                log_path = run_dir / "run.log"

                logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")],
                    force=True,
                )

                self.status.emit("正在连接数据库并运行回测...")
                result = run_engine(self.cfg, cfg_path)
                write_result(run_dir, self.cfg, cfg_path, result, project_root=project_root())

                payload = {
                    "run_dir": str(run_dir),
                    "metrics": collect_result_metrics(result),
                    "metric_rows": _metric_rows(result),
                    "trades": list(getattr(result, "trades", []) or []),
                    "orders": list(getattr(result, "orders", []) or []),
                    "fills": list(getattr(result, "fills", []) or []),
                    "equity_curve": list(getattr(result, "equity_curve", []) or []),
                    "log_tail": _safe_read_text_tail(log_path, lines=200),
                }
                self.completed.emit(payload)
            except Exception:
                detail = traceback.format_exc()
                self.failed.emit({"message": _friendly_exception_message(detail), "detail": detail})


    class SimpleDesktopWindow(QMainWindow):
        def __init__(self):
            super().__init__()
            self.setWindowTitle(APP_TITLE)
            self.resize(1220, 800)

            self.runs_root = project_root() / "runs"
            self.worker: Optional[SimpleBacktestWorker] = None
            self.current_run_dir: Optional[Path] = None

            self.data_presets = {}
            for label, preset in FALLBACK_DATA_PRESETS.items():
                item = dict(preset)
                item.setdefault("status", "可运行")
                item.setdefault("can_run", True)
                item.setdefault("issues", [])
                self.data_presets[f"可运行：{label}"] = item
            self.data_presets.update(_load_yaml_presets())
            self.strategy_presets = _load_strategy_labels()

            self._build_ui()
            self._apply_style()
            self._install_combo_views()
            self._on_data_preset_changed(self.preset_combo.currentText())
            self._on_risk_preset_changed(self.risk_combo.currentText())

        def _apply_style(self) -> None:
            QApplication.setStyle("Fusion")
            self.setStyleSheet(
                """
                QMainWindow, QWidget {
                    background-color: #111827;
                    color: #E5E7EB;
                    font-size: 13px;
                }
                QGroupBox {
                    border: 1px solid #374151;
                    border-radius: 8px;
                    margin-top: 12px;
                    padding-top: 14px;
                    font-weight: 600;
                }
                QGroupBox::title {
                    subcontrol-origin: margin;
                    left: 12px;
                    padding: 0 5px;
                }
                QLineEdit, QComboBox, QDateEdit, QDoubleSpinBox, QPlainTextEdit, QTableWidget {
                    background-color: #0F172A;
                    color: #E5E7EB;
                    border: 1px solid #334155;
                    border-radius: 5px;
                    padding: 4px;
                }
                QComboBox:focus, QLineEdit:focus, QDateEdit:focus, QDoubleSpinBox:focus {
                    border: 2px solid #38BDF8;
                    background-color: #0B1220;
                }
                QComboBox::drop-down {
                    border-left: 1px solid #334155;
                    width: 26px;
                }
                QComboBox QAbstractItemView {
                    background-color: #0F172A;
                    color: #E5E7EB;
                    border: 1px solid #38BDF8;
                    outline: 0;
                }
                QTableWidget::item {
                    background-color: #0F172A;
                    color: #E5E7EB;
                }
                QTableWidget::item:alternate {
                    background-color: #172033;
                    color: #E5E7EB;
                }
                QTableWidget::item:selected {
                    background-color: #0E7490;
                    color: #FFFFFF;
                }
                QPushButton {
                    background-color: #1D4ED8;
                    color: white;
                    border: none;
                    border-radius: 6px;
                    padding: 8px 14px;
                    font-weight: 600;
                }
                QPushButton:hover {
                    background-color: #2563EB;
                }
                QPushButton:disabled {
                    background-color: #475569;
                    color: #CBD5E1;
                }
                QHeaderView::section {
                    background-color: #1F2937;
                    color: #E5E7EB;
                    padding: 5px;
                    border: 1px solid #374151;
                }
                QLabel#title {
                    font-size: 24px;
                    font-weight: 700;
                    color: #F8FAFC;
                }
                QLabel#hint {
                    color: #94A3B8;
                }
                QLabel#presetHint {
                    color: #93C5FD;
                    padding: 4px 0;
                }
                QLabel#riskHint {
                    color: #FBBF24;
                    padding: 3px 0;
                }
                QLabel#status {
                    color: #93C5FD;
                    font-weight: 600;
                    padding: 6px 0;
                }
                QLabel#chartFallback {
                    background-color: #0F172A;
                    color: #94A3B8;
                    border: 1px solid #334155;
                    padding: 12px;
                }
                """
            )

        def _install_combo_views(self) -> None:
            view_style = """
                QListView {
                    background-color: #0F172A;
                    color: #E5E7EB;
                    border: 1px solid #38BDF8;
                    outline: 0;
                    padding: 2px;
                }
                QListView::item {
                    min-height: 30px;
                    padding: 6px 8px;
                    background-color: #0F172A;
                    color: #E5E7EB;
                    border-bottom: 1px solid #1E293B;
                }
                QListView::item:hover {
                    background-color: #0E7490;
                    color: #FFFFFF;
                }
                QListView::item:selected {
                    background-color: #2563EB;
                    color: #FFFFFF;
                }
            """
            for combo in [self.preset_combo, self.period_combo, self.risk_combo, self.strategy_combo]:
                view = QListView(combo)
                view.setMouseTracking(True)
                view.viewport().setMouseTracking(True)
                view.setStyleSheet(view_style)
                combo.setView(view)
                combo.setMaxVisibleItems(16)

        def _build_ui(self) -> None:
            root = QWidget()
            layout = QVBoxLayout(root)
            layout.setContentsMargins(14, 14, 14, 14)

            title = QLabel(APP_TITLE)
            title.setObjectName("title")

            hint = QLabel("面向非程序员：选择品种、周期、日期、资金和风险级别即可运行回测。数据库表名、字段名和 JSON 默认隐藏。")
            hint.setObjectName("hint")

            layout.addWidget(title)
            layout.addWidget(hint)

            body = QHBoxLayout()
            body.addWidget(self._build_form_panel(), 0)
            body.addWidget(self._build_result_panel(), 1)
            layout.addLayout(body, 1)

            self.setCentralWidget(root)

        def _build_form_panel(self) -> QWidget:
            panel = QWidget()
            panel.setFixedWidth(430)
            layout = QVBoxLayout(panel)
            layout.setContentsMargins(0, 0, 8, 0)

            preset_box = QGroupBox("1. 数据与品种")
            form = QFormLayout(preset_box)

            self.preset_combo = QComboBox()
            self.preset_combo.addItems(list(self.data_presets.keys()))
            self.preset_combo.currentTextChanged.connect(self._on_data_preset_changed)

            self.code_edit = QLineEdit("BTCUSDT")

            self.period_combo = QComboBox()
            self.period_combo.addItems(list(PERIOD_PRESETS.keys()))
            self.period_combo.setCurrentText("1分钟")

            self.start_date = QDateEdit()
            self.start_date.setCalendarPopup(True)
            self.start_date.setDisplayFormat("yyyy-MM-dd")

            self.end_date = QDateEdit()
            self.end_date.setCalendarPopup(True)
            self.end_date.setDisplayFormat("yyyy-MM-dd")

            self.preset_hint = QLabel("")
            self.preset_hint.setObjectName("presetHint")
            self.preset_hint.setWordWrap(True)

            form.addRow("数据预设", self.preset_combo)
            form.addRow("交易品种", self.code_edit)
            form.addRow("K线周期", self.period_combo)
            form.addRow("开始日期", self.start_date)
            form.addRow("结束日期", self.end_date)
            form.addRow("说明", self.preset_hint)

            layout.addWidget(preset_box)

            account_box = QGroupBox("2. 账户与风险")
            account_form = QFormLayout(account_box)

            self.cash_spin = QDoubleSpinBox()
            self.cash_spin.setRange(1_000, 10_000_000_000)
            self.cash_spin.setDecimals(0)
            self.cash_spin.setSingleStep(1_000_000)
            self.cash_spin.setValue(100_000_000)

            self.risk_combo = QComboBox()
            self.risk_combo.addItems(list(RISK_PRESETS.keys()))
            self.risk_combo.setCurrentText("平衡 - 单笔风险 0.01%")
            self.risk_combo.currentTextChanged.connect(self._on_risk_preset_changed)

            self.risk_spin = QDoubleSpinBox()
            self.risk_spin.setRange(0.000001, 0.1)
            self.risk_spin.setDecimals(6)
            self.risk_spin.setSingleStep(0.00001)
            self.risk_spin.setValue(0.0001)

            self.risk_hint = QLabel("可直接修改下方比例；选择“自定义”时请在这里输入具体风险比例。")
            self.risk_hint.setObjectName("riskHint")
            self.risk_hint.setWordWrap(True)

            account_form.addRow("初始资金", self.cash_spin)
            account_form.addRow("风险级别", self.risk_combo)
            account_form.addRow("单笔风险比例", self.risk_spin)
            account_form.addRow("提示", self.risk_hint)

            layout.addWidget(account_box)

            strategy_box = QGroupBox("3. 策略")
            strategy_form = QFormLayout(strategy_box)

            self.strategy_combo = QComboBox()
            self.strategy_combo.addItems(list(self.strategy_presets.keys()))
            strategy_form.addRow("策略模板", self.strategy_combo)

            layout.addWidget(strategy_box)

            conn_box = QGroupBox("4. 连接设置")
            conn_form = QFormLayout(conn_box)

            self.ssh_password_edit = QLineEdit()
            self.ssh_password_edit.setEchoMode(QLineEdit.Password)
            self.ssh_password_edit.setPlaceholderText("可留空：使用系统环境变量 SSH_PASSWORD")
            conn_form.addRow("SSH 密码", self.ssh_password_edit)

            layout.addWidget(conn_box)

            self.status_label = QLabel("状态：就绪")
            self.status_label.setObjectName("status")
            layout.addWidget(self.status_label)

            button_row = QHBoxLayout()

            self.run_btn = QPushButton("开始回测")
            self.run_btn.clicked.connect(self.start_backtest)

            self.open_dir_btn = QPushButton("打开结果目录")
            self.open_dir_btn.clicked.connect(self.open_run_dir)
            self.open_dir_btn.setEnabled(False)

            button_row.addWidget(self.run_btn)
            button_row.addWidget(self.open_dir_btn)

            layout.addLayout(button_row)
            layout.addStretch(1)

            return panel

        def _build_result_panel(self) -> QWidget:
            panel = QWidget()
            layout = QVBoxLayout(panel)
            layout.setContentsMargins(8, 0, 0, 0)

            self.tabs = QTabWidget()

            self.summary_table = self._make_table()
            self.trades_table = self._make_table()
            self.orders_table = self._make_table()
            self.fills_table = self._make_table()

            self.log_view = QPlainTextEdit()
            self.log_view.setReadOnly(True)
            self.log_view.setPlaceholderText("运行日志会显示在这里。")

            self.chart_tab = self._build_chart_tab()

            self.tabs.addTab(self.summary_table, "摘要")
            self.tabs.addTab(self.chart_tab, "图表")
            self.tabs.addTab(self.trades_table, "交易")
            self.tabs.addTab(self.orders_table, "委托")
            self.tabs.addTab(self.fills_table, "成交")
            self.tabs.addTab(self.log_view, "日志")

            layout.addWidget(self.tabs)
            return panel

        def _build_chart_tab(self) -> QWidget:
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)

            if QT_CHARTS_AVAILABLE:
                self.equity_chart = QChartView()
                self.drawdown_chart = QChartView()

                self.equity_chart.setMinimumHeight(260)
                self.drawdown_chart.setMinimumHeight(260)

                layout.addWidget(self.equity_chart)
                layout.addWidget(self.drawdown_chart)
            else:
                self.chart_fallback = QLabel("当前环境未启用 QtCharts。可在结果目录查看 CSV/HTML 报告。")
                self.chart_fallback.setObjectName("chartFallback")
                self.chart_fallback.setAlignment(Qt.AlignCenter)
                layout.addWidget(self.chart_fallback)

            return wrapper

        def _make_table(self) -> QTableWidget:
            table = QTableWidget()
            table.setAlternatingRowColors(True)
            table.setStyleSheet(
                "QTableWidget { background-color: #0F172A; alternate-background-color: #172033; gridline-color: #334155; }"
                "QTableWidget::item { background-color: #0F172A; color: #E5E7EB; }"
                "QTableWidget::item:alternate { background-color: #172033; color: #E5E7EB; }"
                "QTableWidget::item:selected { background-color: #0E7490; color: #FFFFFF; }"
            )
            return table

        def _set_table_rows(self, table: QTableWidget, rows: List[Dict[str, Any]]) -> None:
            rows = rows or []
            columns: List[str] = []

            for row in rows:
                for key in row.keys():
                    if key not in columns:
                        columns.append(key)

            table.clear()
            table.setRowCount(len(rows))
            table.setColumnCount(len(columns))
            table.setHorizontalHeaderLabels(columns)

            bg = QColor("#0F172A")
            alt_bg = QColor("#172033")
            fg = QColor("#E5E7EB")

            for r, row in enumerate(rows):
                for c, col in enumerate(columns):
                    item = QTableWidgetItem(str(row.get(col, "")))
                    item.setBackground(alt_bg if r % 2 else bg)
                    item.setForeground(fg)
                    table.setItem(r, c, item)

            table.resizeColumnsToContents()

        def _set_line_chart(self, chart_view, title: str, points: List[Dict[str, float]], y_title: str) -> None:
            if not QT_CHARTS_AVAILABLE or chart_view is None:
                return

            chart = QChart()
            chart.setTitle(title)

            series = QLineSeries()
            for point in points:
                series.append(float(point.get("x", 0.0)), float(point.get("y", 0.0)))

            chart.addSeries(series)

            axis_x = QValueAxis()
            axis_x.setTitleText("bar index")
            axis_x.setLabelFormat("%.0f")

            axis_y = QValueAxis()
            axis_y.setTitleText(y_title)

            chart.addAxis(axis_x, Qt.AlignBottom)
            chart.addAxis(axis_y, Qt.AlignLeft)

            series.attachAxis(axis_x)
            series.attachAxis(axis_y)

            if points:
                xs = [p["x"] for p in points]
                ys = [p["y"] for p in points]

                axis_x.setRange(min(xs), max(xs) if max(xs) > min(xs) else min(xs) + 1)

                y_min, y_max = min(ys), max(ys)
                if y_min == y_max:
                    pad = abs(y_min) * 0.01 or 1.0
                    y_min -= pad
                    y_max += pad

                axis_y.setRange(y_min, y_max)

            chart.legend().hide()
            chart_view.setChart(chart)

        def _update_charts(self, payload: Dict[str, Any]) -> None:
            if not QT_CHARTS_AVAILABLE:
                return

            equity_rows = payload.get("equity_curve", []) or []
            equity_points = _chart_points_from_equity(equity_rows, "value")
            drawdown_points = _drawdown_points(equity_rows)

            self._set_line_chart(self.equity_chart, "资金曲线", equity_points, "equity")
            self._set_line_chart(self.drawdown_chart, "回撤曲线", drawdown_points, "drawdown %")

        def _on_data_preset_changed(self, text: str) -> None:
            preset = self.data_presets.get(text)
            if not preset:
                return

            self.code_edit.setText(str(preset.get("code") or "BTCUSDT"))

            period = str(preset.get("period") or "1分钟")
            if period in PERIOD_PRESETS:
                self.period_combo.setCurrentText(period)

            _set_date(self.start_date, preset.get("start") or "2026-04-10")
            _set_date(self.end_date, preset.get("end") or "2026-04-10")

            strategy_name = str(preset.get("strategy_name") or "")
            if strategy_name:
                for label, value in self.strategy_presets.items():
                    if value == strategy_name:
                        self.strategy_combo.setCurrentText(label)
                        break

            self.preset_hint.setText(str(preset.get("hint") or ""))

        def _on_risk_preset_changed(self, text: str) -> None:
            value = RISK_PRESETS.get(text)

            if value is not None:
                self.risk_spin.setValue(value)
                self.risk_hint.setText("可直接修改下方比例；选择“自定义”时请在这里输入具体风险比例。")
                self.risk_spin.setStyleSheet("")
            else:
                self.risk_hint.setText("当前为自定义风险：请在“单笔风险比例”中输入，例如 0.0001 表示 0.01%。")
                self.risk_spin.setStyleSheet("border: 2px solid #FBBF24; background-color: #0B1220;")

            self.risk_spin.setEnabled(True)

        def _validate(self) -> Optional[str]:
            code = self.code_edit.text().strip()
            if not code:
                return "请填写交易品种，例如 BTCUSDT。"

            if self.start_date.date() > self.end_date.date():
                return "开始日期不能晚于结束日期。"

            preset = self.data_presets.get(self.preset_combo.currentText()) or {}
            base_cfg = preset.get("base_cfg") if isinstance(preset.get("base_cfg"), dict) else None

            # Re-run template precheck at click time so newly-set environment variables are respected.
            if base_cfg:
                status, issues, can_run = _template_precheck(base_cfg)
                if not can_run:
                    return "当前模板暂不能直接运行：\n" + "\n".join(f"- {issue}" for issue in issues[:6])

            needs_ssh = preset.get("preset_type") == "db_tick"
            if not needs_ssh and isinstance(base_cfg, dict):
                pg_cfg = base_cfg.get("postgres", {}) or {}
                needs_ssh = bool((pg_cfg.get("ssh", {}) or {}).get("enabled"))

            if needs_ssh and not self.ssh_password_edit.text().strip() and not os.environ.get("SSH_PASSWORD"):
                return "请填写 SSH 密码，或先设置系统环境变量 SSH_PASSWORD。"

            return None

        def _build_config_from_form(self) -> Dict[str, Any]:
            ssh_password = self.ssh_password_edit.text().strip()
            if ssh_password:
                os.environ["SSH_PASSWORD"] = ssh_password

            timeframe, compression = PERIOD_PRESETS[self.period_combo.currentText()]
            strategy_name = self.strategy_presets[self.strategy_combo.currentText()]
            preset = self.data_presets.get(self.preset_combo.currentText()) or {}

            return _build_runtime_config(
                code=self.code_edit.text().strip(),
                start=_format_date(self.start_date.date()),
                end=_format_date(self.end_date.date()),
                timeframe=timeframe,
                compression=compression,
                initial_cash=float(self.cash_spin.value()),
                risk_per_trade=float(self.risk_spin.value()),
                strategy_name=strategy_name,
                preset=preset,
            )

        def start_backtest(self) -> None:
            error = self._validate()
            if error:
                QMessageBox.warning(self, "请检查输入", error)
                return

            cfg = self._build_config_from_form()

            self.run_btn.setEnabled(False)
            self.open_dir_btn.setEnabled(False)
            self.status_label.setText("状态：运行中，请等待...")
            self.log_view.setPlainText("正在启动回测...\n")

            self.worker = SimpleBacktestWorker(cfg, runs_root=self.runs_root, parent=self)
            self.worker.status.connect(self.status_label.setText)
            self.worker.completed.connect(self._on_completed)
            self.worker.failed.connect(self._on_failed)
            self.worker.start()

        def _on_completed(self, payload: Dict[str, Any]) -> None:
            self.run_btn.setEnabled(True)
            self.open_dir_btn.setEnabled(True)

            self.current_run_dir = Path(payload.get("run_dir", ""))
            run_dir_name = self.current_run_dir.name if self.current_run_dir else "-"
            self.status_label.setText(f"状态：完成，结果目录 {run_dir_name}")

            self._set_table_rows(self.summary_table, payload.get("metric_rows", []))
            self._set_table_rows(self.trades_table, payload.get("trades", []))
            self._set_table_rows(self.orders_table, payload.get("orders", []))
            self._set_table_rows(self.fills_table, payload.get("fills", []))

            self._update_charts(payload)
            self.log_view.setPlainText(str(payload.get("log_tail") or ""))
            self.tabs.setCurrentWidget(self.summary_table)

        def _on_failed(self, payload: Dict[str, str]) -> None:
            self.run_btn.setEnabled(True)
            self.open_dir_btn.setEnabled(False)
            self.status_label.setText("状态：失败")

            message = str((payload or {}).get("message") or "回测失败。")
            detail = str((payload or {}).get("detail") or "")
            self.log_view.setPlainText(f"{message}\n\n技术细节：\n{detail}")
            self.tabs.setCurrentWidget(self.log_view)
            QMessageBox.critical(self, "回测失败", message)

        def open_run_dir(self) -> None:
            if not self.current_run_dir or not self.current_run_dir.exists():
                QMessageBox.information(self, "提示", "暂无可打开的运行目录。")
                return

            os.startfile(str(self.current_run_dir))


def main() -> None:
    if not QT_AVAILABLE:
        raise RuntimeError(f"PySide6 不可用: {QT_IMPORT_ERROR}")

    app = QApplication([])
    win = SimpleDesktopWindow()
    win.show()
    app.exec()


if __name__ == "__main__":
    main()
