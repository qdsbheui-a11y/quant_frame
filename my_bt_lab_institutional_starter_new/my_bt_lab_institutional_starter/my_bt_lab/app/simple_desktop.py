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
        QFileDialog,
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


APP_TITLE = "量化回测助手 - DB Tick 普通版"


DATA_PRESETS: Dict[str, Dict[str, Any]] = {
    "数据库 tick 回测（所有策略统一使用）": {
        "preset_type": "db_tick",
        "code": "BTCUSDT",
        "period": "1分钟",
        "start": "2026-04-10",
        "end": "2026-04-10",
        "hint": (
            "普通版固定使用 PostgreSQL public.tick_data 数据源。"
            "无论选择哪个策略，都会先从数据库读取 tick，再按所选周期聚合成K线后回测。"
        ),
    }
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
    "donchian_daily_mtf": "Donchian 突破策略（DB tick + 日线信号）",
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


def _refresh_strategy_labels() -> Dict[str, str]:
    try:
        from my_bt_lab.registry.strategy_registry import refresh_strategy_registry

        refresh_strategy_registry()
    except Exception:
        pass
    return _load_strategy_labels()


def _label_for_strategy_name(name: str) -> str:
    return STRATEGY_LABELS.get(name, name)


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
) -> Dict[str, Any]:
    if strategy_name == "donchian_daily_mtf":
        return {
            "entry_lookback_days": 20,
            "exit_lookback_days": 10,
            "breakout_add_ticks": 1,
            "entry_mode": "range",
            "atr_period": 20,
            "atr_mult": 2.0,
            "trail_lv1_atr": 2.0,
            "trail_lv2_atr": 5.0,
            "trail_lock_atr": 2.0,
            "risk_cash": float(initial_cash) * float(risk_per_trade),
            "max_positions": 99,
            "min_size": 1,
        }

    if strategy_name == "cta_trend":
        return {
            "fast": 10,
            "slow": 30,
            "atr_period": 14,
            "atr_stop_mult": 2.0,
            "risk_per_trade": risk_per_trade,
            "max_positions": 2,
            "min_size": 1,
        }

    # 未知策略不强塞参数，避免 unexpected keyword argument。
    # 策略会使用自身 params 默认值。
    return {}


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


def _symbol_spec(code: str) -> Dict[str, Any]:
    # BTC/ETH 默认按币本位现货式回测参数处理；其他品种也先给通用规格。
    return {
        "mult": 1,
        "commission": 0.0003,
        "margin": 0,
        "commtype": "perc",
        "size_step": 1,
        "min_size": 1,
    }


def _build_db_tick_data_item(code: str, start: str, end: str, timeframe: str, compression: int) -> Dict[str, Any]:
    data_name = f"{code}_tick"
    return {
        "name": data_name,
        "symbol": code,
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
) -> Dict[str, Any]:
    code = str(code).strip()
    data_name = f"{code}_tick"

    cfg: Dict[str, Any] = {
        "postgres": _default_postgres_cfg(),
        "data": [_build_db_tick_data_item(code, start, end, timeframe, compression)],
        "strategy": {
            "name": strategy_name,
            "params": _strategy_params(strategy_name, initial_cash, risk_per_trade),
        },
        "broker": {
            "starting_cash": float(initial_cash),
            "account_mode": "cash",
            "slip_perc": 0.0,
            "coc": False,
        },
        "commission_default": {
            "commission": 0.0003,
            "mult": 1,
            "margin": 0,
            "commtype": "perc",
        },
        "symbols": {
            code: _symbol_spec(code),
            data_name: _symbol_spec(code),
        },
        "engine": {
            "name": "backtrader",
            "cash": float(initial_cash),
            "commission": 0.0003,
        },
        "output": {
            "tag": "simple_db_tick_backtest",
        },
        "report": {
            "html": False,
        },
    }

    # Donchian 是 MTF 设计：普通版仍然统一用 DB tick 数据；
    # 但会把 DB tick 聚合后的执行K线再 resample 成日线信号源。
    if strategy_name == "donchian_daily_mtf":
        cfg["resample"] = [
            {
                "source": data_name,
                "name": f"{code}_D_signal",
                "symbol": code,
                "role": "signal",
                "timeframe": "days",
                "compression": 1,
            }
        ]

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


def _friendly_exception_message(detail: str) -> str:
    text = str(detail or "")

    if "未找到数据" in text:
        return "数据库中没有查到符合条件的数据：请检查交易品种、开始日期、结束日期。"
    if "Failed to connect" in text or "Timed out" in text or "timeout" in text.lower():
        return "数据库连接超时：请检查网络、SSH 密码和服务器状态。"
    if "Authentication" in text or "password" in text.lower():
        return "数据库或 SSH 认证失败：请检查 SSH 密码和数据库连接配置。"
    if "unexpected keyword argument" in text:
        return "策略参数不匹配：该策略暂未完全适配普通版参数面板。请先使用已验证策略，或在高级界面调参。"
    if "No module named" in text:
        return "运行环境缺少依赖模块：请确认在项目根目录启动，并安装所需依赖。"
    if "IndexError" in text or "not enough" in text.lower():
        return "数据量不足：当前日期范围太短，策略指标无法完成初始化。请扩大回测日期范围。"

    return "回测失败。请查看日志页中的技术细节。"


def _write_runtime_cfg(cfg: Dict[str, Any]) -> Path:
    # 写到 app/configs 下，保证 backtrader_engine 能正确推断 project_root。
    path = configs_root() / "__simple_runtime.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


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

                self.status.emit("正在从数据库读取 tick 并运行回测...")
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

            self.data_presets = DATA_PRESETS
            self.strategy_presets = _refresh_strategy_labels()

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

            hint = QLabel("普通版固定使用数据库 tick_data：交易员只需要选择品种、周期、日期、资金、风险和策略。CSV / Tushare / YAML 数据源请在高级版使用。")
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

            form.addRow("数据源", self.preset_combo)
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
            self.strategy_file_edit = QLineEdit()
            self.strategy_file_edit.setReadOnly(True)
            self.strategy_file_edit.setPlaceholderText("可选：选择框架之外的 .py 策略文件")
            self.choose_strategy_btn = QPushButton("选择策略文件")
            self.choose_strategy_btn.clicked.connect(self.choose_external_strategy_file)
            self.refresh_strategy_btn = QPushButton("刷新策略")
            self.refresh_strategy_btn.clicked.connect(self.refresh_strategy_list)

            file_row = QHBoxLayout()
            file_row.addWidget(self.strategy_file_edit, 1)
            file_row.addWidget(self.choose_strategy_btn)

            btn_row = QHBoxLayout()
            btn_row.addWidget(self.refresh_strategy_btn)
            btn_row.addStretch(1)

            strategy_form.addRow("策略模板", self.strategy_combo)
            strategy_form.addRow("外部策略", file_row)
            strategy_form.addRow("操作", btn_row)

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

        def refresh_strategy_list(self) -> None:
            current_value = self.strategy_presets.get(self.strategy_combo.currentText())
            self.strategy_presets = _refresh_strategy_labels()
            self.strategy_combo.clear()
            self.strategy_combo.addItems(list(self.strategy_presets.keys()))
            if current_value:
                for label, value in self.strategy_presets.items():
                    if value == current_value:
                        self.strategy_combo.setCurrentText(label)
                        break

        def choose_external_strategy_file(self) -> None:
            path, _ = QFileDialog.getOpenFileName(
                self,
                "选择外部策略文件",
                str(project_root()),
                "Python 策略文件 (*.py);;所有文件 (*)",
            )
            if not path:
                return

            try:
                from my_bt_lab.registry.strategy_registry import load_strategy_from_file

                key, _cls = load_strategy_from_file(path)
            except Exception as exc:
                QMessageBox.critical(
                    self,
                    "外部策略加载失败",
                    "无法加载该策略文件。\n\n"
                    "要求：文件里至少有一个继承 BaseStrategy 的策略类；"
                    "策略文件可导入 my_bt_lab 包。\n\n"
                    f"错误：{exc}",
                )
                return

            self.strategy_file_edit.setText(path)
            self.refresh_strategy_list()
            target_label = None
            for label, value in self.strategy_presets.items():
                if value == key:
                    target_label = label
                    break
            if target_label is None:
                target_label = _label_for_strategy_name(key)
                self.strategy_presets[target_label] = key
                self.strategy_combo.addItem(target_label)
            self.strategy_combo.setCurrentText(target_label)
            QMessageBox.information(self, "外部策略已加载", f"已加载策略：{key}")

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
                return "请填写交易品种，例如 BTCUSDT 或 ETHUSDT。"

            if self.start_date.date() > self.end_date.date():
                return "开始日期不能晚于结束日期。"

            if not self.strategy_combo.currentText().strip():
                return "没有可用策略。请检查 strategy_registry.py 是否注册了策略。"

            if not self.ssh_password_edit.text().strip() and not os.environ.get("SSH_PASSWORD"):
                return "请填写 SSH 密码，或先设置系统环境变量 SSH_PASSWORD。"

            strategy_name = self.strategy_presets.get(self.strategy_combo.currentText(), "")
            if strategy_name == "donchian_daily_mtf":
                start_dt = self.start_date.date().toPython()
                end_dt = self.end_date.date().toPython()
                if (end_dt - start_dt).days < 30:
                    return "Donchian 策略需要较长历史数据。请把日期范围至少扩大到 30 天以上。"

            return None

        def _build_config_from_form(self) -> Dict[str, Any]:
            ssh_password = self.ssh_password_edit.text().strip()
            if ssh_password:
                os.environ["SSH_PASSWORD"] = ssh_password

            timeframe, compression = PERIOD_PRESETS[self.period_combo.currentText()]
            strategy_name = self.strategy_presets[self.strategy_combo.currentText()]

            return _build_runtime_config(
                code=self.code_edit.text().strip(),
                start=_format_date(self.start_date.date()),
                end=_format_date(self.end_date.date()),
                timeframe=timeframe,
                compression=compression,
                initial_cash=float(self.cash_spin.value()),
                risk_per_trade=float(self.risk_spin.value()),
                strategy_name=strategy_name,
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
            self.log_view.setPlainText("正在启动 DB tick 回测...\n")

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
