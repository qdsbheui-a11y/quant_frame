from __future__ import annotations

import copy
import csv
import json
import logging
import os
import shutil
import subprocess
import sys
import traceback
from datetime import datetime
from itertools import product
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import yaml

from my_bt_lab.app.desktop_support import (
    DATA_SOURCE_ROW_COLUMNS,
    SYMBOL_SPEC_ROW_COLUMNS,
    build_chart_points,
    build_data_source_rows,
    build_drawdown_points,
    build_export_rows,
    build_history_rows,
    build_market_watch_rows,
    build_postgres_connect_kwargs,
    build_symbol_pnl_rows,
    build_symbol_spec_rows,
    build_trade_distribution_rows,
    collect_result_metrics,
    data_source_rows_to_items,
    infer_data_rows_from_files,
    list_config_files,
    list_postgres_columns,
    list_postgres_databases,
    list_postgres_tables,
    parse_grid_text,
    read_json_if_exists,
    read_text_tail,
    split_symbol_codes,
    symbol_spec_rows_to_config,
    write_temp_cfg,
)

try:
    from PySide6.QtCore import QThread, Qt, Signal
    from PySide6.QtGui import QAction
    from PySide6.QtWidgets import (
        QAbstractItemView,
        QApplication,
        QCheckBox,
        QComboBox,
        QFileDialog,
        QFormLayout,
        QGroupBox,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMainWindow,
        QMdiArea,
        QMessageBox,
        QPushButton,
        QPlainTextEdit,
        QScrollArea,
        QSplitter,
        QStatusBar,
        QTableWidget,
        QTableWidgetItem,
        QTabWidget,
        QTextEdit,
        QToolBar,
        QVBoxLayout,
        QWidget,
    )
    try:
        from PySide6.QtCharts import QChart, QChartView, QLineSeries, QValueAxis
        QT_CHARTS_AVAILABLE = True
        QT_CHARTS_IMPORT_ERROR: Optional[Exception] = None
    except Exception as chart_exc:  # pragma: no cover
        QT_CHARTS_AVAILABLE = False
        QT_CHARTS_IMPORT_ERROR = chart_exc
    QT_AVAILABLE = True
    QT_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover
    QT_AVAILABLE = False
    QT_CHARTS_AVAILABLE = False
    QT_IMPORT_ERROR = exc
    QT_CHARTS_IMPORT_ERROR = exc


TIMEFRAME_OPTIONS = ["minutes", "days", "weeks", "months", "tick"]
DATA_SOURCE_OPTIONS = ["csv", "excel", "db", "tushare"]
ROLE_OPTIONS = ["exec", "signal", "both"]
ACCOUNT_MODE_OPTIONS = ["cash", "futures"]
ENGINE_OPTIONS = ["backtrader", "simple"]
SSL_MODE_OPTIONS = ["disable", "prefer", "require", "verify-ca", "verify-full"]


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def configs_root() -> Path:
    return project_root() / "my_bt_lab" / "app" / "configs"


def startup_template_override() -> Optional[Path]:
    raw = os.environ.get("MY_BT_LAB_DEFAULT_TEMPLATE", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = project_root() / path
    return path.resolve()


def read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def save_summary_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path: Path, limit: int = 500) -> List[Dict[str, Any]]:
    if not path.exists() or path.is_dir():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(reader):
            if idx >= limit:
                break
            rows.append(dict(row))
        return rows


def safe_float(text: str, default: float = 0.0) -> float:
    try:
        return float(str(text).strip())
    except Exception:
        return default


def safe_int(text: str, default: int = 0) -> int:
    try:
        return int(float(str(text).strip()))
    except Exception:
        return default


def iter_param_combinations(grid: Dict[str, List[Any]]):
    keys = sorted(grid.keys())
    for values in product(*(grid[key] for key in keys)):
        yield {key: value for key, value in zip(keys, values)}


def list_strategy_names() -> List[str]:
    try:
        from my_bt_lab.registry.strategy_registry import STRATEGY_REGISTRY

        return sorted(set(str(name) for name in STRATEGY_REGISTRY.keys()))
    except Exception:
        return []


if QT_AVAILABLE:
    class BacktestWorker(QThread):
        completed = Signal(dict)
        failed = Signal(str)
        status = Signal(str)

        def __init__(self, cfg: Dict[str, Any], runs_root: Path, run_mode: str, optimize_grid_text: str, parent=None):
            super().__init__(parent)
            self.cfg = copy.deepcopy(cfg)
            self.runs_root = Path(runs_root)
            self.run_mode = run_mode
            self.optimize_grid_text = optimize_grid_text

        def run(self) -> None:
            try:
                payload = self._run_single() if self.run_mode == "单次回测" else self._run_optimization()
                self.completed.emit(payload)
            except Exception:
                self.failed.emit(traceback.format_exc())

        def _configure_logging(self, log_path: Path) -> None:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                handlers=[logging.StreamHandler(), logging.FileHandler(log_path, encoding="utf-8")],
                force=True,
            )

        def _run_engine_once(self, cfg: Dict[str, Any], tag: str) -> Dict[str, Any]:
            from my_bt_lab.engines.factory import run as run_engine
            from my_bt_lab.reporting.writer import prepare_run_dir, write_result

            cfg = copy.deepcopy(cfg)
            cfg.setdefault("report", {})["html"] = False
            cfg_path = write_temp_cfg(cfg)
            run_dir = prepare_run_dir(self.runs_root, tag=tag)
            log_path = run_dir / "run.log"
            self._configure_logging(log_path)
            self.status.emit(f"运行中: {run_dir.name}")

            try:
                from my_bt_lab.data.cache_cleanup import cleanup_cache

                cleanup_cache(project_root(), cfg)
            except Exception:
                logging.exception("cleanup_cache 执行失败，忽略并继续")

            result = run_engine(cfg, cfg_path)
            write_result(run_dir, cfg, cfg_path, result, project_root=project_root())
            metrics = collect_result_metrics(result)
            time_return_raw = getattr(result, "time_return", {}) or {}
            time_return_rows = [
                {"datetime": str(key), "return": value}
                for key, value in (time_return_raw.items() if isinstance(time_return_raw, dict) else [])
            ]
            return {
                "run_dir": str(run_dir),
                "metrics": metrics,
                "trades": list(getattr(result, "trades", []) or []),
                "orders": list(getattr(result, "orders", []) or []),
                "fills": list(getattr(result, "fills", []) or []),
                "snapshots": list(getattr(result, "snapshots", []) or []),
                "open_positions": list(getattr(result, "open_positions", []) or []),
                "equity_curve": list(getattr(result, "equity_curve", []) or []),
                "time_return": time_return_rows,
                "log_tail": read_text_tail(log_path),
                "exports": build_export_rows(run_dir),
            }

        def _run_single(self) -> Dict[str, Any]:
            output_cfg = self.cfg.setdefault("output", {})
            tag = str(output_cfg.get("tag") or "desktop")
            payload = self._run_engine_once(self.cfg, tag=tag)
            payload["mode"] = "single"
            payload["history_rows"] = build_history_rows(self.runs_root)
            return payload

        def _run_optimization(self) -> Dict[str, Any]:
            grid = parse_grid_text(self.optimize_grid_text)
            if not grid:
                raise ValueError("参数优化模式需要填写参数网格，例如 fast=5,10,20")

            output_cfg = self.cfg.setdefault("output", {})
            base_tag = str(output_cfg.get("tag") or "desktop_opt")
            all_rows: List[Dict[str, Any]] = []
            last_payload: Optional[Dict[str, Any]] = None

            for idx, combo in enumerate(iter_param_combinations(grid), start=1):
                cfg_i = copy.deepcopy(self.cfg)
                strategy_cfg = cfg_i.setdefault("strategy", {})
                params = strategy_cfg.setdefault("params", {})
                params.update(combo)
                combo_tag = f"{base_tag}_{idx:03d}"
                self.status.emit(f"优化中 {idx}: {combo_tag}")
                payload = self._run_engine_once(cfg_i, tag=combo_tag)
                row = {"run_dir": Path(payload["run_dir"]).name, **combo, **payload["metrics"]}
                all_rows.append(row)
                last_payload = payload

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            summary_path = self.runs_root / f"opt_{ts}_{base_tag}.csv"
            save_summary_csv(summary_path, all_rows)

            if not last_payload:
                raise RuntimeError("参数优化没有生成任何结果")

            last_payload["mode"] = "optimization"
            last_payload["optimization_rows"] = all_rows
            last_payload["optimization_csv"] = str(summary_path)
            last_payload["history_rows"] = build_history_rows(self.runs_root)
            last_payload["exports"] = list(last_payload.get("exports", [])) + [
                {"name": summary_path.name, "type": "file", "exists": True, "path": str(summary_path)}
            ]
            return last_payload


    class Mt4DesktopWindow(QMainWindow):
        def __init__(self):
            super().__init__()
            self.setWindowTitle("my_bt_lab MT4策略测试器")
            self.resize(1540, 920)
            self.project_root = project_root()
            self.config_root = configs_root()
            self.current_cfg: Dict[str, Any] = {}
            self.current_template_path: Optional[Path] = None
            self.current_run_dir: Optional[Path] = None
            self.param_widgets: Dict[str, Any] = {}
            self.worker: Optional[BacktestWorker] = None
            self.history_rows: List[Dict[str, Any]] = []

            self._apply_style()
            self._build_ui()
            self._load_template_list()
            self.refresh_history_panel()
            self._update_run_action_buttons()

        def _apply_style(self) -> None:
            QApplication.setStyle("Fusion")
            self.setStyleSheet(
                """
                QMainWindow, QWidget { background-color: #111827; color: #E5E7EB; }
                QGroupBox {
                    border: 1px solid #374151;
                    border-radius: 8px;
                    margin-top: 12px;
                    padding-top: 12px;
                    font-weight: bold;
                }
                QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 4px; }
                QLineEdit, QComboBox, QTextEdit, QPlainTextEdit, QTableWidget {
                    background-color: #0F172A;
                    alternate-background-color: #172033;
                    color: #E5E7EB;
                    border: 1px solid #334155;
                    border-radius: 4px;
                    selection-background-color: #2563EB;
                    selection-color: #FFFFFF;
                    gridline-color: #334155;
                }
                QPushButton {
                    background-color: #1D4ED8;
                    color: white;
                    border: none;
                    border-radius: 5px;
                    padding: 7px 12px;
                }
                QPushButton:hover { background-color: #2563EB; }
                QHeaderView::section {
                    background-color: #1F2937;
                    color: #E5E7EB;
                    padding: 5px;
                    border: 1px solid #374151;
                }
                QTabWidget::pane { border: 1px solid #374151; }
                QTabBar::tab {
                    background: #1F2937;
                    color: #CBD5E1;
                    padding: 8px 12px;
                    margin-right: 2px;
                }
                QTabBar::tab:selected { background: #2563EB; color: white; }
                QToolBar { border-bottom: 1px solid #374151; spacing: 8px; }
                QLabel#modeBanner {
                    background: #172554;
                    border: 1px solid #1d4ed8;
                    border-radius: 6px;
                    padding: 8px 10px;
                    color: #dbeafe;
                    font-weight: 600;
                }
                QWidget#terminalStrip {
                    background: #0B1220;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QLabel#terminalTitle {
                    color: #93C5FD;
                    font-weight: 700;
                    font-size: 13px;
                }
                QLabel#terminalMeta {
                    color: #CBD5E1;
                    padding: 2px 8px;
                    background: #1F2937;
                    border-radius: 10px;
                }
                """
            )

        def _build_ui(self) -> None:
            self._build_menu_and_toolbar()

            root = QWidget()
            root_layout = QVBoxLayout(root)
            root_layout.setContentsMargins(8, 8, 8, 8)

            title = QLabel("MT4 软件风格回测工作台")
            title.setStyleSheet("font-size: 24px; font-weight: 700; color: #F8FAFC;")
            subtitle = QLabel("更接近交易终端，而不是浏览器后台：左侧导航/报价/测试器，右侧看结果与日志。")
            subtitle.setStyleSheet("color: #94A3B8; margin-bottom: 2px;")
            self.mode_banner = QLabel("桌面版已启用｜原生 PySide6 外壳｜适合非程序员")
            self.mode_banner.setObjectName("modeBanner")
            root_layout.addWidget(title)
            root_layout.addWidget(subtitle)
            root_layout.addWidget(self.mode_banner)

            splitter = QSplitter(Qt.Horizontal)
            splitter.addWidget(self._build_left_panel())
            splitter.addWidget(self._build_right_panel())
            splitter.setSizes([500, 1040])
            root_layout.addWidget(splitter)

            self.setCentralWidget(root)
            self.setStatusBar(QStatusBar())
            self.statusBar().showMessage("就绪")

        def _build_menu_and_toolbar(self) -> None:
            menu = self.menuBar()
            file_menu = menu.addMenu("文件")
            run_menu = menu.addMenu("运行")
            tools_menu = menu.addMenu("工具")

            act_load = QAction("载入模板", self)
            act_load.triggered.connect(self.choose_template_file)
            file_menu.addAction(act_load)

            act_quit = QAction("退出", self)
            act_quit.triggered.connect(self.close)
            file_menu.addAction(act_quit)

            act_run = QAction("开始回测", self)
            act_run.triggered.connect(self.start_run)
            run_menu.addAction(act_run)

            act_refresh = QAction("刷新历史", self)
            act_refresh.triggered.connect(self.refresh_history_panel)
            tools_menu.addAction(act_refresh)

            act_form_to_json = QAction("表单同步到高级JSON", self)
            act_form_to_json.triggered.connect(self.sync_form_to_json)
            tools_menu.addAction(act_form_to_json)

            act_json_to_form = QAction("高级JSON覆盖表单", self)
            act_json_to_form.triggered.connect(self.sync_json_to_form)
            tools_menu.addAction(act_json_to_form)

            toolbar = QToolBar("主工具栏")
            toolbar.setMovable(False)
            toolbar.addAction(act_load)
            toolbar.addAction(act_run)
            toolbar.addAction(act_refresh)
            self.addToolBar(toolbar)

        def _build_left_panel(self) -> QWidget:
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)
            layout.setContentsMargins(0, 0, 0, 0)

            self.left_tabs = QTabWidget()
            self.left_tabs.addTab(self._build_tester_tab(), "策略测试器")
            self.left_tabs.addTab(self._build_nav_panel(), "导航")
            self.left_tabs.addTab(self._build_market_watch_panel(), "市场报价")
            layout.addWidget(self.left_tabs)
            return wrapper

        def _build_tester_tab(self) -> QWidget:
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            content = QWidget()
            self.left_layout = QVBoxLayout(content)
            self.left_layout.setContentsMargins(4, 4, 4, 4)

            self.left_layout.addWidget(self._build_template_group())
            self.left_layout.addWidget(self._build_strategy_group())
            self.left_layout.addWidget(self._build_broker_group())
            self.left_layout.addWidget(self._build_data_group())
            self.left_layout.addWidget(self._build_symbol_group())
            self.left_layout.addWidget(self._build_output_group())
            self.left_layout.addWidget(self._build_advanced_group())
            self.left_layout.addStretch(1)
            scroll.setWidget(content)
            return scroll

        def _build_nav_panel(self) -> QWidget:
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)
            layout.setContentsMargins(8, 8, 8, 8)
            title = QLabel("终端导航")
            title.setObjectName("terminalTitle")
            hint = QLabel("这里显示当前模板、策略、引擎和运行目录，尽量接近 MT4 左侧导航感。")
            hint.setStyleSheet("color: #94A3B8;")
            self.navigator_view = QPlainTextEdit()
            self.navigator_view.setReadOnly(True)
            layout.addWidget(title)
            layout.addWidget(hint)
            layout.addWidget(self.navigator_view)
            return wrapper

        def _build_market_watch_panel(self) -> QWidget:
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)
            layout.setContentsMargins(8, 8, 8, 8)
            title = QLabel("市场报价 / 数据源")
            title.setObjectName("terminalTitle")
            hint = QLabel("显示 data 与 resample 条目，方便业务用户理解当前回测用到哪些数据。")
            hint.setStyleSheet("color: #94A3B8;")
            self.market_watch_table = self._make_table()
            layout.addWidget(title)
            layout.addWidget(hint)
            layout.addWidget(self.market_watch_table)
            return wrapper

        def _build_right_panel(self) -> QWidget:
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)
            layout.setContentsMargins(0, 0, 0, 0)

            self.last_run_label = QLabel("最近运行: -")
            self.last_run_label.setStyleSheet("font-weight: 600; color: #93C5FD;")
            layout.addWidget(self.last_run_label)
            layout.addWidget(self._build_terminal_strip())

            right_splitter = QSplitter(Qt.Vertical)
            top_widget = QWidget()
            top_layout = QVBoxLayout(top_widget)
            top_layout.setContentsMargins(0, 0, 0, 0)
            self.summary_hint = QLabel("策略测试器摘要：尚未运行")
            self.summary_hint.setStyleSheet("color: #CBD5E1; padding: 6px 2px;")
            self.summary_table = self._make_table()
            top_layout.addWidget(self.summary_hint)
            top_layout.addWidget(self.summary_table)
            right_splitter.addWidget(top_widget)

            self.tabs = QTabWidget()
            self.chart_panel = self._build_chart_panel()
            self.equity_table = self._make_table()
            self.trades_table = self._make_table()
            self.orders_table = self._make_table()
            self.fills_table = self._make_table()
            self.snapshots_table = self._make_table()
            self.positions_table = self._make_table()
            self.exports_table = self._make_table()
            self.exports_table.cellDoubleClicked.connect(self._handle_export_open)
            self.history_table = self._make_table()
            self.history_table.cellDoubleClicked.connect(self._handle_history_open)
            self.log_view = QPlainTextEdit()
            self.log_view.setReadOnly(True)

            self.tabs.addTab(self.chart_panel, "图表")
            self.tabs.addTab(self.equity_table, "结果/权益")
            self.tabs.addTab(self.trades_table, "交易")
            self.tabs.addTab(self.orders_table, "委托")
            self.tabs.addTab(self.fills_table, "成交")
            self.tabs.addTab(self.snapshots_table, "账户")
            self.tabs.addTab(self.positions_table, "持仓")
            self.tabs.addTab(self.log_view, "Journal")
            self.tabs.addTab(self.exports_table, "导出文件")
            self.tabs.addTab(self.history_table, "历史任务")
            right_splitter.addWidget(self.tabs)
            right_splitter.setSizes([220, 640])
            layout.addWidget(right_splitter)
            return wrapper

        def _build_terminal_strip(self) -> QWidget:
            strip = QWidget()
            strip.setObjectName("terminalStrip")
            layout = QVBoxLayout(strip)
            layout.setContentsMargins(10, 8, 10, 8)

            top_row = QHBoxLayout()
            title = QLabel("策略测试器 / Terminal")
            title.setObjectName("terminalTitle")
            self.term_state = QLabel("状态: 就绪")
            self.term_state.setObjectName("terminalMeta")
            self.term_template = QLabel("模板: -")
            self.term_template.setObjectName("terminalMeta")
            self.term_strategy = QLabel("策略: -")
            self.term_strategy.setObjectName("terminalMeta")
            top_row.addWidget(title)
            top_row.addStretch(1)
            top_row.addWidget(self.term_state)
            top_row.addWidget(self.term_template)
            top_row.addWidget(self.term_strategy)

            btn_row = QHBoxLayout()
            self.open_run_btn = QPushButton("打开运行目录")
            self.open_run_btn.clicked.connect(self.open_current_run_dir)
            self.refresh_chart_btn = QPushButton("刷新图表")
            self.refresh_chart_btn.clicked.connect(self.refresh_current_charts)
            self.refresh_log_btn = QPushButton("刷新日志")
            self.refresh_log_btn.clicked.connect(self.refresh_current_log)
            self.tile_chart_btn = QPushButton("平铺图窗")
            self.tile_chart_btn.clicked.connect(self.tile_chart_windows)
            self.cascade_chart_btn = QPushButton("层叠图窗")
            self.cascade_chart_btn.clicked.connect(self.cascade_chart_windows)
            btn_row.addWidget(self.open_run_btn)
            btn_row.addWidget(self.refresh_chart_btn)
            btn_row.addWidget(self.refresh_log_btn)
            btn_row.addWidget(self.tile_chart_btn)
            btn_row.addWidget(self.cascade_chart_btn)
            btn_row.addStretch(1)

            layout.addLayout(top_row)
            layout.addLayout(btn_row)
            return strip

        def _build_chart_panel(self) -> QWidget:
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)
            layout.setContentsMargins(8, 8, 8, 8)
            self.chart_hint = QLabel("图表将在回测完成后显示。可像 MT4 一样拖动、放大每个图窗。")
            self.chart_hint.setStyleSheet("color: #94A3B8;")
            layout.addWidget(self.chart_hint)

            self.chart_workspace = QMdiArea()
            self.chart_workspace.setStyleSheet("""
            QMdiArea {
                background-color: #0B1220;
                border: 1px solid #334155;
            }
            """)
            self.chart_workspace.setViewMode(QMdiArea.SubWindowView)
            self.chart_workspace.setActivationOrder(QMdiArea.CreationOrder)
            layout.addWidget(self.chart_workspace)

            self.trade_distribution_table = self._make_table()
            self.symbol_pnl_table = self._make_table()

            if QT_CHARTS_AVAILABLE:
                self.equity_chart_view = QChartView()
                self.return_chart_view = QChartView()
                self.drawdown_chart_view = QChartView()
                self.trade_distribution_chart_view = QChartView()
                self.symbol_pnl_chart_view = QChartView()
            else:
                self.equity_chart_view = None
                self.return_chart_view = None
                self.drawdown_chart_view = None
                self.trade_distribution_chart_view = None
                self.symbol_pnl_chart_view = None
                self.equity_chart_fallback = QPlainTextEdit()
                self.equity_chart_fallback.setReadOnly(True)
                self.return_chart_fallback = QPlainTextEdit()
                self.return_chart_fallback.setReadOnly(True)
                self.drawdown_chart_fallback = QPlainTextEdit()
                self.drawdown_chart_fallback.setReadOnly(True)
                self.trade_distribution_chart_fallback = QPlainTextEdit()
                self.trade_distribution_chart_fallback.setReadOnly(True)
                self.symbol_pnl_chart_fallback = QPlainTextEdit()
                self.symbol_pnl_chart_fallback.setReadOnly(True)
                self.equity_chart_fallback.setPlainText("当前环境未启用 QtCharts，回测后会显示图表数据摘要。")
                self.return_chart_fallback.setPlainText("当前环境未启用 QtCharts，回测后会显示收益率摘要。")
                self.drawdown_chart_fallback.setPlainText("当前环境未启用 QtCharts，回测后会显示回撤摘要。")
                self.trade_distribution_chart_fallback.setPlainText("当前环境未启用 QtCharts，回测后会显示成交分布摘要。")
                self.symbol_pnl_chart_fallback.setPlainText("当前环境未启用 QtCharts，回测后会显示按品种收益摘要。")

            workspace_widgets = [
                (self.equity_chart_view or self.equity_chart_fallback, "资金曲线", (20, 20, 420, 260)),
                (self.return_chart_view or self.return_chart_fallback, "收益率", (460, 20, 420, 260)),
                (self.drawdown_chart_view or self.drawdown_chart_fallback, "回撤曲线", (900, 20, 420, 260)),
                (self.trade_distribution_chart_view or self.trade_distribution_chart_fallback, "成交分布", (20, 300, 420, 240)),
                (self.symbol_pnl_chart_view or self.symbol_pnl_chart_fallback, "按品种收益对比", (460, 300, 420, 240)),
                (self.trade_distribution_table, "成交分布表", (900, 300, 320, 240)),
                (self.symbol_pnl_table, "品种收益表", (1240, 300, 320, 240)),
            ]
            for widget, title, geometry in workspace_widgets:
                sub = self.chart_workspace.addSubWindow(widget)
                sub.setWindowTitle(title)
                sub.setGeometry(*geometry)
                widget.show()
                sub.show()
            self.chart_workspace.tileSubWindows()
            return wrapper

        def _set_line_chart(self, chart_view, title: str, points: List[Dict[str, Any]], y_title: str) -> None:
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
                x_values = [float(point.get("x", 0.0)) for point in points]
                y_values = [float(point.get("y", 0.0)) for point in points]
                axis_x.setRange(min(x_values), max(x_values) if len(x_values) > 1 else min(x_values) + 1)
                y_min = min(y_values)
                y_max = max(y_values)
                if y_min == y_max:
                    y_max = y_min + 1.0
                axis_y.setRange(y_min, y_max)
            chart.legend().hide()
            chart_view.setChart(chart)
            chart_view.setMinimumHeight(220)

        def _update_charts(self, payload: Dict[str, Any]) -> None:
            equity_points = build_chart_points(payload.get("equity_curve", []), value_key="value")
            return_points = build_chart_points(payload.get("time_return", []), value_key="return")
            drawdown_points = build_drawdown_points(payload.get("equity_curve", []), value_key="value")
            trade_distribution_rows = build_trade_distribution_rows(payload.get("trades", []))
            trade_distribution_points = build_chart_points(trade_distribution_rows, value_key="count", label_key="bucket")
            symbol_pnl_rows = build_symbol_pnl_rows(payload.get("trades", []))
            symbol_pnl_points = build_chart_points(symbol_pnl_rows, value_key="net_pnl", label_key="symbol")
            self.chart_hint.setText(
                "软件内图表："
                f"权益 {len(equity_points)} 点 ｜ 收益率 {len(return_points)} 点 ｜ "
                f"回撤 {len(drawdown_points)} 点 ｜ 成交分布 {len(trade_distribution_rows)} 桶 ｜ "
                f"品种收益 {len(symbol_pnl_rows)} 项。"
            )
            self._set_table_rows(self.trade_distribution_table, trade_distribution_rows, ["bucket", "count", "lower", "upper"])
            self._set_table_rows(self.symbol_pnl_table, symbol_pnl_rows, ["symbol", "trade_count", "net_pnl", "avg_pnl"])
            if QT_CHARTS_AVAILABLE:
                self._set_line_chart(self.equity_chart_view, "资金曲线", equity_points, "equity")
                self._set_line_chart(self.return_chart_view, "收益率曲线", return_points, "return")
                self._set_line_chart(self.drawdown_chart_view, "回撤曲线", drawdown_points, "drawdown %")
                self._set_line_chart(self.trade_distribution_chart_view, "成交分布", trade_distribution_points, "count")
                self._set_line_chart(self.symbol_pnl_chart_view, "按品种收益对比", symbol_pnl_points, "net pnl")
            else:
                self.equity_chart_fallback.setPlainText(json.dumps(equity_points[:80], ensure_ascii=False, indent=2))
                self.return_chart_fallback.setPlainText(json.dumps(return_points[:80], ensure_ascii=False, indent=2))
                self.drawdown_chart_fallback.setPlainText(json.dumps(drawdown_points[:80], ensure_ascii=False, indent=2))
                self.trade_distribution_chart_fallback.setPlainText(json.dumps(trade_distribution_rows[:80], ensure_ascii=False, indent=2))
                self.symbol_pnl_chart_fallback.setPlainText(json.dumps(symbol_pnl_rows[:80], ensure_ascii=False, indent=2))
            if hasattr(self, "chart_workspace"):
                self.chart_workspace.tileSubWindows()

        def _build_template_group(self) -> QWidget:
            box = QGroupBox("1. 模板与模式")
            form = QFormLayout(box)

            self.template_combo = QComboBox()
            self.template_combo.currentIndexChanged.connect(self._on_template_changed)
            form.addRow("模板 YAML", self.template_combo)

            self.run_mode_combo = QComboBox()
            self.run_mode_combo.addItems(["单次回测", "参数优化"])
            form.addRow("运行模式", self.run_mode_combo)

            self.strategy_combo = QComboBox()
            self.strategy_combo.addItems(list_strategy_names())
            self.strategy_combo.currentIndexChanged.connect(self._on_strategy_changed)
            form.addRow("策略名称", self.strategy_combo)
            return box

        def _build_strategy_group(self) -> QWidget:
            box = QGroupBox("2. 策略参数")
            self.strategy_form = QFormLayout(box)
            self.strategy_placeholder = QLabel("载入模板后会显示策略参数")
            self.strategy_form.addRow(self.strategy_placeholder)
            return box

        def _build_broker_group(self) -> QWidget:
            box = QGroupBox("3. 账户与回测设置")
            form = QFormLayout(box)

            self.engine_combo = QComboBox()
            self.engine_combo.addItems(ENGINE_OPTIONS)
            form.addRow("引擎", self.engine_combo)

            self.starting_cash_edit = QLineEdit()
            form.addRow("初始资金", self.starting_cash_edit)

            self.account_mode_combo = QComboBox()
            self.account_mode_combo.addItems(ACCOUNT_MODE_OPTIONS)
            form.addRow("账户模式", self.account_mode_combo)

            self.slip_perc_edit = QLineEdit()
            form.addRow("滑点比例", self.slip_perc_edit)

            self.coc_check = QCheckBox("收盘成交(coc)")
            form.addRow("成交方式", self.coc_check)

            self.commission_edit = QLineEdit()
            form.addRow("默认手续费", self.commission_edit)

            self.mult_edit = QLineEdit()
            form.addRow("合约乘数", self.mult_edit)

            self.margin_edit = QLineEdit()
            form.addRow("保证金", self.margin_edit)
            return box

        def _build_data_group(self) -> QWidget:
            box = QGroupBox("4. 数据设置（支持多数据源 / 多品种）")
            layout = QVBoxLayout(box)

            form = QFormLayout()
            self.data_name_edit = QLineEdit()
            form.addRow("数据名称", self.data_name_edit)

            self.data_symbol_edit = QLineEdit()
            form.addRow("交易标的", self.data_symbol_edit)

            self.data_source_combo = QComboBox()
            self.data_source_combo.addItems(DATA_SOURCE_OPTIONS)
            form.addRow("数据源", self.data_source_combo)

            self.data_role_combo = QComboBox()
            self.data_role_combo.addItems(ROLE_OPTIONS)
            form.addRow("角色", self.data_role_combo)

            self.csv_path_edit = QLineEdit()
            file_row = QWidget()
            file_row_layout = QHBoxLayout(file_row)
            file_row_layout.setContentsMargins(0, 0, 0, 0)
            file_row_layout.addWidget(self.csv_path_edit)
            self.choose_data_file_btn = QPushButton("选择文件")
            self.choose_data_file_btn.clicked.connect(self.choose_data_file)
            self.import_data_files_btn = QPushButton("导入 Excel/CSV 向导")
            self.import_data_files_btn.clicked.connect(self.import_data_files)
            file_row_layout.addWidget(self.choose_data_file_btn)
            file_row_layout.addWidget(self.import_data_files_btn)
            form.addRow("文件/缓存路径", file_row)

            self.code_edit = QLineEdit()
            form.addRow("代码/ts_code", self.code_edit)

            self.timeframe_combo = QComboBox()
            self.timeframe_combo.addItems(TIMEFRAME_OPTIONS)
            form.addRow("周期单位", self.timeframe_combo)

            self.compression_edit = QLineEdit()
            form.addRow("周期倍数", self.compression_edit)

            self.start_edit = QLineEdit()
            form.addRow("开始时间", self.start_edit)

            self.end_edit = QLineEdit()
            form.addRow("结束时间", self.end_edit)

            self.data_sheet_edit = QLineEdit()
            form.addRow("Excel Sheet", self.data_sheet_edit)

            self.data_table_schema_edit = QLineEdit()
            form.addRow("PG Schema", self.data_table_schema_edit)

            self.data_table_name_edit = QLineEdit()
            form.addRow("PG Table", self.data_table_name_edit)

            self.data_code_col_combo = QComboBox()
            self.data_code_col_combo.setEditable(True)
            form.addRow("代码列", self.data_code_col_combo)

            self.data_datetime_col_combo = QComboBox()
            self.data_datetime_col_combo.setEditable(True)
            form.addRow("时间列", self.data_datetime_col_combo)

            self.data_open_col_combo = QComboBox()
            self.data_open_col_combo.setEditable(True)
            form.addRow("Open 列", self.data_open_col_combo)

            self.data_high_col_combo = QComboBox()
            self.data_high_col_combo.setEditable(True)
            form.addRow("High 列", self.data_high_col_combo)

            self.data_low_col_combo = QComboBox()
            self.data_low_col_combo.setEditable(True)
            form.addRow("Low 列", self.data_low_col_combo)

            self.data_close_col_combo = QComboBox()
            self.data_close_col_combo.setEditable(True)
            form.addRow("Close 列", self.data_close_col_combo)

            self.data_volume_col_combo = QComboBox()
            self.data_volume_col_combo.setEditable(True)
            form.addRow("Volume 列", self.data_volume_col_combo)

            self.multi_symbol_codes_edit = QPlainTextEdit()
            self.multi_symbol_codes_edit.setFixedHeight(54)
            self.multi_symbol_codes_edit.setPlaceholderText("多品种代码，支持逗号/空格/换行分隔，例如：rb,au,ag")
            form.addRow("多品种代码", self.multi_symbol_codes_edit)

            self.data_api_edit = QLineEdit()
            form.addRow("Tushare API", self.data_api_edit)

            self.data_freq_edit = QLineEdit()
            form.addRow("Tushare freq", self.data_freq_edit)

            self.data_extra_edit = QPlainTextEdit()
            self.data_extra_edit.setPlaceholderText('{"schema": {"datetime": "trade_date"}}')
            self.data_extra_edit.setFixedHeight(72)
            form.addRow("额外JSON", self.data_extra_edit)
            layout.addLayout(form)

            action_row = QHBoxLayout()
            self.add_data_row_btn = QPushButton("新增数据行")
            self.add_data_row_btn.clicked.connect(self.add_data_row)
            self.remove_data_row_btn = QPushButton("删除选中数据行")
            self.remove_data_row_btn.clicked.connect(self.remove_selected_data_rows)
            self.add_multi_symbol_rows_btn = QPushButton("按多品种代码批量加入")
            self.add_multi_symbol_rows_btn.clicked.connect(self.add_multi_symbol_rows)
            action_row.addWidget(self.add_data_row_btn)
            action_row.addWidget(self.remove_data_row_btn)
            action_row.addWidget(self.add_multi_symbol_rows_btn)
            action_row.addStretch(1)
            layout.addLayout(action_row)

            self.data_items_table = self._make_table(editable=True)
            self.data_items_table.itemSelectionChanged.connect(self._on_data_table_selection_changed)
            layout.addWidget(self.data_items_table)

            conn_box = QGroupBox("数据连接设置")
            conn_layout = QVBoxLayout(conn_box)
            conn_form = QFormLayout()
            self.pg_host_edit = QLineEdit()
            conn_form.addRow("DB Host", self.pg_host_edit)
            self.pg_port_edit = QLineEdit()
            conn_form.addRow("DB Port", self.pg_port_edit)
            self.pg_dbname_combo = QComboBox()
            self.pg_dbname_combo.setEditable(True)
            conn_form.addRow("DB Name", self.pg_dbname_combo)
            self.pg_user_edit = QLineEdit()
            conn_form.addRow("DB User", self.pg_user_edit)
            self.pg_password_edit = QLineEdit()
            self.pg_password_edit.setEchoMode(QLineEdit.Password)
            conn_form.addRow("DB Password", self.pg_password_edit)
            self.pg_password_env_edit = QLineEdit()
            conn_form.addRow("DB Password Env", self.pg_password_env_edit)
            self.pg_sslmode_combo = QComboBox()
            self.pg_sslmode_combo.addItems(SSL_MODE_OPTIONS)
            conn_form.addRow("SSL Mode", self.pg_sslmode_combo)
            self.pg_search_path_edit = QLineEdit()
            conn_form.addRow("Search Path", self.pg_search_path_edit)
            self.ssh_enabled_check = QCheckBox("通过 SSH 隧道连接数据库")
            conn_form.addRow("SSH Tunnel", self.ssh_enabled_check)
            self.ssh_host_edit = QLineEdit()
            conn_form.addRow("SSH Host", self.ssh_host_edit)
            self.ssh_port_edit = QLineEdit()
            conn_form.addRow("SSH Port", self.ssh_port_edit)
            self.ssh_user_edit = QLineEdit()
            conn_form.addRow("SSH User", self.ssh_user_edit)
            self.ssh_password_edit = QLineEdit()
            self.ssh_password_edit.setEchoMode(QLineEdit.Password)
            conn_form.addRow("SSH Password", self.ssh_password_edit)
            self.ssh_password_env_edit = QLineEdit()
            conn_form.addRow("SSH Password Env", self.ssh_password_env_edit)
            self.ssh_pkey_path_edit = QLineEdit()
            conn_form.addRow("SSH Key Path", self.ssh_pkey_path_edit)
            self.ssh_pkey_passphrase_edit = QLineEdit()
            self.ssh_pkey_passphrase_edit.setEchoMode(QLineEdit.Password)
            conn_form.addRow("SSH Key Passphrase", self.ssh_pkey_passphrase_edit)
            self.ssh_pkey_passphrase_env_edit = QLineEdit()
            conn_form.addRow("SSH Key Passphrase Env", self.ssh_pkey_passphrase_env_edit)
            self.ssh_remote_bind_host_edit = QLineEdit()
            conn_form.addRow("SSH Remote DB Host", self.ssh_remote_bind_host_edit)
            self.ssh_remote_bind_port_edit = QLineEdit()
            conn_form.addRow("SSH Remote DB Port", self.ssh_remote_bind_port_edit)
            self.tushare_token_env_edit = QLineEdit()
            conn_form.addRow("Tushare Token Env", self.tushare_token_env_edit)
            self.tushare_asset_edit = QLineEdit()
            conn_form.addRow("Tushare Asset", self.tushare_asset_edit)
            self.tushare_default_api_edit = QLineEdit()
            conn_form.addRow("Tushare Default API", self.tushare_default_api_edit)
            self.tushare_default_freq_edit = QLineEdit()
            conn_form.addRow("Tushare Default Freq", self.tushare_default_freq_edit)
            conn_layout.addLayout(conn_form)

            pg_action_row = QHBoxLayout()
            self.refresh_pg_dbs_btn = QPushButton("刷新数据库列表")
            self.refresh_pg_dbs_btn.clicked.connect(self.refresh_postgres_databases)
            self.refresh_pg_tables_btn = QPushButton("显示当前库表")
            self.refresh_pg_tables_btn.clicked.connect(self.refresh_postgres_tables)
            self.load_pg_table_btn = QPushButton("载入选中表(可多选)")
            self.load_pg_table_btn.clicked.connect(self.load_selected_postgres_tables)
            self.refresh_pg_columns_btn = QPushButton("显示当前表字段")
            self.refresh_pg_columns_btn.clicked.connect(self.refresh_postgres_columns)
            pg_action_row.addWidget(self.refresh_pg_dbs_btn)
            pg_action_row.addWidget(self.refresh_pg_tables_btn)
            pg_action_row.addWidget(self.load_pg_table_btn)
            pg_action_row.addWidget(self.refresh_pg_columns_btn)
            pg_action_row.addStretch(1)
            conn_layout.addLayout(pg_action_row)

            self.pg_tables_table = self._make_table()
            self.pg_tables_table.setMinimumHeight(180)
            self.pg_tables_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
            self.pg_tables_table.cellDoubleClicked.connect(self._handle_pg_table_open)
            conn_layout.addWidget(self.pg_tables_table)
            self.pg_columns_table = self._make_table()
            self.pg_columns_table.setMinimumHeight(160)
            conn_layout.addWidget(self.pg_columns_table)
            layout.addWidget(conn_box)
            return box

        def _build_symbol_group(self) -> QWidget:
            box = QGroupBox("5. 品种规则（支持多品种）")
            layout = QVBoxLayout(box)
            form = QFormLayout()

            self.symbol_key_edit = QLineEdit()
            form.addRow("symbol key", self.symbol_key_edit)

            self.symbol_tick_size_edit = QLineEdit()
            form.addRow("最小跳动", self.symbol_tick_size_edit)

            self.symbol_size_step_edit = QLineEdit()
            form.addRow("下单步长", self.symbol_size_step_edit)

            self.symbol_min_size_edit = QLineEdit()
            form.addRow("最小手数", self.symbol_min_size_edit)

            self.symbol_price_precision_edit = QLineEdit()
            form.addRow("价格精度", self.symbol_price_precision_edit)

            self.symbol_mult_edit = QLineEdit()
            form.addRow("合约乘数", self.symbol_mult_edit)

            self.symbol_commission_edit = QLineEdit()
            form.addRow("品种手续费", self.symbol_commission_edit)

            self.symbol_margin_edit = QLineEdit()
            form.addRow("品种保证金", self.symbol_margin_edit)

            self.symbol_commtype_edit = QLineEdit()
            form.addRow("手续费类型", self.symbol_commtype_edit)

            self.symbol_margin_rate_edit = QLineEdit()
            form.addRow("保证金率", self.symbol_margin_rate_edit)

            self.symbol_extra_edit = QPlainTextEdit()
            self.symbol_extra_edit.setPlaceholderText('{"exchange": "SHFE"}')
            self.symbol_extra_edit.setFixedHeight(72)
            form.addRow("额外JSON", self.symbol_extra_edit)
            layout.addLayout(form)

            action_row = QHBoxLayout()
            self.add_symbol_row_btn = QPushButton("新增品种行")
            self.add_symbol_row_btn.clicked.connect(self.add_symbol_row)
            self.remove_symbol_row_btn = QPushButton("删除选中品种行")
            self.remove_symbol_row_btn.clicked.connect(self.remove_selected_symbol_rows)
            action_row.addWidget(self.add_symbol_row_btn)
            action_row.addWidget(self.remove_symbol_row_btn)
            action_row.addStretch(1)
            layout.addLayout(action_row)

            self.symbol_items_table = self._make_table(editable=True)
            self.symbol_items_table.itemSelectionChanged.connect(self._on_symbol_table_selection_changed)
            layout.addWidget(self.symbol_items_table)
            return box

        def _build_output_group(self) -> QWidget:
            box = QGroupBox("6. 输出与优化")
            form = QFormLayout(box)

            self.output_tag_edit = QLineEdit()
            form.addRow("运行标签", self.output_tag_edit)

            self.runs_root_edit = QLineEdit(str(self.project_root / "runs"))
            form.addRow("输出目录", self.runs_root_edit)

            self.optimize_grid_edit = QTextEdit()
            self.optimize_grid_edit.setPlaceholderText("fast=5,10,20\nslow=20,30,60")
            self.optimize_grid_edit.setFixedHeight(100)
            form.addRow("参数网格", self.optimize_grid_edit)

            action_widget = QWidget()
            row = QHBoxLayout(action_widget)
            row.setContentsMargins(0, 0, 0, 0)
            self.sync_json_btn = QPushButton("表单→高级JSON")
            self.sync_json_btn.clicked.connect(self.sync_form_to_json)
            self.sync_form_btn = QPushButton("高级JSON→表单")
            self.sync_form_btn.clicked.connect(self.sync_json_to_form)
            self.run_btn = QPushButton("开始回测")
            self.run_btn.clicked.connect(self.start_run)
            row.addWidget(self.sync_json_btn)
            row.addWidget(self.sync_form_btn)
            row.addWidget(self.run_btn)
            form.addRow("快捷操作", action_widget)
            return box

        def _build_advanced_group(self) -> QWidget:
            box = QGroupBox("7. 高级 JSON 编辑")
            layout = QVBoxLayout(box)
            hint = QLabel("高级用户可直接编辑完整配置。普通用户可忽略。")
            hint.setStyleSheet("color: #94A3B8;")
            self.advanced_json = QPlainTextEdit()
            self.advanced_json.setPlaceholderText("这里会显示完整 JSON 配置")
            self.advanced_json.setMinimumHeight(220)
            layout.addWidget(hint)
            layout.addWidget(self.advanced_json)
            return box

        def _make_table(self, editable: bool = False) -> QTableWidget:
            table = QTableWidget()
            table.setAlternatingRowColors(True)
            table.setEditTriggers(QAbstractItemView.AllEditTriggers if editable else QTableWidget.NoEditTriggers)
            table.setSelectionBehavior(QTableWidget.SelectRows)
            table.verticalHeader().setVisible(False)
            return table

        def _set_table_rows(self, table: QTableWidget, rows: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> None:
            rows = rows or []
            if not columns:
                columns = []
                for row in rows:
                    for key in row.keys():
                        if key not in columns:
                            columns.append(key)
            table.clear()
            table.setRowCount(len(rows))
            table.setColumnCount(len(columns))
            table.setHorizontalHeaderLabels(columns)
            for r_idx, row in enumerate(rows):
                for c_idx, col in enumerate(columns):
                    value = row.get(col, "")
                    item = QTableWidgetItem("" if value is None else str(value))
                    table.setItem(r_idx, c_idx, item)
            table.resizeColumnsToContents()

        def _set_editable_table_rows(self, table: QTableWidget, rows: List[Dict[str, Any]], columns: List[str]) -> None:
            self._set_table_rows(table, rows, columns)
            if table.rowCount() > 0:
                table.selectRow(0)

        def _table_rows(self, table: QTableWidget) -> List[Dict[str, Any]]:
            headers = []
            for idx in range(table.columnCount()):
                header = table.horizontalHeaderItem(idx)
                headers.append(header.text() if header else f"col_{idx}")
            rows: List[Dict[str, Any]] = []
            for row_idx in range(table.rowCount()):
                row: Dict[str, Any] = {}
                is_blank = True
                for col_idx, key in enumerate(headers):
                    item = table.item(row_idx, col_idx)
                    value = item.text().strip() if item else ""
                    row[key] = value
                    if value:
                        is_blank = False
                if not is_blank:
                    rows.append(row)
            return rows

        def _selected_row_index(self, table: QTableWidget) -> int:
            indexes = table.selectionModel().selectedRows() if table.selectionModel() else []
            return indexes[0].row() if indexes else -1

        def _row_to_table(self, table: QTableWidget, row_index: int, row_data: Dict[str, Any]) -> None:
            if row_index < 0:
                return
            for col_idx in range(table.columnCount()):
                header = table.horizontalHeaderItem(col_idx)
                key = header.text() if header else ""
                value = row_data.get(key, "")
                item = table.item(row_index, col_idx)
                if item is None:
                    item = QTableWidgetItem()
                    table.setItem(row_index, col_idx, item)
                item.setText("" if value is None else str(value))

        def _set_combo_items(self, combo: QComboBox, values: List[str], current_text: str = "") -> None:
            current = current_text or combo.currentText().strip()
            combo.blockSignals(True)
            combo.clear()
            for value in values:
                if value:
                    combo.addItem(value)
            if current in values:
                combo.setCurrentText(current)
            else:
                combo.setCurrentText("")
            combo.blockSignals(False)

        def _update_run_action_buttons(self) -> None:
            enabled = self.current_run_dir is not None
            for widget_name in ["open_run_btn", "refresh_chart_btn", "refresh_log_btn"]:
                widget = getattr(self, widget_name, None)
                if widget is not None:
                    widget.setEnabled(enabled)

        def add_data_row(self) -> None:
            row = self.data_items_table.rowCount()
            self.data_items_table.insertRow(row)
            for col_idx, key in enumerate(DATA_SOURCE_ROW_COLUMNS):
                self.data_items_table.setItem(row, col_idx, QTableWidgetItem(""))
            self.data_items_table.selectRow(row)

        def remove_selected_data_rows(self) -> None:
            row = self._selected_row_index(self.data_items_table)
            if row >= 0:
                self.data_items_table.removeRow(row)
            if self.data_items_table.rowCount() > 0:
                self.data_items_table.selectRow(0)

        def _on_data_table_selection_changed(self) -> None:
            row = self._selected_row_index(self.data_items_table)
            if row < 0:
                return
            table_rows = self._table_rows(self.data_items_table)
            if row >= len(table_rows):
                return
            row_data = table_rows[row]
            self.data_name_edit.setText(str(row_data.get("name", "")))
            self.data_symbol_edit.setText(str(row_data.get("symbol", "")))
            self.data_source_combo.setCurrentText(str(row_data.get("source", "csv") or "csv"))
            self.data_role_combo.setCurrentText(str(row_data.get("role", "exec") or "exec"))
            self.csv_path_edit.setText(str(row_data.get("file_path", "")))
            self.code_edit.setText(str(row_data.get("code", "")))
            self._set_combo_items(self.data_code_col_combo, [str(row_data.get("code_col", ""))], str(row_data.get("code_col", "")))
            self.timeframe_combo.setCurrentText(str(row_data.get("timeframe", "days") or "days"))
            self.compression_edit.setText(str(row_data.get("compression", 1)))
            self.start_edit.setText(str(row_data.get("start", "")))
            self.end_edit.setText(str(row_data.get("end", "")))
            self.data_sheet_edit.setText(str(row_data.get("sheet", "")))
            self.data_table_schema_edit.setText(str(row_data.get("table_schema", "")))
            self.data_table_name_edit.setText(str(row_data.get("table_name", "")))
            self._set_combo_items(self.data_datetime_col_combo, [str(row_data.get("datetime_col", ""))], str(row_data.get("datetime_col", "")))
            self._set_combo_items(self.data_open_col_combo, [str(row_data.get("open_col", ""))], str(row_data.get("open_col", "")))
            self._set_combo_items(self.data_high_col_combo, [str(row_data.get("high_col", ""))], str(row_data.get("high_col", "")))
            self._set_combo_items(self.data_low_col_combo, [str(row_data.get("low_col", ""))], str(row_data.get("low_col", "")))
            self._set_combo_items(self.data_close_col_combo, [str(row_data.get("close_col", ""))], str(row_data.get("close_col", "")))
            self._set_combo_items(self.data_volume_col_combo, [str(row_data.get("volume_col", ""))], str(row_data.get("volume_col", "")))
            self.data_api_edit.setText(str(row_data.get("api", "")))
            self.data_freq_edit.setText(str(row_data.get("freq", "")))
            self.data_extra_edit.setPlainText(str(row_data.get("extra_json", "")))

        def _store_data_fields_to_selected_row(self) -> None:
            row = self._selected_row_index(self.data_items_table)
            if row < 0:
                if self.data_items_table.rowCount() == 0:
                    self.add_data_row()
                    row = self._selected_row_index(self.data_items_table)
                else:
                    row = 0
            row_data = {
                "name": self.data_name_edit.text().strip(),
                "symbol": self.data_symbol_edit.text().strip(),
                "source": self.data_source_combo.currentText(),
                "role": self.data_role_combo.currentText(),
                "file_path": self.csv_path_edit.text().strip(),
                "code": self.code_edit.text().strip(),
                "code_col": self.data_code_col_combo.currentText().strip(),
                "timeframe": self.timeframe_combo.currentText(),
                "compression": self.compression_edit.text().strip(),
                "start": self.start_edit.text().strip(),
                "end": self.end_edit.text().strip(),
                "sheet": self.data_sheet_edit.text().strip(),
                "table_schema": self.data_table_schema_edit.text().strip(),
                "table_name": self.data_table_name_edit.text().strip(),
                "datetime_col": self.data_datetime_col_combo.currentText().strip(),
                "open_col": self.data_open_col_combo.currentText().strip(),
                "high_col": self.data_high_col_combo.currentText().strip(),
                "low_col": self.data_low_col_combo.currentText().strip(),
                "close_col": self.data_close_col_combo.currentText().strip(),
                "volume_col": self.data_volume_col_combo.currentText().strip(),
                "api": self.data_api_edit.text().strip(),
                "freq": self.data_freq_edit.text().strip(),
                "extra_json": self.data_extra_edit.toPlainText().strip(),
            }
            self._row_to_table(self.data_items_table, row, row_data)

        def choose_data_file(self) -> None:
            current_path = self.csv_path_edit.text().strip() or str(self.project_root)
            selected, _ = QFileDialog.getOpenFileName(
                self,
                "选择数据文件",
                current_path,
                "Data Files (*.csv *.xls *.xlsx);;CSV Files (*.csv);;Excel Files (*.xls *.xlsx)",
            )
            if not selected:
                return
            self.csv_path_edit.setText(selected)
            suffix = Path(selected).suffix.lower()
            if suffix == ".csv":
                self.data_source_combo.setCurrentText("csv")
            elif suffix in {".xls", ".xlsx"}:
                self.data_source_combo.setCurrentText("excel")
            if not self.data_name_edit.text().strip():
                stem = Path(selected).stem
                self.data_name_edit.setText(stem)
                self.data_symbol_edit.setText(stem)
            self.statusBar().showMessage(f"已选择数据文件: {Path(selected).name}")

        def _append_data_rows(self, rows: List[Dict[str, Any]]) -> None:
            if not rows:
                return
            existing_rows = self._table_rows(self.data_items_table)
            merged_rows = existing_rows + rows
            self._set_editable_table_rows(self.data_items_table, merged_rows, DATA_SOURCE_ROW_COLUMNS)
            last_row = len(merged_rows) - 1
            if last_row >= 0:
                self.data_items_table.selectRow(last_row)
                self._on_data_table_selection_changed()

        def _current_data_form_row(self) -> Dict[str, Any]:
            return {
                "name": self.data_name_edit.text().strip(),
                "symbol": self.data_symbol_edit.text().strip(),
                "source": self.data_source_combo.currentText(),
                "role": self.data_role_combo.currentText(),
                "file_path": self.csv_path_edit.text().strip(),
                "code": self.code_edit.text().strip(),
                "code_col": self.data_code_col_combo.currentText().strip(),
                "timeframe": self.timeframe_combo.currentText(),
                "compression": self.compression_edit.text().strip(),
                "start": self.start_edit.text().strip(),
                "end": self.end_edit.text().strip(),
                "sheet": self.data_sheet_edit.text().strip(),
                "table_schema": self.data_table_schema_edit.text().strip(),
                "table_name": self.data_table_name_edit.text().strip(),
                "datetime_col": self.data_datetime_col_combo.currentText().strip(),
                "open_col": self.data_open_col_combo.currentText().strip(),
                "high_col": self.data_high_col_combo.currentText().strip(),
                "low_col": self.data_low_col_combo.currentText().strip(),
                "close_col": self.data_close_col_combo.currentText().strip(),
                "volume_col": self.data_volume_col_combo.currentText().strip(),
                "api": self.data_api_edit.text().strip(),
                "freq": self.data_freq_edit.text().strip(),
                "extra_json": self.data_extra_edit.toPlainText().strip(),
            }

        def add_multi_symbol_rows(self) -> None:
            codes = split_symbol_codes(self.multi_symbol_codes_edit.toPlainText())
            if not codes:
                QMessageBox.information(self, "没有可加入的品种", "请先在“多品种代码”里输入一个或多个代码。")
                return
            base_row = self._current_data_form_row()
            base_name = base_row.get("name") or base_row.get("table_name") or base_row.get("symbol") or "data"
            rows: List[Dict[str, Any]] = []
            for code in codes:
                row = dict(base_row)
                row["code"] = code
                row["symbol"] = code
                row["name"] = f"{base_name}_{code}"
                rows.append(row)
            self._append_data_rows(rows)
            self.statusBar().showMessage(f"已批量加入 {len(rows)} 个品种数据行")

        def import_data_files(self) -> None:
            selected_paths, _ = QFileDialog.getOpenFileNames(
                self,
                "导入 Excel/CSV 数据源",
                str(self.project_root),
                "Data Files (*.csv *.xls *.xlsx);;CSV Files (*.csv);;Excel Files (*.xls *.xlsx)",
            )
            rows = infer_data_rows_from_files(selected_paths)
            if not rows:
                return
            self._append_data_rows(rows)
            excel_count = sum(1 for row in rows if row.get("source") == "excel")
            msg = f"已导入 {len(rows)} 个数据文件到多数据源表格。"
            if excel_count:
                msg += " Excel 文件如需指定 Sheet，可在下方 Excel Sheet 列手动选择/填写。"
            QMessageBox.information(self, "导入完成", msg)
            self.statusBar().showMessage(msg)

        def _current_postgres_cfg(self) -> Dict[str, Any]:
            db_host = self.pg_host_edit.text().strip() or "8.148.188.209"
            db_port = safe_int(self.pg_port_edit.text(), 5432)
            cfg = {
                "host": db_host,
                "port": db_port,
                "dbname": self.pg_dbname_combo.currentText().strip() or "quant_lab",
                "user": self.pg_user_edit.text().strip() or "postgres",
                "password": self.pg_password_edit.text(),
                "password_env": self.pg_password_env_edit.text().strip() or "PGPASSWORD",
                "sslmode": self.pg_sslmode_combo.currentText().strip() or "disable",
                "search_path": self.pg_search_path_edit.text().strip() or "public",
                "ssh": {
                    "enabled": self.ssh_enabled_check.isChecked(),
                    "host": self.ssh_host_edit.text().strip() or db_host,
                    "port": safe_int(self.ssh_port_edit.text(), 22),
                    "user": self.ssh_user_edit.text().strip(),
                    "password": self.ssh_password_edit.text(),
                    "password_env": self.ssh_password_env_edit.text().strip(),
                    "pkey_path": self.ssh_pkey_path_edit.text().strip(),
                    "pkey_passphrase": self.ssh_pkey_passphrase_edit.text(),
                    "pkey_passphrase_env": self.ssh_pkey_passphrase_env_edit.text().strip(),
                    "remote_bind_host": self.ssh_remote_bind_host_edit.text().strip() or "127.0.0.1",
                    "remote_bind_port": safe_int(self.ssh_remote_bind_port_edit.text(), db_port),
                },
            }
            return cfg

        def refresh_postgres_databases(self) -> None:
            try:
                import psycopg2
            except Exception as exc:
                QMessageBox.critical(self, "缺少 psycopg2", f"刷新数据库列表失败：{exc}")
                return
            try:
                current_text = self.pg_dbname_combo.currentText().strip() or "quant_lab"
                databases = list_postgres_databases(psycopg2, self._current_postgres_cfg())
                if current_text and current_text not in databases:
                    databases.append(current_text)
                self.pg_dbname_combo.blockSignals(True)
                self.pg_dbname_combo.clear()
                for name in sorted(set(databases)):
                    self.pg_dbname_combo.addItem(name)
                self.pg_dbname_combo.setCurrentText(current_text)
                self.pg_dbname_combo.blockSignals(False)
                self.statusBar().showMessage(f"数据库列表已刷新，共 {len(databases)} 个库")
            except Exception as exc:
                msg = str(exc)
                hint = ""
                if "timed out" in msg.lower() or "timeout" in msg.lower():
                    if not self.ssh_enabled_check.isChecked():
                        hint = "\n\n提示：检测到连接超时。阿里云等云数据库通常需要通过 SSH 隧道连接，请勾选\"通过 SSH 隧道连接数据库\"并填写 SSH 配置。"
                    else:
                        hint = "\n\n提示：SSH 隧道已启用但仍超时，请检查 SSH Host/Port/User/Password 是否正确，以及服务器是否允许 SSH 连接。"
                QMessageBox.critical(self, "数据库连接失败", msg + hint)

        def refresh_postgres_tables(self) -> None:
            try:
                import psycopg2
            except Exception as exc:
                QMessageBox.critical(self, "缺少 psycopg2", f"读取表列表失败：{exc}")
                return
            try:
                pg_cfg = self._current_postgres_cfg()
                database = self.pg_dbname_combo.currentText().strip() or pg_cfg.get("dbname") or "quant_lab"
                table_rows = list_postgres_tables(psycopg2, pg_cfg, database=database)
                self._set_table_rows(self.pg_tables_table, table_rows, ["schema", "table", "type"])
                self._set_table_rows(self.pg_columns_table, [], ["column_name", "data_type", "is_nullable"])
                self.statusBar().showMessage(f"库 {database} 共显示 {len(table_rows)} 张表")
            except Exception as exc:
                msg = str(exc)
                hint = ""
                if "timed out" in msg.lower() or "timeout" in msg.lower():
                    if not self.ssh_enabled_check.isChecked():
                        hint = "\n\n提示：检测到连接超时。如需连接远程数据库，请勾选\"通过 SSH 隧道连接数据库\"。"
                    else:
                        hint = "\n\n提示：SSH 隧道已启用但仍超时，请检查 SSH 配置。"
                QMessageBox.critical(self, "读取表列表失败", msg + hint)

        def _handle_pg_table_open(self, row: int, _column: int) -> None:
            self.load_selected_postgres_table(row=row)

        def load_selected_postgres_tables(self) -> None:
            selection_model = self.pg_tables_table.selectionModel()
            selected_rows = selection_model.selectedRows() if selection_model else []
            indexes = sorted({index.row() for index in selected_rows})
            if not indexes:
                return
            if len(indexes) == 1:
                self.load_selected_postgres_table(row=indexes[0])
                return
            base_row = self._current_data_form_row()
            rows: List[Dict[str, Any]] = []
            for row in indexes:
                schema_item = self.pg_tables_table.item(row, 0)
                table_item = self.pg_tables_table.item(row, 1)
                if not schema_item or not table_item:
                    continue
                schema_name = schema_item.text().strip()
                table_name = table_item.text().strip()
                item = dict(base_row)
                item["source"] = "db"
                item["table_schema"] = schema_name
                item["table_name"] = table_name
                item["name"] = f"{table_name}"
                item["symbol"] = base_row.get("symbol") or table_name
                rows.append(item)
            self._append_data_rows(rows)
            self.statusBar().showMessage(f"已批量载入 {len(rows)} 张表到数据源表格")

        def load_selected_postgres_table(self, row: Optional[int] = None) -> None:
            row = self._selected_row_index(self.pg_tables_table) if row is None else row
            if row < 0:
                return
            schema_item = self.pg_tables_table.item(row, 0)
            table_item = self.pg_tables_table.item(row, 1)
            if not schema_item or not table_item:
                return
            schema_name = schema_item.text().strip()
            table_name = table_item.text().strip()
            self.data_source_combo.setCurrentText("db")
            self.data_table_schema_edit.setText(schema_name)
            self.data_table_name_edit.setText(table_name)
            if not self.data_name_edit.text().strip():
                self.data_name_edit.setText(table_name)
            if not self.data_symbol_edit.text().strip():
                self.data_symbol_edit.setText(table_name)
            self.refresh_postgres_columns()
            self.statusBar().showMessage(f"已载入表: {schema_name}.{table_name}")

        def refresh_postgres_columns(self) -> None:
            try:
                import psycopg2
            except Exception as exc:
                QMessageBox.critical(self, "缺少 psycopg2", f"读取表字段失败：{exc}")
                return
            schema_name = self.data_table_schema_edit.text().strip() or self.pg_search_path_edit.text().strip() or "public"
            table_name = self.data_table_name_edit.text().strip()
            if not table_name:
                self.statusBar().showMessage("请先在左侧填写或从下方选择 PG Table")
                return
            try:
                pg_cfg = self._current_postgres_cfg()
                database = self.pg_dbname_combo.currentText().strip() or pg_cfg.get("dbname") or "quant_lab"
                column_rows = list_postgres_columns(psycopg2, pg_cfg, database=database, schema=schema_name, table=table_name)
                self._set_table_rows(self.pg_columns_table, column_rows, ["column_name", "data_type", "is_nullable"])
                column_names = [str(row.get("column_name") or "") for row in column_rows]
                self._set_combo_items(self.data_code_col_combo, column_names, self.data_code_col_combo.currentText().strip())
                self._set_combo_items(self.data_datetime_col_combo, column_names, self.data_datetime_col_combo.currentText().strip())
                self._set_combo_items(self.data_open_col_combo, column_names, self.data_open_col_combo.currentText().strip())
                self._set_combo_items(self.data_high_col_combo, column_names, self.data_high_col_combo.currentText().strip())
                self._set_combo_items(self.data_low_col_combo, column_names, self.data_low_col_combo.currentText().strip())
                self._set_combo_items(self.data_close_col_combo, column_names, self.data_close_col_combo.currentText().strip())
                self._set_combo_items(self.data_volume_col_combo, column_names, self.data_volume_col_combo.currentText().strip())
                self.statusBar().showMessage(f"表 {schema_name}.{table_name} 共显示 {len(column_rows)} 个字段")
            except Exception as exc:
                msg = str(exc)
                hint = ""
                if "timed out" in msg.lower() or "timeout" in msg.lower():
                    if not self.ssh_enabled_check.isChecked():
                        hint = "\n\n提示：检测到连接超时。如需连接远程数据库，请勾选\"通过 SSH 隧道连接数据库\"。"
                    else:
                        hint = "\n\n提示：SSH 隧道已启用但仍超时，请检查 SSH 配置。"
                QMessageBox.critical(self, "读取表字段失败", msg + hint)

        def add_symbol_row(self) -> None:
            row = self.symbol_items_table.rowCount()
            self.symbol_items_table.insertRow(row)
            for col_idx, _key in enumerate(SYMBOL_SPEC_ROW_COLUMNS):
                self.symbol_items_table.setItem(row, col_idx, QTableWidgetItem(""))
            self.symbol_items_table.selectRow(row)

        def remove_selected_symbol_rows(self) -> None:
            row = self._selected_row_index(self.symbol_items_table)
            if row >= 0:
                self.symbol_items_table.removeRow(row)
            if self.symbol_items_table.rowCount() > 0:
                self.symbol_items_table.selectRow(0)

        def _on_symbol_table_selection_changed(self) -> None:
            row = self._selected_row_index(self.symbol_items_table)
            if row < 0:
                return
            table_rows = self._table_rows(self.symbol_items_table)
            if row >= len(table_rows):
                return
            row_data = table_rows[row]
            self.symbol_key_edit.setText(str(row_data.get("symbol", "")))
            self.symbol_tick_size_edit.setText(str(row_data.get("tick_size", "")))
            self.symbol_size_step_edit.setText(str(row_data.get("size_step", "")))
            self.symbol_min_size_edit.setText(str(row_data.get("min_size", "")))
            self.symbol_price_precision_edit.setText(str(row_data.get("price_precision", "")))
            self.symbol_mult_edit.setText(str(row_data.get("mult", "")))
            self.symbol_commission_edit.setText(str(row_data.get("commission", "")))
            self.symbol_margin_edit.setText(str(row_data.get("margin", "")))
            self.symbol_commtype_edit.setText(str(row_data.get("commtype", "")))
            self.symbol_margin_rate_edit.setText(str(row_data.get("margin_rate", "")))
            self.symbol_extra_edit.setPlainText(str(row_data.get("extra_json", "")))

        def _store_symbol_fields_to_selected_row(self) -> None:
            row = self._selected_row_index(self.symbol_items_table)
            if row < 0:
                if self.symbol_items_table.rowCount() == 0:
                    self.add_symbol_row()
                    row = self._selected_row_index(self.symbol_items_table)
                else:
                    row = 0
            row_data = {
                "symbol": self.symbol_key_edit.text().strip(),
                "tick_size": self.symbol_tick_size_edit.text().strip(),
                "size_step": self.symbol_size_step_edit.text().strip(),
                "min_size": self.symbol_min_size_edit.text().strip(),
                "price_precision": self.symbol_price_precision_edit.text().strip(),
                "mult": self.symbol_mult_edit.text().strip(),
                "commission": self.symbol_commission_edit.text().strip(),
                "margin": self.symbol_margin_edit.text().strip(),
                "commtype": self.symbol_commtype_edit.text().strip(),
                "margin_rate": self.symbol_margin_rate_edit.text().strip(),
                "extra_json": self.symbol_extra_edit.toPlainText().strip(),
            }
            self._row_to_table(self.symbol_items_table, row, row_data)

        def _load_template_list(self) -> None:
            self.template_combo.blockSignals(True)
            self.template_combo.clear()
            cfg_files = list_config_files(self.config_root)
            for cfg_path in cfg_files:
                self.template_combo.addItem(cfg_path.name, str(cfg_path))
            override_path = startup_template_override()
            if override_path and override_path.exists() and self.template_combo.findData(str(override_path)) < 0:
                self.template_combo.addItem(override_path.name, str(override_path))
            self.template_combo.blockSignals(False)
            preferred_path = override_path if override_path and override_path.exists() else (cfg_files[0] if cfg_files else None)
            if preferred_path:
                self.load_template(preferred_path)

        def choose_template_file(self) -> None:
            selected, _ = QFileDialog.getOpenFileName(self, "选择 YAML 模板", str(self.config_root), "YAML Files (*.yaml *.yml)")
            if selected:
                self.load_template(Path(selected))

        def _on_template_changed(self, index: int) -> None:
            path_text = self.template_combo.itemData(index)
            if path_text:
                self.load_template(Path(path_text))

        def load_template(self, path: Path) -> None:
            self.current_template_path = path
            self.current_cfg = read_yaml(path)
            self._populate_form(self.current_cfg)
            self.sync_form_to_json()
            self._refresh_aux_panels(self.current_cfg)
            self.term_template.setText(f"模板: {path.name}")
            self.term_strategy.setText(f"策略: {self.strategy_combo.currentText() or '-'}")
            self.statusBar().showMessage(f"已载入模板: {path.name}")

            idx = self.template_combo.findData(str(path))
            if idx >= 0:
                self.template_combo.blockSignals(True)
                self.template_combo.setCurrentIndex(idx)
                self.template_combo.blockSignals(False)

        def _clear_strategy_rows(self) -> None:
            while self.strategy_form.rowCount() > 0:
                self.strategy_form.removeRow(0)
            self.param_widgets = {}

        def _populate_form(self, cfg: Dict[str, Any]) -> None:
            strategy_cfg = cfg.get("strategy", {}) or {}
            params = strategy_cfg.get("params", {}) or {}
            broker_cfg = cfg.get("broker", {}) or {}
            commission_cfg = cfg.get("commission_default", {}) or {}
            output_cfg = cfg.get("output", {}) or {}
            data_rows = build_data_source_rows(cfg)
            symbol_rows = build_symbol_spec_rows(cfg)
            optimize_cfg = cfg.get("optimize", {}) or {}
            opt_params = optimize_cfg.get("strategy_params", {}) or {}
            pg_cfg = cfg.get("postgres", {}) or {}
            pg_ssh_cfg = pg_cfg.get("ssh", {}) or {}
            tushare_cfg = cfg.get("tushare", {}) or {}

            self.strategy_combo.setCurrentText(str(strategy_cfg.get("name") or ""))
            self.engine_combo.setCurrentText(str((cfg.get("engine", {}) or {}).get("name") or "backtrader"))
            self.starting_cash_edit.setText(str(broker_cfg.get("starting_cash", broker_cfg.get("cash", 1000000))))
            self.account_mode_combo.setCurrentText(str(broker_cfg.get("account_mode") or "cash"))
            self.slip_perc_edit.setText(str(broker_cfg.get("slip_perc", 0.0)))
            self.coc_check.setChecked(bool(broker_cfg.get("coc", False)))
            self.commission_edit.setText(str(commission_cfg.get("commission", 0.0)))
            self.mult_edit.setText(str(commission_cfg.get("mult", 1.0)))
            self.margin_edit.setText(str(commission_cfg.get("margin", 0.0)))

            self._set_editable_table_rows(self.data_items_table, data_rows, DATA_SOURCE_ROW_COLUMNS)
            self._set_editable_table_rows(self.symbol_items_table, symbol_rows, SYMBOL_SPEC_ROW_COLUMNS)
            self._on_data_table_selection_changed()
            self._on_symbol_table_selection_changed()

            self.pg_host_edit.setText(str(pg_cfg.get("host", "8.148.188.209")))
            self.pg_port_edit.setText(str(pg_cfg.get("port", 5432)))
            self.pg_dbname_combo.setCurrentText(str(pg_cfg.get("dbname", "quant_lab")))
            self.pg_user_edit.setText(str(pg_cfg.get("user", "postgres")))
            self.pg_password_edit.setText(str(pg_cfg.get("password", "postgres")))
            self.pg_password_env_edit.setText(str(pg_cfg.get("password_env", "PGPASSWORD")))
            self.pg_sslmode_combo.setCurrentText(str(pg_cfg.get("sslmode", "disable") or "disable"))
            self.pg_search_path_edit.setText(str(pg_cfg.get("search_path", "public") or "public"))
            self.ssh_enabled_check.setChecked(bool(pg_ssh_cfg.get("enabled", False)))
            self.ssh_host_edit.setText(str(pg_ssh_cfg.get("host", pg_cfg.get("host", "8.148.188.209")) or ""))
            self.ssh_port_edit.setText(str(pg_ssh_cfg.get("port", 22) or 22))
            self.ssh_user_edit.setText(str(pg_ssh_cfg.get("user", "") or ""))
            self.ssh_password_edit.setText(str(pg_ssh_cfg.get("password", "") or ""))
            self.ssh_password_env_edit.setText(str(pg_ssh_cfg.get("password_env", "") or ""))
            self.ssh_pkey_path_edit.setText(str(pg_ssh_cfg.get("pkey_path", pg_ssh_cfg.get("private_key_path", "")) or ""))
            self.ssh_pkey_passphrase_edit.setText(str(pg_ssh_cfg.get("pkey_passphrase", "") or ""))
            self.ssh_pkey_passphrase_env_edit.setText(str(pg_ssh_cfg.get("pkey_passphrase_env", "") or ""))
            self.ssh_remote_bind_host_edit.setText(str(pg_ssh_cfg.get("remote_bind_host", "127.0.0.1") or "127.0.0.1"))
            self.ssh_remote_bind_port_edit.setText(str(pg_ssh_cfg.get("remote_bind_port", pg_cfg.get("port", 5432)) or pg_cfg.get("port", 5432) or 5432))
            self._set_table_rows(self.pg_tables_table, [], ["schema", "table", "type"])
            self.tushare_token_env_edit.setText(str(tushare_cfg.get("token_env", "TUSHARE_TOKEN")))
            self.tushare_asset_edit.setText(str(tushare_cfg.get("asset", "E")))
            self.tushare_default_api_edit.setText(str(tushare_cfg.get("default_api", "pro_bar")))
            self.tushare_default_freq_edit.setText(str(tushare_cfg.get("freq", "D")))

            self.output_tag_edit.setText(str(output_cfg.get("tag") or path_safe_stem(self.current_template_path or Path("desktop"))))
            self.runs_root_edit.setText(str(self.project_root / "runs"))
            self.optimize_grid_edit.setText(grid_text_from_params(opt_params))

            self._rebuild_strategy_param_rows(params)

        def _rebuild_strategy_param_rows(self, params: Dict[str, Any]) -> None:
            self._clear_strategy_rows()
            if not params:
                self.strategy_form.addRow(QLabel("当前模板没有 strategy.params，可直接运行。"))
                return

            for key, value in params.items():
                if isinstance(value, bool):
                    widget = QCheckBox()
                    widget.setChecked(value)
                else:
                    widget = QLineEdit(str(value))
                self.param_widgets[key] = widget
                self.strategy_form.addRow(key, widget)

        def _default_params_for_strategy(self, name: str) -> Dict[str, Any]:
            try:
                from my_bt_lab.registry.strategy_registry import STRATEGY_REGISTRY
                cls = STRATEGY_REGISTRY.get(str(name).strip().lower())
                if cls is None:
                    return {}
                raw = getattr(cls, "params", {})
                if isinstance(raw, dict):
                    return dict(raw)
                # backtrader tuple-list style: ((name, default), ...)
                out: Dict[str, Any] = {}
                for item in raw:
                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                        out[str(item[0])] = item[1]
                    elif isinstance(item, dict):
                        out.update(item)
                return out
            except Exception:
                return {}

        def _on_strategy_changed(self) -> None:
            name = self.strategy_combo.currentText().strip()
            if not name:
                return
            defaults = self._default_params_for_strategy(name)
            # Merge with current params (keep user-modified values where keys match)
            merged = dict(defaults)
            for key, widget in self.param_widgets.items():
                if key in merged:
                    if isinstance(widget, QCheckBox):
                        merged[key] = widget.isChecked()
                    else:
                        merged[key] = coerce_widget_text(widget.text())
            self._rebuild_strategy_param_rows(merged)
            self.sync_form_to_json()

        def build_cfg_from_form(self) -> Dict[str, Any]:
            self._store_data_fields_to_selected_row()
            self._store_symbol_fields_to_selected_row()
            cfg = copy.deepcopy(self.current_cfg or {})
            cfg.setdefault("engine", {})["name"] = self.engine_combo.currentText()

            strategy_cfg = cfg.setdefault("strategy", {})
            strategy_cfg["name"] = self.strategy_combo.currentText()
            strategy_params = strategy_cfg.setdefault("params", {})
            for key, widget in self.param_widgets.items():
                if isinstance(widget, QCheckBox):
                    strategy_params[key] = widget.isChecked()
                else:
                    strategy_params[key] = coerce_widget_text(widget.text())

            broker_cfg = cfg.setdefault("broker", {})
            broker_cfg["starting_cash"] = safe_float(self.starting_cash_edit.text(), 1000000.0)
            broker_cfg["account_mode"] = self.account_mode_combo.currentText()
            broker_cfg["slip_perc"] = safe_float(self.slip_perc_edit.text(), 0.0)
            broker_cfg["coc"] = self.coc_check.isChecked()

            commission_cfg = cfg.setdefault("commission_default", {})
            commission_cfg["commission"] = safe_float(self.commission_edit.text(), 0.0)
            commission_cfg["mult"] = safe_float(self.mult_edit.text(), 1.0)
            commission_cfg["margin"] = safe_float(self.margin_edit.text(), 0.0)
            commission_cfg.setdefault("commtype", "perc")

            cfg["data"] = data_source_rows_to_items(self._table_rows(self.data_items_table))
            cfg["symbols"] = symbol_spec_rows_to_config(self._table_rows(self.symbol_items_table))

            cfg["postgres"] = {
                "host": self.pg_host_edit.text().strip() or "8.148.188.209",
                "port": safe_int(self.pg_port_edit.text(), 5432),
                "dbname": self.pg_dbname_combo.currentText().strip() or "quant_lab",
                "user": self.pg_user_edit.text().strip() or "postgres",
                "password": self.pg_password_edit.text() or "postgres",
                "password_env": self.pg_password_env_edit.text().strip() or "PGPASSWORD",
                "sslmode": self.pg_sslmode_combo.currentText().strip() or "disable",
                "search_path": self.pg_search_path_edit.text().strip() or "public",
                "ssh": {
                    "enabled": self.ssh_enabled_check.isChecked(),
                    "host": self.ssh_host_edit.text().strip() or (self.pg_host_edit.text().strip() or "8.148.188.209"),
                    "port": safe_int(self.ssh_port_edit.text(), 22),
                    "user": self.ssh_user_edit.text().strip(),
                    "password": self.ssh_password_edit.text(),
                    "password_env": self.ssh_password_env_edit.text().strip(),
                    "pkey_path": self.ssh_pkey_path_edit.text().strip(),
                    "pkey_passphrase": self.ssh_pkey_passphrase_edit.text(),
                    "pkey_passphrase_env": self.ssh_pkey_passphrase_env_edit.text().strip(),
                    "remote_bind_host": self.ssh_remote_bind_host_edit.text().strip() or "127.0.0.1",
                    "remote_bind_port": safe_int(self.ssh_remote_bind_port_edit.text(), safe_int(self.pg_port_edit.text(), 5432)),
                },
            }
            cfg["tushare"] = {
                "token_env": self.tushare_token_env_edit.text().strip() or "TUSHARE_TOKEN",
                "asset": self.tushare_asset_edit.text().strip() or "E",
                "default_api": self.tushare_default_api_edit.text().strip() or "pro_bar",
                "freq": self.tushare_default_freq_edit.text().strip() or "D",
                "use_cache": bool((cfg.get("tushare", {}) or {}).get("use_cache", True)),
                "incremental": bool((cfg.get("tushare", {}) or {}).get("incremental", True)),
                "overlap_days": int((cfg.get("tushare", {}) or {}).get("overlap_days", 3) or 3),
            }

            output_cfg = cfg.setdefault("output", {})
            output_cfg["tag"] = self.output_tag_edit.text().strip() or "desktop"

            optimize_grid = parse_grid_text(self.optimize_grid_edit.toPlainText())
            if optimize_grid:
                cfg.setdefault("optimize", {})["strategy_params"] = optimize_grid
            elif "optimize" in cfg:
                cfg["optimize"] = {}

            cfg.setdefault("report", {})["html"] = False
            return cfg

        def sync_form_to_json(self) -> None:
            cfg = self.build_cfg_from_form()
            self.advanced_json.setPlainText(json.dumps(cfg, ensure_ascii=False, indent=2))
            self.current_cfg = cfg
            self._refresh_aux_panels(cfg)
            self.term_strategy.setText(f"策略: {self.strategy_combo.currentText() or '-'}")

        def sync_json_to_form(self) -> None:
            try:
                cfg = json.loads(self.advanced_json.toPlainText() or "{}")
            except Exception as exc:
                QMessageBox.critical(self, "JSON 解析失败", str(exc))
                return
            self.current_cfg = cfg
            self._populate_form(cfg)
            self._refresh_aux_panels(cfg)
            self.term_strategy.setText(f"策略: {self.strategy_combo.currentText() or '-'}")
            self.statusBar().showMessage("已从高级 JSON 回填表单")

        def start_run(self) -> None:
            try:
                self.sync_form_to_json()
                cfg = json.loads(self.advanced_json.toPlainText())
            except Exception as exc:
                QMessageBox.critical(self, "配置错误", f"配置无法解析:\n{exc}")
                return

            runs_root = Path(self.runs_root_edit.text().strip() or (self.project_root / "runs"))
            runs_root.mkdir(parents=True, exist_ok=True)

            self.current_run_dir = None
            self._update_run_action_buttons()
            self.run_btn.setEnabled(False)
            self.term_state.setText("状态: 运行中")
            self.term_template.setText(f"模板: {self.current_template_path.name if self.current_template_path else '-'}")
            self.term_strategy.setText(f"策略: {self.strategy_combo.currentText() or '-'}")
            self.statusBar().showMessage("准备开始运行...")
            self.worker = BacktestWorker(
                cfg=cfg,
                runs_root=runs_root,
                run_mode=self.run_mode_combo.currentText(),
                optimize_grid_text=self.optimize_grid_edit.toPlainText(),
                parent=self,
            )
            self.worker.status.connect(self.statusBar().showMessage)
            self.worker.failed.connect(self._handle_worker_error)
            self.worker.completed.connect(self._handle_worker_completed)
            self.worker.finished.connect(lambda: self.run_btn.setEnabled(True))
            self.worker.start()

        def _handle_worker_error(self, message: str) -> None:
            self.term_state.setText("状态: 失败")
            self.statusBar().showMessage("运行失败")
            self.log_view.setPlainText(message)
            QMessageBox.critical(self, "回测失败", message)

        def _handle_worker_completed(self, payload: Dict[str, Any]) -> None:
            run_dir = Path(payload.get("run_dir", ""))
            self.last_run_label.setText(f"最近运行: {run_dir}")
            self.statusBar().showMessage(f"运行完成: {run_dir.name}")
            self.load_run_payload(payload)

        def load_run_payload(self, payload: Dict[str, Any]) -> None:
            metrics = payload.get("metrics", {}) or {}
            self.summary_hint.setText(
                f"策略测试器摘要：结束资金 {metrics.get('end_value', '-')} ｜收益率 {metrics.get('total_return_pct', '-')} ｜回撤 {metrics.get('max_drawdown_pct', '-')}"
            )
            self._set_table_rows(self.summary_table, [{"指标": k, "数值": v} for k, v in metrics.items()], ["指标", "数值"])
            self._update_charts(payload)

            if payload.get("mode") == "optimization" and payload.get("optimization_rows"):
                self._set_table_rows(self.equity_table, payload.get("optimization_rows", []))
            else:
                self._set_table_rows(self.equity_table, payload.get("equity_curve", []))
            self._set_table_rows(self.trades_table, payload.get("trades", []))
            self._set_table_rows(self.orders_table, payload.get("orders", []))
            self._set_table_rows(self.fills_table, payload.get("fills", []))
            self._set_table_rows(self.snapshots_table, payload.get("snapshots", []))
            self._set_table_rows(self.positions_table, payload.get("open_positions", []))
            self._set_table_rows(self.exports_table, payload.get("exports", []), ["name", "type", "path"])
            self.log_view.setPlainText(payload.get("log_tail", ""))
            self.history_rows = payload.get("history_rows", []) or []
            self._set_table_rows(self.history_table, self.history_rows)
            self.current_run_dir = Path(payload["run_dir"]) if payload.get("run_dir") else None
            self._update_run_action_buttons()
            self.term_state.setText("状态: 已完成")
            self.term_strategy.setText(f"策略: {self.strategy_combo.currentText() or '-'}")
            self.tabs.setCurrentWidget(self.chart_panel)

        def refresh_history_panel(self) -> None:
            runs_root = Path(self.runs_root_edit.text().strip() or (self.project_root / "runs"))
            runs_root.mkdir(parents=True, exist_ok=True)
            self.history_rows = build_history_rows(runs_root)
            self._set_table_rows(self.history_table, self.history_rows)

        def _handle_history_open(self, row: int, _column: int) -> None:
            if row < 0 or row >= len(self.history_rows):
                return
            run_dir_name = self.history_rows[row].get("run_dir")
            if not run_dir_name:
                return
            run_dir = Path(self.runs_root_edit.text().strip() or (self.project_root / "runs")) / str(run_dir_name)
            self.load_run_dir(run_dir)

        def load_run_dir(self, run_dir: Path) -> None:
            result_payload = read_json_if_exists(run_dir / "result.json")
            metrics = collect_result_metrics(SimpleNamespace(**result_payload)) if result_payload else {}
            payload = {
                "mode": "single",
                "run_dir": str(run_dir),
                "metrics": metrics,
                "trades": read_csv_rows(run_dir / "trades.csv"),
                "orders": read_csv_rows(run_dir / "orders.csv"),
                "fills": read_csv_rows(run_dir / "fills.csv"),
                "snapshots": read_csv_rows(run_dir / "snapshots.csv"),
                "open_positions": read_csv_rows(run_dir / "open_positions.csv"),
                "equity_curve": read_csv_rows(run_dir / "equity_curve.csv"),
                "time_return": read_csv_rows(run_dir / "time_return.csv"),
                "exports": build_export_rows(run_dir),
                "log_tail": read_text_tail(run_dir / "run.log"),
                "history_rows": self.history_rows,
            }
            self.last_run_label.setText(f"最近运行: {run_dir}")
            self.load_run_payload(payload)
            self.statusBar().showMessage(f"已载入历史任务: {run_dir.name}")

        def _handle_export_open(self, row: int, _column: int) -> None:
            path_item = self.exports_table.item(row, 2)
            if not path_item:
                return
            open_path(Path(path_item.text()))

        def _refresh_aux_panels(self, cfg: Dict[str, Any]) -> None:
            self.market_watch_rows = build_market_watch_rows(cfg)
            self._set_table_rows(self.market_watch_table, self.market_watch_rows, ["data_name", "symbol", "source", "role", "period"])
            self.navigator_view.setPlainText(self._build_navigator_text(cfg))

        def _build_navigator_text(self, cfg: Dict[str, Any]) -> str:
            strategy_cfg = cfg.get("strategy", {}) or {}
            engine_cfg = cfg.get("engine", {}) or {}
            output_cfg = cfg.get("output", {}) or {}
            lines = [
                f"模板: {self.current_template_path.name if self.current_template_path else '-'}",
                f"策略: {strategy_cfg.get('name', '-')}",
                f"引擎: {engine_cfg.get('name', '-')}",
                f"运行模式: {self.run_mode_combo.currentText()}",
                f"输出标签: {output_cfg.get('tag', '-')}",
                f"输出目录: {self.runs_root_edit.text().strip() or (self.project_root / 'runs')}",
                "",
                "数据列表:",
            ]
            for row in build_market_watch_rows(cfg):
                lines.append(f"- {row['data_name']} | {row['symbol']} | {row['source']} | {row['period']} | {row['role']}")
            return "\n".join(lines)

        def refresh_current_log(self) -> None:
            if not self.current_run_dir:
                self.statusBar().showMessage("暂无运行日志，请先完成一次回测或载入历史任务")
                return
            self.log_view.setPlainText(read_text_tail(self.current_run_dir / "run.log"))
            self.statusBar().showMessage("日志已刷新")

        def refresh_current_charts(self) -> None:
            if not self.current_run_dir:
                self.statusBar().showMessage("暂无运行结果，请先完成一次回测或载入历史任务")
                return
            self.load_run_dir(self.current_run_dir)
            self.statusBar().showMessage("图表已刷新")

        def tile_chart_windows(self) -> None:
            if hasattr(self, "chart_workspace"):
                self.chart_workspace.tileSubWindows()
                self.statusBar().showMessage("图窗已平铺")

        def cascade_chart_windows(self) -> None:
            if hasattr(self, "chart_workspace"):
                self.chart_workspace.cascadeSubWindows()
                self.statusBar().showMessage("图窗已层叠")

        def open_current_run_dir(self) -> None:
            if not self.current_run_dir:
                self.statusBar().showMessage("暂无运行目录，请先完成一次回测")
                return
            if open_path(self.current_run_dir):
                self.statusBar().showMessage(f"已打开运行目录: {self.current_run_dir}")
            else:
                self.statusBar().showMessage("运行目录打开失败")

        def open_current_report_dir(self) -> None:
            if not self.current_run_dir:
                return
            for row in build_export_rows(self.current_run_dir):
                if row.get("type") == "dir" and str(row.get("name", "")).startswith("report"):
                    open_path(Path(str(row["path"])))
                    return
            self.statusBar().showMessage("当前运行没有 HTML 报告目录")


def path_safe_stem(path: Path) -> str:
    return path.stem if path else "desktop"


def grid_text_from_params(params: Dict[str, Any]) -> str:
    lines: List[str] = []
    for key, value in (params or {}).items():
        if isinstance(value, (list, tuple, set)):
            lines.append(f"{key}=" + ",".join(str(item) for item in value))
        else:
            lines.append(f"{key}={value}")
    return "\n".join(lines)


def coerce_widget_text(text: str) -> Any:
    raw = str(text).strip()
    if raw == "":
        return ""
    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
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


def open_path(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        path_str = str(path)
        if os.name == "nt":
            os.startfile(path_str)  # type: ignore[attr-defined]
            return True
        if path_str.startswith("/mnt/") and Path("/mnt/c/Windows/explorer.exe").exists():
            subprocess.Popen(["/mnt/c/Windows/explorer.exe", path_str])
            return True
        if shutil.which("xdg-open"):
            subprocess.Popen(["xdg-open", path_str])
            return True
    except Exception:
        return False
    return False


def main() -> int:
    if not QT_AVAILABLE:
        msg = (
            "PySide6 未安装，无法启动原生桌面版。\n"
            "请先执行: pip install PySide6\n"
            f"原始导入错误: {QT_IMPORT_ERROR}"
        )
        print(msg)
        return 1

    app = QApplication(sys.argv)
    window = Mt4DesktopWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
