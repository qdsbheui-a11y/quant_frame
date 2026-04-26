from __future__ import annotations

import copy
import logging
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from PySide6.QtCore import QThread, Qt, Signal
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
        QMainWindow,
        QMessageBox,
        QPushButton,
        QPlainTextEdit,
        QSpinBox,
        QTableWidget,
        QTableWidgetItem,
        QTabWidget,
        QVBoxLayout,
        QWidget,
    )
    QT_AVAILABLE = True
    QT_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover
    QT_AVAILABLE = False
    QT_IMPORT_ERROR = exc


APP_TITLE = "量化回测助手 - 普通用户版"


RISK_PRESETS = {
    "保守 - 单笔风险 0.005%": 0.00005,
    "平衡 - 单笔风险 0.01%": 0.0001,
    "积极 - 单笔风险 0.05%": 0.0005,
    "自定义": None,
}


PERIOD_PRESETS = {
    "1分钟": 1,
    "5分钟": 5,
    "15分钟": 15,
    "30分钟": 30,
    "60分钟": 60,
}


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _format_date(qdate) -> str:
    return qdate.toString("yyyy-MM-dd")


def _safe_read_text_tail(path: Path, lines: int = 200) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    parts = text.splitlines()
    return "\n".join(parts[-lines:])


def _metric_rows(result) -> List[Dict[str, Any]]:
    trade_stats = getattr(result, "trade_stats", {}) or {}
    drawdown = getattr(result, "drawdown", {}) or {}
    return [
        {"指标": "初始资金", "数值": f"{float(getattr(result, 'start_value', 0.0) or 0.0):,.2f}"},
        {"指标": "结束资金", "数值": f"{float(getattr(result, 'end_value', 0.0) or 0.0):,.2f}"},
        {"指标": "净利润", "数值": f"{float(trade_stats.get('net_pnl', 0.0) or 0.0):,.2f}"},
        {"指标": "已平仓交易", "数值": str(int(trade_stats.get('closed_trades', 0) or 0))},
        {"指标": "最大回撤", "数值": f"{float(drawdown.get('max_drawdown_pct', 0.0) or 0.0):.4f}%"},
    ]


def _build_btcusdt_tick_config(
    *,
    code: str,
    start: str,
    end: str,
    compression: int,
    initial_cash: float,
    risk_per_trade: float,
    strategy_name: str,
) -> Dict[str, Any]:
    data_name = f"{code}_tick"
    return {
        "postgres": {
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
        },
        "data": [
            {
                "name": data_name,
                "symbol": data_name,
                "source": "postgres",
                "role": "exec",
                "code": code,
                "code_col": "instrument_id",
                "data_type": "tick",
                "table_schema": "public",
                "table_name": "tick_data",
                "timeframe": "minutes",
                "compression": compression,
                "start": start,
                "end": end,
            }
        ],
        "strategy": {
            "name": strategy_name,
            "params": {
                "fast": 10,
                "slow": 30,
                "atr_period": 14,
                "atr_stop_mult": 2.0,
                "risk_per_trade": risk_per_trade,
                "max_positions": 2,
                "min_size": 1,
            },
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
            data_name: {
                "mult": 1,
                "commission": 0.0003,
                "margin": 0,
                "commtype": "perc",
            }
        },
        "engine": {
            "name": "backtrader",
            "cash": float(initial_cash),
            "commission": 0.0003,
        },
        "output": {
            "tag": "simple_btcusdt_db",
        },
        "report": {
            "html": False,
        },
    }


if QT_AVAILABLE:
    class SimpleBacktestWorker(QThread):
        completed = Signal(dict)
        failed = Signal(str)
        status = Signal(str)

        def __init__(self, cfg: Dict[str, Any], runs_root: Path, parent=None):
            super().__init__(parent)
            self.cfg = copy.deepcopy(cfg)
            self.runs_root = Path(runs_root)

        def run(self) -> None:
            try:
                from my_bt_lab.app.desktop_support import collect_result_metrics, write_temp_cfg
                from my_bt_lab.engines.factory import run as run_engine
                from my_bt_lab.reporting.writer import prepare_run_dir, write_result

                cfg_path = write_temp_cfg(self.cfg)
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
                self.failed.emit(traceback.format_exc())


    class SimpleDesktopWindow(QMainWindow):
        def __init__(self):
            super().__init__()
            self.setWindowTitle(APP_TITLE)
            self.resize(1180, 760)
            self.runs_root = project_root() / "runs"
            self.worker: Optional[SimpleBacktestWorker] = None
            self.current_run_dir: Optional[Path] = None
            self._build_ui()
            self._apply_style()

        def _apply_style(self) -> None:
            QApplication.setStyle("Fusion")
            self.setStyleSheet(
                """
                QMainWindow, QWidget { background-color: #111827; color: #E5E7EB; font-size: 13px; }
                QGroupBox { border: 1px solid #374151; border-radius: 8px; margin-top: 12px; padding-top: 14px; font-weight: 600; }
                QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 5px; }
                QLineEdit, QComboBox, QDateEdit, QSpinBox, QDoubleSpinBox, QPlainTextEdit, QTableWidget {
                    background-color: #0F172A; color: #E5E7EB; border: 1px solid #334155; border-radius: 5px; padding: 4px;
                }
                QPushButton { background-color: #1D4ED8; color: white; border: none; border-radius: 6px; padding: 8px 14px; font-weight: 600; }
                QPushButton:hover { background-color: #2563EB; }
                QPushButton:disabled { background-color: #475569; color: #CBD5E1; }
                QHeaderView::section { background-color: #1F2937; color: #E5E7EB; padding: 5px; border: 1px solid #374151; }
                QLabel#title { font-size: 24px; font-weight: 700; color: #F8FAFC; }
                QLabel#hint { color: #94A3B8; }
                QLabel#status { color: #93C5FD; font-weight: 600; padding: 6px 0; }
                """
            )

        def _build_ui(self) -> None:
            root = QWidget()
            layout = QVBoxLayout(root)
            layout.setContentsMargins(14, 14, 14, 14)

            title = QLabel(APP_TITLE)
            title.setObjectName("title")
            hint = QLabel("面向非程序员：选择品种、周期、日期、资金和风险级别即可运行数据库回测。数据库表名、字段名和 JSON 已隐藏。")
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
            panel.setFixedWidth(410)
            layout = QVBoxLayout(panel)
            layout.setContentsMargins(0, 0, 8, 0)

            preset_box = QGroupBox("1. 数据与品种")
            form = QFormLayout(preset_box)
            self.preset_combo = QComboBox()
            self.preset_combo.addItems(["BTCUSDT tick 数据库回测"])
            self.code_edit = QLineEdit("BTCUSDT")
            self.period_combo = QComboBox()
            self.period_combo.addItems(list(PERIOD_PRESETS.keys()))
            self.period_combo.setCurrentText("1分钟")
            self.start_date = QDateEdit()
            self.start_date.setCalendarPopup(True)
            self.start_date.setDisplayFormat("yyyy-MM-dd")
            self.start_date.setDate(datetime.strptime("2026-04-10", "%Y-%m-%d").date())
            self.end_date = QDateEdit()
            self.end_date.setCalendarPopup(True)
            self.end_date.setDisplayFormat("yyyy-MM-dd")
            self.end_date.setDate(datetime.strptime("2026-04-10", "%Y-%m-%d").date())
            form.addRow("数据预设", self.preset_combo)
            form.addRow("交易品种", self.code_edit)
            form.addRow("K线周期", self.period_combo)
            form.addRow("开始日期", self.start_date)
            form.addRow("结束日期", self.end_date)
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
            self.risk_spin = QDoubleSpinBox()
            self.risk_spin.setRange(0.000001, 0.1)
            self.risk_spin.setDecimals(6)
            self.risk_spin.setSingleStep(0.00001)
            self.risk_spin.setValue(0.0001)
            self.risk_spin.setEnabled(False)
            self.risk_combo.currentTextChanged.connect(self._on_risk_preset_changed)
            account_form.addRow("初始资金", self.cash_spin)
            account_form.addRow("风险级别", self.risk_combo)
            account_form.addRow("单笔风险比例", self.risk_spin)
            layout.addWidget(account_box)

            strategy_box = QGroupBox("3. 策略")
            strategy_form = QFormLayout(strategy_box)
            self.strategy_combo = QComboBox()
            self.strategy_combo.addItems(["cta_trend"])
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
            self.tabs.addTab(self.summary_table, "摘要")
            self.tabs.addTab(self.trades_table, "交易")
            self.tabs.addTab(self.orders_table, "委托")
            self.tabs.addTab(self.fills_table, "成交")
            self.tabs.addTab(self.log_view, "日志")
            layout.addWidget(self.tabs)
            return panel

        def _make_table(self) -> QTableWidget:
            table = QTableWidget()
            table.setAlternatingRowColors(True)
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
            for r, row in enumerate(rows):
                for c, col in enumerate(columns):
                    table.setItem(r, c, QTableWidgetItem(str(row.get(col, ""))))
            table.resizeColumnsToContents()

        def _on_risk_preset_changed(self, text: str) -> None:
            value = RISK_PRESETS.get(text)
            self.risk_spin.setEnabled(value is None)
            if value is not None:
                self.risk_spin.setValue(value)

        def _validate(self) -> Optional[str]:
            code = self.code_edit.text().strip()
            if not code:
                return "请填写交易品种，例如 BTCUSDT。"
            start = self.start_date.date()
            end = self.end_date.date()
            if start > end:
                return "开始日期不能晚于结束日期。"
            if not self.ssh_password_edit.text().strip() and not os.environ.get("SSH_PASSWORD"):
                return "请填写 SSH 密码，或先设置系统环境变量 SSH_PASSWORD。"
            return None

        def _build_config_from_form(self) -> Dict[str, Any]:
            ssh_password = self.ssh_password_edit.text().strip()
            if ssh_password:
                os.environ["SSH_PASSWORD"] = ssh_password
            code = self.code_edit.text().strip()
            start = _format_date(self.start_date.date())
            end = _format_date(self.end_date.date())
            compression = PERIOD_PRESETS[self.period_combo.currentText()]
            return _build_btcusdt_tick_config(
                code=code,
                start=start,
                end=end,
                compression=compression,
                initial_cash=float(self.cash_spin.value()),
                risk_per_trade=float(self.risk_spin.value()),
                strategy_name=self.strategy_combo.currentText(),
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
            self.status_label.setText(f"状态：完成，结果目录 {self.current_run_dir.name if self.current_run_dir else '-'}")
            self._set_table_rows(self.summary_table, payload.get("metric_rows", []))
            self._set_table_rows(self.trades_table, payload.get("trades", []))
            self._set_table_rows(self.orders_table, payload.get("orders", []))
            self._set_table_rows(self.fills_table, payload.get("fills", []))
            self.log_view.setPlainText(str(payload.get("log_tail") or ""))
            self.tabs.setCurrentWidget(self.summary_table)

        def _on_failed(self, detail: str) -> None:
            self.run_btn.setEnabled(True)
            self.open_dir_btn.setEnabled(False)
            self.status_label.setText("状态：失败")
            self.log_view.setPlainText(detail)
            self.tabs.setCurrentWidget(self.log_view)
            QMessageBox.critical(self, "回测失败", "回测失败。请查看日志页。")

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
