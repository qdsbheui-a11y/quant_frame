from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from contextlib import contextmanager

from my_bt_lab.app.desktop_support import (
    build_chart_points,
    build_data_source_rows,
    build_drawdown_points,
    build_export_rows,
    build_history_rows,
    build_market_watch_rows,
    build_postgres_connect_kwargs,
    build_postgres_query,
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
    read_text_tail,
    split_symbol_codes,
    symbol_spec_rows_to_config,
)


class DesktopSupportTests(unittest.TestCase):
    def test_list_config_files_sorted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "b.yaml").write_text("x: 1\n", encoding="utf-8")
            (root / "a.yaml").write_text("x: 2\n", encoding="utf-8")
            (root / "ignore.txt").write_text("x", encoding="utf-8")

            files = list_config_files(root)

            self.assertEqual([p.name for p in files], ["a.yaml", "b.yaml"])

    def test_parse_grid_text_coerces_types(self):
        grid = parse_grid_text("fast=5,10\nflag=true,false\nratio=1.5,2.0\nmode=trend,mean\n")

        self.assertEqual(grid["fast"], [5, 10])
        self.assertEqual(grid["flag"], [True, False])
        self.assertEqual(grid["ratio"], [1.5, 2.0])
        self.assertEqual(grid["mode"], ["trend", "mean"])

    def test_collect_result_metrics_handles_missing_fields(self):
        result = SimpleNamespace(
            start_value=100000.0,
            end_value=110000.0,
            drawdown={"max_drawdown_pct": 5.5, "max_moneydown": 5500.0},
            trade_stats={"closed_trades": 3, "net_pnl": 10000.0},
            realized_pnl=10000.0,
            floating_pnl=800.0,
        )

        metrics = collect_result_metrics(result)

        self.assertEqual(metrics["start_value"], 100000.0)
        self.assertEqual(metrics["end_value"], 110000.0)
        self.assertEqual(metrics["closed_trades"], 3)
        self.assertAlmostEqual(metrics["total_return_pct"], 10.0)
        self.assertEqual(metrics["floating_pnl"], 800.0)

    def test_build_history_rows_reads_run_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            runs_root = Path(tmp)
            run_dir = runs_root / "20260418_demo"
            run_dir.mkdir()
            (run_dir / "result.json").write_text(
                json.dumps(
                    {
                        "end_value": 120000.0,
                        "trade_stats": {"net_pnl": 20000.0, "closed_trades": 4},
                        "drawdown": {"max_drawdown_pct": 8.2},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (run_dir / "run_meta.json").write_text(
                json.dumps({"cfg_path": "my_bt_lab/app/configs/cta.yaml", "utc_time": "2026-04-18T15:00:00Z"}, ensure_ascii=False),
                encoding="utf-8",
            )

            rows = build_history_rows(runs_root)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_dir"], "20260418_demo")
            self.assertEqual(rows[0]["config"], "cta.yaml")
            self.assertEqual(rows[0]["net_pnl"], 20000.0)

    def test_read_text_tail_returns_latest_characters(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.log"
            path.write_text("0123456789abcdef", encoding="utf-8")

            tail = read_text_tail(path, max_chars=6)

            self.assertEqual(tail, "abcdef")

    def test_build_export_rows_lists_standard_artifacts_and_report_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "20260418_demo"
            run_dir.mkdir()
            (run_dir / "run.log").write_text("ok\n", encoding="utf-8")
            (run_dir / "config.yaml").write_text("x: 1\n", encoding="utf-8")
            (run_dir / "result.json").write_text("{}", encoding="utf-8")
            report_dir = run_dir / "report_html"
            report_dir.mkdir()
            (report_dir / "index.html").write_text("<html></html>", encoding="utf-8")

            rows = build_export_rows(run_dir)
            names = {row["name"]: row for row in rows}

            self.assertIn("run.log", names)
            self.assertIn("config.yaml", names)
            self.assertIn("result.json", names)
            self.assertIn("report_html", names)
            self.assertEqual(names["report_html"]["type"], "dir")
            self.assertTrue(names["report_html"]["exists"])

    def test_build_market_watch_rows_collects_data_and_resample_items(self):
        cfg = {
            "data": [
                {"name": "rb2605_5m", "symbol": "rb2605", "source": "csv", "role": "exec", "timeframe": "minutes", "compression": 5},
            ],
            "resample": [
                {"name": "rb2605_1d", "source": "rb2605_5m", "symbol": "rb2605", "role": "signal", "timeframe": "days", "compression": 1},
            ],
        }

        rows = build_market_watch_rows(cfg)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["data_name"], "rb2605_5m")
        self.assertEqual(rows[0]["period"], "minutes/5")
        self.assertEqual(rows[1]["source"], "resample:rb2605_5m")
        self.assertEqual(rows[1]["role"], "signal")
    def test_build_data_source_rows_and_roundtrip_items(self):
        cfg = {
            "data": [
                {
                    "name": "rb_main",
                    "symbol": "rb",
                    "source": "csv",
                    "role": "exec",
                    "csv": "my_bt_lab/data/rb.csv",
                    "timeframe": "minutes",
                    "compression": 5,
                    "start": "2026-01-01 09:00:00",
                    "end": "2026-01-31 15:00:00",
                    "schema": {"datetime": "dt"},
                },
                {
                    "name": "stock_pool",
                    "symbol": "000001.SZ",
                    "source": "db",
                    "role": "exec",
                    "code": "000001.SZ",
                    "timeframe": "days",
                    "compression": 1,
                },
                {
                    "name": "rb_from_table",
                    "symbol": "rb",
                    "source": "postgres",
                    "role": "exec",
                    "code": "rb",
                    "code_col": "symbol_code",
                    "table_schema": "public",
                    "table_name": "ohlcv_bars",
                    "timeframe": "minutes",
                    "compression": 5,
                    "start": "2026-03-01 09:00:00",
                    "end": "2026-03-31 15:00:00",
                    "schema": {
                        "datetime": "bar_time",
                        "open": "open_px",
                        "high": "high_px",
                        "low": "low_px",
                        "close": "close_px",
                        "volume": "vol",
                    },
                },
                {
                    "name": "if_tushare",
                    "symbol": "IF",
                    "source": "tushare",
                    "role": "signal",
                    "ts_code": "IF9999.CFX",
                    "cache_csv": "my_bt_lab/data/cache/IF9999.csv",
                    "api": "pro_bar",
                    "freq": "5min",
                    "start_date": "2026-02-01 09:00:00",
                    "end_date": "2026-02-10 15:00:00",
                    "asset": "FT",
                },
                {
                    "name": "excel_demo",
                    "symbol": "AU",
                    "source": "excel",
                    "role": "both",
                    "excel": "my_bt_lab/data/demo.xlsx",
                    "sheet_name": "Sheet1",
                    "timeframe": "days",
                    "compression": 1,
                },
            ]
        }

        rows = build_data_source_rows(cfg)

        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[1]["source"], "db")
        self.assertEqual(rows[2]["table_name"], "ohlcv_bars")
        self.assertEqual(rows[2]["datetime_col"], "bar_time")
        self.assertEqual(rows[2]["code_col"], "symbol_code")
        self.assertEqual(rows[3]["file_path"], "my_bt_lab/data/cache/IF9999.csv")
        self.assertEqual(rows[4]["sheet"], "Sheet1")

        items = data_source_rows_to_items(rows)
        self.assertEqual(items[0]["csv"], "my_bt_lab/data/rb.csv")
        self.assertEqual(items[1]["source"], "postgres")
        self.assertEqual(items[2]["table_name"], "ohlcv_bars")
        self.assertEqual(items[2]["table_schema"], "public")
        self.assertEqual(items[2]["code_col"], "symbol_code")
        self.assertEqual(items[2]["schema"]["datetime"], "bar_time")
        self.assertEqual(items[3]["ts_code"], "IF9999.CFX")
        self.assertEqual(items[3]["start_date"], "2026-02-01 09:00:00")
        self.assertEqual(items[4]["excel"], "my_bt_lab/data/demo.xlsx")

    def test_build_symbol_spec_rows_roundtrip(self):
        cfg = {
            "symbols": {
                "rb": {
                    "tick_size": 1,
                    "size_step": 1,
                    "min_size": 1,
                    "price_precision": 0,
                    "mult": 10,
                    "commission": 0.0001,
                    "margin": 8000,
                    "commtype": "perc",
                },
                "au": {
                    "tick_size": 0.02,
                    "size_step": 1,
                    "min_size": 1,
                    "price_precision": 2,
                    "mult": 1000,
                    "commission": 0.00005,
                    "margin_rate": 0.12,
                },
            }
        }

        rows = build_symbol_spec_rows(cfg)
        roundtrip = symbol_spec_rows_to_config(rows)

        self.assertEqual(len(rows), 2)
        self.assertEqual(roundtrip["rb"]["mult"], 10)
        self.assertEqual(roundtrip["au"]["margin_rate"], 0.12)

    def test_build_chart_points_extracts_numeric_series(self):
        rows = [
            {"datetime": "2026-04-18 09:00:00", "value": 100000.0},
            {"datetime": "2026-04-18 09:05:00", "value": 100500.5},
            {"datetime": "2026-04-18 09:10:00", "value": "101200.0"},
        ]

        points = build_chart_points(rows, value_key="value")

        self.assertEqual(len(points), 3)
        self.assertEqual(points[0]["x"], 0)
        self.assertEqual(points[1]["label"], "2026-04-18 09:05:00")
        self.assertAlmostEqual(points[2]["y"], 101200.0)

    def test_prepare_run_dir_adds_suffix_when_timestamp_collides(self):
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 4, 18, 17, 2, 40, tzinfo=tz)

        pandas_backup = sys.modules.get("pandas")
        writer_backup = sys.modules.pop("my_bt_lab.reporting.writer", None)
        html_backup = sys.modules.pop("my_bt_lab.reporting.html_report", None)
        sys.modules["pandas"] = types.SimpleNamespace(Timestamp=type("Timestamp", (), {}))

        try:
            writer = importlib.import_module("my_bt_lab.reporting.writer")
            with tempfile.TemporaryDirectory() as tmp, patch.object(writer, "datetime", FixedDateTime):
                runs_root = Path(tmp)
                first = writer.prepare_run_dir(runs_root, tag="cta_demo")
                second = writer.prepare_run_dir(runs_root, tag="cta_demo")

                self.assertEqual(first.name, "20260418_170240_cta_demo")
                self.assertEqual(second.name, "20260418_170240_cta_demo_001")
                self.assertTrue(second.is_dir())
        finally:
            sys.modules.pop("my_bt_lab.reporting.writer", None)
            sys.modules.pop("my_bt_lab.reporting.html_report", None)
            if writer_backup is not None:
                sys.modules["my_bt_lab.reporting.writer"] = writer_backup
            if html_backup is not None:
                sys.modules["my_bt_lab.reporting.html_report"] = html_backup
            if pandas_backup is not None:
                sys.modules["pandas"] = pandas_backup
            else:
                sys.modules.pop("pandas", None)

    def test_build_postgres_connect_kwargs_supports_password_ssl_and_search_path(self):
        cfg = {
            "host": "8.148.188.209",
            "port": 5432,
            "dbname": "quant_lab",
            "user": "postgre",
            "password": "postgre",
            "sslmode": "disable",
            "search_path": "public",
        }

        kwargs = build_postgres_connect_kwargs(cfg)

        self.assertEqual(kwargs["host"], "8.148.188.209")
        self.assertEqual(kwargs["port"], 5432)
        self.assertEqual(kwargs["dbname"], "quant_lab")
        self.assertEqual(kwargs["user"], "postgre")
        self.assertEqual(kwargs["password"], "postgre")
        self.assertEqual(kwargs["sslmode"], "disable")
        self.assertEqual(kwargs["options"], "-c search_path=public")

    def test_open_postgres_connection_retries_with_sslmode_disable_when_server_has_no_ssl(self):
        class FakeOperationalError(Exception):
            pass

        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        class FakePsycopg2:
            OperationalError = FakeOperationalError

            def __init__(self):
                self.calls = []
                self.connection = FakeConnection()

            def connect(self, **kwargs):
                self.calls.append(kwargs)
                if len(self.calls) == 1:
                    raise FakeOperationalError('server does not support SSL, but SSL was required')
                return self.connection

        from my_bt_lab.app import desktop_support as desktop_support_module

        fake_psycopg2 = FakePsycopg2()
        cfg = {
            "host": "127.0.0.1",
            "port": 5432,
            "dbname": "quant_lab",
            "user": "postgres",
            "password": "postgres",
            "sslmode": "require",
        }

        with desktop_support_module.open_postgres_connection(fake_psycopg2, cfg) as conn:
            self.assertIs(conn, fake_psycopg2.connection)

        self.assertEqual(fake_psycopg2.calls[0]["sslmode"], "require")
        self.assertEqual(fake_psycopg2.calls[1]["sslmode"], "disable")
        self.assertTrue(fake_psycopg2.connection.closed)

    def test_load_sshtunnel_module_reports_paramiko_compatibility_issue(self):
        original_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "sshtunnel":
                raise AttributeError("module 'paramiko' has no attribute 'DSSKey'")
            return original_import(name, globals, locals, fromlist, level)

        import builtins
        from my_bt_lab.app import desktop_support as desktop_support_module

        with patch.object(builtins, "__import__", side_effect=fake_import):
            with self.assertRaises(ImportError) as ctx:
                desktop_support_module.load_sshtunnel_module()

        self.assertIn("paramiko", str(ctx.exception).lower())
        self.assertIn("<4", str(ctx.exception))
        self.assertIn("DSSKey", str(ctx.exception))

    def test_open_postgres_connection_uses_ssh_tunnel_when_enabled(self):
        class FakeTunnel:
            def __init__(self, ssh_address_or_host=None, ssh_username=None, ssh_password=None, ssh_pkey=None, ssh_private_key_password=None, remote_bind_address=None, local_bind_address=None):
                self.kwargs = {
                    "ssh_address_or_host": ssh_address_or_host,
                    "ssh_username": ssh_username,
                    "ssh_password": ssh_password,
                    "ssh_pkey": ssh_pkey,
                    "ssh_private_key_password": ssh_private_key_password,
                    "remote_bind_address": remote_bind_address,
                    "local_bind_address": local_bind_address,
                }
                self.local_bind_host = local_bind_address[0]
                self.local_bind_port = 6543
                self.started = False
                self.stopped = False

            def start(self):
                self.started = True

            def stop(self):
                self.stopped = True

        class FakeSshTunnelModule:
            def __init__(self):
                self.instances = []

            def SSHTunnelForwarder(self, *args, **kwargs):
                tunnel = FakeTunnel(*args, **kwargs)
                self.instances.append(tunnel)
                return tunnel

        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        class FakePsycopg2:
            def __init__(self):
                self.calls = []
                self.connection = FakeConnection()

            def connect(self, **kwargs):
                self.calls.append(kwargs)
                return self.connection

        from my_bt_lab.app import desktop_support as desktop_support_module

        fake_psycopg2 = FakePsycopg2()
        fake_sshtunnel = FakeSshTunnelModule()
        cfg = {
            "host": "8.148.188.209",
            "port": 5432,
            "dbname": "quant_lab",
            "user": "postgre",
            "password": "***",
            "sslmode": "disable",
            "search_path": "public",
            "ssh": {
                "enabled": True,
                "host": "8.148.188.209",
                "port": 22,
                "user": "root",
                "pkey_path": "~/.ssh/id_rsa",
                "remote_bind_host": "127.0.0.1",
                "remote_bind_port": 5432,
                "local_bind_host": "127.0.0.1",
                "local_bind_port": 0,
            },
        }

        with desktop_support_module.open_postgres_connection(fake_psycopg2, cfg, sshtunnel_module=fake_sshtunnel) as conn:
            self.assertIs(conn, fake_psycopg2.connection)

        self.assertEqual(len(fake_sshtunnel.instances), 1)
        tunnel = fake_sshtunnel.instances[0]
        self.assertTrue(tunnel.started)
        self.assertTrue(tunnel.stopped)
        self.assertEqual(tunnel.kwargs["ssh_address_or_host"], ("8.148.188.209", 22))
        self.assertEqual(tunnel.kwargs["ssh_username"], "root")
        self.assertEqual(tunnel.kwargs["ssh_pkey"], "~/.ssh/id_rsa")
        self.assertEqual(tunnel.kwargs["remote_bind_address"], ("127.0.0.1", 5432))
        self.assertEqual(fake_psycopg2.calls[0]["host"], "127.0.0.1")
        self.assertEqual(fake_psycopg2.calls[0]["port"], 6543)
        self.assertEqual(fake_psycopg2.calls[0]["dbname"], "quant_lab")
        self.assertTrue(fake_psycopg2.connection.closed)

    def test_list_postgres_databases_and_tables_uses_selected_database(self):
        class FakeCursor:
            def __init__(self, rows):
                self.rows = rows
                self.executed = []

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchall(self):
                return self.rows

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            def __init__(self, rows):
                self.rows = rows
                self.closed = False
                self.cursors = []

            def cursor(self):
                cursor = FakeCursor(self.rows)
                self.cursors.append(cursor)
                return cursor

            def close(self):
                self.closed = True

        class FakePsycopg2:
            def __init__(self):
                self.calls = []
                self.connections = []

            def connect(self, **kwargs):
                self.calls.append(kwargs)
                rows = [("quant_lab",), ("postgres",)] if len(self.calls) == 1 else [("public", "bar_data", "BASE TABLE"), ("public", "instrument", "BASE TABLE")]
                conn = FakeConnection(rows)
                self.connections.append(conn)
                return conn

        fake = FakePsycopg2()
        cfg = {
            "host": "8.148.188.209",
            "port": 5432,
            "dbname": "postgres",
            "user": "postgre",
            "password": "postgre",
            "sslmode": "disable",
            "search_path": "public",
        }

        databases = list_postgres_databases(fake, cfg)
        tables = list_postgres_tables(fake, cfg, database="quant_lab")

        self.assertEqual(databases, ["postgres", "quant_lab"])
        self.assertEqual(tables[0]["schema"], "public")
        self.assertEqual(tables[0]["table"], "bar_data")
        self.assertEqual(fake.calls[0]["dbname"], "postgres")
        self.assertEqual(fake.calls[1]["dbname"], "quant_lab")
        self.assertEqual(fake.calls[1]["sslmode"], "disable")
        self.assertTrue(fake.connections[0].closed)
        self.assertTrue(fake.connections[1].closed)

    def test_list_postgres_columns_and_build_postgres_query_support_generic_table_mapping(self):
        class FakeCursor:
            def __init__(self, rows):
                self.rows = rows

            def execute(self, query, params=None):
                self.query = query
                self.params = params

            def fetchall(self):
                return self.rows

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeConnection:
            def __init__(self, rows):
                self.rows = rows
                self.closed = False

            def cursor(self):
                return FakeCursor(self.rows)

            def close(self):
                self.closed = True

        class FakePsycopg2:
            def __init__(self):
                self.calls = []
                self.connection = FakeConnection([
                    ("bar_time", "timestamp without time zone", "NO"),
                    ("open_px", "numeric", "YES"),
                    ("symbol_code", "text", "YES"),
                ])

            def connect(self, **kwargs):
                self.calls.append(kwargs)
                return self.connection

        fake = FakePsycopg2()
        cfg = {
            "host": "8.148.188.209",
            "port": 5432,
            "dbname": "quant_lab",
            "user": "postgre",
            "password": "postgre",
            "sslmode": "disable",
            "search_path": "public",
        }
        columns = list_postgres_columns(fake, cfg, database="quant_lab", schema="public", table="ohlcv_bars")
        item = {
            "table_schema": "public",
            "table_name": "ohlcv_bars",
            "code": "rb",
            "code_col": "symbol_code",
            "start": "2026-03-01 09:00:00",
            "end": "2026-03-31 15:00:00",
            "schema": {"datetime": "bar_time", "open": "open_px", "high": "high_px", "low": "low_px", "close": "close_px", "volume": "vol"},
        }

        query, params = build_postgres_query(item)

        self.assertEqual(columns[0]["column_name"], "bar_time")
        self.assertEqual(columns[1]["data_type"], "numeric")
        self.assertIn('FROM "public"."ohlcv_bars"', query)
        self.assertIn('"symbol_code" = %s', query)
        self.assertIn('"bar_time" >= %s', query)
        self.assertIn('ORDER BY "bar_time"', query)
        self.assertEqual(params, ["rb", "2026-03-01 09:00:00", "2026-03-31 15:00:00"])
        self.assertTrue(fake.connection.closed)

    def test_infer_data_rows_from_files_detects_csv_and_excel(self):
        rows = infer_data_rows_from_files([
            "/tmp/data/rb_main.csv",
            "/tmp/data/au_daily.xlsx",
        ])

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["source"], "csv")
        self.assertEqual(rows[0]["file_path"], "/tmp/data/rb_main.csv")
        self.assertEqual(rows[0]["name"], "rb_main")
        self.assertEqual(rows[1]["source"], "excel")
        self.assertEqual(rows[1]["file_path"], "/tmp/data/au_daily.xlsx")
        self.assertEqual(rows[1]["sheet"], "")

    def test_split_symbol_codes_supports_newlines_commas_and_spaces(self):
        codes = split_symbol_codes("rb, au\nzn  ag\n\nIF9999.CFX")

        self.assertEqual(codes, ["rb", "au", "zn", "ag", "IF9999.CFX"])

    def test_build_drawdown_points_trade_distribution_and_symbol_pnl_rows(self):
        equity_rows = [
            {"datetime": "2026-04-18 09:00:00", "value": 100000},
            {"datetime": "2026-04-18 09:05:00", "value": 110000},
            {"datetime": "2026-04-18 09:10:00", "value": 99000},
            {"datetime": "2026-04-18 09:15:00", "value": 120000},
        ]
        trades = [
            {"symbol": "rb", "pnlcomm": 1200},
            {"symbol": "rb", "pnlcomm": -200},
            {"symbol": "au", "pnlcomm": 500},
            {"symbol": "ag", "pnlcomm": -100},
        ]

        drawdown_points = build_drawdown_points(equity_rows)
        distribution_rows = build_trade_distribution_rows(trades, bucket_count=3)
        symbol_rows = build_symbol_pnl_rows(trades)

        self.assertEqual(len(drawdown_points), 4)
        self.assertAlmostEqual(drawdown_points[2]["y"], -10.0)
        self.assertEqual(sum(int(row["count"]) for row in distribution_rows), 4)
        self.assertEqual(symbol_rows[0]["symbol"], "rb")
        self.assertEqual(symbol_rows[0]["trade_count"], 2)
        self.assertEqual(symbol_rows[0]["net_pnl"], 1000.0)


if __name__ == "__main__":
    unittest.main()
