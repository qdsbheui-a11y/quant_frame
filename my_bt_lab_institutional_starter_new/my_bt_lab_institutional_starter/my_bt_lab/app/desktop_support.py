from __future__ import annotations

import json
import math
import os
import re
import tempfile
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import yaml

STANDARD_EXPORT_NAMES = [
    "run.log",
    "config.yaml",
    "run_meta.json",
    "result.json",
    "orders.csv",
    "fills.csv",
    "trades.csv",
    "equity_curve.csv",
    "snapshots.csv",
    "open_positions.csv",
    "time_return.csv",
    "report_error.txt",
]

DATA_SOURCE_ROW_COLUMNS = [
    "name",
    "symbol",
    "source",
    "role",
    "file_path",
    "code",
    "code_col",
    "timeframe",
    "compression",
    "start",
    "end",
    "sheet",
    "table_schema",
    "table_name",
    "datetime_col",
    "open_col",
    "high_col",
    "low_col",
    "close_col",
    "volume_col",
    "api",
    "freq",
    "extra_json",
]

SYMBOL_SPEC_ROW_COLUMNS = [
    "symbol",
    "tick_size",
    "size_step",
    "min_size",
    "price_precision",
    "mult",
    "commission",
    "margin",
    "commtype",
    "margin_rate",
    "extra_json",
]


def list_config_files(config_root: Path) -> List[Path]:
    if not config_root.exists():
        return []
    return sorted([p for p in config_root.glob("*.yaml") if p.is_file()], key=lambda p: p.name.lower())


def coerce_text_value(text: str) -> Any:
    raw = str(text).strip()
    if raw == "":
        return ""
    lower = raw.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in {"null", "none"}:
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


def normalize_source_name(source: Any) -> str:
    text = str(source or "csv").strip().lower()
    if text in {"db", "postgres", "postgresql"}:
        return "postgres"
    if text in {"excel", "xlsx", "xls"}:
        return "excel"
    if text in {"csv", "tushare"}:
        return text
    return text or "csv"


def display_source_name(source: Any) -> str:
    normalized = normalize_source_name(source)
    return "db" if normalized == "postgres" else normalized


def parse_grid_text(text: str) -> Dict[str, List[Any]]:
    out: Dict[str, List[Any]] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, values = line.split("=", 1)
        parts = [part.strip() for part in values.split(",") if part.strip()]
        if key.strip() and parts:
            out[key.strip()] = [coerce_text_value(part) for part in parts]
    return out


def split_symbol_codes(text: str) -> List[str]:
    parts = re.split(r"[\s,;，；]+", str(text or "").strip())
    out: List[str] = []
    for part in parts:
        value = str(part).strip()
        if value and value not in out:
            out.append(value)
    return out


def collect_result_metrics(result: Any) -> Dict[str, Any]:
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


def read_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def read_text_tail(path: Path, max_chars: int = 8000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    return text[-max_chars:]


def list_run_dirs(runs_root: Path, limit: int = 30) -> List[Path]:
    if not runs_root.exists():
        return []
    dirs = [p for p in runs_root.iterdir() if p.is_dir()]
    dirs.sort(key=lambda p: p.name, reverse=True)
    return dirs[:limit]


def build_history_rows(runs_root: Path, limit: int = 50) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for run_dir in list_run_dirs(runs_root, limit=limit):
        result_payload = read_json_if_exists(run_dir / "result.json")
        meta_payload = read_json_if_exists(run_dir / "run_meta.json")
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
    return rows


def build_export_rows(run_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name in STANDARD_EXPORT_NAMES:
        path = run_dir / name
        if path.exists():
            rows.append(
                {
                    "name": name,
                    "type": "dir" if path.is_dir() else "file",
                    "exists": True,
                    "path": str(path),
                }
            )

    for child in sorted(run_dir.iterdir(), key=lambda p: p.name.lower()) if run_dir.exists() else []:
        if child.name in STANDARD_EXPORT_NAMES:
            continue
        if child.is_dir() and (child.name.startswith("report") or (child / "index.html").exists()):
            rows.append(
                {
                    "name": child.name,
                    "type": "dir",
                    "exists": True,
                    "path": str(child),
                }
            )
    return rows


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True) if value not in (None, {}, []) else ""


def _json_parse_or_empty(text: Any) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _mapping_from_item(item: Dict[str, Any]) -> Dict[str, str]:
    mapping = dict(item.get("schema") or {})
    for std_key in ["datetime", "open", "high", "low", "close", "volume", "openinterest"]:
        raw_key = item.get(f"{std_key}_col")
        if raw_key:
            mapping[std_key] = str(raw_key)
    return {str(key): str(value) for key, value in mapping.items() if value not in (None, "")}


def _mapping_from_row(raw: Dict[str, Any]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for std_key in ["datetime", "open", "high", "low", "close", "volume", "openinterest"]:
        value = str(raw.get(f"{std_key}_col") or "").strip()
        if value:
            mapping[std_key] = value
    return mapping


def _quote_postgres_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("PostgreSQL 标识符不能为空")
    return '"' + text.replace('"', '""') + '"'


def _pg_bar_bucket_expr(ts_col: str, timeframe: str, compression: int) -> str:
    tf = str(timeframe or "minutes").lower()
    comp = int(compression or 1)
    if tf in {"m", "min", "mins", "minute", "minutes"}:
        if comp == 1:
            return f"date_trunc('minute', {ts_col})"
        elif comp in (5, 10, 15, 30):
            return f"date_trunc('hour', {ts_col}) + interval '1 min' * ((extract(minute from {ts_col})::int / {comp}) * {comp})"
        elif comp == 60:
            return f"date_trunc('hour', {ts_col})"
        else:
            return f"date_trunc('hour', {ts_col}) + interval '1 min' * ((extract(minute from {ts_col})::int / {comp}) * {comp})"
    elif tf in {"h", "hour", "hours"}:
        if comp == 1:
            return f"date_trunc('hour', {ts_col})"
        else:
            return f"date_trunc('day', {ts_col}) + interval '1 hour' * ((extract(hour from {ts_col})::int / {comp}) * {comp})"
    elif tf in {"d", "day", "days"}:
        return f"date_trunc('day', {ts_col})"
    else:
        return f"date_trunc('minute', {ts_col})"


def build_postgres_tick_to_bar_query(item: Dict[str, Any]) -> tuple[str, List[Any]]:
    """Build a server-side tick-to-bar aggregation query for PostgreSQL.

    Assumes CTP-style tick table: trading_day (date), update_time (time),
    update_millisec (int), last_price, volume, instrument_id (text).
    """
    code = item.get("code") or item.get("ts_code") or item.get("symbol")
    start = item.get("start")
    end = item.get("end")
    code_col = str(item.get("code_col") or "instrument_id").strip()
    table_name = str(item.get("table_name") or item.get("table") or "tick_data").strip()
    table_schema = str(item.get("table_schema") or item.get("pg_schema") or "public").strip() or "public"
    timeframe = str(item.get("timeframe") or "minutes").strip()
    compression = int(item.get("compression", 1) or 1)

    # Build timestamp expression from CTP columns
    ts_expr = "trading_day + update_time"
    bucket = _pg_bar_bucket_expr(ts_expr, timeframe, compression)

    # Safe identifiers
    tbl = f"{_quote_postgres_identifier(table_schema)}.{_quote_postgres_identifier(table_name)}"
    code_id = _quote_postgres_identifier(code_col)

    query = f"""
        WITH ranked AS (
            SELECT
                {bucket} AS bar_time,
                last_price AS price,
                volume AS vol,
                ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY trading_day, update_time, update_millisec) AS rn_asc,
                ROW_NUMBER() OVER (PARTITION BY {bucket} ORDER BY trading_day DESC, update_time DESC, update_millisec DESC) AS rn_desc
            FROM {tbl}
            WHERE {code_id} = %s
    """
    params: List[Any] = [code]
    if start:
        query += " AND trading_day >= %s"
        params.append(start)
    if end:
        query += " AND trading_day <= %s"
        params.append(end)
    query += f"""
        )
        SELECT
            bar_time AS datetime,
            MAX(CASE WHEN rn_asc = 1 THEN price END) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            MAX(CASE WHEN rn_desc = 1 THEN price END) AS close,
            SUM(vol) AS volume
        FROM ranked
        GROUP BY bar_time
        ORDER BY bar_time
    """
    return query, params


def build_postgres_query(item: Dict[str, Any]) -> tuple[str, List[Any]]:
    table_name = str(item.get("table_name") or item.get("table") or "").strip()
    table_schema = str(item.get("table_schema") or item.get("pg_schema") or "public").strip() or "public"
    schema_map = _mapping_from_item(item)
    datetime_col = str(schema_map.get("datetime") or item.get("datetime_col") or "").strip()
    code_col = str(item.get("code_col") or "").strip()
    code = item.get("code") or item.get("ts_code") or item.get("symbol")
    start = item.get("start")
    end = item.get("end")
    data_type = str(item.get("data_type") or item.get("type") or "bar").strip().lower()
    is_tick = data_type in {"tick", "ticks"}

    if table_name:
        # For known tick tables, always use the default tick query shape
        # instead of SELECT * (which would pull millions of rows)
        if table_name in {"tick_data", "ticks"}:
            query = f"""
                SELECT trading_day, update_time, update_millisec, instrument_id, last_price, volume
                FROM {_quote_postgres_identifier(table_schema)}.{_quote_postgres_identifier(table_name)}
                WHERE instrument_id = %s
            """
            params: List[Any] = [code]
            if start:
                query += " AND trading_day >= %s"
                params.append(start)
            if end:
                query += " AND trading_day <= %s"
                params.append(end)
            query += " ORDER BY trading_day, update_time, update_millisec"
            return query, params

        query = f"SELECT * FROM {_quote_postgres_identifier(table_schema)}.{_quote_postgres_identifier(table_name)} WHERE 1=1"
        params: List[Any] = []
        if code_col and code not in (None, ""):
            query += f" AND {_quote_postgres_identifier(code_col)} = %s"
            params.append(code)
        if datetime_col and start:
            query += f" AND {_quote_postgres_identifier(datetime_col)} >= %s"
            params.append(start)
        if datetime_col and end:
            query += f" AND {_quote_postgres_identifier(datetime_col)} <= %s"
            params.append(end)
        if datetime_col:
            query += f" ORDER BY {_quote_postgres_identifier(datetime_col)}"
        return query, params

    # Default table inference
    if is_tick:
        # Tick data default: tick_data table
        query = """
            SELECT trading_day, update_time, update_millisec, instrument_id, last_price, volume
            FROM tick_data
            WHERE instrument_id = %s
        """
        params = [code]
        if start:
            query += " AND trading_day >= %s"
            params.append(start)
        if end:
            query += " AND trading_day <= %s"
            params.append(end)
        query += " ORDER BY trading_day, update_time, update_millisec"
        return query, params

    # Default table inference: quant_lab only has tick_data, no bar_data/instrument tables
    # When table_name is empty and user didn't specify data_type=bar, fall back to tick_data
    query = """
        SELECT trading_day, update_time, update_millisec, instrument_id, last_price, volume
        FROM tick_data
        WHERE instrument_id = %s
    """
    params = [code]
    if start:
        query += " AND trading_day >= %s"
        params.append(start)
    if end:
        query += " AND trading_day <= %s"
        params.append(end)
    query += " ORDER BY trading_day, update_time, update_millisec"
    return query, params


def list_postgres_columns(
    psycopg2_module: Any,
    pg_cfg: Dict[str, Any],
    database: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> List[Dict[str, Any]]:
    table_name = str(table or "").strip()
    if not table_name:
        return []
    table_schema = str(schema or pg_cfg.get("search_path") or "public").strip() or "public"
    with open_postgres_connection(psycopg2_module, pg_cfg, database=database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_schema, table_name),
            )
            return [
                {"column_name": str(row[0]), "data_type": str(row[1]), "is_nullable": str(row[2])}
                for row in cur.fetchall()
                if len(row) >= 3
            ]


def build_data_source_rows(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in (cfg.get("data", []) or []):
        source = normalize_source_name(item.get("source"))
        extra = dict(item)
        schema_map = _mapping_from_item(item)
        for key in [
            "name",
            "symbol",
            "source",
            "role",
            "csv",
            "excel",
            "cache_csv",
            "code",
            "code_col",
            "ts_code",
            "timeframe",
            "compression",
            "start",
            "end",
            "start_date",
            "end_date",
            "sheet_name",
            "table_schema",
            "pg_schema",
            "table_name",
            "table",
            "datetime_col",
            "open_col",
            "high_col",
            "low_col",
            "close_col",
            "volume_col",
            "openinterest_col",
            "schema",
            "api",
            "freq",
        ]:
            extra.pop(key, None)
        rows.append(
            {
                "name": str(item.get("name") or ""),
                "symbol": str(item.get("symbol") or item.get("name") or ""),
                "source": display_source_name(source),
                "role": str(item.get("role") or "exec"),
                "file_path": str(item.get("csv") or item.get("excel") or item.get("cache_csv") or ""),
                "code": str(item.get("code") or item.get("ts_code") or ""),
                "code_col": str(item.get("code_col") or ""),
                "timeframe": str(item.get("timeframe") or "days"),
                "compression": item.get("compression", 1),
                "start": str(item.get("start") or item.get("start_date") or ""),
                "end": str(item.get("end") or item.get("end_date") or ""),
                "sheet": str(item.get("sheet_name") or ""),
                "table_schema": str(item.get("table_schema") or item.get("pg_schema") or ""),
                "table_name": str(item.get("table_name") or item.get("table") or ""),
                "datetime_col": str(schema_map.get("datetime") or item.get("datetime_col") or ""),
                "open_col": str(schema_map.get("open") or item.get("open_col") or ""),
                "high_col": str(schema_map.get("high") or item.get("high_col") or ""),
                "low_col": str(schema_map.get("low") or item.get("low_col") or ""),
                "close_col": str(schema_map.get("close") or item.get("close_col") or ""),
                "volume_col": str(schema_map.get("volume") or item.get("volume_col") or ""),
                "api": str(item.get("api") or ""),
                "freq": str(item.get("freq") or ""),
                "extra_json": _json_text(extra),
            }
        )
    return rows


def data_source_rows_to_items(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for raw in rows or []:
        name = str(raw.get("name") or "").strip()
        if not name:
            continue
        source = normalize_source_name(raw.get("source"))
        item = _json_parse_or_empty(raw.get("extra_json"))
        item["name"] = name
        item["symbol"] = str(raw.get("symbol") or name).strip() or name
        item["source"] = source
        item["role"] = str(raw.get("role") or "exec").strip() or "exec"

        timeframe = str(raw.get("timeframe") or "days").strip() or "days"
        item["timeframe"] = timeframe
        item["compression"] = coerce_text_value(str(raw.get("compression") or 1)) or 1

        file_path = str(raw.get("file_path") or "").strip()
        code = str(raw.get("code") or "").strip()
        code_col = str(raw.get("code_col") or "").strip()
        start = str(raw.get("start") or "").strip()
        end = str(raw.get("end") or "").strip()
        sheet = str(raw.get("sheet") or "").strip()
        table_schema = str(raw.get("table_schema") or "").strip()
        table_name = str(raw.get("table_name") or "").strip()
        api = str(raw.get("api") or "").strip()
        freq = str(raw.get("freq") or "").strip()
        schema_map = _mapping_from_row(raw)

        for key in [
            "csv",
            "excel",
            "cache_csv",
            "code",
            "code_col",
            "ts_code",
            "start",
            "end",
            "start_date",
            "end_date",
            "sheet_name",
            "table_schema",
            "pg_schema",
            "table_name",
            "table",
            "datetime_col",
            "open_col",
            "high_col",
            "low_col",
            "close_col",
            "volume_col",
            "openinterest_col",
            "schema",
            "api",
            "freq",
        ]:
            item.pop(key, None)

        if source == "csv" and file_path:
            item["csv"] = file_path
        elif source == "excel" and file_path:
            item["excel"] = file_path
        elif source == "tushare":
            if file_path:
                item["cache_csv"] = file_path
            if code:
                item["ts_code"] = code
            if start:
                item["start_date"] = start
            if end:
                item["end_date"] = end
            if api:
                item["api"] = api
            if freq:
                item["freq"] = freq

        if source in {"csv", "excel", "postgres"} and code:
            item["code"] = code
        if source == "postgres" and code_col:
            item["code_col"] = code_col
        if source in {"csv", "excel", "postgres"} and start:
            item["start"] = start
        if source in {"csv", "excel", "postgres"} and end:
            item["end"] = end
        if source == "excel" and sheet:
            item["sheet_name"] = sheet
        if source == "postgres":
            if table_schema:
                item["table_schema"] = table_schema
            if table_name:
                item["table_name"] = table_name
            if schema_map:
                item["schema"] = schema_map
        if source in {"csv", "excel"} and api:
            item["api"] = api
        if source in {"csv", "excel"} and freq:
            item["freq"] = freq

        items.append(item)
    return items


def build_symbol_spec_rows(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for symbol, spec in (cfg.get("symbols", {}) or {}).items():
        extra = dict(spec or {})
        for key in ["tick_size", "size_step", "min_size", "price_precision", "mult", "commission", "margin", "commtype", "margin_rate"]:
            extra.pop(key, None)
        rows.append(
            {
                "symbol": str(symbol),
                "tick_size": (spec or {}).get("tick_size", ""),
                "size_step": (spec or {}).get("size_step", ""),
                "min_size": (spec or {}).get("min_size", ""),
                "price_precision": (spec or {}).get("price_precision", ""),
                "mult": (spec or {}).get("mult", ""),
                "commission": (spec or {}).get("commission", ""),
                "margin": (spec or {}).get("margin", ""),
                "commtype": (spec or {}).get("commtype", ""),
                "margin_rate": (spec or {}).get("margin_rate", ""),
                "extra_json": _json_text(extra),
            }
        )
    return rows


def symbol_spec_rows_to_config(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    cfg: Dict[str, Dict[str, Any]] = {}
    for raw in rows or []:
        symbol = str(raw.get("symbol") or "").strip()
        if not symbol:
            continue
        spec = _json_parse_or_empty(raw.get("extra_json"))
        for key in ["tick_size", "size_step", "min_size", "price_precision", "mult", "commission", "margin", "margin_rate"]:
            value = coerce_text_value(str(raw.get(key, "")))
            if value != "":
                spec[key] = value
        commtype = str(raw.get("commtype") or "").strip()
        if commtype:
            spec["commtype"] = commtype
        cfg[symbol] = spec
    return cfg


def build_chart_points(rows: List[Dict[str, Any]], value_key: str = "value", label_key: str = "datetime") -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows or []):
        try:
            y = float(row.get(value_key))
        except Exception:
            continue
        label = str(row.get(label_key) or idx)
        points.append({"x": idx, "y": y, "label": label})
    return points


def build_drawdown_points(rows: List[Dict[str, Any]], value_key: str = "value", label_key: str = "datetime") -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    peak: Optional[float] = None
    for idx, row in enumerate(rows or []):
        try:
            value = float(row.get(value_key))
        except Exception:
            continue
        peak = value if peak is None else max(peak, value)
        drawdown_pct = 0.0 if not peak else (value / peak - 1.0) * 100.0
        label = str(row.get(label_key) or idx)
        points.append({"x": idx, "y": drawdown_pct, "label": label})
    return points


def _trade_pnl_values(trades: List[Dict[str, Any]], pnl_key: str = "pnlcomm") -> List[float]:
    values: List[float] = []
    for row in trades or []:
        raw = row.get(pnl_key, row.get("pnl"))
        try:
            values.append(float(raw))
        except Exception:
            continue
    return values


def build_trade_distribution_rows(trades: List[Dict[str, Any]], pnl_key: str = "pnlcomm", bucket_count: int = 8) -> List[Dict[str, Any]]:
    values = _trade_pnl_values(trades, pnl_key=pnl_key)
    if not values:
        return []
    bucket_count = max(1, int(bucket_count or 1))
    lower = min(values)
    upper = max(values)
    if math.isclose(lower, upper):
        return [{"bucket": f"{lower:.2f}", "count": len(values), "lower": lower, "upper": upper, "center": lower}]

    step = (upper - lower) / bucket_count
    rows: List[Dict[str, Any]] = []
    for idx in range(bucket_count):
        bucket_lower = lower + idx * step
        bucket_upper = upper if idx == bucket_count - 1 else lower + (idx + 1) * step
        if idx == bucket_count - 1:
            count = sum(1 for value in values if bucket_lower <= value <= bucket_upper)
        else:
            count = sum(1 for value in values if bucket_lower <= value < bucket_upper)
        rows.append(
            {
                "bucket": f"{bucket_lower:.2f} ~ {bucket_upper:.2f}",
                "count": count,
                "lower": bucket_lower,
                "upper": bucket_upper,
                "center": (bucket_lower + bucket_upper) / 2.0,
            }
        )
    return rows


def build_symbol_pnl_rows(trades: List[Dict[str, Any]], symbol_key: str = "symbol", pnl_key: str = "pnlcomm") -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in trades or []:
        symbol = str(row.get(symbol_key) or "未知品种").strip() or "未知品种"
        raw = row.get(pnl_key, row.get("pnl"))
        try:
            pnl = float(raw)
        except Exception:
            pnl = 0.0
        item = grouped.setdefault(symbol, {"symbol": symbol, "trade_count": 0, "net_pnl": 0.0, "avg_pnl": 0.0})
        item["trade_count"] += 1
        item["net_pnl"] += pnl
    rows = list(grouped.values())
    for row in rows:
        trade_count = int(row.get("trade_count") or 0)
        row["avg_pnl"] = (float(row["net_pnl"]) / trade_count) if trade_count else 0.0
    rows.sort(key=lambda item: (-float(item.get("net_pnl") or 0.0), str(item.get("symbol") or "")))
    return rows


def infer_data_rows_from_files(paths: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw_path in paths or []:
        path = Path(str(raw_path)).expanduser()
        suffix = path.suffix.lower()
        if suffix not in {".csv", ".xls", ".xlsx"}:
            continue
        source = "excel" if suffix in {".xls", ".xlsx"} else "csv"
        stem = path.stem
        rows.append(
            {
                "name": stem,
                "symbol": stem,
                "source": source,
                "role": "exec",
                "file_path": str(path),
                "code": "",
                "timeframe": "days",
                "compression": 1,
                "start": "",
                "end": "",
                "sheet": "",
                "api": "",
                "freq": "",
                "extra_json": "",
            }
        )
    return rows


def _secret_from_cfg(
    cfg: Dict[str, Any],
    value_key: str = "password",
    env_key: str = "password_env",
    default_env_name: str = "",
) -> Any:
    env_name = str(cfg.get(env_key) or default_env_name or "").strip()
    if env_name:
        env_value = os.environ.get(env_name)
        if env_value not in (None, ""):
            return env_value
    return cfg.get(value_key)



def normalize_postgres_ssh_cfg(pg_cfg: Dict[str, Any]) -> Dict[str, Any]:
    ssh_cfg = dict(pg_cfg.get("ssh") or {})
    enabled_raw = ssh_cfg.get("enabled", False)
    enabled = bool(enabled_raw) if isinstance(enabled_raw, bool) else str(enabled_raw).strip().lower() in {"1", "true", "yes", "on"}
    host = str(ssh_cfg.get("host") or pg_cfg.get("host") or "").strip()
    if enabled and not host:
        raise ValueError("启用 SSH 隧道时必须填写 ssh.host")
    return {
        "enabled": enabled,
        "host": host,
        "port": int(ssh_cfg.get("port", 22) or 22),
        "user": str(ssh_cfg.get("user") or "").strip(),
        "password": _secret_from_cfg(ssh_cfg),
        "password_env": str(ssh_cfg.get("password_env") or "").strip(),
        "pkey_path": str(ssh_cfg.get("pkey_path") or ssh_cfg.get("private_key_path") or "").strip(),
        "pkey_passphrase": _secret_from_cfg(ssh_cfg, value_key="pkey_passphrase", env_key="pkey_passphrase_env"),
        "pkey_passphrase_env": str(ssh_cfg.get("pkey_passphrase_env") or "").strip(),
        "remote_bind_host": str(ssh_cfg.get("remote_bind_host") or "127.0.0.1").strip() or "127.0.0.1",
        "remote_bind_port": int(ssh_cfg.get("remote_bind_port", pg_cfg.get("port", 5432)) or pg_cfg.get("port", 5432) or 5432),
        "local_bind_host": str(ssh_cfg.get("local_bind_host") or "127.0.0.1").strip() or "127.0.0.1",
        "local_bind_port": int(ssh_cfg.get("local_bind_port", 0) or 0),
    }



def build_postgres_connect_kwargs(
    pg_cfg: Dict[str, Any],
    database: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
) -> Dict[str, Any]:
    password = _secret_from_cfg(pg_cfg, default_env_name="PGPASSWORD")
    kwargs: Dict[str, Any] = {
        "host": host or pg_cfg.get("host", "localhost"),
        "port": int(port if port is not None else (pg_cfg.get("port", 5432) or 5432)),
        "dbname": database or pg_cfg.get("dbname", "postgres"),
        "user": pg_cfg.get("user", "postgres"),
        "password": password,
        "connect_timeout": int(pg_cfg.get("connect_timeout", 5) or 5),
    }
    sslmode = str(pg_cfg.get("sslmode") or "").strip()
    if sslmode:
        kwargs["sslmode"] = sslmode
    # Combine search_path and statement_timeout into options
    options_parts: List[str] = []
    search_path = str(pg_cfg.get("search_path") or "").strip()
    if search_path:
        options_parts.append(f"-c search_path={search_path}")
    # Default 60s query timeout to avoid hanging on missing indexes
    stmt_timeout = int(pg_cfg.get("statement_timeout", 60) or 60)
    if stmt_timeout > 0:
        options_parts.append(f"-c statement_timeout={stmt_timeout * 1000}")
    if options_parts:
        kwargs["options"] = " ".join(options_parts)
    return kwargs



def _build_sshtunnel_forwarder_kwargs(ssh_cfg: Dict[str, Any]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "ssh_address_or_host": (ssh_cfg["host"], int(ssh_cfg["port"])),
        "ssh_username": ssh_cfg.get("user") or None,
        "remote_bind_address": (ssh_cfg["remote_bind_host"], int(ssh_cfg["remote_bind_port"])),
        "local_bind_address": (ssh_cfg["local_bind_host"], int(ssh_cfg["local_bind_port"])),
    }
    if ssh_cfg.get("password") not in (None, ""):
        kwargs["ssh_password"] = ssh_cfg.get("password")
    if ssh_cfg.get("pkey_path"):
        kwargs["ssh_pkey"] = ssh_cfg.get("pkey_path")
    if ssh_cfg.get("pkey_passphrase") not in (None, ""):
        kwargs["ssh_private_key_password"] = ssh_cfg.get("pkey_passphrase")
    return kwargs



def load_sshtunnel_module() -> Any:
    try:
        import sshtunnel
    except AttributeError as exc:  # pragma: no cover
        message = str(exc)
        if "paramiko" in message and "DSSKey" in message:
            raise ImportError(
                "sshtunnel 与当前 paramiko 版本不兼容（检测到缺少 DSSKey）。"
                "请重新安装兼容版本：pip install --upgrade \"paramiko<4\" \"sshtunnel>=0.4,<0.5\""
            ) from exc
        raise ImportError(f"sshtunnel 导入失败：{exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise ImportError("启用 SSH 隧道需要安装 sshtunnel（pip install sshtunnel）") from exc
    return sshtunnel



def _should_retry_without_ssl(exc: Exception) -> bool:
    message = str(exc).lower()
    return "does not support ssl" in message and "ssl was required" in message


@contextmanager
def open_postgres_connection(
    psycopg2_module: Any,
    pg_cfg: Dict[str, Any],
    database: Optional[str] = None,
    sshtunnel_module: Any = None,
) -> Iterator[Any]:
    ssh_cfg = normalize_postgres_ssh_cfg(pg_cfg)
    tunnel_cm = nullcontext(None)
    if ssh_cfg.get("enabled"):
        if sshtunnel_module is None:
            sshtunnel_module = load_sshtunnel_module()
        tunnel = sshtunnel_module.SSHTunnelForwarder(**_build_sshtunnel_forwarder_kwargs(ssh_cfg))
        tunnel.start()
        tunnel_cm = _managed_tunnel(tunnel)

    with tunnel_cm as tunnel:
        connect_kwargs = build_postgres_connect_kwargs(
            pg_cfg,
            database=database,
            host=(getattr(tunnel, "local_bind_host", None) or ssh_cfg.get("local_bind_host")) if tunnel else None,
            port=(getattr(tunnel, "local_bind_port", None) or ssh_cfg.get("local_bind_port")) if tunnel else None,
        )
        try:
            conn = psycopg2_module.connect(**connect_kwargs)
        except Exception as exc:
            if connect_kwargs.get("sslmode") == "disable" or not _should_retry_without_ssl(exc):
                raise
            retry_kwargs = dict(connect_kwargs)
            retry_kwargs["sslmode"] = "disable"
            conn = psycopg2_module.connect(**retry_kwargs)
        try:
            yield conn
        finally:
            conn.close()



@contextmanager
def _managed_tunnel(tunnel: Any) -> Iterator[Any]:
    try:
        yield tunnel
    finally:
        try:
            tunnel.stop()
        except Exception:
            pass



def list_postgres_databases(psycopg2_module: Any, pg_cfg: Dict[str, Any]) -> List[str]:
    with open_postgres_connection(psycopg2_module, pg_cfg, database=str(pg_cfg.get("maintenance_db") or "postgres")) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                ORDER BY datname
                """
            )
            return sorted({str(row[0]) for row in cur.fetchall() if row and row[0]})



def list_postgres_tables(psycopg2_module: Any, pg_cfg: Dict[str, Any], database: Optional[str] = None) -> List[Dict[str, Any]]:
    with open_postgres_connection(psycopg2_module, pg_cfg, database=database) as conn:
        with conn.cursor() as cur:
            search_path = str(pg_cfg.get("search_path") or "").strip()
            if search_path:
                cur.execute(
                    """
                    SELECT table_schema, table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    ORDER BY table_schema, table_name
                    """,
                    (search_path,),
                )
            else:
                cur.execute(
                    """
                    SELECT table_schema, table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY table_schema, table_name
                    """
                )
            return [
                {"schema": str(row[0]), "table": str(row[1]), "type": str(row[2])}
                for row in cur.fetchall()
                if len(row) >= 3
            ]


def build_market_watch_rows(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for item in (cfg.get("data", []) or []):
        rows.append(
            {
                "data_name": str(item.get("name") or "-"),
                "symbol": str(item.get("symbol") or item.get("name") or "-"),
                "source": display_source_name(item.get("source") or "-"),
                "role": str(item.get("role") or "exec"),
                "period": f"{item.get('timeframe', 'days')}/{item.get('compression', 1)}",
            }
        )

    for item in (cfg.get("resample", []) or []):
        rows.append(
            {
                "data_name": str(item.get("name") or "-"),
                "symbol": str(item.get("symbol") or item.get("name") or "-"),
                "source": f"resample:{item.get('source', '-')}",
                "role": str(item.get("role") or "signal"),
                "period": f"{item.get('timeframe', 'minutes')}/{item.get('compression', 1)}",
            }
        )

    return rows


def write_temp_cfg(cfg: Dict[str, Any]) -> Path:
    tmp = tempfile.NamedTemporaryFile(prefix="mt4_desktop_cfg_", suffix=".yaml", delete=False, mode="w", encoding="utf-8")
    try:
        yaml.safe_dump(cfg, tmp, allow_unicode=True, sort_keys=False)
        tmp.flush()
    finally:
        tmp.close()
    return Path(tmp.name).resolve()
