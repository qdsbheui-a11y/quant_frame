from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import backtrader as bt
import pandas as pd

from my_bt_lab.app.desktop_support import (
    build_postgres_connect_kwargs,
    build_postgres_query,
    build_postgres_tick_to_bar_query,
    open_postgres_connection,
)
from my_bt_lab.data.normalize import normalize_ohlcv_df
from my_bt_lab.data.tick_aggregator import aggregate_tick_to_bar, is_tick_dataframe
from my_bt_lab.data.tushare_loader import fetch_tushare_ohlcv

logger = logging.getLogger(__name__)


class PandasOHLCVData(bt.feeds.PandasData):
    params = (
        ("datetime", None),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("openinterest", "openinterest"),
    )


def normalize_source_name(source: Any) -> str:
    text = str(source or "csv").strip().lower()
    if text in {"db", "postgres", "postgresql"}:
        return "postgres"
    if text in {"excel", "xlsx", "xls"}:
        return "excel"
    if text in {"csv", "tushare"}:
        return text
    return text or "csv"


def _parse_bt_timeframe(value: Optional[str]) -> bt.TimeFrame:
    text = str(value or "days").strip().lower()
    mapping = {
        "m": bt.TimeFrame.Minutes,
        "min": bt.TimeFrame.Minutes,
        "mins": bt.TimeFrame.Minutes,
        "minute": bt.TimeFrame.Minutes,
        "minutes": bt.TimeFrame.Minutes,
        "h": bt.TimeFrame.Minutes,
        "hour": bt.TimeFrame.Minutes,
        "hours": bt.TimeFrame.Minutes,
        "d": bt.TimeFrame.Days,
        "day": bt.TimeFrame.Days,
        "days": bt.TimeFrame.Days,
        "w": bt.TimeFrame.Weeks,
        "week": bt.TimeFrame.Weeks,
        "weeks": bt.TimeFrame.Weeks,
        "mo": bt.TimeFrame.Months,
        "month": bt.TimeFrame.Months,
        "months": bt.TimeFrame.Months,
    }
    return mapping.get(text, bt.TimeFrame.Days)


def df_to_bt_pandasdata(
    df: pd.DataFrame,
    timeframe: Optional[str] = None,
    compression: Optional[int] = None,
) -> bt.feeds.PandasData:
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"])
    out = out.sort_values("datetime")
    out = out.drop_duplicates(subset=["datetime"], keep="last")

    if "volume" not in out.columns:
        out["volume"] = 0.0
    if "openinterest" not in out.columns:
        out["openinterest"] = 0.0

    out = out[["datetime", "open", "high", "low", "close", "volume", "openinterest"]]
    out = out.set_index("datetime")

    return PandasOHLCVData(
        dataname=out,
        timeframe=_parse_bt_timeframe(timeframe),
        compression=int(compression or 1),
    )


def _infer_ts_code_from_csv_path(csv_path: Path) -> Optional[str]:
    stem = csv_path.stem
    m = re.search(r"(\d{6})[_-]([A-Za-z]{2})", stem)
    if not m:
        return None
    return f"{m.group(1)}.{m.group(2).upper()}"


def _build_read_table_kwargs(item: Dict[str, Any]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if item.get("header_row") is not None:
        kwargs["header"] = item["header_row"]
    if item.get("skiprows") is not None:
        kwargs["skiprows"] = item["skiprows"]
    return kwargs


def _normalize_loaded_df(df_raw: pd.DataFrame, item: Dict[str, Any]) -> pd.DataFrame:
    return normalize_ohlcv_df(
        df_raw=df_raw,
        schema=item.get("schema"),
        datetime_format=item.get("datetime_format"),
        date_col=item.get("date_col"),
        time_col=item.get("time_col"),
        datetime_col=item.get("datetime_col"),
    )


def load_csv_item(
    item: Dict[str, Any],
    project_root: Path,
    cfg: Optional[Dict[str, Any]] = None,
) -> bt.feeds.PandasData:
    csv_path = (project_root / item["csv"]).resolve()

    if not csv_path.exists():
        cfg = cfg or {}
        ts_code = item.get("ts_code") or _infer_ts_code_from_csv_path(csv_path)
        if ts_code and cfg.get("tushare"):
            logger.info("CSV缓存不存在，尝试从Tushare拉取并生成缓存: %s (ts_code=%s)", csv_path, ts_code)
            t_item = dict(item)
            t_item["ts_code"] = ts_code
            t_item["cache_csv"] = item["csv"]

            df = fetch_tushare_ohlcv(
                item=t_item,
                global_cfg=cfg.get("tushare", {}),
                project_root=project_root,
            )
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_path, index=False, encoding="utf-8")
            logger.info("Tushare数据加载成功，已生成CSV缓存: %s rows=%d", csv_path, len(df))
            return df_to_bt_pandasdata(
                df,
                timeframe=item.get("timeframe"),
                compression=item.get("compression"),
            )

        raise FileNotFoundError(
            f"找不到数据文件: {csv_path}\n"
            f"并且无法自动从Tushare生成缓存。"
        )

    read_csv_kwargs: Dict[str, Any] = {
        "sep": item.get("sep", ","),
        "encoding": item.get("encoding", "utf-8"),
    }
    read_csv_kwargs.update(_build_read_table_kwargs(item))
    df_raw = pd.read_csv(csv_path, **read_csv_kwargs)

    # Detect tick data
    data_type = str(item.get("data_type") or item.get("type") or "bar").strip().lower()
    is_tick = data_type in {"tick", "ticks"}
    if not is_tick and is_tick_dataframe(df_raw):
        logger.info("CSV 自动检测到 tick 数据，即将聚合为 bar")
        is_tick = True

    if is_tick:
        bar_df = aggregate_tick_to_bar(
            df_raw,
            timeframe=item.get("timeframe"),
            compression=item.get("compression"),
            schema=item.get("schema"),
            datetime_format=item.get("datetime_format"),
        )
        df = normalize_ohlcv_df(bar_df, schema=None, datetime_format=None)
    else:
        df = _normalize_loaded_df(df_raw, item)

    return df_to_bt_pandasdata(
        df,
        timeframe=item.get("timeframe"),
        compression=item.get("compression"),
    )


def load_excel_item(
    item: Dict[str, Any],
    project_root: Path,
    cfg: Optional[Dict[str, Any]] = None,
) -> bt.feeds.PandasData:
    excel_key = "excel" if item.get("excel") else "csv"
    excel_path = (project_root / item[excel_key]).resolve()
    if not excel_path.exists():
        raise FileNotFoundError(f"找不到Excel文件: {excel_path}")

    read_excel_kwargs = _build_read_table_kwargs(item)
    if item.get("sheet_name") is not None:
        read_excel_kwargs["sheet_name"] = item.get("sheet_name")
    if item.get("engine"):
        read_excel_kwargs["engine"] = item.get("engine")
    df_raw = pd.read_excel(excel_path, **read_excel_kwargs)
    df = _normalize_loaded_df(df_raw, item)
    return df_to_bt_pandasdata(
        df,
        timeframe=item.get("timeframe"),
        compression=item.get("compression"),
    )


def load_postgres_item(
    item: Dict[str, Any],
    project_root: Path,
    cfg: Optional[Dict[str, Any]] = None,
) -> bt.feeds.PandasData:
    cfg = cfg or {}
    pg_cfg = cfg.get("postgres", {}) or {}

    try:
        import psycopg2
    except Exception as exc:  # pragma: no cover
        raise ImportError("使用数据库数据源需要安装 psycopg2 或 psycopg2-binary") from exc

    code = item.get("code") or item.get("ts_code") or item.get("symbol")
    start = item.get("start")
    end = item.get("end")
    data_type = str(item.get("data_type") or item.get("type") or "bar").strip().lower()
    is_tick = data_type in {"tick", "ticks"}

    # ------------------------------------------------------------------
    # Tick data: try server-side aggregation first (avoids pulling
    # millions of rows over SSH)
    # ------------------------------------------------------------------
    table_name = str(item.get("table_name") or item.get("table") or "").strip()
    is_tick_table = table_name in {"tick_data", "ticks"} or is_tick
    if is_tick_table:
        logger.info("Tick 数据尝试数据库端聚合: code=%s table=%s", code, table_name or "tick_data")
        try:
            query, params = build_postgres_tick_to_bar_query(item)
            logger.info("服务器端聚合 SQL:\n%s", query)
            logger.info("服务器端聚合参数: %s", params)
            with open_postgres_connection(psycopg2, pg_cfg) as conn:
                df_raw = pd.read_sql_query(query, conn, params=params)
            if not df_raw.empty:
                logger.info("数据库端聚合成功: code=%s rows=%d", code, len(df_raw))
                df = _normalize_loaded_df(df_raw, item)
                return df_to_bt_pandasdata(
                    df,
                    timeframe=item.get("timeframe"),
                    compression=item.get("compression"),
                )
            else:
                logger.warning("数据库端聚合返回空结果: code=%s", code)
        except Exception as exc:
            logger.warning("数据库端聚合失败，回退到本地聚合: %s", exc)

    query, params = build_postgres_query(item)
    logger.info("数据库查询 SQL:\n%s", query)
    logger.info("数据库查询参数: %s", params)

    with open_postgres_connection(psycopg2, pg_cfg) as conn:
        df_raw = pd.read_sql_query(query, conn, params=params)

    if df_raw.empty:
        raise ValueError(f"未找到数据: code={code}, start={start}, end={end}")

    logger.info("数据库数据加载成功: code=%s rows=%d type=%s", code, len(df_raw), data_type)

    # Auto-detect tick data if user didn't specify
    if not is_tick and is_tick_dataframe(df_raw):
        logger.info("自动检测到 tick 数据，即将聚合为 bar")
        is_tick = True

    if is_tick:
        # Tick data: aggregate to bars
        bar_df = aggregate_tick_to_bar(
            df_raw,
            timeframe=item.get("timeframe"),
            compression=item.get("compression"),
            schema=item.get("schema"),
            datetime_format=item.get("datetime_format"),
        )
        df = normalize_ohlcv_df(bar_df, schema=None, datetime_format=None)
    else:
        df = _normalize_loaded_df(df_raw, item)

    return df_to_bt_pandasdata(
        df,
        timeframe=item.get("timeframe"),
        compression=item.get("compression"),
    )


def load_data_item(item: Dict[str, Any], project_root: Path, cfg: Dict[str, Any]) -> bt.feeds.PandasData:
    source = normalize_source_name(item.get("source", "csv"))

    if source == "csv":
        return load_csv_item(item=item, project_root=project_root, cfg=cfg)

    if source == "excel":
        return load_excel_item(item=item, project_root=project_root, cfg=cfg)

    if source == "postgres":
        return load_postgres_item(item=item, project_root=project_root, cfg=cfg)

    if source == "tushare":
        df = fetch_tushare_ohlcv(
            item=item,
            global_cfg=cfg.get("tushare", {}),
            project_root=project_root,
        )
        logger.info("Tushare数据加载成功: ts_code=%s rows=%d", item.get("ts_code"), len(df))
        return df_to_bt_pandasdata(
            df,
            timeframe=item.get("timeframe"),
            compression=item.get("compression"),
        )

    raise ValueError(f"不支持的数据源 source={source}，当前仅支持 csv / excel / db(postgres) / tushare")
