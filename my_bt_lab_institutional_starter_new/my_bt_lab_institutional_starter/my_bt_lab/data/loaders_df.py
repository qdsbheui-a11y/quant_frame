from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from my_bt_lab.app.desktop_support import (
    build_postgres_query,
    build_postgres_tick_to_bar_query,
    open_postgres_connection,
)
from my_bt_lab.data.normalize import normalize_ohlcv_df
from my_bt_lab.data.tushare_loader import fetch_tushare_ohlcv

logger = logging.getLogger(__name__)


def _ensure_norm_df(df: pd.DataFrame) -> pd.DataFrame:
    required = {"datetime", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame缺少列: {missing}")
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"])
    out = out.sort_values("datetime")
    out = out.drop_duplicates(subset=["datetime"], keep="last")
    if "volume" not in out.columns:
        out["volume"] = 0.0
    return out.reset_index(drop=True)


def normalize_source_name(source: Any) -> str:
    text = str(source or "csv").strip().lower()
    if text in {"db", "postgres", "postgresql"}:
        return "postgres"
    if text in {"excel", "xlsx", "xls"}:
        return "excel"
    if text in {"csv", "tushare"}:
        return text
    return text or "csv"


def _build_read_table_kwargs(item: Dict[str, Any]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {}
    if item.get("header_row") is not None:
        kwargs["header"] = item["header_row"]
    if item.get("skiprows") is not None:
        kwargs["skiprows"] = item["skiprows"]
    return kwargs


def _normalize_loaded_df(df_raw: pd.DataFrame, item: Dict[str, Any]) -> pd.DataFrame:
    df_norm = normalize_ohlcv_df(
        df_raw=df_raw,
        schema=item.get("schema"),
        datetime_format=item.get("datetime_format"),
        date_col=item.get("date_col"),
        time_col=item.get("time_col"),
        datetime_col=item.get("datetime_col"),
    )
    return _ensure_norm_df(df_norm)


def load_csv_item_to_df(item: Dict[str, Any], project_root: Path) -> pd.DataFrame:
    csv_path = (project_root / item["csv"]).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"找不到数据文件: {csv_path}")

    read_csv_kwargs: Dict[str, Any] = {
        "sep": item.get("sep", ","),
        "encoding": item.get("encoding", "utf-8"),
    }
    read_csv_kwargs.update(_build_read_table_kwargs(item))
    df_raw = pd.read_csv(csv_path, **read_csv_kwargs)
    return _normalize_loaded_df(df_raw, item)


def load_excel_item_to_df(item: Dict[str, Any], project_root: Path) -> pd.DataFrame:
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
    return _normalize_loaded_df(df_raw, item)


def load_postgres_item_to_df(item: Dict[str, Any], project_root: Path, cfg: Dict[str, Any]) -> pd.DataFrame:
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

    # Try server-side aggregation for tick data first
    if is_tick and not item.get("table_name") and not item.get("table"):
        logger.info("Tick 数据尝试数据库端聚合 (DataFrame loader): code=%s", code)
        try:
            query, params = build_postgres_tick_to_bar_query(item)
            with open_postgres_connection(psycopg2, pg_cfg) as conn:
                df_raw = pd.read_sql_query(query, conn, params=params)
            if not df_raw.empty:
                logger.info("数据库端聚合成功 (DataFrame loader): code=%s rows=%d", code, len(df_raw))
                return _normalize_loaded_df(df_raw, item)
        except Exception as exc:
            logger.warning("数据库端聚合失败，回退到本地聚合 (DataFrame loader): %s", exc)

    query, params = build_postgres_query(item)

    with open_postgres_connection(psycopg2, pg_cfg) as conn:
        df_raw = pd.read_sql_query(query, conn, params=params)

    if df_raw.empty:
        raise ValueError(f"未找到数据: code={code}, start={start}, end={end}")

    logger.info("数据库数据加载成功: code=%s rows=%d", code, len(df_raw))
    return _normalize_loaded_df(df_raw, item)


def load_data_item_to_df(item: Dict[str, Any], project_root: Path, cfg: Dict[str, Any]) -> pd.DataFrame:
    source = normalize_source_name(item.get("source", "csv"))
    if source == "csv":
        return load_csv_item_to_df(item=item, project_root=project_root)

    if source == "excel":
        return load_excel_item_to_df(item=item, project_root=project_root)

    if source == "postgres":
        return load_postgres_item_to_df(item=item, project_root=project_root, cfg=cfg)

    if source == "tushare":
        df = fetch_tushare_ohlcv(item=item, global_cfg=cfg.get("tushare", {}), project_root=project_root)
        logger.info("Tushare数据加载成功: ts_code=%s rows=%d", item.get("ts_code"), len(df))
        return _ensure_norm_df(df)

    raise ValueError(f"不支持的数据源 source={source}，当前仅支持 csv / excel / db(postgres) / tushare")
