"""Tick data aggregation to OHLCV bars."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

TICK_SCHEMA_ALIASES = {
    "datetime": ["datetime", "dt", "timestamp", "tick_time", "time"],
    "date": ["trading_day", "date", "trade_date", "day"],
    "time": ["update_time", "tick_time", "time", "ticktime"],
    "millisec": ["update_millisec", "millisec", "ms", "millisecond"],
    "price": ["last_price", "price", "last", "close", "tick_price"],
    "volume": ["volume", "vol", "qty", "quantity", "tick_volume"],
    "symbol": ["instrument_id", "symbol", "code", "ts_code", "ticker"],
}


def _norm_colname(x: str) -> str:
    return str(x).strip().lower().replace(" ", "_")


def _find_col(df_cols: List[str], candidates: List[str]) -> Optional[str]:
    col_set = set(_norm_colname(c) for c in df_cols)
    for c in candidates:
        if _norm_colname(c) in col_set:
            return next(orig for orig in df_cols if _norm_colname(orig) == _norm_colname(c))
    return None


def _resolve_tick_cols(df: pd.DataFrame, schema: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Map raw tick columns to standard names."""
    cols = list(df.columns)
    mapping: Dict[str, str] = {}

    # User-provided schema wins
    if schema:
        for std, raw in schema.items():
            if raw and _norm_colname(raw) in set(_norm_colname(c) for c in cols):
                mapping[std] = next(
                    (c for c in cols if _norm_colname(c) == _norm_colname(raw)), raw
                )

    # Auto-resolve remaining columns
    for std, candidates in TICK_SCHEMA_ALIASES.items():
        if std not in mapping:
            found = _find_col(cols, candidates)
            if found:
                mapping[std] = found

    return mapping


def _build_tick_datetime(
    df: pd.DataFrame,
    mapping: Dict[str, str],
    datetime_format: Optional[str] = None,
) -> pd.Series:
    """Build a proper datetime series from tick date/time columns."""
    if "datetime" in mapping:
        dt_col = mapping["datetime"]
        if datetime_format:
            return pd.to_datetime(df[dt_col].astype(str), format=datetime_format, errors="coerce")
        return pd.to_datetime(df[dt_col], errors="coerce")

    if "date" in mapping and "time" in mapping:
        date_col = mapping["date"]
        time_col = mapping["time"]
        date_str = df[date_col].astype(str).str.strip()
        time_str = df[time_col].astype(str).str.strip()
        combined = date_str + " " + time_str
        return pd.to_datetime(combined, errors="coerce")

    raise ValueError(
        f"无法构建 tick datetime。当前列: {list(df.columns)}。"
        f"请配置 schema 映射或确保存在 trading_day+update_time 或 datetime 列。"
    )


def _parse_bt_freq(timeframe: str, compression: int) -> str:
    """Convert Backtrader-style timeframe to pandas frequency string."""
    tf = str(timeframe or "days").strip().lower()
    if tf in {"tick", "ticks"}:
        return "1min"  # Default for tick data when no aggregation specified
    if tf in {"m", "min", "mins", "minute", "minutes"}:
        return f"{compression}min"
    if tf in {"h", "hour", "hours"}:
        return f"{compression}H"
    if tf in {"d", "day", "days"}:
        return f"{compression}D"
    if tf in {"w", "week", "weeks"}:
        return f"{compression}W"
    if tf in {"mo", "month", "months"}:
        return f"{compression}M"
    return f"{compression}min"


def aggregate_tick_to_bar(
    df_raw: pd.DataFrame,
    timeframe: Optional[str] = None,
    compression: Optional[int] = None,
    schema: Optional[Dict[str, str]] = None,
    datetime_format: Optional[str] = None,
) -> pd.DataFrame:
    """Aggregate tick DataFrame to OHLCV bars.

    Parameters
    ----------
    df_raw: tick-level DataFrame
    timeframe: target bar timeframe (default 'minutes')
    compression: bar compression (default 1)
    schema: optional column mapping, e.g. {'price': 'last_price', 'volume': 'vol'}
    datetime_format: optional strftime format for datetime parsing

    Returns
    -------
    DataFrame with columns: datetime, open, high, low, close, volume
    """
    if df_raw is None or df_raw.empty:
        raise ValueError("tick DataFrame 为空")

    df = df_raw.copy()
    mapping = _resolve_tick_cols(df, schema)
    logger.info("Tick column mapping resolved: %s", mapping)

    # Build datetime
    df["datetime"] = _build_tick_datetime(df, mapping, datetime_format)
    bad_dt = df["datetime"].isna().sum()
    if bad_dt > 0:
        logger.warning("Tick datetime parse failed for %d rows, dropping", int(bad_dt))
        df = df.dropna(subset=["datetime"])

    # Resolve price and volume columns
    price_col = mapping.get("price")
    volume_col = mapping.get("volume")

    if not price_col:
        raise ValueError(
            f"未找到 price 列。当前列: {list(df.columns)}。"
            f"请在 YAML schema 中配置 price -> last_price 或类似列。"
        )

    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    if volume_col:
        df[volume_col] = pd.to_numeric(df[volume_col], errors="coerce")
    else:
        df["volume"] = 0.0
        volume_col = "volume"

    df = df.dropna(subset=["datetime", price_col])

    # Aggregate
    freq = _parse_bt_freq(timeframe, int(compression or 1))
    logger.info("Aggregating tick -> %s bars (freq=%s)", f"{compression or 1}{timeframe or 'min'}", freq)

    agg_dict = {
        price_col: ["first", "max", "min", "last"],
    }
    if volume_col in df.columns:
        agg_dict[volume_col] = "sum"

    bar_df = df.set_index("datetime").resample(freq).agg(agg_dict).dropna()

    # Flatten multi-index columns
    bar_df.columns = ["open", "high", "low", "close", "volume"]
    bar_df = bar_df.reset_index()

    # Round volume if it's very small (crypto-style)
    bar_df["volume"] = bar_df["volume"].fillna(0)

    logger.info("Aggregation complete: %d bars", len(bar_df))
    return bar_df


def is_tick_dataframe(df: pd.DataFrame) -> bool:
    """Heuristic: determine if a DataFrame looks like tick data."""
    cols = set(_norm_colname(c) for c in df.columns)
    # Tick data typically has last_price / update_time / trading_day
    tick_indicators = {"last_price", "update_time", "trading_day", "update_millisec", "instrument_id"}
    bar_indicators = {"open", "high", "low", "close"}
    return len(tick_indicators & cols) >= 2 and len(bar_indicators & cols) < 2
