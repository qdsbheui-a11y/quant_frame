from __future__ import annotations

from typing import Optional, Dict, List

import pandas as pd

# Backtest engine internal standard columns
_STD_REQUIRED = ["datetime", "open", "high", "low", "close"]
_ALIAS_CANDIDATES = {
    "datetime": ["datetime", "date", "trade_date", "trade_time", "time", "timestamp", "dt"],
    "open": ["open", "open_price", "o"],
    "high": ["high", "high_price", "h"],
    "low": ["low", "low_price", "l"],
    "close": ["close", "close_price", "c", "last"],
    "volume": ["volume", "vol", "qty", "trade_volume"],
    "openinterest": ["openinterest", "oi", "open_int", "open_interest"],
}


def _norm_colname(x: str) -> str:
    return str(x).strip().lower()


def _build_rename_map_from_schema(df_cols, schema: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not schema:
        return {}
    rename_map: Dict[str, str] = {}
    col_set = set(df_cols)
    for target_std_col, raw_col in schema.items():
        if raw_col is None:
            continue
        raw_col_norm = _norm_colname(raw_col)
        if raw_col_norm in col_set:
            rename_map[raw_col_norm] = _norm_colname(target_std_col)
    return rename_map


def _build_rename_map_from_aliases(df_cols) -> Dict[str, str]:
    rename_map: Dict[str, str] = {}
    used_raw = set()
    for target_std_col, candidates in _ALIAS_CANDIDATES.items():
        for c in candidates:
            c_norm = _norm_colname(c)
            if c_norm in df_cols and c_norm not in used_raw:
                rename_map[c_norm] = target_std_col
                used_raw.add(c_norm)
                break
    return rename_map


def _try_parse_datetime_series(series: pd.Series, datetime_format: Optional[str]) -> pd.Series:
    if datetime_format:
        return pd.to_datetime(series.astype(str), format=datetime_format, errors="coerce")

    if pd.api.types.is_numeric_dtype(series):
        values = series.astype("Int64").astype(str)
    else:
        values = series.astype(str).str.strip()

    # Common first attempt for Tushare/cache style YYYYMMDD
    parsed = pd.to_datetime(values, format="%Y%m%d", errors="coerce")
    bad_mask = parsed.isna()
    if bad_mask.any():
        parsed2 = pd.to_datetime(values[bad_mask], errors="coerce")
        parsed.loc[bad_mask] = parsed2
    return parsed


def normalize_ohlcv_df(
    df_raw: pd.DataFrame,
    schema: Optional[Dict[str, str]] = None,
    datetime_format: Optional[str] = None,
    date_col: Optional[str] = None,
    time_col: Optional[str] = None,
    datetime_col: Optional[str] = None,
    keep_extra_cols: bool = False,
    extra_keep_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Normalize arbitrary OHLCV-like DataFrame to internal standard columns.

    Output columns include at least: datetime/open/high/low/close/volume/openinterest
    (volume/openinterest will be synthesized if absent).
    Returned DataFrame keeps datetime as a normal column (not index), sorted ascending and de-duplicated.
    """
    if df_raw is None or df_raw.empty:
        raise ValueError("输入DataFrame为空")

    df = df_raw.copy()
    df.columns = [_norm_colname(c) for c in df.columns]
    df_cols = set(df.columns)

    # Build rename map: user schema wins; aliases fill gaps
    rename_map = _build_rename_map_from_schema(df_cols, schema)
    auto_map = _build_rename_map_from_aliases(df_cols)
    for raw_col, std_col in auto_map.items():
        if std_col not in rename_map.values():
            rename_map.setdefault(raw_col, std_col)

    if rename_map:
        df = df.rename(columns=rename_map)

    # Explicit datetime column override (e.g., source column named ts)
    if datetime_col and "datetime" not in df.columns:
        dt_col_norm = _norm_colname(datetime_col)
        if dt_col_norm in df.columns:
            df = df.rename(columns={dt_col_norm: "datetime"})

    # Combine date + time into datetime when a single datetime column doesn't exist
    if "datetime" not in df.columns and date_col and time_col:
        dcol = _norm_colname(date_col)
        tcol = _norm_colname(time_col)
        if dcol in df.columns and tcol in df.columns:
            df["datetime"] = (
                df[dcol].astype(str).str.strip() + " " + df[tcol].astype(str).str.strip()
            )

    missing = set(_STD_REQUIRED) - set(df.columns)
    if missing:
        raise ValueError(
            f"DataFrame缺少列: {missing}. 当前列={list(df.columns)}。"
            f"可在 YAML 的 data[].schema 中配置列映射，或配置 date_col/time_col。"
        )

    if "volume" not in df.columns:
        df["volume"] = 0.0
    if "openinterest" not in df.columns:
        df["openinterest"] = 0.0

    df["datetime"] = _try_parse_datetime_series(df["datetime"], datetime_format=datetime_format)
    if df["datetime"].isna().any():
        bad_n = int(df["datetime"].isna().sum())
        raise ValueError(f"datetime 解析失败，存在 {bad_n} 行无效日期。请检查日期格式或配置 datetime_format。")

    for c in ["open", "high", "low", "close", "volume", "openinterest"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["open", "high", "low", "close"])
    dropped = before - len(df)
    if dropped > 0:
        print(f"[CSV Normalize] dropped rows with invalid OHLC: {dropped}")

    if keep_extra_cols:
        keep_cols = list(df.columns)
        # ensure standard order first
        ordered_first = [c for c in ["datetime", "open", "high", "low", "close", "volume", "openinterest"] if c in keep_cols]
        ordered_rest = [c for c in keep_cols if c not in ordered_first]
        if extra_keep_cols:
            # move specified extras earlier but keep stable uniqueness
            extra_order = []
            for c in [_norm_colname(x) for x in extra_keep_cols]:
                if c in ordered_rest and c not in extra_order:
                    extra_order.append(c)
            ordered_rest = extra_order + [c for c in ordered_rest if c not in extra_order]
        df = df[ordered_first + ordered_rest]
    else:
        keep_cols = ["datetime", "open", "high", "low", "close", "volume", "openinterest"]
        df = df[[c for c in keep_cols if c in df.columns]]

    df = df.sort_values("datetime")
    dup_cnt = int(df.duplicated(subset=["datetime"]).sum())
    if dup_cnt > 0:
        print(f"[CSV Normalize] duplicated datetime rows detected: {dup_cnt}, keep=last")
        df = df.drop_duplicates(subset=["datetime"], keep="last")

    return df.reset_index(drop=True)
