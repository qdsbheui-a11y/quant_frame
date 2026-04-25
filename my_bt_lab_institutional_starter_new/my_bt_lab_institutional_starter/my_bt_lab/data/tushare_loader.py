from __future__ import annotations

import os
import logging
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from my_bt_lab.data.normalize import normalize_ohlcv_df


logger = logging.getLogger(__name__)


class TushareConfigError(Exception):
    pass


_INTRADAY_FREQS = {"1min", "5min", "15min", "30min", "60min"}


def _is_pandas_tushare_pro_bar_compat_error(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "fillna() got an unexpected keyword argument 'method'" in msg
        or "NDFrame.fillna() got an unexpected keyword argument 'method'" in msg
    )


def _is_intraday_freq(freq: Optional[str]) -> bool:
    return str(freq or "").lower() in _INTRADAY_FREQS

def _is_tushare_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "每分钟最多访问该接口" in msg
        or "frequency limit" in msg.lower()
        or "too many requests" in msg.lower()
    )


def _sleep_to_respect_rate_limit(*, last_call_ts: Optional[float], min_interval_seconds: float) -> float:
    if min_interval_seconds <= 0:
        return time.time()
    now = time.time()
    if last_call_ts is not None:
        elapsed = now - last_call_ts
        remain = min_interval_seconds - elapsed
        if remain > 0:
            logger.info("[Tushare数据] Sleep %.1fs to respect rate limit", remain)
            time.sleep(remain)
    return time.time()



def _get_tushare_token(global_cfg: Optional[dict]) -> str:
    global_cfg = global_cfg or {}
    token_env = str(global_cfg.get("token_env", "TUSHARE_TOKEN"))
    token = os.getenv(token_env) or global_cfg.get("token")
    if not token:
        raise TushareConfigError(
            f"未找到 Tushare token。请先设置环境变量 {token_env}，或在配置里填写 tushare.token（不推荐明文）。"
        )
    return str(token)


def _tushare_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Tushare raw/cache DataFrame to internal standard format.

    Supports both raw Tushare columns (trade_date/vol) and normalized cache columns (datetime/volume).
    """
    if df is None or df.empty:
        raise ValueError("Tushare 返回空数据")

    df_norm = normalize_ohlcv_df(
        df_raw=df,
        schema={
            "datetime": "trade_date",  # when raw cache/daily data uses trade_date
            "volume": "vol",
        },
        # trade_time / datetime / trade_date are also covered by aliases
        keep_extra_cols=True,
        extra_keep_cols=["amount"],
    )
    return df_norm


def _load_cache_csv(cache_path: Path) -> pd.DataFrame:
    logger.info("[Tushare数据] Load cache: %s", cache_path)
    df = pd.read_csv(cache_path)
    df_norm = _tushare_normalize(df)
    logger.info("[Tushare数据] Cache normalized: rows=%d", len(df_norm))
    return df_norm


def _save_cache_csv(df_norm: pd.DataFrame, cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df_norm.to_csv(cache_path, index=False, encoding="utf-8")
    logger.info("[Tushare数据] Cache saved: %s, rows=%d", cache_path, len(df_norm))


def _format_ymd(dt: pd.Timestamp) -> str:
    return pd.Timestamp(dt).strftime("%Y%m%d")


def _format_ts_datetime(dt: pd.Timestamp) -> str:
    return pd.Timestamp(dt).strftime("%Y-%m-%d %H:%M:%S")


def _parse_ymd(s: Optional[str]) -> Optional[pd.Timestamp]:
    if not s:
        return None
    try:
        return pd.to_datetime(str(s), format="%Y%m%d", errors="coerce")
    except Exception:
        return pd.to_datetime(str(s), errors="coerce")


def _parse_any_datetime(s: Optional[str]) -> Optional[pd.Timestamp]:
    if not s:
        return None
    dt = pd.to_datetime(str(s), errors="coerce")
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt)


def _clip_df_norm_to_requested_window(
    df_norm: pd.DataFrame,
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    intraday: bool,
    ts_code: str,
) -> pd.DataFrame:
    """Hard-trim normalized data to the user requested time window.

    This guarantees that the backtest engine only receives rows inside
    [start_date, end_date]. For instruments that start trading later than
    start_date, the leading empty portion is naturally skipped.
    """
    if df_norm is None or df_norm.empty:
        return df_norm

    if "datetime" not in df_norm.columns:
        raise ValueError("标准化后的数据缺少 datetime 列，无法按区间裁剪")

    out = df_norm.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"])

    start_ts = _parse_any_datetime(start_date) if intraday else _parse_ymd(start_date)
    end_ts = _parse_any_datetime(end_date) if intraday else _parse_ymd(end_date)

    before_rows = len(out)
    if start_ts is not None and pd.notna(start_ts):
        out = out[out["datetime"] >= pd.Timestamp(start_ts)]
    if end_ts is not None and pd.notna(end_ts):
        if intraday:
            out = out[out["datetime"] <= pd.Timestamp(end_ts)]
        else:
            end_day = pd.Timestamp(end_ts).normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            out = out[out["datetime"] <= end_day]

    out = out.sort_values("datetime").reset_index(drop=True)
    logger.info(
        "[Tushare数据] Window clip done: ts_code=%s, intraday=%s, start=%s, end=%s, rows=%d -> %d",
        ts_code,
        intraday,
        start_date,
        end_date,
        before_rows,
        len(out),
    )
    return out


def _compute_incremental_start(
    *,
    cache_last_dt: pd.Timestamp,
    item_start_date: Optional[str],
    overlap_days: int,
    intraday: bool,
) -> str:
    start_dt = pd.Timestamp(cache_last_dt) - pd.Timedelta(days=max(0, int(overlap_days)))
    cfg_start = _parse_any_datetime(item_start_date) if intraday else _parse_ymd(item_start_date)
    if cfg_start is not None and pd.notna(cfg_start):
        start_dt = max(start_dt, cfg_start)
    return _format_ts_datetime(start_dt) if intraday else _format_ymd(start_dt)


def _merge_cache_and_new(df_cache_norm: pd.DataFrame, df_new_norm: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([df_cache_norm, df_new_norm], ignore_index=True)
    merged = merged.sort_values("datetime")
    dup_cnt = int(merged.duplicated(subset=["datetime"]).sum())
    if dup_cnt > 0:
        logger.info("[Tushare数据] Merge dedup: duplicated rows=%d (keep=last)", dup_cnt)
        merged = merged.drop_duplicates(subset=["datetime"], keep="last")
    return merged.reset_index(drop=True)


def _get_cache_flags(global_cfg: dict, item: dict) -> Tuple[bool, bool, int]:
    use_cache = bool(global_cfg.get("use_cache", True))
    incremental = bool(global_cfg.get("incremental", True))
    overlap_days = int(global_cfg.get("overlap_days", 3))

    # data item can override globals if needed
    if "use_cache" in item:
        use_cache = bool(item.get("use_cache"))
    if "incremental" in item:
        incremental = bool(item.get("incremental"))
    if "overlap_days" in item:
        overlap_days = int(item.get("overlap_days"))

    return use_cache, incremental, overlap_days


def _resolve_chunk_days(global_cfg: dict, item: dict, freq: str) -> int:
    default_days = 10 if _is_intraday_freq(freq) else 3650
    val = item.get("chunk_days", global_cfg.get("chunk_days", default_days))
    try:
        days = int(val)
    except Exception:
        days = default_days
    return max(1, days)


def _fetch_pro_bar_once(ts_module, *, ts_code: str, start_date: Optional[str], end_date: Optional[str], asset: str, freq: str, adj):
    kwargs = dict(ts_code=ts_code, start_date=start_date, end_date=end_date, asset=asset, freq=freq)
    if adj is not None:
        kwargs["adj"] = adj
    return ts_module.pro_bar(**kwargs)


def _fetch_pro_bar_chunked(
    ts_module,
    *,
    ts_code: str,
    start_date: Optional[str],
    end_date: Optional[str],
    asset: str,
    freq: str,
    adj,
    chunk_days: int,
    min_interval_seconds: float = 31.0,
    max_retries: int = 3,
    retry_sleep_seconds: float = 65.0,
) -> Optional[pd.DataFrame]:
    intraday = _is_intraday_freq(freq)
    if not intraday:
        return _fetch_pro_bar_once(
            ts_module,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            asset=asset,
            freq=freq,
            adj=adj,
        )

    start_ts = _parse_any_datetime(start_date)
    end_ts = _parse_any_datetime(end_date) if end_date else pd.Timestamp.now().floor("min")

    if start_ts is None:
        raise TushareConfigError(f"分钟数据必须提供 start_date，当前 ts_code={ts_code}, freq={freq}")
    if end_ts is None:
        end_ts = pd.Timestamp.now().floor("min")
    if start_ts > end_ts:
        raise TushareConfigError(
            f"start_date 不能晚于 end_date: ts_code={ts_code}, start={start_ts}, end={end_ts}"
        )

    cur = start_ts
    parts = []
    chunk_index = 0
    last_call_ts: Optional[float] = None
    while cur <= end_ts:
        nxt = min(cur + pd.Timedelta(days=chunk_days) - pd.Timedelta(minutes=1), end_ts)
        part_start = _format_ts_datetime(cur)
        part_end = _format_ts_datetime(nxt)
        logger.info(
            "[Tushare数据] Fetch pro_bar(chunk %d): ts_code=%s, start=%s, end=%s, asset=%s, freq=%s, adj=%s",
            chunk_index,
            ts_code,
            part_start,
            part_end,
            asset,
            freq,
            adj,
        )

        retry_idx = 0
        while True:
            last_call_ts = _sleep_to_respect_rate_limit(
                last_call_ts=last_call_ts,
                min_interval_seconds=min_interval_seconds,
            )
            try:
                df_part = _fetch_pro_bar_once(
                    ts_module,
                    ts_code=ts_code,
                    start_date=part_start,
                    end_date=part_end,
                    asset=asset,
                    freq=freq,
                    adj=adj,
                )
                break
            except Exception as e:
                if _is_pandas_tushare_pro_bar_compat_error(e):
                    raise RuntimeError(
                        "检测到 tushare.pro_bar 与当前 pandas 版本不兼容（内部仍使用 fillna(method=...)）。\n"
                        "请先执行：pip install -U \"pandas<3\"\n"
                        "然后重试；或临时将配置改为 api: daily（A股日线，通常无复权）。"
                    ) from e
                if _is_tushare_rate_limit_error(e) and retry_idx < max_retries:
                    retry_idx += 1
                    logger.warning(
                        "[Tushare数据] Hit rate limit, sleep %.1fs then retry: ts_code=%s, chunk=%d, retry=%d/%d",
                        retry_sleep_seconds,
                        ts_code,
                        chunk_index,
                        retry_idx,
                        max_retries,
                    )
                    time.sleep(retry_sleep_seconds)
                    last_call_ts = time.time()
                    continue
                raise

        rows = 0 if df_part is None else len(df_part)
        logger.info(
            "[Tushare数据] Chunk done: ts_code=%s, chunk=%d, rows=%d, start=%s, end=%s",
            ts_code,
            chunk_index,
            rows,
            part_start,
            part_end,
        )
        if df_part is not None and not df_part.empty:
            parts.append(df_part)

        cur = nxt + pd.Timedelta(minutes=1)
        chunk_index += 1

    if not parts:
        return None

    merged = pd.concat(parts, ignore_index=True)
    if "trade_time" in merged.columns:
        sort_col = "trade_time"
    elif "trade_date" in merged.columns:
        sort_col = "trade_date"
    elif "datetime" in merged.columns:
        sort_col = "datetime"
    else:
        sort_col = None

    if sort_col:
        merged = merged.sort_values(sort_col)
        merged = merged.drop_duplicates(subset=[sort_col], keep="last")
    else:
        merged = merged.drop_duplicates(keep="last")

    merged = merged.reset_index(drop=True)
    logger.info("[Tushare数据] Chunk merge done: ts_code=%s, freq=%s, rows=%d", ts_code, freq, len(merged))
    return merged


def fetch_tushare_ohlcv(item: Dict, global_cfg: Optional[dict], project_root: Path) -> pd.DataFrame:
    """Fetch Tushare OHLCV and normalize to internal standard DataFrame.

    支持缓存模式：
    - refresh=true: 强制全量拉取并覆盖缓存
    - refresh=false + use_cache=true + incremental=true: 增量更新缓存（回补 overlap_days）
    - refresh=false + use_cache=true + incremental=false: 仅读缓存

    对分钟级数据（1min/5min/15min/30min/60min），自动按 chunk_days 分段抓取，
    避免单次请求区间过长导致只拿到最近一段数据。
    """
    global_cfg = global_cfg or {}

    ts_code = item.get("ts_code")
    if not ts_code:
        raise TushareConfigError("tushare 数据项必须包含 ts_code")

    start_date = str(item.get("start_date", "")) or None
    end_date = str(item.get("end_date", "")) or None
    api_name = str(item.get("api", global_cfg.get("default_api", "pro_bar"))).lower()
    asset = str(item.get("asset", global_cfg.get("asset", "E")))
    freq = str(item.get("freq", global_cfg.get("freq", "D")))
    adj = item.get("adj", global_cfg.get("adj", None))
    intraday = _is_intraday_freq(freq)

    cache_csv = item.get("cache_csv")
    refresh = bool(item.get("refresh", False))
    use_cache, incremental, overlap_days = _get_cache_flags(global_cfg, item)
    chunk_days = _resolve_chunk_days(global_cfg, item, freq)
    cache_path = (project_root / cache_csv).resolve() if cache_csv else None

    # 1) Cache-only or incremental branch if cache exists and refresh=False
    if cache_path and cache_path.exists() and not refresh and use_cache:
        df_cache_norm = _load_cache_csv(cache_path)

        if not incremental:
            logger.info("[Tushare数据] Cache-only mode hit: ts_code=%s, rows=%d", ts_code, len(df_cache_norm))
            return _clip_df_norm_to_requested_window(
                df_cache_norm,
                start_date=start_date,
                end_date=end_date,
                intraday=intraday,
                ts_code=ts_code,
            )

        # Incremental update path
        if df_cache_norm.empty:
            logger.info("[Tushare数据] Cache is empty, fallback to full fetch: ts_code=%s", ts_code)
        else:
            last_dt = pd.to_datetime(df_cache_norm["datetime"]).max()
            inc_start = _compute_incremental_start(
                cache_last_dt=last_dt,
                item_start_date=start_date,
                overlap_days=overlap_days,
                intraday=intraday,
            )
            logger.info(
                "[Tushare数据] Incremental fetch: ts_code=%s, cache_last=%s, overlap_days=%s, start=%s, end=%s, chunk_days=%s",
                ts_code,
                pd.Timestamp(last_dt).strftime("%Y-%m-%d %H:%M:%S") if intraday else pd.Timestamp(last_dt).strftime("%Y-%m-%d"),
                overlap_days,
                inc_start,
                end_date,
                chunk_days,
            )

            try:
                import tushare as ts
            except ImportError as e:
                raise ImportError("未安装 tushare，请先执行: pip install tushare") from e

            token = _get_tushare_token(global_cfg)
            ts.set_token(token)
            pro = ts.pro_api(token)

            if api_name == "daily":
                logger.info("[Tushare数据] Fetch daily(incremental): ts_code=%s, start=%s, end=%s", ts_code, inc_start, end_date)
                df_new_raw = pro.daily(ts_code=ts_code, start_date=inc_start, end_date=end_date)
            else:
                df_new_raw = _fetch_pro_bar_chunked(
                    ts,
                    ts_code=ts_code,
                    start_date=inc_start,
                    end_date=end_date,
                    asset=asset,
                    freq=freq,
                    adj=adj,
                    chunk_days=chunk_days,
                )

            if df_new_raw is None or df_new_raw.empty:
                logger.info("[Tushare数据] Incremental fetch returned empty, keep cache: ts_code=%s", ts_code)
                return _clip_df_norm_to_requested_window(
                    df_cache_norm,
                    start_date=start_date,
                    end_date=end_date,
                    intraday=intraday,
                    ts_code=ts_code,
                )

            logger.info("[Tushare数据] Incremental raw rows=%d", len(df_new_raw))
            df_new_norm = _tushare_normalize(df_new_raw)
            merged = _merge_cache_and_new(df_cache_norm, df_new_norm)
            _save_cache_csv(merged, cache_path)
            logger.info("[Tushare数据] Incremental merge done: ts_code=%s, rows=%d", ts_code, len(merged))
            return _clip_df_norm_to_requested_window(
                merged,
                start_date=start_date,
                end_date=end_date,
                intraday=intraday,
                ts_code=ts_code,
            )

    # 2) Full fetch path (cache missing OR refresh=true OR use_cache=false)
    try:
        import tushare as ts
    except ImportError as e:
        raise ImportError("未安装 tushare，请先执行: pip install tushare") from e

    token = _get_tushare_token(global_cfg)
    ts.set_token(token)
    pro = ts.pro_api(token)

    if api_name == "daily":
        logger.info("[Tushare数据] Fetch daily(full): ts_code=%s, start=%s, end=%s", ts_code, start_date, end_date)
        df_raw = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    else:
        logger.info(
            "[Tushare数据] Fetch pro_bar(full): ts_code=%s, start=%s, end=%s, asset=%s, freq=%s, adj=%s, chunk_days=%s",
            ts_code,
            start_date,
            end_date,
            asset,
            freq,
            adj,
            chunk_days,
        )
        request_interval_seconds = float(item.get("request_interval_seconds", global_cfg.get("request_interval_seconds", 31)))
        max_retries = int(item.get("max_retries", global_cfg.get("max_retries", 3)))
        retry_sleep_seconds = float(item.get("retry_sleep_seconds", global_cfg.get("retry_sleep_seconds", 65)))
        df_raw = _fetch_pro_bar_chunked(
            ts,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            asset=asset,
            freq=freq,
            adj=adj,
            chunk_days=chunk_days,
            min_interval_seconds=request_interval_seconds,
            max_retries=max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )

    if df_raw is None or df_raw.empty:
        logger.warning(
            "[Tushare数据] Empty result: ts_code=%s, api=%s, start=%s, end=%s",
            ts_code,
            api_name,
            start_date,
            end_date,
        )
        raise ValueError(f"Tushare 返回空数据: ts_code={ts_code}, api={api_name}")

    logger.info("[Tushare数据] Fetch success(full): ts_code=%s, api=%s, rows=%d", ts_code, api_name, len(df_raw))

    df_norm = _tushare_normalize(df_raw)
    logger.info("[Tushare数据] Normalize done: ts_code=%s, rows=%d", ts_code, len(df_norm))

    df_norm_clipped = _clip_df_norm_to_requested_window(
        df_norm,
        start_date=start_date,
        end_date=end_date,
        intraday=intraday,
        ts_code=ts_code,
    )

    if cache_path and use_cache:
        _save_cache_csv(df_norm_clipped, cache_path)

    return df_norm_clipped
