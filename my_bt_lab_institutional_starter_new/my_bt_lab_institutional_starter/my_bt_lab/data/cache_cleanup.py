from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple, Optional

logger = logging.getLogger(__name__)


@dataclass
class CleanupSummary:
    scanned: int = 0
    deleted: int = 0
    kept: int = 0
    bytes_before: int = 0
    bytes_after: int = 0
    bytes_freed: int = 0


def _resolve(p: str, project_root: Path) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (project_root / pp).resolve()


def _gather_pinned_cache_files(cfg: Dict[str, Any], project_root: Path) -> Set[Path]:
    """Files referenced by current config that we should avoid deleting."""
    pinned: Set[Path] = set()
    for item in (cfg.get("data") or []):
        # 只把 cache_csv 当做“框架缓存”，csv 一般是用户手动导入，不应该由清理器管理
        cache_csv = item.get("cache_csv")
        # 兼容旧配置：source=csv 且没有 cache_csv 时，csv 被当成缓存
        if not cache_csv and str(item.get("source", "")).lower() == "csv":
            cache_csv = item.get("csv")
        if cache_csv:
            try:
                pinned.add(_resolve(str(cache_csv), project_root))
            except Exception:
                pass
    return pinned


def _iter_cache_files(cache_dir: Path) -> List[Path]:
    if not cache_dir.exists():
        return []
    # 你后面也可以扩展到 parquet/feather
    return [p for p in cache_dir.rglob("*") if p.is_file() and p.suffix.lower() in (".csv",)]


def _file_mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except Exception:
        return 0.0


def _file_size(p: Path) -> int:
    try:
        return p.stat().st_size
    except Exception:
        return 0


def cleanup_cache(project_root: Path, cfg: Dict[str, Any]) -> CleanupSummary:
    ccfg = cfg.get("cache_cleanup") or {}
    if not ccfg.get("enabled", False):
        return CleanupSummary()

    cache_dirs = ccfg.get("cache_dirs") or ["my_bt_lab/data_cache"]
    max_age_days = int(ccfg.get("max_age_days", 30))
    max_total_mb = float(ccfg.get("max_total_mb", 2048))
    keep_current = bool(ccfg.get("keep_current", True))
    dry_run = bool(ccfg.get("dry_run", False))

    pinned = _gather_pinned_cache_files(cfg, project_root) if keep_current else set()

    all_files: List[Path] = []
    for d in cache_dirs:
        cache_dir = _resolve(str(d), project_root)
        all_files.extend(_iter_cache_files(cache_dir))

    summary = CleanupSummary()
    summary.scanned = len(all_files)

    # 统计清理前大小
    total_before = sum(_file_size(p) for p in all_files)
    summary.bytes_before = total_before

    now = time.time()
    ttl_seconds = max_age_days * 86400

    # 1) TTL 清理：删除过期文件（不删 pinned）
    kept_files: List[Path] = []
    for p in all_files:
        if p in pinned:
            kept_files.append(p)
            summary.kept += 1
            continue

        age = now - _file_mtime(p)
        if age > ttl_seconds:
            logger.info("[CACHE] TTL delete: %s (age_days=%.1f)", p, age / 86400)
            summary.deleted += 1
            if not dry_run:
                try:
                    p.unlink(missing_ok=True)
                except Exception as e:
                    logger.warning("[CACHE] delete failed: %s err=%s", p, e)
        else:
            kept_files.append(p)

    # 2) Size cap 清理：超过总大小则删除最老的（不删 pinned）
    # 重新计算当前保留文件大小
    kept_files = [p for p in kept_files if p.exists()]
    total_now = sum(_file_size(p) for p in kept_files)
    cap_bytes = int(max_total_mb * 1024 * 1024)

    if total_now > cap_bytes:
        # 按最老优先删除（mtime 最小）
        candidates = [p for p in kept_files if p not in pinned]
        candidates.sort(key=_file_mtime)  # oldest first
        for p in candidates:
            if total_now <= cap_bytes:
                break
            sz = _file_size(p)
            logger.info("[CACHE] SIZE delete: %s (size_mb=%.2f)", p, sz / 1024 / 1024)
            summary.deleted += 1
            if not dry_run:
                try:
                    p.unlink(missing_ok=True)
                except Exception as e:
                    logger.warning("[CACHE] delete failed: %s err=%s", p, e)
                    continue
            total_now -= sz

    # 清理后统计
    # 注意：这里仅统计 cache_dirs 下面的现存文件，不再扫描全盘
    all_after: List[Path] = []
    for d in cache_dirs:
        all_after.extend(_iter_cache_files(_resolve(str(d), project_root)))

    summary.bytes_after = sum(_file_size(p) for p in all_after)
    summary.bytes_freed = max(0, summary.bytes_before - summary.bytes_after)

    logger.info(
        "[CACHE] Cleanup done: scanned=%d deleted=%d kept=%d freed=%.2fMB",
        summary.scanned,
        summary.deleted,
        summary.kept,
        summary.bytes_freed / 1024 / 1024,
    )
    if dry_run:
        logger.info("[CACHE] dry_run=true: no files were actually deleted.")

    return summary