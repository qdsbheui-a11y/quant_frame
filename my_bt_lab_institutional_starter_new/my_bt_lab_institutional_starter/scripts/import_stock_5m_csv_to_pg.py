#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator, Optional
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values

SH_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_BATCH_SIZE = 2000


@dataclass
class BarRow:
    code: str
    market: str
    symbol: str
    bar_time: datetime
    trading_day: datetime.date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Optional[Decimal]
    turnover: Optional[Decimal]
    source_file: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import stock 5m OHLCV CSV/ZIP files into instrument + bar_data."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--zip", dest="zip_path", help="Path to ZIP that contains CSV files.")
    src.add_argument("--csv", dest="csv_path", help="Path to a single CSV file.")
    src.add_argument("--dir", dest="dir_path", help="Path to a directory that contains CSV files.")

    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", help="Database password. If omitted, read from PGPASSWORD env.")

    parser.add_argument("--asset-type", default="stock", help="Default: stock")
    parser.add_argument("--timeframe-unit", default="minute", help="Default: minute")
    parser.add_argument("--compression", type=int, default=5, help="Default: 5")
    parser.add_argument("--source", default="csv", help="Source label written to bar_data.source")
    parser.add_argument("--timezone", default="Asia/Shanghai", help="Default: Asia/Shanghai")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate only, do not write DB.")
    return parser.parse_args()


def get_connection(args: argparse.Namespace):
    password = args.password or os.getenv("PGPASSWORD")
    if not password:
        raise ValueError("Database password not provided. Use --password or set PGPASSWORD.")
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=password,
    )


def iter_input_files(args: argparse.Namespace) -> Iterator[tuple[str, io.TextIOBase]]:
    if args.csv_path:
        path = Path(args.csv_path)
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            yield path.name, f
        return

    if args.dir_path:
        root = Path(args.dir_path)
        for path in sorted(root.glob("*.csv")):
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                yield path.name, f
        return

    if args.zip_path:
        zip_path = Path(args.zip_path)
        with zipfile.ZipFile(zip_path) as zf:
            for name in sorted(zf.namelist()):
                if not name.lower().endswith(".csv"):
                    continue
                with zf.open(name) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                    yield Path(name).name, text
        return

    raise ValueError("No input source provided.")


def split_code(code: str) -> tuple[str, str]:
    code = code.strip()
    if "." in code:
        symbol, market = code.split(".", 1)
        return symbol, market.upper()
    return code, "UNKNOWN"


def parse_decimal(value: str | None) -> Optional[Decimal]:
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    return Decimal(value)


def parse_bar_time(value: str, tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    value = value.strip()
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=tz)


def read_rows(file_name: str, fh: io.TextIOBase, tz_name: str) -> Iterator[BarRow]:
    reader = csv.DictReader(fh)
    required = {"code", "date", "open", "high", "low", "close"}
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"{file_name}: missing required columns: {sorted(missing)}")

    for idx, row in enumerate(reader, start=2):
        try:
            code = (row.get("code") or "").strip()
            if not code:
                raise ValueError("empty code")
            symbol, market = split_code(code)
            bar_time = parse_bar_time(row["date"], tz_name)
            yield BarRow(
                code=code,
                market=market,
                symbol=symbol,
                bar_time=bar_time,
                trading_day=bar_time.date(),
                open=parse_decimal(row.get("open")) or Decimal("0"),
                high=parse_decimal(row.get("high")) or Decimal("0"),
                low=parse_decimal(row.get("low")) or Decimal("0"),
                close=parse_decimal(row.get("close")) or Decimal("0"),
                volume=parse_decimal(row.get("volume")),
                turnover=parse_decimal(row.get("turnover")),
                source_file=file_name,
            )
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"{file_name}: line {idx}: {e}") from e


UPSERT_INSTRUMENT_SQL = """
INSERT INTO instrument (
    market, code, symbol, asset_type, exchange, currency, timezone, status
) VALUES %s
ON CONFLICT (code) DO UPDATE SET
    market = EXCLUDED.market,
    symbol = EXCLUDED.symbol,
    asset_type = EXCLUDED.asset_type,
    exchange = EXCLUDED.exchange,
    currency = EXCLUDED.currency,
    timezone = EXCLUDED.timezone,
    status = EXCLUDED.status,
    updated_at = now()
"""

FETCH_INSTRUMENT_IDS_SQL = """
SELECT code, instrument_id
FROM instrument
WHERE code = ANY(%s)
"""

UPSERT_BARS_SQL = """
INSERT INTO bar_data (
    instrument_id, bar_time, timeframe_unit, compression,
    open, high, low, close, volume, turnover, open_interest,
    trading_day, is_final, source, source_file, ingest_batch_id, extra
) VALUES %s
ON CONFLICT (instrument_id, bar_time, timeframe_unit, compression) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    turnover = EXCLUDED.turnover,
    open_interest = EXCLUDED.open_interest,
    trading_day = EXCLUDED.trading_day,
    is_final = EXCLUDED.is_final,
    source = EXCLUDED.source,
    source_file = EXCLUDED.source_file,
    ingest_batch_id = EXCLUDED.ingest_batch_id,
    updated_at = now()
"""


def chunked(iterable: Iterable[BarRow], size: int) -> Iterator[list[BarRow]]:
    batch: list[BarRow] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def upsert_instruments(conn, rows: list[BarRow], args: argparse.Namespace) -> dict[str, int]:
    codes = sorted({r.code for r in rows})
    instrument_values = []
    for code in codes:
        symbol, market = split_code(code)
        instrument_values.append((market, code, symbol, args.asset_type, market, "CNY", args.timezone, "active"))

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_INSTRUMENT_SQL, instrument_values, page_size=500)
        cur.execute(FETCH_INSTRUMENT_IDS_SQL, (codes,))
        mapping = {code: instrument_id for code, instrument_id in cur.fetchall()}
    return mapping


def upsert_bars(conn, rows: list[BarRow], instrument_map: dict[str, int], args: argparse.Namespace, ingest_batch_id: str) -> int:
    values = []
    for r in rows:
        instrument_id = instrument_map.get(r.code)
        if instrument_id is None:
            raise ValueError(f"instrument_id not found for code={r.code}")
        values.append(
            (
                instrument_id,
                r.bar_time,
                args.timeframe_unit,
                args.compression,
                r.open,
                r.high,
                r.low,
                r.close,
                r.volume,
                r.turnover,
                None,
                r.trading_day,
                True,
                args.source,
                r.source_file,
                ingest_batch_id,
                "{}",
            )
        )
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_BARS_SQL, values, page_size=args.batch_size)
    return len(values)


def main() -> int:
    args = parse_args()
    ingest_batch_id = datetime.now(tz=SH_TZ).strftime("stock5m_%Y%m%d_%H%M%S")

    total_files = 0
    total_rows = 0
    sample_codes: set[str] = set()

    if args.dry_run:
        for file_name, fh in iter_input_files(args):
            total_files += 1
            count = 0
            for row in read_rows(file_name, fh, args.timezone):
                count += 1
                total_rows += 1
                if len(sample_codes) < 10:
                    sample_codes.add(row.code)
            print(f"[DRY-RUN] {file_name}: {count} rows")
        print(f"[DRY-RUN] files={total_files}, rows={total_rows}, sample_codes={sorted(sample_codes)}")
        return 0

    conn = get_connection(args)
    conn.autocommit = False
    try:
        for file_name, fh in iter_input_files(args):
            total_files += 1
            rows = list(read_rows(file_name, fh, args.timezone))
            if not rows:
                print(f"[SKIP] {file_name}: empty")
                continue
            instrument_map = upsert_instruments(conn, rows, args)
            inserted = 0
            for batch in chunked(rows, args.batch_size):
                inserted += upsert_bars(conn, batch, instrument_map, args, ingest_batch_id)
            conn.commit()
            total_rows += inserted
            sample_codes.add(rows[0].code)
            print(f"[OK] {file_name}: rows={inserted}, code={rows[0].code}")

        print(
            f"Done. files={total_files}, rows={total_rows}, "
            f"sample_codes={sorted(sample_codes)[:10]}, ingest_batch_id={ingest_batch_id}"
        )
        return 0
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
