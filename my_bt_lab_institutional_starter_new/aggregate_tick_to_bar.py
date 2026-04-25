# -*- coding: utf-8 -*-
"""
将远程 tick_data 聚合为分钟 K 线并导出 CSV
支持通过 SSH 隧道连接阿里云 PG
"""
import os
import sys
import csv
import argparse

project_root = os.path.join(os.path.dirname(__file__), "my_bt_lab_institutional_starter")
sys.path.insert(0, project_root)

import psycopg2
from my_bt_lab.app.desktop_support import open_postgres_connection, build_postgres_tick_to_bar_query


def aggregate_tick_to_bar(
    instrument_id="BTCUSDT",
    trading_day="2026-04-10",
    timeframe="minutes",
    compression=1,
    output_csv=None,
    pg_cfg=None,
):
    pg_cfg = pg_cfg or {}

    # 强制约束交易日范围
    start = trading_day
    end = trading_day

    item = {
        "code": instrument_id,
        "code_col": "instrument_id",
        "data_type": "tick",
        "timeframe": timeframe,
        "compression": compression,
        "start": start,
        "end": end,
    }

    print("[1/2] 连接数据库并执行数据库端聚合 ...")
    query, params = build_postgres_tick_to_bar_query(item)

    with open_postgres_connection(psycopg2, pg_cfg) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

    print("[1/2] 聚合完成: %d 条 bar" % len(rows))

    if not output_csv:
        tf_short = str(timeframe).lower()[0]
        output_csv = "%s_%s_%d%s.csv" % (instrument_id, trading_day.replace("-", ""), compression, tf_short)

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(rows)

    print("[2/2] 已保存到 %s" % output_csv)


def main():
    parser = argparse.ArgumentParser(description="将远程 tick_data 聚合为 bar CSV")
    parser.add_argument("--instrument", default="BTCUSDT", help="品种代码")
    parser.add_argument("--day", default="2026-04-10", help="交易日 (YYYY-MM-DD)")
    parser.add_argument("--timeframe", default="minutes", help="bar 周期: minutes/hours/days")
    parser.add_argument("--compression", type=int, default=1, help="压缩倍数")
    parser.add_argument("--output", default=None, help="输出 CSV 路径")
    args = parser.parse_args()

    pg_cfg = {
        "host": "8.148.188.209",
        "port": 5432,
        "dbname": "quant_lab",
        "user": "postgres",
        "password": os.environ.get("PGPASSWORD", "postgres"),
        "sslmode": "disable",
        "search_path": "public",
        "ssh": {
            "enabled": True,
            "host": "8.148.188.209",
            "port": 22,
            "user": "Administrator",
            "password": os.environ.get("SSH_PASSWORD", ""),
            "remote_bind_host": "127.0.0.1",
            "remote_bind_port": 5432,
        },
    }

    if not pg_cfg["ssh"]["password"]:
        pg_cfg["ssh"]["password"] = input("请输入 SSH 密码: ").strip()

    aggregate_tick_to_bar(
        instrument_id=args.instrument,
        trading_day=args.day,
        timeframe=args.timeframe,
        compression=args.compression,
        output_csv=args.output,
        pg_cfg=pg_cfg,
    )


if __name__ == "__main__":
    main()
