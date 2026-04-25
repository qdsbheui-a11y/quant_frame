#!/usr/bin/env python3
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="quant_lab",
    user="postgres",
    password="root"
)

cur = conn.cursor()

# 查询 instrument 表的结构
print("instrument 表结构：")
print("-" * 50)
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'instrument' 
    ORDER BY ordinal_position;
""")
columns = cur.fetchall()
for col in columns:
    print(f"  {col[0]}: {col[1]}")

# 查询前几条数据
print("\ninstrument 表前 10 条数据：")
print("-" * 50)
cur.execute("SELECT * FROM instrument LIMIT 10")
rows = cur.fetchall()
for row in rows:
    print(f"  {row}")

# 查询关联数据
print("\nbar_data 和 instrument 关联查询（前 5 条）：")
print("-" * 50)
cur.execute("""
    SELECT i.code, i.name, b.bar_time, b.open, b.high, b.low, b.close, b.volume
    FROM bar_data b
    JOIN instrument i ON b.instrument_id = i.instrument_id
    ORDER BY b.bar_time
    LIMIT 5
""")
rows = cur.fetchall()
for row in rows:
    print(f"  {row}")

conn.close()
