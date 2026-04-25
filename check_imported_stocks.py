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

# 查询所有已导入的股票代码
cur.execute("""
    SELECT DISTINCT code, market 
    FROM instrument 
    WHERE asset_type = 'stock'
    ORDER BY code;
""")

stocks = cur.fetchall()

print("已导入的股票代码：")
print("-" * 50)
for code, market in stocks:
    print(f"  {code} ({market})")

print(f"\n总计: {len(stocks)} 只股票")

conn.close()
