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

# 查询 bar_data 表的结构
print("bar_data 表结构：")
print("-" * 50)
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'bar_data' 
    ORDER BY ordinal_position;
""")
columns = cur.fetchall()
for col in columns:
    print(f"  {col[0]}: {col[1]}")

# 查询前几条数据
print("\nbar_data 表前 5 条数据：")
print("-" * 50)
cur.execute("SELECT * FROM bar_data LIMIT 5")
rows = cur.fetchall()
for row in rows:
    print(f"  {row}")

# 查询不同的股票代码
print("\nbar_data 表中的股票代码：")
print("-" * 50)
cur.execute("SELECT DISTINCT code FROM bar_data ORDER BY code LIMIT 20")
codes = cur.fetchall()
for code in codes:
    print(f"  {code[0]}")

conn.close()
