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

# 查询所有表
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")

tables = cur.fetchall()

print("数据库中的表：")
print("-" * 50)
for table in tables:
    print(f"  {table[0]}")

# 查询 bars 表的结构
if any('bars' in t for t in tables):
    print("\nbars 表结构：")
    print("-" * 50)
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'bars' 
        ORDER BY ordinal_position;
    """)
    columns = cur.fetchall()
    for col in columns:
        print(f"  {col[0]}: {col[1]}")

# 查询前几条数据
if any('bars' in t for t in tables):
    print("\nbars 表前 5 条数据：")
    print("-" * 50)
    cur.execute("SELECT * FROM bars LIMIT 5")
    rows = cur.fetchall()
    for row in rows:
        print(f"  {row}")

conn.close()
