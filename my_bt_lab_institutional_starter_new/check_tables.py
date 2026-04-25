import psycopg2
conn = psycopg2.connect(host='localhost', port=5432, dbname='quant_lab', user='postgres', password='root')
cur = conn.cursor()

# 查询所有表
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
tables = [t[0] for t in cur.fetchall()]

print('quant_lab 数据库中的表：')
print('-' * 50)
for t in tables:
    print(f'  {t}')

# 检查是否有 tick_data
if 'tick_data' in tables:
    print('\ntick_data 表结构：')
    print('-' * 50)
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'tick_data'
        ORDER BY ordinal_position;
    """)
    columns = cur.fetchall()
    for col in columns:
        print(f'  {col[0]}: {col[1]}')
    
    cur.execute('SELECT COUNT(*) FROM tick_data')
    print(f'\ntick_data 总记录数: {cur.fetchone()[0]}')

conn.close()
