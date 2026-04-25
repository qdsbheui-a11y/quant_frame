# -*- coding: utf-8 -*-
"""
诊断脚本：通过 SSH 隧道连接远程 PG，查询 tick_data 表结构和样例数据
"""
import os
import sys

try:
    import psycopg2
    from sshtunnel import SSHTunnelForwarder
except ImportError as e:
    print("导入失败: %s" % e)
    sys.exit(1)

# SSH 配置
ssh_host = "8.148.188.209"
ssh_port = 22
ssh_user = "Administrator"
ssh_password = os.environ.get("SSH_PASSWORD", "")
if not ssh_password:
    ssh_password = input("请输入 SSH 密码: ").strip()

# PG 配置
pg_host = "127.0.0.1"
pg_port = 5432
pg_db = "quant_lab"
pg_user = "postgres"
pg_password = os.environ.get("PGPASSWORD", "postgres")

print("[1/4] 正在建立 SSH 隧道 %s:%d ..." % (ssh_host, ssh_port))
try:
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        remote_bind_address=(pg_host, pg_port),
        local_bind_address=("127.0.0.1", 0)
    )
    tunnel.start()
    local_port = tunnel.local_bind_port
    print("[1/4] SSH 隧道已建立，本地端口: %d" % local_port)
except Exception as e:
    print("[1/4] SSH 隧道失败: %s" % e)
    sys.exit(1)

try:
    print("[2/4] 正在连接 PostgreSQL (localhost:%d/%s) ..." % (local_port, pg_db))
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=local_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
        sslmode="disable"
    )
    cur = conn.cursor()
    print("[2/4] 连接成功")

    print("[3/4] 查询数据库表 ...")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        ORDER BY table_name
    """)
    tables = [r[0] for r in cur.fetchall()]
    print("    表列表: %s" % tables)

    if 'tick_data' in tables:
        print("[4/4] tick_data 表结构:")
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'tick_data'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        for col in columns:
            print("    %s: %s (nullable=%s)" % (col[0], col[1], col[2]))
        
        print("\n    tick_data 前 3 条数据:")
        cur.execute("SELECT * FROM tick_data LIMIT 3")
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        print("    列: %s" % col_names)
        for row in rows:
            print("    %s" % str(row))
        
        cur.execute("SELECT COUNT(*) FROM tick_data")
        count = cur.fetchone()[0]
        print("\n    tick_data 总记录数: %d" % count)
        
        print("\n    tick_data 中的品种:")
        cur.execute("SELECT DISTINCT instrument_id FROM tick_data LIMIT 10")
        symbols = [r[0] for r in cur.fetchall()]
        for s in symbols:
            print("      %s" % s)
    else:
        print("[4/4] 警告: tick_data 表不存在!")

    conn.close()
except Exception as e:
    print("[ERROR] 数据库操作失败: %s" % e)
finally:
    tunnel.stop()
    print("\nSSH 隧道已关闭")
