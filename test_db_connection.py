#!/usr/bin/env python3
import psycopg2
import time

def test_connection(host, port, dbname, user, password):
    try:
        print(f"尝试连接到 {host}:{port} 数据库 {dbname}...")
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print("✅ 连接成功！")
        
        # 测试查询
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"PostgreSQL 版本: {version}")
        
        cur.execute("SELECT current_user;")
        current_user = cur.fetchone()[0]
        print(f"当前用户: {current_user}")
        
        cur.execute("SELECT datname FROM pg_database;")
        databases = [row[0] for row in cur.fetchall()]
        print(f"可用数据库: {databases}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False

def create_database(host, port, user, password, dbname):
    try:
        # 先连接到默认的 postgres 数据库
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname="postgres",
            user=user,
            password=password
        )
        conn.autocommit = True
        
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}';")
        if cur.fetchone():
            print(f"数据库 {dbname} 已存在")
        else:
            cur.execute(f"CREATE DATABASE {dbname};")
            print(f"✅ 数据库 {dbname} 创建成功")
        
        conn.close()
        return True
    except Exception as e:
        print(f"❌ 创建数据库失败: {e}")
        return False

if __name__ == "__main__":
    # 测试不同的密码组合
    test_cases = [
        ("postgres", "postgres"),
        ("postgres", "123456"),
        ("postgres", "root"),
        ("root", "root"),
    ]
    
    for user, password in test_cases:
        print(f"\n=== 测试用户: {user}, 密码: {password} ===")
        test_connection("localhost", 5432, "postgres", user, password)
        
    # 尝试创建数据库
    print("\n=== 尝试创建数据库 ===")
    create_database("localhost", 5432, "postgres", "postgres", "my_bt_lab")
