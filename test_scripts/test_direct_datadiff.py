#!/usr/bin/env python3
"""
直接测试data-diff库获取PostgreSQL表列表
"""

import sys
import os
sys.path.append('/Users/liangmo/Documents/GitHub/data-diff-n8n')

try:
    from data_diff import connect
    from data_diff.queries.api import Expr
    print("✅ 成功导入data-diff库")
except ImportError as e:
    print(f"❌ 无法导入data-diff库: {e}")
    exit(1)

def test_postgresql_connection():
    """测试PostgreSQL连接和表列表获取"""
    print("🔍 测试PostgreSQL连接...")

    # 连接字符串
    connection_string = "postgresql://metabase:metasample123@106.120.41.178:5436/sample"

    try:
        # 连接到数据库
        db = connect(connection_string)
        print(f"✅ 成功连接到数据库: {type(db)}")

        # 测试简单查询
        print("\n🔍 测试查询表列表...")
        tables_query = Expr("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)

        tables_result = db.query(tables_query, list)
        print(f"查询结果类型: {type(tables_result)}")
        print(f"查询结果: {tables_result}")

        # 提取表名
        if tables_result:
            tables = [row[0] for row in tables_result]
            print(f"✅ 找到 {len(tables)} 个表:")
            for table in tables:
                print(f"  - {table}")
        else:
            print("❌ 未找到任何表")

        # 测试每个表的模式信息
        print("\n🔍 测试获取表模式信息...")
        for table_name in tables[:3]:  # 只测试前3个表
            try:
                table_path = [table_name]  # public schema
                schema = db.query_table_schema(table_path)
                print(f"\n表 {table_name} 的模式:")
                for col_name, col_info in schema.items():
                    print(f"  - {col_name}: {col_info.data_type} (nullable: {col_info.nullable})")
            except Exception as e:
                print(f"❌ 获取表 {table_name} 模式失败: {e}")

    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False

    return True

if __name__ == "__main__":
    print("🚀 开始测试data-diff直接连接...")
    result = test_postgresql_connection()
    if result:
        print("\n✅ 测试完成")
    else:
        print("\n❌ 测试失败")
