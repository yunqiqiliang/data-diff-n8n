#!/usr/bin/env python3
"""
测试 DataComparison 节点的上游数据提取功能
验证 ClickZetta 和 PostgreSQL 连接的正确识别
"""

import json
import time
from datetime import datetime

def test_data_extraction():
    """测试数据提取功能"""
    print("=== 测试 DataComparison 节点数据提取 ===")

    # 模拟 ClickZetta 连接器输出
    clickzetta_connection = {
        "connectionUrl": "clickzetta://test_user:test_pass@instance1.uat-api.clickzetta.com/test_db?virtualcluster=default_ap&schema=public",
        "databaseType": "clickzetta",
        "connectionConfig": {
            "type": "clickzetta",
            "instance": "instance1",
            "service": "uat-api.clickzetta.com",
            "username": "test_user",
            "password": "test_pass",
            "workspace": "test_db",
            "db_schema": "public",
            "vcluster": "default_ap"
        },
        "tables": [
            {"name": "users", "schema": "public"},
            {"name": "orders", "schema": "public"}
        ]
    }

    # 模拟 PostgreSQL 连接器输出
    postgresql_connection = {
        "connectionUrl": "postgresql://pg_user:pg_pass@localhost:5432/test_db",
        "databaseType": "postgresql",
        "connectionConfig": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "username": "pg_user",
            "password": "pg_pass",
            "database": "test_db",
            "schema": "public"
        },
        "tables": [
            {"name": "users", "schema": "public"},
            {"name": "products", "schema": "public"}
        ]
    }

    # 测试数据
    test_data = [
        {
            "name": "ClickZetta 连接 - 标准格式",
            "data": clickzetta_connection,
            "expected_type": "clickzetta"
        },
        {
            "name": "PostgreSQL 连接 - 标准格式",
            "data": postgresql_connection,
            "expected_type": "postgresql"
        },
        {
            "name": "ClickZetta 连接 - 仅 connectionConfig",
            "data": {
                "connectionConfig": {
                    "type": "clickzetta",
                    "instance": "instance2",
                    "service": "uat-api.clickzetta.com"
                }
            },
            "expected_type": "clickzetta"
        },
        {
            "name": "PostgreSQL 连接 - 仅连接字符串",
            "data": {
                "connectionUrl": "postgresql://user:pass@host:5432/db"
            },
            "expected_type": "postgresql"
        }
    ]

    print("测试数据已准备完成")
    print("请在 n8n 中创建一个工作流，使用 DataComparison 节点")
    print("然后查看 n8n 容器日志以验证调试输出:")
    print("docker-compose -f docker-compose.dev.yml logs n8n | grep -A 20 'extractUpstreamData'")
    print("\n预期的调试输出应该包含:")
    print("1. 数据库类型检测日志")
    print("2. 连接字符串解析日志")
    print("3. 提取的上游数据摘要")
    print("4. 正确的 databaseType 字段值")

    # 保存测试数据到文件供手动测试使用
    with open('/Users/liangmo/Documents/GitHub/data-diff-n8n/test_extraction_data.json', 'w', encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)

    print(f"\n测试数据已保存到: test_extraction_data.json")
    print("可以在 n8n 工作流中使用此数据进行测试")

if __name__ == "__main__":
    test_data_extraction()
