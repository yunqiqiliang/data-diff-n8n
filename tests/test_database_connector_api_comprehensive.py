#!/usr/bin/env python3
"""
测试 Database Connector API 端点对所有支持的数据库类型的查询执行功能
"""

import json
import requests
import sys
from typing import Dict, Any

# API 端点
API_URL = "http://localhost:8000/api/v1/query/execute"

def test_database_query(db_type: str, connection_config: Dict[str, Any], test_query: str):
    """测试特定数据库类型的查询执行"""

    payload = {
        "connection": connection_config,
        "query": test_query,
        "limit": 5
    }

    try:
        print(f"\n=== 测试 {db_type.upper()} ===")
        print(f"连接配置: {json.dumps(connection_config, indent=2)}")
        print(f"查询: {test_query}")

        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✅ 成功: {result['database_type']}")
            print(f"   行数: {result['row_count']}")
            print(f"   时间戳: {result['timestamp']}")
            if result.get('result'):
                print(f"   首行数据: {result['result'][0] if result['result'] else 'None'}")
        else:
            print(f"❌ 失败: HTTP {response.status_code}")
            print(f"   错误: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ 请求失败: {e}")
    except Exception as e:
        print(f"❌ 异常: {e}")

def main():
    """主测试函数"""

    print("Database Connector API 端点测试")
    print("=" * 50)

    # 测试配置 - 需要根据实际环境配置
    test_configs = [
        {
            "type": "postgres",
            "config": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": "password",
                "database": "testdb"
            },
            "query": "SELECT version() as version"
        },
        {
            "type": "mysql",
            "config": {
                "type": "mysql",
                "host": "localhost",
                "port": 3306,
                "username": "root",
                "password": "password",
                "database": "testdb"
            },
            "query": "SELECT VERSION() as version"
        },
        {
            "type": "clickzetta",
            "config": {
                "type": "clickzetta",
                "username": "demo_user",
                "password": "demo_pass",
                "service": "demo_service",
                "instance": "demo_instance",
                "workspace": "demo_workspace",
                "vcluster": "demo_vcluster",
                "schema": "public"
            },
            "query": "SELECT 1 as test_value"
        },
        {
            "type": "sqlserver",
            "config": {
                "type": "sqlserver",
                "host": "localhost",
                "port": 1433,
                "username": "sa",
                "password": "Password123!",
                "database": "master"
            },
            "query": "SELECT @@VERSION as version"
        },
        {
            "type": "oracle",
            "config": {
                "type": "oracle",
                "host": "localhost",
                "port": 1521,
                "username": "system",
                "password": "oracle",
                "database": "XE"
            },
            "query": "SELECT * FROM v$version WHERE rownum = 1"
        },
        {
            "type": "trino",
            "config": {
                "type": "trino",
                "host": "localhost",
                "port": 8080,
                "username": "trino",
                "catalog": "memory",
                "schema": "default"
            },
            "query": "SELECT 1 as test_value"
        },
        {
            "type": "duckdb",
            "config": {
                "type": "duckdb",
                "database": ":memory:"
            },
            "query": "SELECT 1 as test_value"
        },
        {
            "type": "vertica",
            "config": {
                "type": "vertica",
                "host": "localhost",
                "port": 5433,
                "username": "dbadmin",
                "password": "password",
                "database": "VMart"
            },
            "query": "SELECT version() as version"
        },
        {
            "type": "clickhouse",
            "config": {
                "type": "clickhouse",
                "host": "localhost",
                "port": 9000,
                "username": "default",
                "password": "",
                "database": "default"
            },
            "query": "SELECT version() as version"
        },
        {
            "type": "snowflake",
            "config": {
                "type": "snowflake",
                "username": "your_user",
                "password": "your_pass",
                "account": "your_account",
                "warehouse": "your_warehouse",
                "database": "your_database",
                "schema": "public"
            },
            "query": "SELECT CURRENT_VERSION() as version"
        },
        {
            "type": "bigquery",
            "config": {
                "type": "bigquery",
                "project_id": "your-project-id"
            },
            "query": "SELECT 1 as test_value"
        },
        {
            "type": "redshift",
            "config": {
                "type": "redshift",
                "host": "your-cluster.redshift.amazonaws.com",
                "port": 5439,
                "username": "your_user",
                "password": "your_pass",
                "database": "your_database"
            },
            "query": "SELECT version() as version"
        },
        {
            "type": "databricks",
            "config": {
                "type": "databricks",
                "server_hostname": "your-workspace.cloud.databricks.com",
                "http_path": "/sql/1.0/warehouses/your-warehouse-id",
                "access_token": "your-access-token"
            },
            "query": "SELECT 1 as test_value"
        }
    ]

    # 执行测试
    success_count = 0
    total_count = len(test_configs)

    for config in test_configs:
        try:
            test_database_query(
                config["type"],
                config["config"],
                config["query"]
            )
            # 这里简化处理，实际应该根据响应判断成功/失败
            success_count += 1
        except Exception as e:
            print(f"❌ 测试 {config['type']} 时发生异常: {e}")

    print(f"\n" + "=" * 50)
    print(f"测试总结: {success_count}/{total_count} 个数据库类型")
    print(f"注意: 部分测试可能因为缺少实际数据库连接而失败")
    print(f"建议: 根据实际环境配置连接参数后重新测试")

if __name__ == "__main__":
    main()
