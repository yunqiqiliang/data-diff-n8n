#!/usr/bin/env python3
"""
测试 DataComparison 节点的连接信息提取功能
"""

import requests
import json
import time

def test_connection_extraction():
    """测试连接信息提取功能"""

    # 模拟上游数据，包含连接信息
    test_data = {
        "workflowData": {
            "nodes": [
                {
                    "name": "Database Connector",
                    "type": "databaseConnector",
                    "data": {
                        "connectionUrl": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
                        "databaseType": "postgresql"
                    }
                },
                {
                    "name": "Clickzetta Connector",
                    "type": "clickzettaConnector",
                    "data": {
                        "connectionUrl": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
                        "databaseType": "clickzetta"
                    }
                },
                {
                    "name": "Data Comparison",
                    "type": "dataComparison",
                    "data": {
                        "operation": "compareTables",
                        "sourceTable": "accounts",
                        "targetTable": "accounts",
                        "autoFillFromUpstream": True
                    }
                }
            ]
        }
    }

    print("🔍 测试连接信息提取功能...")
    print("=" * 50)

    # 测试用例1：检查上游数据提取
    print("📋 测试用例1: 上游数据提取")
    print("输入数据包含:")
    print(f"  - PostgreSQL 连接: postgresql://metabase:***@106.120.41.178:5436/sample")
    print(f"  - ClickZetta 连接: clickzetta://qiliang:***@jnsxwfyr.uat-api.clickzetta.com/quick_start")
    print(f"  - 表名: accounts")
    print()

    # 测试用例2：API 健康检查
    print("📋 测试用例2: API 健康检查")
    try:
        response = requests.get("http://localhost:8000/health")
        if response.status_code == 200:
            print("✅ API 服务健康")
        else:
            print(f"❌ API 服务不健康: {response.status_code}")
    except Exception as e:
        print(f"❌ API 连接失败: {e}")
    print()

    # 测试用例3：直接测试连接
    print("📋 测试用例3: 连接验证")
    pg_config = {
        "database_type": "postgresql",
        "host": "106.120.41.178",
        "port": 5436,
        "username": "metabase",
        "password": "metasample123",
        "database": "sample"
    }

    try:
        response = requests.post("http://localhost:8000/api/v1/connections/test", json=pg_config)
        if response.status_code == 200:
            print("✅ PostgreSQL 连接验证成功")
        else:
            print(f"⚠️ PostgreSQL 连接验证: {response.status_code}")
    except Exception as e:
        print(f"❌ 连接验证失败: {e}")
    print()

    # 测试用例4：模拟数据比较请求
    print("📋 测试用例4: 数据比较请求")
    comparison_config = {
        "source_config": {
            "database_type": "postgresql",
            "host": "106.120.41.178",
            "port": 5436,
            "username": "metabase",
            "password": "metasample123",
            "database": "sample"
        },
        "target_config": {
            "database_type": "clickzetta",
            "instance": "jnsxwfyr",
            "service": "uat-api.clickzetta.com",
            "username": "qiliang",
            "password": "Ql123456!",
            "database": "quick_start",
            "db_schema": "from_pg",
            "vcluster": "default_ap"
        },
        "comparison_config": {
            "source_table": "accounts",
            "target_table": "accounts",
            "key_columns": ["id"]
        }
    }

    try:
        response = requests.post("http://localhost:8000/api/v1/compare/tables/nested", json=comparison_config)
        if response.status_code == 200:
            print("✅ 数据比较请求成功")
            result = response.json()
            print(f"  - 比较ID: {result.get('comparison_id', 'N/A')}")
            print(f"  - 状态: {result.get('status', 'N/A')}")
        else:
            print(f"⚠️ 数据比较请求: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"  - 错误详情: {error_detail.get('detail', 'N/A')}")
            except:
                print(f"  - 响应内容: {response.text[:200]}...")
    except Exception as e:
        print(f"❌ 数据比较请求失败: {e}")
    print()

    print("🎯 测试建议:")
    print("1. 确保上游节点输出包含 connectionUrl 或 connectionString 字段")
    print("2. 使用表达式引用: {{ $('Database Connector').item.json.connectionUrl }}")
    print("3. 启用 'Auto-fill from upstream' 选项")
    print("4. 检查节点执行顺序，确保连接器节点在数据比较节点之前")
    print()

    return True

if __name__ == "__main__":
    test_connection_extraction()
