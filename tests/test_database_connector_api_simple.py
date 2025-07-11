#!/usr/bin/env python3
"""
简单测试 Database Connector API 端点功能
"""

import json
import requests
import sys

# API 端点
API_URL = "http://localhost:8000/api/v1/query/execute"

def test_basic_functionality():
    """测试基本功能"""

    print("测试 Database Connector API 端点")
    print("=" * 40)

    # 测试 1: DuckDB 内存数据库查询
    print("\n1. 测试 DuckDB 内存数据库查询")
    payload = {
        "connection": {
            "type": "duckdb",
            "database": ":memory:"
        },
        "query": "SELECT 1 as test_value, 'Hello World' as message",
        "limit": 5
    }

    try:
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✅ 成功: {result['database_type']}")
            print(f"   行数: {result['row_count']}")
            print(f"   数据: {result['result']}")
        else:
            print(f"❌ 失败: HTTP {response.status_code}")
            print(f"   错误: {response.text}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    # 测试 2: 错误处理 - 不支持的数据库类型
    print("\n2. 测试错误处理 - 不支持的数据库类型")
    payload = {
        "connection": {
            "type": "unsupported_db",
            "host": "localhost"
        },
        "query": "SELECT 1"
    }

    try:
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 400:
            print("✅ 正确处理了不支持的数据库类型")
        else:
            print(f"❌ 未正确处理错误: HTTP {response.status_code}")
            print(f"   响应: {response.text}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    # 测试 3: 错误处理 - 空查询
    print("\n3. 测试错误处理 - 空查询")
    payload = {
        "connection": {
            "type": "duckdb",
            "database": ":memory:"
        },
        "query": ""
    }

    try:
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 400:
            print("✅ 正确处理了空查询")
        else:
            print(f"❌ 未正确处理错误: HTTP {response.status_code}")
            print(f"   响应: {response.text}")
    except Exception as e:
        print(f"❌ 异常: {e}")

    # 测试 4: 复杂查询
    print("\n4. 测试复杂查询")
    payload = {
        "connection": {
            "type": "duckdb",
            "database": ":memory:"
        },
        "query": """
            WITH test_data AS (
                SELECT 1 as id, 'Alice' as name, 25 as age
                UNION ALL
                SELECT 2 as id, 'Bob' as name, 30 as age
                UNION ALL
                SELECT 3 as id, 'Charlie' as name, 35 as age
            )
            SELECT * FROM test_data WHERE age > 25
        """,
        "limit": 10
    }

    try:
        response = requests.post(
            API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✅ 成功: {result['database_type']}")
            print(f"   行数: {result['row_count']}")
            print(f"   数据: {result['result']}")
        else:
            print(f"❌ 失败: HTTP {response.status_code}")
            print(f"   错误: {response.text}")
    except Exception as e:
        print(f"❌ 异常: {e}")

def test_api_health():
    """测试 API 健康状态"""

    print("\n检查 API 健康状态")
    print("-" * 20)

    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("✅ API 服务正常运行")
        else:
            print(f"❌ API 服务异常: HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ API 服务不可用: {e}")
        print("请确保 API 服务正在运行:")
        print("  cd /Users/liangmo/Documents/GitHub/data-diff-n8n/n8n")
        print("  python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000")
        return False

    return True

def main():
    """主函数"""

    # 首先检查 API 健康状态
    if not test_api_health():
        sys.exit(1)

    # 执行功能测试
    test_basic_functionality()

    print("\n" + "=" * 40)
    print("测试完成")
    print("注意: 某些数据库类型需要实际的数据库连接才能测试")

if __name__ == "__main__":
    main()
