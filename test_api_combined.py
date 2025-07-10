#!/usr/bin/env python3
"""
测试脚本：直接调用数据比对API，同时使用查询参数和请求体
"""

import requests
import json

# 测试 API 端点
API_URL = "http://localhost:8000/api/v1/compare/tables"

# 查询参数
query_params = {
    "source_connection": "postgresql://postgres:password@postgres:5432/postgres",
    "target_connection": "postgresql://postgres:password@postgres:5432/postgres",
    "source_table": "users",
    "target_table": "users",
    "operation_type": "compareTables",
    "kwargs": "{}"  # 添加 kwargs 参数
}

# 请求体
body_data = {
    "primary_key_columns": ["id"],
    "update_key_columns": [],
    "columns_to_compare": ["id", "name", "email"],
    "sample_size": 10000,
    "threads": 4,
    "case_sensitive": True
}

def test_combined_approach():
    """同时使用查询参数和请求体"""
    print("同时使用查询参数和请求体...")

    try:
        response = requests.post(
            API_URL,
            params=query_params,
            json=body_data,
            headers={"Content-Type": "application/json"}
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

if __name__ == "__main__":
    test_combined_approach()
