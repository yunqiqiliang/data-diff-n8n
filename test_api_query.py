#!/usr/bin/env python3
"""
测试脚本：直接调用数据比对API，专注于查询参数格式
"""

import requests
import json

# 测试 API 端点
API_URL = "http://localhost:8000/api/v1/compare/tables"

# 测试数据
test_data = {
    "source_connection": "postgresql://postgres:password@postgres:5432/postgres",
    "target_connection": "postgresql://postgres:password@postgres:5432/postgres",
    "source_table": "users",
    "target_table": "users",
    "primary_key_columns[]": "id",
    "operation_type": "compareTables",
    "sample_size": 10000,
    "threads": 4,
    "columns_to_compare[]": "id",
    "case_sensitive": True,
    "kwargs": "{}"  # 添加 kwargs 参数
}

def test_query_params():
    """使用查询参数测试API"""
    print("使用查询参数测试API...")

    try:
        response = requests.post(
            API_URL,
            params=test_data,
            headers={"Content-Type": "application/json"},
            data="{}"
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

if __name__ == "__main__":
    test_query_params()
