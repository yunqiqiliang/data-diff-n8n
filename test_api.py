#!/usr/bin/env python3
"""
测试脚本：直接调用数据比对API，测试不同的请求格式和参数
"""

import requests
import json
import sys

# 测试 API 端点
API_URL = "http://localhost:8000/api/v1/compare/tables"

# 测试数据
test_data = {
    "source_connection": "postgresql://postgres:password@postgres:5432/postgres",
    "target_connection": "postgresql://postgres:password@postgres:5432/postgres",
    "source_table": "users",
    "target_table": "users",
    "primary_key_columns": ["id"],
    "operation_type": "compareTables",
    "sample_size": 10000,
    "threads": 4,
    "columns_to_compare": ["id", "name", "email"],
    "case_sensitive": True
}

def test_json_request():
    """测试 JSON 请求体格式"""
    print("测试 JSON 格式请求...")
    try:
        response = requests.post(
            API_URL,
            json=test_data,
            headers={"Content-Type": "application/json"}
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

def test_form_request():
    """测试表单数据格式"""
    print("测试 x-www-form-urlencoded 格式请求...")

    # 准备表单数据
    form_data = {}
    for key, value in test_data.items():
        if isinstance(value, list):
            # 处理数组参数
            for i, item in enumerate(value):
                form_data[f"{key}[{i}]"] = item
        else:
            form_data[key] = value

    try:
        response = requests.post(
            API_URL,
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

def test_query_request():
    """测试查询参数格式"""
    print("测试查询参数格式请求...")

    # 构建查询URL
    query_params = {}
    for key, value in test_data.items():
        if isinstance(value, list):
            # 处理数组参数
            query_params[key] = ",".join(str(x) for x in value)
        else:
            query_params[key] = value

    try:
        response = requests.post(
            API_URL,
            params=query_params,
            headers={"Content-Type": "application/json"}
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

def test_multipart_form_request():
    """测试 multipart/form-data 格式"""
    print("测试 multipart/form-data 格式请求...")

    # 准备表单数据
    form_data = {}
    for key, value in test_data.items():
        if isinstance(value, list):
            # 特殊处理数组
            form_data[key] = json.dumps(value)
        else:
            form_data[key] = value

    try:
        response = requests.post(
            API_URL,
            files=form_data
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

def test_get_request():
    """测试 GET 请求格式"""
    print("测试 GET 请求格式...")

    # 构建查询URL
    query_params = {}
    for key, value in test_data.items():
        if isinstance(value, list):
            # 处理数组参数
            query_params[key] = ",".join(str(x) for x in value)
        else:
            query_params[key] = value

    try:
        response = requests.get(
            API_URL,
            params=query_params
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"请求失败: {e}")
        return False

if __name__ == "__main__":
    # 根据命令行参数选择测试哪种请求格式
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
        if test_type == "json":
            test_json_request()
        elif test_type == "form":
            test_form_request()
        elif test_type == "query":
            test_query_request()
        elif test_type == "multipart":
            test_multipart_form_request()
        elif test_type == "get":
            test_get_request()
        else:
            print(f"未知测试类型: {test_type}")
    else:
        # 默认测试所有格式
        print("测试所有请求格式...")
        test_json_request()
        test_form_request()
        test_query_request()
        test_multipart_form_request()
        test_get_request()
