#!/usr/bin/env python3
"""
模拟n8n节点发送的请求格式
"""

import json
import urllib.parse
import urllib.request

def test_n8n_style_request():
    """模拟n8n节点发送的请求"""
    api_url = "http://localhost:8000/api/v1/compare/tables"

    # 模拟n8n节点构建的查询参数
    query_params = {
        'source_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'target_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'source_table': 'users',
        'target_table': 'users',
        'operation_type': 'compareTables',
        'primary_key_columns[]': 'id',  # n8n格式的数组参数
        'columns_to_compare[]': 'id,name,email',  # n8n格式的数组参数
        'kwargs': '{}'
    }

    # 构建请求体
    body_data = {
        'primary_key_columns': ['id'],
        'update_key_columns': [],
        'columns_to_compare': ['id', 'name', 'email'],
        'sample_size': 10000,
        'threads': 4,
        'case_sensitive': True,
        'bisection_threshold': 1024,
        'where_condition': None
    }

    # 构建完整URL
    query_string = urllib.parse.urlencode(query_params)
    full_url = f"{api_url}?{query_string}"

    print(f"发送请求到: {full_url}")
    print(f"请求体: {json.dumps(body_data, indent=2)}")

    try:
        # 创建请求
        request = urllib.request.Request(
            full_url,
            data=json.dumps(body_data).encode('utf-8'),
            headers={
                'Content-Type': 'application/json'
            },
            method='POST'
        )

        # 发送请求
        response = urllib.request.urlopen(request)
        response_data = response.read().decode('utf-8')

        print(f"响应状态码: {response.status}")
        print(f"响应内容: {response_data}")

        return response.status == 200

    except urllib.error.HTTPError as e:
        print(f"HTTP错误: {e.code}")
        print(f"错误内容: {e.read().decode('utf-8')}")
        return False
    except Exception as e:
        print(f"请求失败: {e}")
        return False

if __name__ == "__main__":
    test_n8n_style_request()
