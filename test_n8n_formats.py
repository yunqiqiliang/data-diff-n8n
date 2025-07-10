#!/usr/bin/env python3
"""
测试n8n风格的请求格式
"""

import json
import urllib.parse
import urllib.request

def test_pure_json_request():
    """纯JSON请求，包含n8n可能发送的参数"""
    api_url = "http://localhost:8000/api/v1/compare/tables"

    # 模拟n8n节点可能发送的参数格式
    request_data = {
        'source_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'target_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'source_table': 'users',
        'target_table': 'users',
        'operation_type': 'compareTables',
        'primary_key_columns': ['id'],  # n8n可能用这个参数名
        'columns_to_compare': ['id', 'name', 'email'],
        'sample_size': 10000,
        'threads': 4,
        'case_sensitive': True,
        'bisection_threshold': 1024,
        'where_condition': None,
        'kwargs': {}
    }

    print(f"发送纯JSON POST请求")
    print(f"请求体: {json.dumps(request_data, indent=2)}")

    try:
        request = urllib.request.Request(
            api_url,
            data=json.dumps(request_data).encode('utf-8'),
            headers={
                'Content-Type': 'application/json'
            },
            method='POST'
        )

        response = urllib.request.urlopen(request)
        response_data = response.read().decode('utf-8')

        print(f"响应状态码: {response.status}")
        print(f"响应内容: {response_data}")

        return response.status == 200

    except urllib.error.HTTPError as e:
        print(f"HTTP错误: {e.code}")
        try:
            error_content = e.read().decode('utf-8')
            print(f"错误内容: {error_content}")
        except:
            print("无法读取错误内容")
        return False
    except Exception as e:
        print(f"请求失败: {e}")
        return False

def test_form_data_request():
    """测试表单数据请求"""
    api_url = "http://localhost:8000/api/v1/compare/tables"

    # 表单数据格式
    form_data = {
        'source_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'target_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'source_table': 'users',
        'target_table': 'users',
        'primary_key_columns[]': 'id',  # 数组格式
        'columns_to_compare[]': 'id,name,email',
        'sample_size': '10000',
        'threads': '4',
        'case_sensitive': 'true',
        'bisection_threshold': '1024'
    }

    print(f"\n发送表单数据POST请求")
    print(f"表单数据: {form_data}")

    try:
        # 编码表单数据
        data = urllib.parse.urlencode(form_data).encode('utf-8')

        request = urllib.request.Request(
            api_url,
            data=data,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            method='POST'
        )

        response = urllib.request.urlopen(request)
        response_data = response.read().decode('utf-8')

        print(f"响应状态码: {response.status}")
        print(f"响应内容: {response_data}")

        return response.status == 200

    except urllib.error.HTTPError as e:
        print(f"HTTP错误: {e.code}")
        try:
            error_content = e.read().decode('utf-8')
            print(f"错误内容: {error_content}")
        except:
            print("无法读取错误内容")
        return False
    except Exception as e:
        print(f"请求失败: {e}")
        return False

if __name__ == "__main__":
    print("=== 测试纯JSON请求（使用primary_key_columns参数） ===")
    test_pure_json_request()

    print("\n=== 测试表单数据请求 ===")
    test_form_data_request()
