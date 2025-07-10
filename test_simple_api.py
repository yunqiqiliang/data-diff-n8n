#!/usr/bin/env python3
"""
简化的API测试，只使用JSON请求体
"""

import json
import urllib.request

def test_simple_json_request():
    """发送简单的JSON请求"""
    api_url = "http://localhost:8000/api/v1/compare/tables"

    # 构建请求体，包含所有必要参数
    request_data = {
        'source_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'target_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'source_table': 'users',
        'target_table': 'users',
        'key_columns': ['id'],  # 使用标准的 key_columns 参数
        'columns_to_compare': ['id', 'name', 'email'],
        'sample_size': 10000,
        'threads': 4,
        'case_sensitive': True,
        'bisection_threshold': 1024
    }

    print(f"发送POST请求到: {api_url}")
    print(f"请求体: {json.dumps(request_data, indent=2)}")

    try:
        # 创建请求
        request = urllib.request.Request(
            api_url,
            data=json.dumps(request_data).encode('utf-8'),
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
        try:
            error_content = e.read().decode('utf-8')
            print(f"错误内容: {error_content}")
        except:
            print("无法读取错误内容")
        return False
    except Exception as e:
        print(f"请求失败: {e}")
        return False

def test_get_request():
    """测试GET请求方式"""
    import urllib.parse

    # 构建查询参数
    params = {
        'source_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'target_connection': 'postgresql://postgres:password@postgres:5432/postgres',
        'source_table': 'users',
        'target_table': 'users',
        'key_columns': 'id',  # 字符串格式
        'columns_to_compare': 'id,name,email',  # 逗号分隔的字符串
        'sample_size': '10000',
        'threads': '4',
        'case_sensitive': 'true',
        'bisection_threshold': '1024'
    }

    query_string = urllib.parse.urlencode(params)
    api_url = f"http://localhost:8000/api/v1/compare/tables?{query_string}"

    print(f"\n发送GET请求到: {api_url}")

    try:
        response = urllib.request.urlopen(api_url)
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
    print("=== 测试POST JSON请求 ===")
    test_simple_json_request()

    print("\n=== 测试GET请求 ===")
    test_get_request()
