#!/usr/bin/env python3
"""
简单的API测试 - 检查API是否正常启动
"""

import json
import urllib.parse
import urllib.request

def test_api_health():
    """测试API健康状态"""
    api_url = "http://localhost:8000/health"

    print(f"检查API健康状态: {api_url}")

    try:
        # 创建请求
        request = urllib.request.Request(api_url, method='GET')

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

def test_api_root():
    """测试API根端点"""
    api_url = "http://localhost:8000/"

    print(f"检查API根端点: {api_url}")

    try:
        # 创建请求
        request = urllib.request.Request(api_url, method='GET')

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
    print("=== API健康检查 ===")
    health_ok = test_api_health()
    print(f"健康检查结果: {'通过' if health_ok else '失败'}")

    print("\n=== API根端点检查 ===")
    root_ok = test_api_root()
    print(f"根端点检查结果: {'通过' if root_ok else '失败'}")

    print(f"\n总体结果: {'API运行正常' if health_ok and root_ok else 'API有问题'}")
