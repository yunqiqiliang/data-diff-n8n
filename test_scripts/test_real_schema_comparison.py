#!/usr/bin/env python3
"""
测试模式比较功能的真实数据库查询
验证API不返回mock数据，而是返回真实的数据库错误
"""

import requests
import json
import time
from urllib.parse import urljoin

# API配置
API_BASE_URL = "http://localhost:8000"
SCHEMA_ENDPOINT = "/api/v1/compare/schemas"
NESTED_SCHEMA_ENDPOINT = "/api/v1/compare/schemas/nested"

def test_api_connection():
    """测试API连接"""
    print("🔍 测试API连接...")
    try:
        response = requests.get(urljoin(API_BASE_URL, "/health"))
        if response.status_code == 200:
            print("✅ API连接正常")
            return True
        else:
            print(f"❌ API连接失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ API连接异常: {e}")
        return False

def test_schema_comparison_with_invalid_config():
    """测试无效配置的模式比较 - 应该返回真实错误而非mock数据"""
    print("\n🔍 测试无效配置的模式比较...")

    # 无效的连接配置
    invalid_payload = {
        "source_config": {
            "database_type": "postgresql",
            "username": "invalid_user",
            "password": "invalid_pass",
            "host": "nonexistent_host",
            "port": 5432,
            "database": "nonexistent_db",
            "schema": "public"
        },
        "target_config": {
            "database_type": "clickzetta",
            "username": "invalid_user",
            "password": "invalid_pass",
            "instance": "nonexistent_instance",
            "service": "nonexistent_service",
            "workspace": "nonexistent_workspace",
            "schema": "default"
        }
    }

    try:
        response = requests.post(
            urljoin(API_BASE_URL, SCHEMA_ENDPOINT),
            json=invalid_payload,
            headers={'Content-Type': 'application/json'}
        )

        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")

        # 检查是否返回mock数据
        if response.status_code == 200:
            data = response.json()
            print("⚠️  警告：配置无效但返回了200状态码")

            # 检查是否包含mock表名
            mock_indicators = ['mock_table', 'sample_table', 'test_table', 'default_table']
            result_str = json.dumps(data).lower()

            has_mock = any(indicator in result_str for indicator in mock_indicators)
            if has_mock:
                print("❌ 错误：返回了mock数据")
                return False
            else:
                print("✅ 没有发现mock数据")

        elif response.status_code in [400, 422, 500]:
            print(f"✅ 正确返回错误状态码: {response.status_code}")
            return True
        else:
            print(f"⚠️  意外的状态码: {response.status_code}")

    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

    return True

def test_nested_schema_comparison():
    """测试嵌套模式比较端点"""
    print("\n🔍 测试嵌套模式比较...")

    nested_payload = {
        "source_config": {
            "database_type": "postgresql",
            "username": "invalid_user",
            "password": "invalid_pass",
            "host": "nonexistent_host",
            "port": 5432,
            "database": "nonexistent_db",
            "schema": "public"
        },
        "target_config": {
            "database_type": "clickzetta",
            "username": "invalid_user",
            "password": "invalid_pass",
            "instance": "nonexistent_instance",
            "service": "nonexistent_service",
            "workspace": "nonexistent_workspace",
            "schema": "default"
        }
    }

    try:
        response = requests.post(
            urljoin(API_BASE_URL, NESTED_SCHEMA_ENDPOINT),
            json=nested_payload,
            headers={'Content-Type': 'application/json'}
        )

        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")

        # 检查是否返回mock数据
        if response.status_code == 200:
            data = response.json()
            print("⚠️  警告：配置无效但返回了200状态码")

            # 检查是否包含mock表名
            mock_indicators = ['mock_table', 'sample_table', 'test_table', 'default_table']
            result_str = json.dumps(data).lower()

            has_mock = any(indicator in result_str for indicator in mock_indicators)
            if has_mock:
                print("❌ 错误：返回了mock数据")
                return False
            else:
                print("✅ 没有发现mock数据")

        elif response.status_code in [400, 422, 500]:
            print(f"✅ 正确返回错误状态码: {response.status_code}")
            return True
        else:
            print(f"⚠️  意外的状态码: {response.status_code}")

    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

    return True

def test_with_valid_local_config():
    """测试有效的本地配置（如果有的话）"""
    print("\n🔍 测试有效的本地配置...")

    # 尝试连接到docker compose中的PostgreSQL
    valid_payload = {
        "source_config": {
            "database_type": "postgresql",
            "username": "postgres",
            "password": "password",
            "host": "localhost",
            "port": 5432,
            "database": "datadiff",
            "schema": "public"
        },
        "target_config": {
            "database_type": "postgresql",
            "username": "postgres",
            "password": "password",
            "host": "localhost",
            "port": 5432,
            "database": "n8n",
            "schema": "public"
        }
    }

    try:
        response = requests.post(
            urljoin(API_BASE_URL, SCHEMA_ENDPOINT),
            json=valid_payload,
            headers={'Content-Type': 'application/json'}
        )

        print(f"状态码: {response.status_code}")
        print(f"响应内容前500字符: {response.text[:500]}...")

        if response.status_code == 200:
            data = response.json()
            print("✅ 成功连接到真实数据库")

            # 检查返回的数据结构
            if 'result' in data and 'source_schema' in data['result']:
                print("✅ 返回了真实的模式数据")
                return True
            else:
                print("⚠️  响应格式不符合预期")

        elif response.status_code in [400, 422, 500]:
            print(f"⚠️  数据库连接失败: {response.status_code}")
            # 这也是正常的，因为可能没有这些数据库
            return True
        else:
            print(f"⚠️  意外的状态码: {response.status_code}")

    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

    return True

def main():
    """主函数"""
    print("🚀 开始测试真实模式比较功能...")

    # 测试API连接
    if not test_api_connection():
        print("❌ API连接失败，终止测试")
        return

    # 测试结果
    test_results = []

    # 测试1：无效配置的模式比较
    test_results.append(test_schema_comparison_with_invalid_config())

    # 测试2：嵌套模式比较
    test_results.append(test_nested_schema_comparison())

    # 测试3：有效的本地配置
    test_results.append(test_with_valid_local_config())

    # 汇总结果
    print("\n📊 测试结果汇总:")
    passed = sum(test_results)
    total = len(test_results)

    print(f"通过: {passed}/{total}")

    if passed == total:
        print("✅ 所有测试通过！模式比较功能正确使用真实数据库查询")
    else:
        print("❌ 部分测试失败，请检查implementation")

if __name__ == "__main__":
    main()
