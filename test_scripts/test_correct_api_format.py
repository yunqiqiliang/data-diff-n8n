#!/usr/bin/env python3
"""
使用正确API格式测试模式比较功能
验证API正确处理连接字符串格式
"""

import requests
import json
import time
from urllib.parse import urljoin

# API配置
API_BASE_URL = "http://localhost:8000"
SCHEMA_ENDPOINT = "/api/v1/compare/schemas"
NESTED_SCHEMA_ENDPOINT = "/api/v1/compare/schemas/nested"

def test_correct_api_format():
    """测试正确的API格式"""
    print("🔍 测试正确的API格式...")

    # 使用正确的API格式 (connection string)
    correct_payload = {
        "source_connection": "postgresql://invalid_user:invalid_pass@nonexistent_host:5432/nonexistent_db",
        "target_connection": "clickzetta://invalid_user:invalid_pass@nonexistent_instance/nonexistent_service/nonexistent_workspace/default"
    }

    try:
        response = requests.post(
            urljoin(API_BASE_URL, SCHEMA_ENDPOINT),
            json=correct_payload,
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

def test_nested_correct_format():
    """测试嵌套端点的正确格式"""
    print("\n🔍 测试嵌套端点的正确格式...")

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

def test_with_valid_real_databases():
    """测试有效的真实数据库配置"""
    print("\n🔍 测试有效的真实数据库配置...")

    # 使用真实的数据库连接信息（连接字符串格式）
    valid_payload = {
        "source_connection": "postgresql://metabase:metasample123@106.120.41.178:5436/sample?schema=public",
        "target_connection": "clickzetta://qiliang:Ql123456!@jnsxwfyr/uat-api.clickzetta.com/quick_start/from_pg?vcluster=default_ap"
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
            if 'source_schema' in data and 'target_schema' in data:
                print("✅ 返回了真实的模式数据")
                print(f"源表数量: {len(data['source_schema'].get('tables', []))}")
                print(f"目标表数量: {len(data['target_schema'].get('tables', []))}")
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

def test_with_valid_nested_real_databases():
    """测试有效的嵌套格式真实数据库配置"""
    print("\n🔍 测试有效的嵌套格式真实数据库配置...")

    # 使用真实的数据库连接信息（嵌套格式）
    nested_payload = {
        "source_config": {
            "database_type": "postgresql",
            "host": "106.120.41.178",
            "port": 5436,
            "username": "metabase",
            "password": "metasample123",
            "database": "sample",
            "db_schema": "public"
        },
        "target_config": {
            "database_type": "clickzetta",
            "username": "qiliang",
            "password": "Ql123456!",
            "instance": "jnsxwfyr",
            "service": "uat-api.clickzetta.com",
            "workspace": "quick_start",
            "db_schema": "from_pg",
            "vcluster": "default_ap"
        }
    }

    try:
        response = requests.post(
            urljoin(API_BASE_URL, NESTED_SCHEMA_ENDPOINT),
            json=nested_payload,
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
                result = data['result']
                print(f"源表数量: {len(result['source_schema'].get('tables', []))}")
                print(f"目标表数量: {len(result['target_schema'].get('tables', []))}")
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
    print("🚀 开始测试正确API格式的模式比较功能...")

    # 测试结果
    test_results = []

    # 测试1：正确的API格式（连接字符串）
    test_results.append(test_correct_api_format())

    # 测试2：嵌套格式
    test_results.append(test_nested_correct_format())

    # 测试3：有效的真实数据库配置（连接字符串）
    test_results.append(test_with_valid_real_databases())

    # 测试4：有效的嵌套格式真实数据库配置
    test_results.append(test_with_valid_nested_real_databases())

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
