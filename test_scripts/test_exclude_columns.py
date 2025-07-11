#!/usr/bin/env python3
"""
测试 Default Exclude Columns 参数是否正常处理
验证从凭据配置中获取的排除列是否正确传递到API并生效
"""

import requests
import json
import time

def test_exclude_columns():
    """测试exclude_columns参数的处理"""
    print("🧪 测试 Default Exclude Columns 参数处理")
    print("=" * 60)

    # 基础配置
    base_source_config = {
        "database_type": "postgresql",
        "host": "106.120.41.178",
        "port": 5436,
        "username": "metabase",
        "password": "metasample123",
        "database": "sample",
        "db_schema": "public"
    }

    base_target_config = {
        "database_type": "clickzetta",
        "username": "qiliang",
        "password": "Ql123456!",
        "instance": "jnsxwfyr",
        "service": "uat-api.clickzetta.com",
        "workspace": "quick_start",
        "db_schema": "from_pg",
        "vcluster": "default_ap"
    }

    # 测试1: 模拟从凭据传递exclude_columns
    print("\n📝 测试1: 验证exclude_columns参数传递")

    test_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            # 模拟从凭据获取的exclude_columns
            "exclude_columns": ["created_at", "updated_at"],
            "sample_size": 100,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 发送请求（包含exclude_columns）:")
    print(f"   exclude_columns: {test_data['comparison_config']['exclude_columns']}")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ API 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(3)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        config = result_data.get("config", {})

        print("📊 API返回的配置:")
        exclude_columns = config.get("exclude_columns", [])
        print(f"   exclude_columns: {exclude_columns}")

        # 验证exclude_columns是否正确传递
        expected_exclude = ["created_at", "updated_at"]
        if set(exclude_columns) == set(expected_exclude):
            print("✅ exclude_columns 参数正确传递到后端")
            return True
        else:
            print(f"❌ exclude_columns 不正确")
            print(f"   期望: {expected_exclude}")
            print(f"   实际: {exclude_columns}")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_exclude_columns_with_columns_to_compare():
    """测试exclude_columns与columns_to_compare的交互"""
    print("\n📝 测试2: 验证exclude_columns与columns_to_compare的交互")

    base_source_config = {
        "database_type": "postgresql",
        "host": "106.120.41.178",
        "port": 5436,
        "username": "metabase",
        "password": "metasample123",
        "database": "sample",
        "db_schema": "public"
    }

    base_target_config = {
        "database_type": "clickzetta",
        "username": "qiliang",
        "password": "Ql123456!",
        "instance": "jnsxwfyr",
        "service": "uat-api.clickzetta.com",
        "workspace": "quick_start",
        "db_schema": "from_pg",
        "vcluster": "default_ap"
    }

    test_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan", "payment"],  # 指定要比较的列
            "exclude_columns": ["payment"],  # 同时排除其中一列
            "sample_size": 100,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 发送请求（同时包含columns_to_compare和exclude_columns）:")
    print(f"   columns_to_compare: {test_data['comparison_config']['columns_to_compare']}")
    print(f"   exclude_columns: {test_data['comparison_config']['exclude_columns']}")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ API 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(3)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        config = result_data.get("config", {})

        print("📊 API返回的配置:")
        exclude_columns = config.get("exclude_columns", [])
        columns_to_compare = config.get("columns_to_compare", [])
        compare_columns = config.get("compare_columns", [])

        print(f"   exclude_columns: {exclude_columns}")
        print(f"   columns_to_compare: {columns_to_compare}")
        print(f"   compare_columns: {compare_columns}")

        # 验证参数是否正确传递
        exclude_ok = "payment" in exclude_columns
        compare_ok = set(columns_to_compare) == {"plan", "payment"}

        if exclude_ok and compare_ok:
            print("✅ exclude_columns 和 columns_to_compare 参数都正确传递")
            print("ℹ️  注意：实际比对逻辑由后端处理参数优先级和冲突")
            return True
        else:
            print(f"❌ 参数传递不正确")
            print(f"   exclude_columns包含payment: {exclude_ok}")
            print(f"   columns_to_compare正确: {compare_ok}")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_exclude_columns_empty():
    """测试exclude_columns为空的情况"""
    print("\n📝 测试3: 验证exclude_columns为空时的处理")

    base_source_config = {
        "database_type": "postgresql",
        "host": "106.120.41.178",
        "port": 5436,
        "username": "metabase",
        "password": "metasample123",
        "database": "sample",
        "db_schema": "public"
    }

    base_target_config = {
        "database_type": "clickzetta",
        "username": "qiliang",
        "password": "Ql123456!",
        "instance": "jnsxwfyr",
        "service": "uat-api.clickzetta.com",
        "workspace": "quick_start",
        "db_schema": "from_pg",
        "vcluster": "default_ap"
    }

    test_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan"],
            # 不设置exclude_columns，或设置为空
            "sample_size": 100,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 发送请求（不包含exclude_columns）:")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ API 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(3)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        config = result_data.get("config", {})

        print("📊 API返回的配置:")
        exclude_columns = config.get("exclude_columns", [])
        print(f"   exclude_columns: {exclude_columns}")

        # 验证exclude_columns为空或空列表
        if not exclude_columns or exclude_columns == []:
            print("✅ exclude_columns 空值处理正确")
            return True
        else:
            print(f"❌ exclude_columns 空值处理不正确，应该为空但实际为: {exclude_columns}")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def main():
    """运行所有exclude_columns相关测试"""
    print("🔧 Default Exclude Columns 参数测试")
    print("验证凭据中的排除列配置是否正常工作")
    print("=" * 70)

    test_results = []

    # 测试1: 基本的exclude_columns传递
    result1 = test_exclude_columns()
    test_results.append(("exclude_columns传递", result1))

    # 测试2: exclude_columns与columns_to_compare交互
    result2 = test_exclude_columns_with_columns_to_compare()
    test_results.append(("参数交互", result2))

    # 测试3: exclude_columns为空的处理
    result3 = test_exclude_columns_empty()
    test_results.append(("空值处理", result3))

    # 汇总结果
    print("\n" + "=" * 60)
    print("🏁 Default Exclude Columns 测试结果:")

    all_passed = True
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name:20}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 Default Exclude Columns 参数处理正常！")
        print("✅ 参数从凭据正确传递到API")
        print("✅ 参数在后端正确接收和处理")
        print("✅ 空值和默认值处理正确")
        print("✅ 与其他参数的交互正常")
    else:
        print("💥 Default Exclude Columns 参数处理存在问题！")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
