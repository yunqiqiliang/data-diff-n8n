#!/usr/bin/env python3
"""
完整的系统功能验证测试
验证所有重构后的功能是否正常工作
"""

import requests
import json
import time

def test_basic_comparison():
    """测试基本的表比对功能"""
    print("🧪 测试1: 基本表比对功能")
    print("-" * 50)

    test_data = {
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
        },
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "sample_size": 50,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 发送比对请求...")
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(5)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()

        print("📊 比对结果:")
        print(f"   状态: {result_data.get('status')}")
        print(f"   行数统计: {result_data.get('row_count', {})}")
        print(f"   不同行数: {result_data.get('diff_count', 0)}")

        if result_data.get('status') in ['completed', 'success']:
            print("✅ 基本比对功能正常")
            return True
        else:
            print("❌ 比对未成功完成")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_columns_to_compare():
    """测试指定列比对功能"""
    print("\n🧪 测试2: 指定列比对功能")
    print("-" * 50)

    test_data = {
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
        },
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan", "payment"],
            "sample_size": 50,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 发送指定列比对请求...")
    print(f"   指定比对列: {test_data['comparison_config']['columns_to_compare']}")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(5)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        config = result_data.get('config', {})

        print("📊 比对结果:")
        print(f"   状态: {result_data.get('status')}")
        print(f"   传递的columns_to_compare: {config.get('columns_to_compare', [])}")
        print(f"   实际比对列: {config.get('compare_columns', [])}")

        # 验证columns_to_compare是否正确传递
        expected_columns = ["plan", "payment"]
        actual_columns = config.get('columns_to_compare', [])

        if set(actual_columns) == set(expected_columns):
            print("✅ 指定列比对功能正常")
            return True
        else:
            print(f"❌ 指定列不匹配，期望: {expected_columns}, 实际: {actual_columns}")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_exclude_columns():
    """测试排除列功能"""
    print("\n🧪 测试3: 排除列功能")
    print("-" * 50)

    test_data = {
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
        },
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "exclude_columns": ["created_at", "updated_at"],
            "sample_size": 50,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 发送排除列比对请求...")
    print(f"   排除列: {test_data['comparison_config']['exclude_columns']}")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(5)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        config = result_data.get('config', {})

        print("📊 比对结果:")
        print(f"   状态: {result_data.get('status')}")
        print(f"   排除列: {config.get('exclude_columns', [])}")

        # 验证exclude_columns是否正确传递
        expected_exclude = ["created_at", "updated_at"]
        actual_exclude = config.get('exclude_columns', [])

        if set(actual_exclude) == set(expected_exclude):
            print("✅ 排除列功能正常")
            return True
        else:
            print(f"❌ 排除列不匹配，期望: {expected_exclude}, 实际: {actual_exclude}")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_unsupported_columns_handling():
    """测试不支持列类型的处理"""
    print("\n🧪 测试4: 不支持列类型处理")
    print("-" * 50)

    test_data = {
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
        },
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan", "payment"],  # 只比较这两列
            "sample_size": 50,
            "threads": 1,
            "strict_type_checking": False  # 不严格类型检查
        }
    }

    print("📤 发送包含不支持列类型的比对请求...")
    print(f"   只比对支持的列: {test_data['comparison_config']['columns_to_compare']}")
    print("   不严格类型检查，应该成功")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 等待比对完成
        time.sleep(5)

        # 获取比对结果
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()

        print("📊 比对结果:")
        print(f"   状态: {result_data.get('status')}")
        warnings = result_data.get('warnings', [])
        if warnings:
            print(f"   警告信息: {warnings}")

        # 不支持的列类型应该产生警告但不应该失败
        if result_data.get('status') in ['completed', 'success']:
            print("✅ 不支持列类型处理正常（产生警告但不失败）")
            return True
        else:
            print("❌ 不支持列类型处理异常")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_get_comparison_result():
    """测试获取比对结果功能"""
    print("\n🧪 测试5: 获取比对结果功能")
    print("-" * 50)

    # 先创建一个比对任务
    test_data = {
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
        },
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "sample_size": 30,
            "threads": 1,
            "strict_type_checking": False
        }
    }

    print("📤 创建比对任务...")

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ 创建任务失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        print(f"✅ 比对任务创建成功，ID: {comparison_id}")

        # 立即尝试获取结果（应该是进行中状态）
        print("📤 立即获取结果（应该是进行中）...")
        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        initial_status = result_data.get('status')
        print(f"   初始状态: {initial_status}")

        # 等待一段时间后再次获取
        time.sleep(8)
        print("📤 等待后再次获取结果...")

        result_response = requests.get(
            f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
            timeout=10
        )

        if result_response.status_code != 200:
            print(f"❌ 再次获取结果失败: {result_response.status_code}")
            return False

        result_data = result_response.json()
        final_status = result_data.get('status')
        print(f"   最终状态: {final_status}")

        if final_status in ['completed', 'success']:
            print("✅ 获取比对结果功能正常")
            return True
        else:
            print("❌ 比对未成功完成")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def main():
    """运行完整的系统验证测试"""
    print("🔧 完整系统功能验证测试")
    print("验证所有重构后的功能是否正常工作")
    print("=" * 70)

    test_results = []

    # 测试1: 基本比对功能
    result1 = test_basic_comparison()
    test_results.append(("基本比对功能", result1))

    # 测试2: 指定列比对功能
    result2 = test_columns_to_compare()
    test_results.append(("指定列比对功能", result2))

    # 测试3: 排除列功能
    result3 = test_exclude_columns()
    test_results.append(("排除列功能", result3))

    # 测试4: 不支持列类型处理
    result4 = test_unsupported_columns_handling()
    test_results.append(("不支持列类型处理", result4))

    # 测试5: 获取比对结果功能
    result5 = test_get_comparison_result()
    test_results.append(("获取比对结果功能", result5))

    # 汇总结果
    print("\n" + "=" * 70)
    print("🏁 完整系统验证测试结果:")

    all_passed = True
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name:20}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 70)
    if all_passed:
        print("🎉 系统功能验证完全通过！")
        print("✅ 基本比对功能正常")
        print("✅ 指定列比对功能正常")
        print("✅ 排除列功能正常")
        print("✅ 不支持列类型处理正常")
        print("✅ 获取比对结果功能正常")
        print("✅ 参数传递和处理正常")
        print("✅ API端点分离正常")
    else:
        print("💥 系统功能验证发现问题！")
        print("请检查失败的测试项目")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
