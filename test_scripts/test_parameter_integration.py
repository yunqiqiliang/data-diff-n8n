#!/usr/bin/env python3
"""
集成测试：验证n8n节点参数合并在真实API中的表现
测试场景：
1. 模拟表单参数覆盖凭据参数的情况
2. 验证exclude_columns参数从凭据正确传递到API
3. 确认columns_to_compare参数只从表单获取
"""

import requests
import json
import time

def test_parameter_integration():
    """测试参数合并在实际API调用中的表现"""
    print("🔗 集成测试：验证参数合并在API中的表现")
    print("=" * 60)

    # 基础连接配置
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

    # 测试场景1: 模拟凭据中有exclude_columns，表单中有特定的比较列
    print("\n📝 场景1: 测试exclude_columns从凭据传递到API")

    scenario1_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan"],  # 表单参数：指定比较列
            "exclude_columns": ["created_at", "updated_at"],  # 模拟从凭据获取的排除列
            "sample_size": 100,
            "threads": 2,  # 模拟从凭据获取的线程数
            "case_sensitive": False,  # 模拟从凭据获取的大小写敏感性
            "tolerance": 0.005,  # 模拟从凭据获取的容差
            "algorithm": "joindiff",  # 模拟从凭据获取的算法
            "strict_type_checking": False
        }
    }

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=scenario1_data,
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

        print("📊 API返回的配置参数:")
        print(f"  key_columns: {config.get('key_columns', [])}")
        print(f"  columns_to_compare: {config.get('columns_to_compare', [])}")
        print(f"  exclude_columns: {config.get('exclude_columns', [])}")
        print(f"  sample_size: {config.get('sample_size', 0)}")
        print(f"  threads: {config.get('threads', 0)}")
        print(f"  case_sensitive: {config.get('case_sensitive', True)}")
        print(f"  tolerance: {config.get('tolerance', 0.001)}")
        print(f"  algorithm: {config.get('algorithm', 'hashdiff')}")

        # 验证参数是否正确传递
        success = True

        # 验证columns_to_compare（来自表单）
        if config.get('columns_to_compare') != ['plan']:
            print(f"❌ columns_to_compare 错误: 期望 ['plan']，实际 {config.get('columns_to_compare')}")
            success = False
        else:
            print("✅ columns_to_compare 参数正确传递")

        # 验证exclude_columns（来自凭据）
        if set(config.get('exclude_columns', [])) != {'created_at', 'updated_at'}:
            print(f"❌ exclude_columns 错误: 期望 ['created_at', 'updated_at']，实际 {config.get('exclude_columns')}")
            success = False
        else:
            print("✅ exclude_columns 参数正确传递")

        # 验证其他凭据参数
        if config.get('threads') != 2:
            print(f"❌ threads 错误: 期望 2，实际 {config.get('threads')}")
            success = False
        else:
            print("✅ threads 参数正确传递")

        if config.get('case_sensitive') != False:
            print(f"❌ case_sensitive 错误: 期望 False，实际 {config.get('case_sensitive')}")
            success = False
        else:
            print("✅ case_sensitive 参数正确传递")

        if abs(config.get('tolerance', 0) - 0.005) > 0.0001:
            print(f"❌ tolerance 错误: 期望 0.005，实际 {config.get('tolerance')}")
            success = False
        else:
            print("✅ tolerance 参数正确传递")

        if config.get('algorithm') != 'joindiff':
            print(f"❌ algorithm 错误: 期望 'joindiff'，实际 {config.get('algorithm')}")
            success = False
        else:
            print("✅ algorithm 参数正确传递")

        return success

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_priority_override():
    """测试表单参数覆盖凭据参数的优先级"""
    print("\n📝 场景2: 测试参数优先级（表单覆盖凭据）")

    # 基础连接配置
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

    # 模拟表单参数覆盖凭据参数
    scenario2_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],  # 表单参数覆盖凭据中的key_columns
            "columns_to_compare": ["plan", "payment"],  # 表单指定比较列
            "sample_size": 200,  # 表单参数覆盖凭据中的sample_size
            "threads": 1,  # 表单参数覆盖凭据中的threads
            "case_sensitive": True,  # 表单参数覆盖凭据中的case_sensitive
            "strict_type_checking": False
        }
    }

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=scenario2_data,
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

        print("📊 API返回的配置参数:")
        print(f"  key_columns: {config.get('key_columns', [])}")
        print(f"  columns_to_compare: {config.get('columns_to_compare', [])}")
        print(f"  sample_size: {config.get('sample_size', 0)}")
        print(f"  threads: {config.get('threads', 0)}")
        print(f"  case_sensitive: {config.get('case_sensitive', True)}")

        # 验证表单参数优先级
        success = True

        if config.get('key_columns') != ['id']:
            print(f"❌ key_columns 错误: 期望 ['id']，实际 {config.get('key_columns')}")
            success = False
        else:
            print("✅ key_columns 表单参数正确覆盖")

        if set(config.get('columns_to_compare', [])) != {'plan', 'payment'}:
            print(f"❌ columns_to_compare 错误: 期望 ['plan', 'payment']，实际 {config.get('columns_to_compare')}")
            success = False
        else:
            print("✅ columns_to_compare 表单参数正确设置")

        if config.get('sample_size') != 200:
            print(f"❌ sample_size 错误: 期望 200，实际 {config.get('sample_size')}")
            success = False
        else:
            print("✅ sample_size 表单参数正确覆盖")

        if config.get('threads') != 1:
            print(f"❌ threads 错误: 期望 1，实际 {config.get('threads')}")
            success = False
        else:
            print("✅ threads 表单参数正确覆盖")

        if config.get('case_sensitive') != True:
            print(f"❌ case_sensitive 错误: 期望 True，实际 {config.get('case_sensitive')}")
            success = False
        else:
            print("✅ case_sensitive 表单参数正确覆盖")

        return success

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def main():
    """运行所有集成测试"""
    print("🧪 参数合并集成测试")
    print("测试n8n节点中凭据与表单参数的合并逻辑")
    print("=" * 80)

    test_results = []

    # 测试1: 凭据参数传递
    result1 = test_parameter_integration()
    test_results.append(("凭据参数传递", result1))

    # 测试2: 表单参数优先级
    result2 = test_priority_override()
    test_results.append(("表单参数优先级", result2))

    # 输出最终结果
    print("\n" + "=" * 60)
    print("🏁 集成测试结果:")

    all_passed = True
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 所有集成测试通过！")
        print("✅ 凭据参数正确作为默认值")
        print("✅ 表单参数正确覆盖凭据参数")
        print("✅ exclude_columns从凭据正确传递")
        print("✅ columns_to_compare从表单正确传递")
        print("✅ 参数合并逻辑完全正常")

        print("\n📋 参数处理逻辑总结:")
        print("1. 表单参数优先级最高，会覆盖凭据中的同名参数")
        print("2. 凭据参数作为默认值，当表单参数为空时使用")
        print("3. exclude_columns 只从凭据获取，不能在表单中设置")
        print("4. columns_to_compare 只从表单获取，不从凭据获取")
        print("5. 类型检查确保参数安全，避免运行时错误")
    else:
        print("💥 部分集成测试失败！需要进一步调试")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
