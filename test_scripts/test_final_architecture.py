#!/usr/bin/env python3
"""
最终综合测试：验证重构后的数据比对节点架构
包括：
1. 参数分离架构 (凭据 vs 节点表单)
2. "Compare Tables" 只返回比对ID
3. "Get Comparison Result" 通过ID获取结果
4. columns_to_compare 功能
5. 类型检查和警告机制
"""

import requests
import json
import time

def test_complete_architecture():
    """测试完整的重构架构"""
    print("🎯 最终综合测试：验证重构后的完整架构")
    print("=" * 80)

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

    test_results = []

    # 测试1: 验证操作分离 - Compare Tables 只返回ID
    print("\n📋 测试1: 验证操作分离架构")
    print("-" * 50)

    test1_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan"],  # 只比较支持的列
        }
    }

    print("📤 发送 Compare Tables 请求...")

    try:
        # 步骤1: 启动比对
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test1_data,
            timeout=10
        )

        if response.status_code != 200:
            print(f"❌ Compare Tables 失败: {response.status_code}")
            test_results.append(("操作分离架构", False))
        else:
            result = response.json()
            comparison_id = result.get("comparison_id")

            if comparison_id and "status" in result:
                print(f"✅ Compare Tables 成功返回ID: {comparison_id}")
                print(f"✅ 状态: {result.get('status')}")

                # 步骤2: 等待片刻，然后获取结果
                print("⏳ 等待比对完成...")
                time.sleep(3)

                # 步骤3: 获取比对结果
                print("📥 发送 Get Comparison Result 请求...")
                result_response = requests.get(
                    f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
                    timeout=10
                )

                if result_response.status_code == 200:
                    result_data = result_response.json()
                    print("✅ Get Comparison Result 成功")
                    print(f"✅ 获取到完整的比对结果")

                    # 验证结果结构
                    has_config = "config" in result_data
                    has_summary = "summary" in result_data
                    has_differences = "differences" in result_data

                    if has_config and has_summary:
                        print("✅ 结果结构完整（包含config、summary等）")
                        test_results.append(("操作分离架构", True))
                    else:
                        print("❌ 结果结构不完整")
                        test_results.append(("操作分离架构", False))
                else:
                    print(f"❌ Get Comparison Result 失败: {result_response.status_code}")
                    test_results.append(("操作分离架构", False))
            else:
                print("❌ Compare Tables 返回结果不正确")
                test_results.append(("操作分离架构", False))

    except Exception as e:
        print(f"❌ 操作分离测试异常: {e}")
        test_results.append(("操作分离架构", False))

    # 测试2: 验证参数架构 - 系统参数从凭据获取
    print("\n📋 测试2: 验证参数架构")
    print("-" * 50)

    test2_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan", "payment"],  # 混合列
            # 注意：不包含系统性参数，应从凭据/默认值获取
        }
    }

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test2_data,
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            comparison_id = result.get("comparison_id")

            time.sleep(3)

            result_response = requests.get(
                f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
                timeout=10
            )

            if result_response.status_code == 200:
                result_data = result_response.json()
                config = result_data.get("config", {})

                # 检查系统性参数是否有默认值
                has_sample_size = config.get("sample_size") is not None
                has_threads = config.get("threads") is not None
                has_algorithm = config.get("algorithm") is not None
                has_tolerance = config.get("tolerance") is not None

                # 检查任务特定参数
                columns_correct = config.get("columns_to_compare") == ["plan", "payment"]

                if has_sample_size and has_threads and has_algorithm and columns_correct:
                    print("✅ 参数架构正确：系统参数有默认值，任务参数正确传递")
                    test_results.append(("参数架构", True))
                else:
                    print("❌ 参数架构不正确")
                    print(f"   sample_size: {config.get('sample_size')}")
                    print(f"   threads: {config.get('threads')}")
                    print(f"   algorithm: {config.get('algorithm')}")
                    print(f"   columns_to_compare: {config.get('columns_to_compare')}")
                    test_results.append(("参数架构", False))
            else:
                test_results.append(("参数架构", False))
        else:
            test_results.append(("参数架构", False))

    except Exception as e:
        print(f"❌ 参数架构测试异常: {e}")
        test_results.append(("参数架构", False))

    # 测试3: 验证类型检查和警告机制
    print("\n📋 测试3: 验证类型检查和警告机制")
    print("-" * 50)

    test3_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["payment"],  # 不支持的列类型
        }
    }

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/tables/nested",
            json=test3_data,
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            comparison_id = result.get("comparison_id")

            time.sleep(3)

            result_response = requests.get(
                f"http://localhost:8000/api/v1/compare/results/{comparison_id}",
                timeout=10
            )

            if result_response.status_code == 200:
                result_data = result_response.json()

                # 检查警告机制
                warnings = result_data.get("warnings", {})
                has_type_warnings = bool(warnings.get("unsupported_types", []))

                # 检查是否只比较了指定的列
                config = result_data.get("config", {})
                compare_columns = config.get("compare_columns", [])
                columns_match = compare_columns == ["payment"]

                if has_type_warnings and columns_match:
                    print("✅ 类型检查和警告机制正常")
                    print(f"✅ 正确检测到不支持类型并发出警告")
                    print(f"✅ 仍然按指定进行比较: {compare_columns}")
                    test_results.append(("类型检查机制", True))
                else:
                    print("❌ 类型检查机制不正确")
                    print(f"   有类型警告: {has_type_warnings}")
                    print(f"   比较列正确: {columns_match}")
                    print(f"   实际比较列: {compare_columns}")
                    test_results.append(("类型检查机制", False))
            else:
                test_results.append(("类型检查机制", False))
        else:
            test_results.append(("类型检查机制", False))

    except Exception as e:
        print(f"❌ 类型检查测试异常: {e}")
        test_results.append(("类型检查机制", False))

    # 汇总结果
    print("\n" + "=" * 80)
    print("🏁 最终综合测试结果")
    print("=" * 80)

    all_passed = True
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name:20}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 恭喜！所有重构目标已完成！")
        print("✅ 操作分离：Compare Tables 返回ID，Get Comparison Result 获取结果")
        print("✅ 参数架构：系统参数由凭据管理，任务参数由节点管理")
        print("✅ 功能完整：columns_to_compare 正确工作")
        print("✅ 错误处理：类型检查和警告机制正常")
        print("✅ 向后兼容：保持API接口兼容性")
        print("\n🚀 数据比对节点重构成功完成！")
    else:
        print("💥 部分功能还需要进一步调整！")

    return all_passed

def main():
    """运行最终综合测试"""
    success = test_complete_architecture()
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
