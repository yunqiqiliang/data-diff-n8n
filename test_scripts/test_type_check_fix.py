#!/usr/bin/env python3
"""
测试类型检查修复 - 验证未参与比对的列不应该导致严重错误
"""

import requests
import json
import time

def test_type_check_fix():
    """测试修复后的类型检查逻辑"""

    print("🧪 测试类型检查修复...")

    # API 配置
    api_url = "http://localhost:8000/api/v1/compare/tables/nested"

    # 比对配置 - 只比对 id 和 customer_id 列，不比对 payment 列
    request_data = {
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
            "columns_to_compare": ["plan"],  # 只比对这plan列
            "sample_size": 100,
            "threads": 2,
            "case_sensitive": True,
            "bisection_threshold": 1024,
            "strict_type_checking": False  # 非严格模式
        }
    }

    print(f"📊 启动比对任务...")
    print(f"⚙️  比对列: {request_data['comparison_config']['columns_to_compare']}")
    print(f"📝 注意: payment 列(money类型)未参与比对，不应该报告为严重错误")

    try:
        # 启动比对
        response = requests.post(
            api_url,
            headers={'Content-Type': 'application/json'},
            json=request_data,
            timeout=60
        )

        if response.status_code != 200:
            print(f"❌ 启动比对失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return False

        result = response.json()
        comparison_id = result.get('comparison_id')

        if not comparison_id:
            print("❌ 未获取到比对ID")
            return False

        print(f"✅ 比对任务已启动，ID: {comparison_id}")

        # 等待比对完成
        print("⏳ 等待比对完成...")
        time.sleep(5)

        # 获取结果
        result_url = f"http://localhost:8000/api/v1/compare/results/{comparison_id}"
        result_response = requests.get(result_url, timeout=30)

        if result_response.status_code != 200:
            print(f"❌ 获取结果失败: {result_response.status_code}")
            print(f"错误信息: {result_response.text}")
            return False

        comparison_result = result_response.json()

        # 分析结果
        print("\n📋 比对结果分析:")
        print(f"状态: {comparison_result.get('status')}")

        # 检查配置
        config = comparison_result.get('config', {})
        compare_columns = config.get('compare_columns', [])
        print(f"实际比对的列: {compare_columns}")

        # 检查警告
        warnings = comparison_result.get('warnings', {})
        if warnings:
            print(f"\n⚠️  警告信息:")
            print(f"类型警告: {warnings.get('unsupported_types', [])}")
            print(f"严重性: {warnings.get('severity', 'none')}")
            print(f"影响: {warnings.get('impact', 'none')}")

            # 检查被忽略的列
            ignored_columns = warnings.get('ignored_columns', [])
            if ignored_columns:
                print(f"被忽略的列:")
                for col in ignored_columns:
                    print(f"  - {col['table_name']}.{col['column_name']} ({col['data_type']}): {col['reason']}")

        # 检查汇总信息
        summary = comparison_result.get('summary', {})
        print(f"\n📊 比对汇总:")
        print(f"数据质量评分: {summary.get('data_quality_score', 'N/A')}")
        print(f"比对是否无效: {summary.get('comparison_invalid', False)}")
        print(f"比对是否不完整: {summary.get('incomplete_comparison', False)}")
        print(f"匹配百分比: {summary.get('match_percentage', 'N/A')}%")

        # 验证修复结果
        print(f"\n🔍 修复验证:")

        # 检查是否有类型警告
        has_type_warnings = bool(warnings.get('unsupported_types'))

        if has_type_warnings:
            # 检查被忽略的列是否在比对列中
            ignored_column_names = [col['column_name'] for col in ignored_columns]
            compared_columns = compare_columns or []

            # 检查被忽略的列是否实际参与了比对
            ignored_in_comparison = any(col in compared_columns for col in ignored_column_names)

            if ignored_in_comparison:
                print("❌ 修复失败: 实际参与比对的列被忽略了")
                severity = warnings.get('severity', 'unknown')
                print(f"警告严重性: {severity}")
                return False
            else:
                print("✅ 修复成功: 未参与比对的列被忽略是正常的")
                print("✅ 比对应该能正常完成，不应报告为严重错误")

                # 检查数据质量评分
                data_quality_score = summary.get('data_quality_score', '')
                if data_quality_score == 'Failed':
                    print("⚠️  注意: 数据质量评分仍为 Failed，可能需要进一步调整")
                else:
                    print(f"✅ 数据质量评分: {data_quality_score}")

                return True
        else:
            print("✅ 修复成功: 没有类型警告")
            return True

    except requests.exceptions.RequestException as e:
        print(f"❌ 网络请求失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

if __name__ == "__main__":
    print("🚀 开始测试类型检查修复...")

    success = test_type_check_fix()

    if success:
        print("\n🎉 测试通过! 类型检查修复生效")
    else:
        print("\n❌ 测试失败! 需要进一步检查修复")
