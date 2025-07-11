#!/usr/bin/env python3
"""
测试类型检查修复：
验证当不兼容的字段未参与比对时，不应报告为严重错误
"""

import requests
import json
import time
import sys

def test_type_checking_fix():
    """测试类型检查修复"""
    print("=== 测试类型检查修复 ===")

    # API 端点
    api_url = "http://localhost:8000/api/v1/compare/tables/nested"

    # 请求数据 - 不指定 columns_to_compare，这样会比对所有列（除了不兼容的）
    request_data_all_columns = {
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
            # 不指定 columns_to_compare，比对所有列
            "sample_size": 100,
            "threads": 1,
            "case_sensitive": True,
            "bisection_threshold": 1000,
            "strict_type_checking": False
        }
    }

    # 请求数据 - 指定不包含 payment 列的比对
    request_data_specific_columns = {
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
            "columns_to_compare": ["id", "invoice_number", "customer_name"],  # 不包含 payment 列
            "sample_size": 100,
            "threads": 1,
            "case_sensitive": True,
            "bisection_threshold": 1000,
            "strict_type_checking": False
        }
    }

    test_cases = [
        ("比对所有列（包含不兼容字段）", request_data_all_columns),
        ("比对指定列（不包含不兼容字段）", request_data_specific_columns)
    ]

    for test_name, request_data in test_cases:
        print(f"\n--- {test_name} ---")
        print(f"请求URL: {api_url}")
        print(f"比对列: {request_data['comparison_config'].get('columns_to_compare', '所有列')}")

        try:
            # 发送请求
            response = requests.post(
                api_url,
                json=request_data,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            print(f"响应状态码: {response.status_code}")

            if response.status_code == 200:
                result = response.json()
                comparison_id = result.get('comparison_id')
                if comparison_id:
                    print(f"✅ 比对任务启动成功！")
                    print(f"📋 比对ID: {comparison_id}")

                    # 等待一下再获取结果
                    time.sleep(3)

                    # 获取结果
                    result_url = f"http://localhost:8000/api/v1/compare/results/{comparison_id}"
                    result_response = requests.get(result_url)

                    if result_response.status_code == 200:
                        result_data = result_response.json()

                        print(f"📊 比对状态: {result_data.get('status', 'unknown')}")
                        print(f"🔍 数据质量评分: {result_data.get('summary', {}).get('data_quality_score', 'N/A')}")
                        print(f"⚠️ 可靠性: {result_data.get('statistics', {}).get('reliability', 'N/A')}")

                        # 检查警告信息
                        warnings = result_data.get('warnings', {})
                        if warnings:
                            print(f"🚨 警告严重程度: {warnings.get('severity', 'N/A')}")
                            print(f"📝 警告消息: {warnings.get('message', 'N/A')}")
                            print(f"💥 影响: {warnings.get('impact', 'N/A')}")

                            ignored_columns = warnings.get('ignored_columns', [])
                            if ignored_columns:
                                print(f"🚫 被忽略的列:")
                                for col in ignored_columns:
                                    print(f"   - {col['table_name']}.{col['column_name']} ({col['data_type']}): {col['reason']}")
                        else:
                            print("✅ 无警告信息")

                        # 验证期望结果
                        if test_name == "比对指定列（不包含不兼容字段）":
                            if warnings.get('severity') != 'critical':
                                print("✅ 修复验证成功：未参与比对的不兼容字段不会导致严重错误")
                            else:
                                print("❌ 修复验证失败：仍然报告严重错误")

                    else:
                        print(f"❌ 获取结果失败: {result_response.status_code}")
                else:
                    print("❌ 响应中没有找到 comparison_id")
            else:
                print(f"❌ 启动比对任务失败: {response.status_code}")
                print(f"错误详情: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"❌ 请求失败: {e}")

def main():
    """主测试函数"""
    print("🚀 开始测试类型检查修复")
    test_type_checking_fix()
    print("\n🎉 测试完成")

if __name__ == "__main__":
    main()
