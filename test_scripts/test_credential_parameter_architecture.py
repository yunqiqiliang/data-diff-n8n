#!/usr/bin/env python3
"""
测试新的参数架构：
- 系统性参数从凭据获取
- 节点表单只保留特定任务的参数
"""

import requests
import json
import time

def test_architecture():
    """测试新的参数架构"""
    print("🧪 测试新的参数架构")
    print("=" * 60)
    print("✅ 系统性参数应该从凭据配置获取：")
    print("   - sampleSize (默认值)")
    print("   - threads (默认值)")
    print("   - caseSensitive (默认值)")
    print("   - tolerance (默认值)")
    print("   - method (默认值)")
    print("   - bisectionThreshold (默认值)")
    print("   - strictTypeChecking (默认值)")
    print("   - excludeColumns (默认值)")
    print("\n✅ 节点表单参数（特定任务）：")
    print("   - sourceConnection")
    print("   - targetConnection")
    print("   - sourceTable")
    print("   - targetTable")
    print("   - keyColumns (可覆盖凭据默认值)")
    print("   - columnsToCompare (任务特定)")
    print("   - whereCondition (任务特定)")

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

    # 测试请求 - 只包含必要的任务特定参数
    test_data = {
        "source_config": base_source_config,
        "target_config": base_target_config,
        "comparison_config": {
            "source_table": "invoices",
            "target_table": "invoices",
            "key_columns": ["id"],
            "columns_to_compare": ["plan"],  # 只比较支持的列
            # 注意：不再包含 sample_size, threads, case_sensitive 等系统性参数
            # 这些应该从凭据配置中获取
        }
    }

    print(f"\n📤 发送测试请求（只包含任务特定参数）:")
    print(f"   source_table: {test_data['comparison_config']['source_table']}")
    print(f"   target_table: {test_data['comparison_config']['target_table']}")
    print(f"   key_columns: {test_data['comparison_config']['key_columns']}")
    print(f"   columns_to_compare: {test_data['comparison_config']['columns_to_compare']}")
    print(f"   注意：未包含系统性参数（应从凭据获取）")

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
        print(f"\n✅ 比对任务创建成功，ID: {comparison_id}")

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

        # 检查配置是否正确传递
        config = result_data.get("config", {})
        print(f"\n📊 后端接收到的配置:")

        # 检查任务特定参数
        task_params = {
            "source_table": config.get("source_table"),
            "target_table": config.get("target_table"),
            "key_columns": config.get("key_columns"),
            "columns_to_compare": config.get("columns_to_compare"),
            "compare_columns": config.get("compare_columns")
        }

        # 检查系统性参数（应该有默认值）
        system_params = {
            "sample_size": config.get("sample_size"),
            "threads": config.get("threads"),
            "case_sensitive": config.get("case_sensitive"),
            "tolerance": config.get("tolerance"),
            "algorithm": config.get("algorithm"),
            "bisection_threshold": config.get("bisection_threshold"),
            "strict_type_checking": config.get("strict_type_checking")
        }

        print("   任务特定参数:")
        for key, value in task_params.items():
            print(f"     {key}: {value}")

        print("   系统性参数（应从凭据获取）:")
        for key, value in system_params.items():
            print(f"     {key}: {value}")

        # 验证参数是否正确
        all_ok = True

        # 任务参数验证
        if task_params["columns_to_compare"] != ["plan"]:
            print(f"❌ columns_to_compare 不正确: 期望 ['plan'], 实际 {task_params['columns_to_compare']}")
            all_ok = False

        # 系统参数验证（应该有合理的默认值）
        if system_params["sample_size"] is None:
            print(f"❌ sample_size 缺失（应从凭据获取默认值）")
            all_ok = False

        if system_params["threads"] is None:
            print(f"❌ threads 缺失（应从凭据获取默认值）")
            all_ok = False

        if all_ok:
            print(f"\n✅ 参数架构测试通过！")
            print(f"✅ 任务特定参数正确传递")
            print(f"✅ 系统性参数有默认值")
            return True
        else:
            print(f"\n❌ 参数架构测试失败！")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def main():
    """运行参数架构测试"""
    print("🔧 参数架构测试：验证凭据与节点表单的参数分离")
    success = test_architecture()

    print("\n" + "=" * 60)
    if success:
        print("🎉 参数架构设计正确！")
        print("✅ 系统性参数由凭据管理")
        print("✅ 任务特定参数由节点表单管理")
        print("✅ 参数传递和默认值机制正常")
    else:
        print("💥 参数架构需要调整！")

    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
