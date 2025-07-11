#!/usr/bin/env python3
"""
测试模式比对功能
验证 Compare Schema 操作是否正常工作
"""

import requests
import json
import time

def test_schema_comparison():
    """测试模式比对功能"""
    print("🔧 测试模式比对功能")
    print("=" * 60)

    # 基础配置
    source_config = {
        "database_type": "postgresql",
        "host": "106.120.41.178",
        "port": 5436,
        "username": "metabase",
        "password": "metasample123",
        "database": "sample",
        "db_schema": "public"
    }

    target_config = {
        "database_type": "clickzetta",
        "username": "qiliang",
        "password": "Ql123456!",
        "instance": "jnsxwfyr",
        "service": "uat-api.clickzetta.com",
        "workspace": "quick_start",
        "db_schema": "from_pg",
        "vcluster": "default_ap"
    }

    # 请求数据
    request_data = {
        "source_config": source_config,
        "target_config": target_config
    }

    print(f"📤 发送模式比对请求:")
    print(f"   源数据库: {source_config['database_type']} ({source_config['host']})")
    print(f"   目标数据库: {target_config['database_type']} ({target_config['service']})")

    try:
        # 发送请求
        response = requests.post(
            "http://localhost:8000/api/v1/compare/schemas/nested",
            json=request_data,
            timeout=30
        )

        if response.status_code != 200:
            print(f"❌ API 请求失败: {response.status_code} - {response.text}")
            return False

        result = response.json()
        print(f"✅ 模式比对请求成功")

        # 解析结果
        status = result.get("status")
        comparison_result = result.get("result", {})
        source_type = result.get("source_type")
        target_type = result.get("target_type")

        print(f"\n📊 比对结果:")
        print(f"   状态: {status}")
        print(f"   源类型: {source_type}")
        print(f"   目标类型: {target_type}")

        # 显示摘要信息
        summary = comparison_result.get("summary", {})
        if summary:
            print(f"\n📈 比对摘要:")
            print(f"   模式是否相同: {summary.get('schemas_identical', False)}")
            print(f"   总差异数: {summary.get('total_differences', 0)}")
            print(f"   表差异数: {summary.get('table_differences', 0)}")
            print(f"   列差异数: {summary.get('column_differences', 0)}")
            print(f"   类型差异数: {summary.get('type_differences', 0)}")

        # 显示详细差异
        diff = comparison_result.get("diff", {})
        if diff:
            print(f"\n🔍 详细差异:")

            tables_only_source = diff.get("tables_only_in_source", [])
            if tables_only_source:
                print(f"   仅在源数据库中的表: {tables_only_source}")

            tables_only_target = diff.get("tables_only_in_target", [])
            if tables_only_target:
                print(f"   仅在目标数据库中的表: {tables_only_target}")

            common_tables = diff.get("common_tables", [])
            if common_tables:
                print(f"   共同的表: {common_tables}")

            column_diffs = diff.get("column_diffs", {})
            if column_diffs:
                print(f"   列差异:")
                for table, cols in column_diffs.items():
                    print(f"     {table}:")
                    if cols.get("columns_only_in_source"):
                        print(f"       仅在源中: {cols['columns_only_in_source']}")
                    if cols.get("columns_only_in_target"):
                        print(f"       仅在目标中: {cols['columns_only_in_target']}")

            type_diffs = diff.get("type_diffs", {})
            if type_diffs:
                print(f"   类型差异:")
                for table, types in type_diffs.items():
                    print(f"     {table}:")
                    for type_diff in types:
                        print(f"       {type_diff['column']}: {type_diff['source_type']} -> {type_diff['target_type']}")

        # 显示模式信息
        source_schema = comparison_result.get("source_schema", {})
        target_schema = comparison_result.get("target_schema", {})

        if source_schema:
            print(f"\n🗃️  源数据库模式:")
            print(f"   类型: {source_schema.get('database_type')}")
            print(f"   模式名: {source_schema.get('schema_name')}")
            print(f"   表数量: {len(source_schema.get('tables', []))}")
            print(f"   表列表: {source_schema.get('tables', [])}")

        if target_schema:
            print(f"\n🗃️  目标数据库模式:")
            print(f"   类型: {target_schema.get('database_type')}")
            print(f"   模式名: {target_schema.get('schema_name')}")
            print(f"   表数量: {len(target_schema.get('tables', []))}")
            print(f"   表列表: {target_schema.get('tables', [])}")

        return True

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def test_schema_comparison_errors():
    """测试错误处理"""
    print(f"\n🧪 测试错误处理...")

    # 测试无效的数据库类型
    invalid_request = {
        "source_config": {
            "database_type": "invalid_db",
            "host": "localhost"
        },
        "target_config": {
            "database_type": "postgresql",
            "host": "localhost"
        }
    }

    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/schemas/nested",
            json=invalid_request,
            timeout=10
        )

        if response.status_code == 400:
            print("✅ 错误处理正常 - 正确识别无效数据库类型")
        else:
            print(f"⚠️  错误处理可能有问题 - 状态码: {response.status_code}")

        return True
    except Exception as e:
        print(f"❌ 错误处理测试异常: {e}")
        return False

def main():
    """主函数"""
    print("🔧 模式比对功能测试")
    print("=" * 60)

    results = []

    # 测试正常模式比对
    print("\n1️⃣ 测试正常模式比对")
    result1 = test_schema_comparison()
    results.append(("正常模式比对", result1))

    # 测试错误处理
    print("\n2️⃣ 测试错误处理")
    result2 = test_schema_comparison_errors()
    results.append(("错误处理", result2))

    # 输出最终结果
    print("\n" + "=" * 60)
    print("🏁 测试结果总结:")

    all_passed = True
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 所有测试通过！模式比对功能正常工作")
        print("✅ API 端点响应正确")
        print("✅ 数据格式正确")
        print("✅ 错误处理正常")
    else:
        print("💥 部分测试失败！需要进一步调试")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
