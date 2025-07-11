#!/usr/bin/env python3
"""
详细检查真实数据库的模式比较结果
"""

import requests
import json
from urllib.parse import urljoin

# API配置
API_BASE_URL = "http://localhost:8000"
NESTED_SCHEMA_ENDPOINT = "/api/v1/compare/schemas/nested"

def test_detailed_schema_comparison():
    """详细测试真实数据库的模式比较"""
    print("🔍 详细测试真实数据库的模式比较...")

    # 使用真实的数据库连接信息
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

        if response.status_code == 200:
            data = response.json()
            print("✅ 成功连接到真实数据库")

            # 详细分析返回的数据
            if 'result' in data:
                result = data['result']
                print("\n📊 详细分析返回数据:")
                print(f"完整响应结构: {json.dumps(data, indent=2, ensure_ascii=False)}")

                # 检查源数据库信息
                if 'source_schema' in result:
                    source_schema = result['source_schema']
                    print(f"\n🔍 源数据库 (PostgreSQL):")
                    print(f"  - 数据库类型: {source_schema.get('database_type', 'N/A')}")
                    print(f"  - 模式名: {source_schema.get('schema_name', 'N/A')}")
                    print(f"  - 表列表: {source_schema.get('tables', [])}")
                    print(f"  - 表数量: {len(source_schema.get('tables', []))}")

                    # 显示列信息
                    if 'columns' in source_schema:
                        print(f"  - 列信息:")
                        for table_name, columns in source_schema['columns'].items():
                            print(f"    {table_name}: {len(columns)} 列")
                            for col in columns:
                                print(f"      - {col['name']}: {col['type']}")

                # 检查目标数据库信息
                if 'target_schema' in result:
                    target_schema = result['target_schema']
                    print(f"\n🔍 目标数据库 (Clickzetta):")
                    print(f"  - 数据库类型: {target_schema.get('database_type', 'N/A')}")
                    print(f"  - 模式名: {target_schema.get('schema_name', 'N/A')}")
                    print(f"  - 表列表: {target_schema.get('tables', [])}")
                    print(f"  - 表数量: {len(target_schema.get('tables', []))}")

                    # 显示列信息
                    if 'columns' in target_schema:
                        print(f"  - 列信息:")
                        for table_name, columns in target_schema['columns'].items():
                            print(f"    {table_name}: {len(columns)} 列")
                            for col in columns:
                                print(f"      - {col['name']}: {col['type']}")

                # 检查差异信息
                if 'diff' in result:
                    diff = result['diff']
                    print(f"\n🔍 模式差异:")
                    print(f"  - 仅在源中的表: {diff.get('tables_only_in_source', [])}")
                    print(f"  - 仅在目标中的表: {diff.get('tables_only_in_target', [])}")
                    print(f"  - 共同表: {diff.get('common_tables', [])}")
                    print(f"  - 列差异: {diff.get('column_diffs', {})}")
                    print(f"  - 类型差异: {diff.get('type_diffs', {})}")

                return True
            else:
                print("⚠️  响应格式不符合预期 - 没有 result 字段")

        elif response.status_code in [400, 422, 500]:
            print(f"❌ 数据库连接失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return False
        else:
            print(f"⚠️  意外的状态码: {response.status_code}")
            print(f"响应内容: {response.text}")

    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

    return True

def main():
    """主函数"""
    print("🚀 开始详细检查真实数据库的模式比较...")

    result = test_detailed_schema_comparison()

    if result:
        print("\n✅ 详细检查完成")
    else:
        print("\n❌ 详细检查失败")

if __name__ == "__main__":
    main()
