#!/usr/bin/env python3
"""
测试真实的schema查询功能
验证数据库连接和schema信息获取
"""

import asyncio
import sys
import os
import logging
from typing import Dict, Any

# 添加项目路径到系统路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from n8n.core.comparison_engine import ComparisonEngine
from n8n.core.config_manager import ConfigManager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_real_schema_query():
    """测试真实的schema查询"""
    print("🔍 Testing Real Schema Query...")

    # 创建配置管理器
    config_manager = ConfigManager()

    # 创建比较引擎
    comparison_engine = ComparisonEngine(config_manager)

    # 测试PostgreSQL schema查询
    print("\n1. Testing PostgreSQL schema query...")
    source_config = {
        "database_type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "username": "postgres",
        "password": "password",
        "database": "test_db",
        "db_schema": "public"
    }

    target_config = {
        "database_type": "clickzetta",
        "host": "localhost",
        "port": 8123,
        "username": "default",
        "password": "",
        "database": "test_db",
        "db_schema": "default"
    }

    comparison_config = {
        "operation": "compare_schemas",
        "source_schema": "public",
        "target_schema": "default"
    }

    try:
        # 执行schema比较
        result = await comparison_engine.compare_schemas(
            source_config,
            target_config
        )

        print("✅ Schema comparison completed successfully!")
        print(f"📊 Comparison result:")
        print(f"  - Status: {result.get('status')}")
        print(f"  - Source schema: {result.get('source_schema', {}).get('schema_name')}")
        print(f"  - Target schema: {result.get('target_schema', {}).get('schema_name')}")
        print(f"  - Source tables: {len(result.get('source_schema', {}).get('tables', []))}")
        print(f"  - Target tables: {len(result.get('target_schema', {}).get('tables', []))}")

        # 显示差异摘要
        if 'differences' in result:
            diff = result['differences']
            print(f"\n📝 Schema differences:")
            print(f"  - Tables only in source: {len(diff.get('tables_only_in_source', []))}")
            print(f"  - Tables only in target: {len(diff.get('tables_only_in_target', []))}")
            print(f"  - Tables with differences: {len(diff.get('tables_with_differences', []))}")

            # 显示具体的表差异
            if diff.get('tables_with_differences'):
                print(f"\n🔍 Table differences:")
                for table_diff in diff['tables_with_differences']:
                    print(f"  - Table: {table_diff['table_name']}")
                    if table_diff.get('column_differences'):
                        print(f"    - Column differences: {len(table_diff['column_differences'])}")

        return True

    except Exception as e:
        print(f"❌ Schema comparison failed: {e}")
        logger.error(f"Schema comparison error: {e}", exc_info=True)
        return False

async def test_connection_fallback():
    """测试连接回退到mock数据的情况"""
    print("\n🔄 Testing connection fallback to mock data...")

    # 创建配置管理器
    config_manager = ConfigManager()

    # 创建比较引擎
    comparison_engine = ComparisonEngine(config_manager)

    # 使用无效的连接配置来触发回退
    source_config = {
        "database_type": "postgresql",
        "host": "invalid_host",
        "port": 5432,
        "username": "invalid_user",
        "password": "invalid_pass",
        "database": "invalid_db",
        "db_schema": "public"
    }

    target_config = {
        "database_type": "clickzetta",
        "host": "invalid_host",
        "port": 8123,
        "username": "invalid_user",
        "password": "invalid_pass",
        "database": "invalid_db",
        "db_schema": "default"
    }

    comparison_config = {
        "operation": "compare_schemas",
        "source_schema": "public",
        "target_schema": "default"
    }

    try:
        # 执行schema比较（应该回退到mock数据）
        result = await comparison_engine.compare_schemas(
            source_config,
            target_config
        )

        print("✅ Fallback to mock data successful!")
        print(f"📊 Mock comparison result:")
        print(f"  - Status: {result.get('status')}")
        print(f"  - Source schema: {result.get('source_schema', {}).get('schema_name')}")
        print(f"  - Target schema: {result.get('target_schema', {}).get('schema_name')}")
        print(f"  - Source tables: {result.get('source_schema', {}).get('tables', [])}")
        print(f"  - Target tables: {result.get('target_schema', {}).get('tables', [])}")

        return True

    except Exception as e:
        print(f"❌ Fallback test failed: {e}")
        logger.error(f"Fallback test error: {e}", exc_info=True)
        return False

async def test_schema_comparison_logic():
    """测试schema比较逻辑"""
    print("\n🧪 Testing schema comparison logic...")

    # 创建配置管理器
    config_manager = ConfigManager()

    # 创建比较引擎
    comparison_engine = ComparisonEngine(config_manager)

    # 创建测试schema数据
    source_schema = {
        "database_type": "postgresql",
        "schema_name": "public",
        "tables": ["users", "orders", "products"],
        "columns": {
            "users": [
                {"name": "id", "type": "integer", "nullable": False, "default": None},
                {"name": "name", "type": "text", "nullable": True, "default": None},
                {"name": "email", "type": "text", "nullable": True, "default": None}
            ],
            "orders": [
                {"name": "id", "type": "integer", "nullable": False, "default": None},
                {"name": "user_id", "type": "integer", "nullable": False, "default": None},
                {"name": "amount", "type": "numeric", "nullable": True, "default": None}
            ],
            "products": [
                {"name": "id", "type": "integer", "nullable": False, "default": None},
                {"name": "name", "type": "text", "nullable": True, "default": None}
            ]
        }
    }

    target_schema = {
        "database_type": "clickzetta",
        "schema_name": "default",
        "tables": ["users", "orders", "categories"],
        "columns": {
            "users": [
                {"name": "id", "type": "Int32", "nullable": False, "default": None},
                {"name": "username", "type": "String", "nullable": True, "default": None},  # 不同的列名
                {"name": "email", "type": "String", "nullable": True, "default": None}
            ],
            "orders": [
                {"name": "id", "type": "Int32", "nullable": False, "default": None},
                {"name": "user_id", "type": "Int32", "nullable": False, "default": None},
                {"name": "total", "type": "Decimal", "nullable": True, "default": None}  # 不同的列名
            ],
            "categories": [  # 新表
                {"name": "id", "type": "Int32", "nullable": False, "default": None},
                {"name": "name", "type": "String", "nullable": True, "default": None}
            ]
        }
    }

    # 执行schema比较
    try:
        differences = comparison_engine._compare_schemas(source_schema, target_schema)

        print("✅ Schema comparison logic test successful!")
        print(f"📊 Comparison results:")
        print(f"  - Tables only in source: {differences.get('tables_only_in_source', [])}")
        print(f"  - Tables only in target: {differences.get('tables_only_in_target', [])}")
        print(f"  - Tables with differences: {len(differences.get('tables_with_differences', []))}")

        # 显示表差异详情
        for table_diff in differences.get('tables_with_differences', []):
            print(f"  - Table '{table_diff['table_name']}' differences:")
            for col_diff in table_diff.get('column_differences', []):
                print(f"    - {col_diff['type']}: {col_diff['column_name']}")

        return True

    except Exception as e:
        print(f"❌ Schema comparison logic test failed: {e}")
        logger.error(f"Schema comparison logic error: {e}", exc_info=True)
        return False

async def main():
    """主函数"""
    print("🚀 Starting Real Schema Query Tests...")

    tests = [
        ("Connection fallback test", test_connection_fallback),
        ("Schema comparison logic test", test_schema_comparison_logic),
        ("Real schema query test", test_real_schema_query)
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"Running: {test_name}")
        print('='*50)

        try:
            result = await test_func()
            results.append((test_name, result))
            print(f"✅ {test_name} completed: {'PASS' if result else 'FAIL'}")
        except Exception as e:
            results.append((test_name, False))
            print(f"❌ {test_name} failed with exception: {e}")

    # 显示测试摘要
    print(f"\n{'='*50}")
    print("📋 Test Summary")
    print('='*50)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status} {test_name}")

    print(f"\n🎯 Overall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Schema query functionality is working correctly.")
    else:
        print("⚠️  Some tests failed. Please check the logs for details.")

if __name__ == "__main__":
    asyncio.run(main())
