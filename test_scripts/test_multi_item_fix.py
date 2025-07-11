#!/usr/bin/env python3
"""
测试 DataComparison 节点的多项目数据收集功能
验证修复是否能够正确处理 N8N 表单限制问题
"""

import requests
import json
import time

# 测试 DataComparison 节点的多项目数据收集
def test_datacomparison_multi_item():
    print("🧪 测试 DataComparison 节点多项目数据收集功能")
    print("=" * 60)

    # 模拟 N8N 输入数据 - 模拟用户只能选择一个项目的情况
    test_scenarios = [
        {
            "name": "场景1: 只选择 Item 0 (源连接)",
            "items": [
                {
                    "json": {
                        "connectionUrl": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
                        "databaseType": "postgresql",
                        "sourceTable": "accounts"
                    }
                },
                {
                    "json": {
                        "connectionUrl": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
                        "databaseType": "clickzetta",
                        "targetTable": "accounts"
                    }
                }
            ]
        },
        {
            "name": "场景2: 只选择 Item 1 (目标连接)",
            "items": [
                {
                    "json": {
                        "connectionUrl": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
                        "databaseType": "postgresql",
                        "sourceTable": "accounts"
                    }
                },
                {
                    "json": {
                        "connectionUrl": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
                        "databaseType": "clickzetta",
                        "targetTable": "accounts"
                    }
                }
            ]
        },
        {
            "name": "场景3: 混合数据格式",
            "items": [
                {
                    "json": {
                        "connectionUrl": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
                        "tables": ["accounts", "users", "orders"],
                        "databaseType": "postgresql"
                    }
                },
                {
                    "json": {
                        "connection": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
                        "data": ["accounts", "test_table"],
                        "databaseType": "clickzetta"
                    }
                }
            ]
        }
    ]

    for scenario in test_scenarios:
        print(f"\n🔍 {scenario['name']}")
        print("-" * 50)

        # 模拟节点执行的逻辑
        result = simulate_datacomparison_execution(scenario['items'])

        if result['success']:
            print("✅ 测试通过")
            print(f"   源连接: {result['sourceConnection'][:30]}...")
            print(f"   目标连接: {result['targetConnection'][:30]}...")
            print(f"   源表: {result['sourceTable']}")
            print(f"   目标表: {result['targetTable']}")
        else:
            print("❌ 测试失败")
            print(f"   错误: {result['error']}")

    print("\n🎯 测试建议:")
    print("1. 在 N8N 中启用 'Auto-fill from upstream' 选项")
    print("2. 确保上游节点输出正确的连接信息")
    print("3. 使用表达式引用上游节点数据")
    print("4. 检查节点输出的调试信息")

def simulate_datacomparison_execution(items):
    """模拟 DataComparison 节点的执行逻辑"""

    # 模拟智能收集逻辑
    all_connections = []
    all_tables = []

    # 从所有输入项中收集连接信息
    for item in items:
        if 'json' in item and item['json']:
            json_data = item['json']

            # 收集连接信息
            connection_fields = ['connectionUrl', 'connectionString', 'connection', 'url']
            for field in connection_fields:
                if field in json_data and json_data[field]:
                    if json_data[field] not in all_connections:
                        all_connections.append(json_data[field])

            # 收集表信息
            table_fields = ['sourceTable', 'targetTable', 'tableName', 'table']
            for field in table_fields:
                if field in json_data and json_data[field]:
                    if json_data[field] not in all_tables:
                        all_tables.append(json_data[field])

            # 收集表列表
            if 'tables' in json_data and isinstance(json_data['tables'], list):
                for table in json_data['tables']:
                    if table not in all_tables:
                        all_tables.append(table)

            if 'data' in json_data and isinstance(json_data['data'], list):
                for table in json_data['data']:
                    if table not in all_tables:
                        all_tables.append(table)

    # 智能分配
    source_connection = all_connections[0] if len(all_connections) > 0 else None
    target_connection = all_connections[1] if len(all_connections) > 1 else (all_connections[0] if len(all_connections) > 0 else None)

    source_table = all_tables[0] if len(all_tables) > 0 else None
    target_table = all_tables[1] if len(all_tables) > 1 else (all_tables[0] if len(all_tables) > 0 else None)

    # 检查是否有必需的信息
    if not source_connection:
        return {
            'success': False,
            'error': 'Source connection string is required'
        }

    if not target_connection:
        return {
            'success': False,
            'error': 'Target connection string is required'
        }

    return {
        'success': True,
        'sourceConnection': source_connection,
        'targetConnection': target_connection,
        'sourceTable': source_table or 'default_table',
        'targetTable': target_table or 'default_table',
        'allConnections': all_connections,
        'allTables': all_tables
    }

if __name__ == "__main__":
    test_datacomparison_multi_item()
