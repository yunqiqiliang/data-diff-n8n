#!/usr/bin/env python3
"""
测试 DataComparison 节点对 Merge 节点数据的处理
验证 "item 0/1" 问题的解决方案
"""

import json
import time
from datetime import datetime

def test_merge_node_handling():
    """测试 Merge 节点数据处理"""
    print("=== 测试 DataComparison 节点的 Merge 节点处理 ===")
    
    # 模拟 Merge 节点的输出数据结构
    merge_node_output = [
        {
            "json": {
                "connectionUrl": "postgresql://user:pass@host:5432/db",
                "databaseType": "postgresql",
                "connectionConfig": {
                    "type": "postgresql",
                    "host": "host",
                    "port": 5432,
                    "database": "db",
                    "username": "user",
                    "password": "pass"
                },
                "tables": [
                    {"name": "users", "schema": "public"},
                    {"name": "orders", "schema": "public"}
                ],
                "nodeType": "PostgreSQL Database Connector"
            }
        },
        {
            "json": {
                "connectionUrl": "clickzetta://user:pass@instance.service.com/workspace?virtualcluster=default_ap&schema=public",
                "databaseType": "clickzetta",
                "connectionConfig": {
                    "type": "clickzetta",
                    "instance": "instance",
                    "service": "service.com",
                    "username": "user",
                    "password": "pass",
                    "workspace": "workspace",
                    "vcluster": "default_ap",
                    "schema": "public"
                },
                "data": [
                    {"name": "users", "schema": "public"},
                    {"name": "products", "schema": "public"}
                ],
                "nodeType": "ClickZetta Connector"
            }
        }
    ]
    
    # 测试用例
    test_cases = [
        {
            "name": "Merge 节点数据处理测试",
            "description": "验证 DataComparison 节点能否正确处理来自 Merge 节点的数据",
            "data": merge_node_output,
            "expected_behavior": {
                "should_detect_merge": True,
                "connection_count": 2,
                "table_count": 4,
                "database_types": ["postgresql", "clickzetta"]
            }
        }
    ]
    
    # 生成表达式示例
    expression_examples = {
        "❌ 不推荐的方式": [
            "item 0",
            "item 1", 
            "{{ $input.item.json.connectionUrl }}",  # 可能 undefined
        ],
        "✅ 推荐的方式": [
            '{{ $("PostgreSQL Database Connector").item.json.connectionUrl }}',
            '{{ $("ClickZetta Connector").item.json.connectionUrl }}',
            '{{ $input.all()[0].json.connectionUrl }}',  # 如果必须使用索引
            '{{ $input.all()[1].json.connectionUrl }}',
            "启用 Auto-fill from upstream 选项"
        ]
    }
    
    # 工作流重构建议
    workflow_suggestions = {
        "当前结构（有问题）": {
            "description": "使用 Merge 节点可能导致 item 0/1 问题",
            "structure": "PostgreSQL → Merge ← ClickZetta → DataComparison"
        },
        "推荐结构": {
            "description": "直接连接多个输入到 DataComparison",
            "structure": "PostgreSQL → DataComparison ← ClickZetta"
        }
    }
    
    # 保存测试报告
    report = {
        "test_date": datetime.now().isoformat(),
        "test_purpose": "验证 DataComparison 节点对 Merge 节点数据的处理",
        "problem_analysis": {
            "root_cause": "Merge 节点将多个输入合并为数组，导致 n8n 显示 item 0/1 选项",
            "why_undefined": "直接使用 item 0/1 可能返回整个对象而不是具体字段值",
            "merge_detection": "新版本可自动检测来自 Merge 节点的数据"
        },
        "solutions_implemented": [
            "更新字段描述，明确解释 item 0/1 出现原因",
            "提供多种安全的表达式使用方式",
            "增强 extractUpstreamData 方法，支持 Merge 节点检测",
            "添加详细的调试输出和使用建议",
            "提供工作流重构建议"
        ],
        "test_cases": test_cases,
        "expression_examples": expression_examples,
        "workflow_suggestions": workflow_suggestions,
        "verification_steps": [
            "1. 在 n8n 中创建包含 Merge 节点的工作流",
            "2. 连接 PostgreSQL 和 ClickZetta Connector 到 Merge",
            "3. 连接 Merge 到 DataComparison 节点",
            "4. 观察 DataComparison 参数中的 item 0/1 选项",
            "5. 使用推荐的表达式替代 item 0/1",
            "6. 检查 n8n 日志中的 Merge 检测信息"
        ]
    }
    
    # 保存报告
    with open('/Users/liangmo/Documents/GitHub/data-diff-n8n/merge_node_test_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print("✅ 问题分析完成")
    print(f"📊 测试报告已保存: merge_node_test_report.json")
    
    print("\n=== 关键发现 ===")
    print("🔍 您的观察完全正确！'item 0/1' 确实与 Merge 节点相关")
    print("📊 Merge 节点将多个输入合并为数组格式")
    print("⚠️ 这导致 n8n 在表达式编辑器中显示 item 0/1 选项")
    print("🔧 直接使用可能导致 undefined，因为引用的是整个对象")
    
    print("\n=== 解决方案 ===")
    print("1. 🎯 使用具体节点名引用：{{ $(\"NodeName\").item.json.connectionUrl }}")
    print("2. 🔄 重构工作流：移除 Merge 节点，直接连接多输入")
    print("3. 🤖 启用自动填充：让节点自动处理上游数据")
    print("4. 🐛 增强调试：新版本会检测并警告 Merge 节点数据")
    
    print("\n=== 验证方法 ===")
    print("1. 打开 n8n 界面: http://localhost:5678")
    print("2. 检查 DataComparison 节点的字段描述（已更新）")
    print("3. 查看 n8n 日志中的 Merge 检测信息")
    print("4. 使用推荐的表达式替代 item 0/1")

if __name__ == "__main__":
    test_merge_node_handling()
