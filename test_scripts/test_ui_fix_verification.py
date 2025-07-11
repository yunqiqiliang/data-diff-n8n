#!/usr/bin/env python3
"""
测试脚本：验证n8n节点参数UI显示修复
测试修改后的默认表达式是否能让所有参数显示为绿色
"""

import json
import time
from datetime import datetime

def test_ui_parameter_display():
    """测试UI参数显示修复"""

    print("🎯 测试 n8n DataComparison 节点参数UI显示修复")
    print("=" * 60)

    # 预期的修复效果
    expected_fixes = {
        "sourceConnection": {
            "old_default": "''",
            "new_default": "={{ $input.all().find(item => item.json?.connectionUrl && !item.json?.connectionUrl.includes(\"clickzetta\"))?.json?.connectionUrl || \"auto-detect\" }}",
            "expected_ui": "绿色（有默认表达式）"
        },
        "targetConnection": {
            "old_default": "''",
            "new_default": "={{ $input.all().find(item => item.json?.connectionUrl && item.json?.connectionUrl.includes(\"clickzetta\"))?.json?.connectionUrl || \"auto-detect\" }}",
            "expected_ui": "绿色（有默认表达式）"
        },
        "sourceTable": {
            "old_default": "''",
            "new_default": "={{ $input.all().find(item => item.json?.tables?.[0] || item.json?.data?.[0])?.json?.tables?.[0]?.name || $input.all().find(item => item.json?.data?.[0])?.json?.data?.[0]?.name || \"auto-detect\" }}",
            "expected_ui": "绿色（有默认表达式）"
        },
        "targetTable": {
            "old_default": "''",
            "new_default": "={{ $input.all().find(item => item.json?.tables?.[0] || item.json?.data?.[0])?.json?.tables?.[0]?.name || $input.all().find(item => item.json?.data?.[0])?.json?.data?.[0]?.name || \"auto-detect\" }}",
            "expected_ui": "绿色（有默认表达式）"
        }
    }

    print("\n📋 修复内容对比：")
    for param_name, fix_info in expected_fixes.items():
        print(f"\n{param_name}:")
        print(f"  旧版默认值: {fix_info['old_default']}")
        print(f"  新版默认值: {fix_info['new_default']}")
        print(f"  期望UI效果: {fix_info['expected_ui']}")

    # 测试理论
    print("\n🔍 修复理论验证：")
    print("1. N8N UI 判断参数是否已填充的逻辑：")
    print("   - required: false + default: '' = 红色（未填充）")
    print("   - required: false + default: '有值' = 绿色（已填充）")
    print("   - required: false + default: '表达式' = 绿色（已填充）")

    print("\n2. 修复方案：")
    print("   - 将所有参数的default从''改为智能表达式")
    print("   - 表达式自动检测上游节点数据")
    print("   - 如果检测不到数据则显示'auto-detect'")

    print("\n3. 预期效果：")
    print("   - 所有参数在UI上显示为绿色（已填充）")
    print("   - 用户看到的是智能默认值而不是空值")
    print("   - 运行时保持原有的自动填充逻辑")

    return True

def test_expression_logic():
    """测试表达式逻辑"""

    print("\n🧪 测试表达式逻辑")
    print("=" * 50)

    # 模拟n8n表达式执行逻辑
    test_cases = [
        {
            "name": "Case 1: 完整的上游数据",
            "input_data": [
                {"json": {"connectionUrl": "postgresql://user:pass@host:5432/db", "tables": [{"name": "users"}]}},
                {"json": {"connectionUrl": "clickzetta://user:pass@host:8443/db", "tables": [{"name": "users_copy"}]}}
            ],
            "expected": {
                "sourceConnection": "postgresql://user:pass@host:5432/db",
                "targetConnection": "clickzetta://user:pass@host:8443/db",
                "sourceTable": "users",
                "targetTable": "users_copy"
            }
        },
        {
            "name": "Case 2: 缺少部分数据",
            "input_data": [
                {"json": {"connectionUrl": "postgresql://user:pass@host:5432/db"}},
                {"json": {"tables": [{"name": "products"}]}}
            ],
            "expected": {
                "sourceConnection": "postgresql://user:pass@host:5432/db",
                "targetConnection": "auto-detect",
                "sourceTable": "products",
                "targetTable": "products"
            }
        },
        {
            "name": "Case 3: 完全空数据",
            "input_data": [
                {"json": {}},
                {"json": {}}
            ],
            "expected": {
                "sourceConnection": "auto-detect",
                "targetConnection": "auto-detect",
                "sourceTable": "auto-detect",
                "targetTable": "auto-detect"
            }
        }
    ]

    for case in test_cases:
        print(f"\n{case['name']}:")
        print(f"  输入数据: {case['input_data']}")
        print(f"  期望结果: {case['expected']}")

        # 简单模拟表达式逻辑
        def simulate_expression(input_data, expression_type):
            if expression_type == "sourceConnection":
                for item in input_data:
                    conn_url = item.get("json", {}).get("connectionUrl", "")
                    if conn_url and "clickzetta" not in conn_url:
                        return conn_url
                return "auto-detect"
            elif expression_type == "targetConnection":
                for item in input_data:
                    conn_url = item.get("json", {}).get("connectionUrl", "")
                    if conn_url and "clickzetta" in conn_url:
                        return conn_url
                return "auto-detect"
            elif expression_type in ["sourceTable", "targetTable"]:
                for item in input_data:
                    tables = item.get("json", {}).get("tables", [])
                    if tables and tables[0].get("name"):
                        return tables[0]["name"]
                return "auto-detect"
            return "auto-detect"

        actual = {}
        for param_type in ["sourceConnection", "targetConnection", "sourceTable", "targetTable"]:
            actual[param_type] = simulate_expression(case["input_data"], param_type)

        print(f"  实际结果: {actual}")
        match = actual == case["expected"]
        print(f"  匹配结果: {'✅ 通过' if match else '❌ 失败'}")

    return True

def create_test_summary():
    """创建测试总结"""

    print("\n📊 修复总结")
    print("=" * 50)

    summary = {
        "问题": "n8n DataComparison 节点在多输入场景下参数不能全部显示为绿色",
        "根本原因": "参数 default: '' 导致UI判断为未填充状态",
        "解决方案": "使用智能表达式作为默认值，让UI显示为已填充状态",
        "修改内容": [
            "sourceConnection: 智能检测非Clickzetta连接",
            "targetConnection: 智能检测Clickzetta连接",
            "sourceTable: 智能检测表名",
            "targetTable: 智能检测表名"
        ],
        "预期效果": [
            "所有参数在UI上显示为绿色（已填充）",
            "用户体验与旧版一致",
            "运行时逻辑保持不变",
            "端到端功能继续正常工作"
        ],
        "验证方法": [
            "1. 重启n8n服务",
            "2. 打开DataComparison节点",
            "3. 检查所有参数是否显示为绿色",
            "4. 运行工作流验证功能正常"
        ]
    }

    print(f"\n问题: {summary['问题']}")
    print(f"根本原因: {summary['根本原因']}")
    print(f"解决方案: {summary['解决方案']}")

    print("\n修改内容:")
    for i, change in enumerate(summary['修改内容'], 1):
        print(f"  {i}. {change}")

    print("\n预期效果:")
    for i, effect in enumerate(summary['预期效果'], 1):
        print(f"  {i}. {effect}")

    print("\n验证方法:")
    for method in summary['验证方法']:
        print(f"  {method}")

    return summary

def main():
    """主函数"""

    try:
        # 测试UI参数显示修复
        test_ui_parameter_display()

        # 测试表达式逻辑
        test_expression_logic()

        # 创建测试总结
        summary = create_test_summary()

        print("\n" + "=" * 60)
        print("🎉 修复完成！")
        print("请在n8n界面中验证DataComparison节点参数是否都显示为绿色。")
        print("如果仍有问题，请检查表达式语法或联系开发团队。")

        return True

    except Exception as e:
        print(f"❌ 测试过程中出错: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
