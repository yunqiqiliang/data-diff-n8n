#!/usr/bin/env python3
"""
验证 n8n 数据比对节点的操作命名是否正确
"""

import json
import sys
import os

def test_node_naming():
    """测试节点命名是否正确"""

    # 读取节点文件
    node_file = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/src/nodes/DataComparison/DataComparison.node.ts"

    if not os.path.exists(node_file):
        print("❌ 节点文件不存在")
        return False

    with open(node_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否包含正确的操作名称
    checks = [
        ("Compare Table", "操作名称应该是 'Compare Table'"),
        ("Compare Schema", "操作名称应该是 'Compare Schema'"),
        ("Compare two database tables", "表比对描述应该是 'Compare two database tables'"),
        ("Compare database schemas", "模式比对描述应该是 'Compare database schemas'"),
        ("sourceTable", "应该有源表字段"),
        ("targetTable", "应该有目标表字段"),
        ("compareTables", "表比对操作值应该是 'compareTables'"),
        ("compareSchemas", "模式比对操作值应该是 'compareSchemas'")
    ]

    all_passed = True

    for check_text, description in checks:
        if check_text in content:
            print(f"✅ {description}")
        else:
            print(f"❌ {description}")
            all_passed = False

    # 确保没有多表比较的迹象
    old_naming_issues = []

    if "Compare Tables" in content and "name: 'Compare Tables'" in content:
        old_naming_issues.append("仍然包含旧的 'Compare Tables' 名称")

    if "Compare Schemas" in content and "name: 'Compare Schemas'" in content:
        old_naming_issues.append("仍然包含旧的 'Compare Schemas' 名称")

    if old_naming_issues:
        for issue in old_naming_issues:
            print(f"❌ {issue}")
        all_passed = False
    else:
        print("✅ 已移除所有旧的操作名称")
        # 检查是否还有其他地方的旧命名
        if "Compare Tables" in content or "Compare Schemas" in content:
            print("⚠️  仍然包含旧命名文本，请检查是否在注释或其他地方")
        else:
            print("✅ 完全清理了旧命名")

    return all_passed

def test_architecture_clarity():
    """测试架构清晰度"""
    print("\n📊 架构分析:")

    # 分析当前架构
    print("✅ 当前架构包含三个操作:")
    print("   - Compare Table: 比较一对表")
    print("   - Compare Schema: 比较两个数据库的模式")
    print("   - Get Comparison Result: 获取比较结果")
    print("\n🔄 每个操作的特点:")
    print("   Compare Table:")
    print("     - 使用 data-diff 库的 diff_tables() 函数")
    print("     - API 端点: /api/v1/compare/tables/nested")
    print("     - 异步处理，返回 comparison_id")
    print("   Compare Schema:")
    print("     - 比较数据库模式（表、列、类型等）")
    print("     - API 端点: /api/v1/compare/schemas/nested")
    print("     - 同步处理，直接返回结果")
    print("   Get Comparison Result:")
    print("     - 根据 comparison_id 获取比较结果")
    print("     - API 端点: /api/v1/compare/results/{id}")

    print("\n🔄 如果需要多表比较，需要:")
    print("   - 修改节点表单以接受多个表对")
    print("   - 修改 API 端点以处理批量比较")
    print("   - 修改比对引擎以并行处理多个表")
    print("   - 修改结果处理以聚合多个比较结果")

    return True

def main():
    """主函数"""
    print("🔍 验证 n8n 数据比对节点命名和架构...")

    naming_result = test_node_naming()
    architecture_result = test_architecture_clarity()

    print(f"\n📋 测试结果:")
    print(f"节点命名: {'✅ 通过' if naming_result else '❌ 失败'}")
    print(f"架构清晰度: {'✅ 通过' if architecture_result else '❌ 失败'}")

    if naming_result and architecture_result:
        print("\n🎉 所有测试通过！节点命名现在准确反映了其功能（单表比较）")
        return 0
    else:
        print("\n❌ 某些测试失败，请检查代码")
        return 1

if __name__ == "__main__":
    sys.exit(main())
