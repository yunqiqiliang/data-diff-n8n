#!/usr/bin/env python3
"""
简单测试：验证模拟数据是否已被删除
"""

import os
import re

def test_mock_data_removed():
    """检查comparison_engine.py文件中是否还有模拟数据"""
    engine_file = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/core/comparison_engine.py"

    if not os.path.exists(engine_file):
        print("❌ comparison_engine.py 文件不存在")
        return False

    with open(engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否还有模拟数据的痕迹
    mock_patterns = [
        r'"invoices",\s*"users",\s*"orders"',
        r'"invoices",\s*"users",\s*"products"',
        r'Using mock.*data',
        r'模拟.*数据',
        r'return.*模拟数据',
        r'async def _execute_mock_comparison',
        r'生成模拟结果',
        r'模拟处理时间',
        r'alice@example\.com',
        r'bob@company\.com',
        r'total_rows_source = 100000',
        r'total_rows_target = 99950'
    ]

    found_mock_data = []
    for pattern in mock_patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        if matches:
            found_mock_data.extend(matches)

    if found_mock_data:
        print("❌ 发现残留的模拟数据:")
        for match in found_mock_data:
            print(f"   - {match}")
        return False
    else:
        print("✅ 未发现模拟数据，已成功删除")
        return True

def test_error_handling():
    """检查是否正确处理错误而不是返回模拟数据"""
    engine_file = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/core/comparison_engine.py"

    with open(engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否有适当的错误处理
    error_patterns = [
        r'raise Exception.*data-diff library.*required',
        r'raise Exception.*Database connection.*not support',
        r'raise Exception.*Failed to get.*schema'
    ]

    found_error_handling = []
    for pattern in error_patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        if matches:
            found_error_handling.extend(matches)

    if found_error_handling:
        print("✅ 发现适当的错误处理:")
        for match in found_error_handling:
            print(f"   - {match}")
        return True
    else:
        print("⚠️  未发现错误处理代码")
        return False

def test_no_fallback_mock():
    """检查是否删除了fallback模拟数据"""
    engine_file = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/core/comparison_engine.py"

    with open(engine_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查是否还有fallback到模拟数据的逻辑
    fallback_patterns = [
        r'如果查询失败.*返回模拟数据',
        r'如果.*失败.*模拟数据',
        r'except.*return.*invoices',
        r'except.*return.*users.*orders'
    ]

    found_fallback = []
    for pattern in fallback_patterns:
        matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
        if matches:
            found_fallback.extend(matches)

    if found_fallback:
        print("❌ 发现fallback模拟数据逻辑:")
        for match in found_fallback:
            print(f"   - {match}")
        return False
    else:
        print("✅ 未发现fallback模拟数据逻辑")
        return True

def main():
    """主测试函数"""
    print("🚀 开始检查模拟数据删除情况...")

    test1 = test_mock_data_removed()
    test2 = test_error_handling()
    test3 = test_no_fallback_mock()

    if test1 and test2 and test3:
        print("\n✅ 所有检查通过！模拟数据已成功删除，比较引擎现在只使用真实数据库查询。")
        print("\n📋 删除总结:")
        print("  - ✅ 删除了PostgreSQL的模拟schema数据")
        print("  - ✅ 删除了Clickzetta的模拟schema数据")
        print("  - ✅ 删除了_execute_mock_comparison方法")
        print("  - ✅ 删除了查询失败时的fallback模拟数据")
        print("  - ✅ 添加了适当的错误处理，要求data-diff库")
        print("\n🎯 现在schema比较会在以下情况抛出真实错误:")
        print("  - data-diff库未安装")
        print("  - 数据库连接失败")
        print("  - 数据库查询失败")
        print("  - 表或schema不存在")
        return True
    else:
        print("\n❌ 部分检查失败，可能还需要进一步清理。")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
