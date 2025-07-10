#!/usr/bin/env python3
"""
修复脚本：检查后端 comparison_engine 初始化问题
"""

import sys

def fix_main_py(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    # 检查 comparison_engine 的初始化是否正确
    if 'comparison_engine = ComparisonEngine(config_manager)' in content:
        print("[INFO] comparison_engine 已正确初始化")
    else:
        print("[ERROR] comparison_engine 初始化不正确")
        return False

    # 确保 execute_comparison_async 中使用全局 comparison_engine
    target_line = 'result = await comparison_engine.compare_tables('
    if target_line in content:
        # 检查 comparison_engine 是否被声明为全局变量
        if 'global comparison_engine' not in content:
            # 添加 global 声明
            fixed_content = content.replace(
                'async def execute_comparison_async(',
                'async def execute_comparison_async('
            )
            fixed_content = fixed_content.replace(
                'try:\n        logger.info',
                'try:\n        global comparison_engine\n        logger.info'
            )
            with open(file_path, 'w') as f:
                f.write(fixed_content)
            print("[INFO] 已添加 global comparison_engine 声明")
            return True
        else:
            print("[INFO] execute_comparison_async 中已有 global comparison_engine 声明")
            return True
    else:
        print("[ERROR] 找不到 'result = await comparison_engine.compare_tables('")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        file_path = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/api/main.py"
    else:
        file_path = sys.argv[1]

    print(f"正在检查 {file_path}...")
    result = fix_main_py(file_path)

    if result:
        print("修复完成")
    else:
        print("无法自动修复，请手动检查代码")
