#!/usr/bin/env python3
"""
修复脚本：修复 advanced_routes.py 中的 ComparisonEngine 初始化问题
"""

import sys

def fix_advanced_routes(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    # 1. 导入 ConfigManager 类（应该已经导入）
    if 'ConfigManager,' not in content:
        print("[ERROR] ConfigManager 没有被导入")
        return False

    # 2. 添加 config_manager 实例
    if 'config_manager = ConfigManager()' not in content:
        # 在 router = APIRouter() 之前添加 config_manager
        insert_pos = content.find('router = APIRouter()')
        if insert_pos > 0:
            content = content[:insert_pos] + 'config_manager = ConfigManager()\n\n' + content[insert_pos:]
            print("[INFO] 已添加 config_manager 实例")
        else:
            print("[ERROR] 找不到插入点 'router = APIRouter()'")
            return False
    else:
        print("[INFO] config_manager 实例已存在")

    # 3. 修复 ComparisonEngine 初始化
    content = content.replace(
        'comparison_engine = ComparisonEngine()',
        'comparison_engine = ComparisonEngine(config_manager)'
    )
    print("[INFO] 已修复 ComparisonEngine 初始化")

    # 保存修改
    with open(file_path, 'w') as f:
        f.write(content)

    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        file_path = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/api/advanced_routes.py"
    else:
        file_path = sys.argv[1]

    print(f"正在修复 {file_path}...")
    result = fix_advanced_routes(file_path)

    if result:
        print("修复完成")
    else:
        print("无法自动修复，请手动检查代码")
