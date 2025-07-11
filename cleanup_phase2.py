#!/usr/bin/env python3
"""
第二阶段根目录清理脚本
进一步整理根目录中的文件
"""

import os
import shutil
from pathlib import Path

def cleanup_root_directory_phase2():
    """第二阶段清理根目录"""

    root_dir = Path("/Users/liangmo/Documents/GitHub/data-diff-n8n")

    # 要移动的文件分类
    file_moves = {
        # 移动到 docs_archive/
        "docs_archive": [
            "CONNECTION_RETRY_SOLUTION_DETAILED.md",
            "CONNECTION_STABILITY_ANALYSIS.md",
            "DATACOMPARISON_MULTI_ITEM_SOLUTION.md",
            "MERGE_NODE_ITEM_EXPLANATION.md",
            "N8N_EXPRESSION_ITEM_INDEX_GUIDE.md",
            "PROJECT_DELIVERY_CHECKLIST.md",
            "PROJECT_STRUCTURE.md"
        ],

        # 移动到 scripts/
        "scripts": [
            "connection_retry_demo.py",
            "create_test_data.py",
            "create_test_workflow.js",
            "end_to_end_test.py",
            "docker-start.sh",
            "health-check.sh",
            "quick_verify.sh",
            "start.sh"
        ],

        # 移动到 test_scripts/
        "test_scripts": [
            "test_extraction_data.json",
            "test_input_data.json",
            "test_workflow.json"
        ],

        # 移动到 reports/ (新建目录)
        "reports": [
            "merge_node_test_report.json",
            "node_parameter_fix_report.json",
            "schema_field_test_report.json"
        ]
    }

    # 创建 reports 目录
    reports_dir = root_dir / "reports"
    reports_dir.mkdir(exist_ok=True)
    print(f"✓ 创建目录: reports/")

    # 移动文件
    moved_files = []
    skipped_files = []

    for target_dir_name, files in file_moves.items():
        target_dir = root_dir / target_dir_name

        for file_name in files:
            source_path = root_dir / file_name
            dest_path = target_dir / file_name

            if source_path.exists():
                if dest_path.exists():
                    print(f"⚠️  目标文件已存在，跳过: {file_name}")
                    skipped_files.append(file_name)
                else:
                    try:
                        shutil.move(str(source_path), str(dest_path))
                        moved_files.append(f"{file_name} -> {target_dir_name}/")
                        print(f"✓ 移动: {file_name} -> {target_dir_name}/")
                    except Exception as e:
                        print(f"❌ 移动失败 {file_name}: {e}")
                        skipped_files.append(file_name)
            else:
                print(f"⚠️  文件不存在，跳过: {file_name}")

    # 清理 __pycache__ 目录
    pycache_dir = root_dir / "__pycache__"
    if pycache_dir.exists():
        try:
            shutil.rmtree(pycache_dir)
            print("✓ 删除 __pycache__ 目录")
        except Exception as e:
            print(f"❌ 删除 __pycache__ 失败: {e}")

    # 生成清理报告
    print("\n" + "="*60)
    print("📋 第二阶段清理报告")
    print("="*60)
    print(f"✓ 成功移动文件: {len(moved_files)}")
    print(f"⚠️  跳过文件: {len(skipped_files)}")

    if moved_files:
        print("\n📦 移动的文件:")
        for file_move in moved_files:
            print(f"  - {file_move}")

    if skipped_files:
        print("\n⚠️  跳过的文件:")
        for file_name in skipped_files:
            print(f"  - {file_name}")

    # 显示根目录现状
    print("\n📁 清理后的根目录文件:")
    remaining_files = []
    for item in root_dir.iterdir():
        if item.is_file() and not item.name.startswith('.'):
            remaining_files.append(item.name)

    # 按类别分类显示
    core_files = [f for f in remaining_files if f in ['README.md', 'LICENSE', 'pyproject.toml', 'poetry.lock']]
    config_files = [f for f in remaining_files if f.endswith(('.yml', '.yaml', '.toml', '.json', '.ruff.toml', '.code-workspace')) and f not in core_files]
    docker_files = [f for f in remaining_files if f.startswith('Dockerfile') or f.startswith('docker-')]
    md_files = [f for f in remaining_files if f.endswith('.md') and f not in core_files]
    script_files = [f for f in remaining_files if f.endswith('.py') and f not in ['cleanup_root_directory.py']]
    other_files = [f for f in remaining_files if f not in core_files + config_files + docker_files + md_files + script_files]

    if core_files:
        print("  📄 核心文件:", ", ".join(core_files))
    if config_files:
        print("  ⚙️  配置文件:", ", ".join(config_files))
    if docker_files:
        print("  🐳 Docker文件:", ", ".join(docker_files))
    if md_files:
        print("  📝 文档文件:", ", ".join(md_files))
    if script_files:
        print("  🐍 脚本文件:", ", ".join(script_files))
    if other_files:
        print("  📄 其他文件:", ", ".join(other_files))

    print(f"\n✅ 第二阶段根目录清理完成！剩余文件: {len(remaining_files)}")

    # 显示目录结构
    print("\n📁 完整目录结构:")
    for item in sorted(root_dir.iterdir()):
        if item.is_dir() and not item.name.startswith('.'):
            file_count = len(list(item.glob("*")))
            print(f"📁 {item.name}/ ({file_count} 个文件)")
        elif item.is_file() and not item.name.startswith('.'):
            print(f"📄 {item.name}")

    return {
        'moved_files': moved_files,
        'skipped_files': skipped_files,
        'remaining_files': remaining_files
    }

if __name__ == "__main__":
    cleanup_root_directory_phase2()
