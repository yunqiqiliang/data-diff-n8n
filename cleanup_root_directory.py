#!/usr/bin/env python3
"""
根目录清理脚本
将调试文件、测试文件和文档文件分类整理
"""

import os
import shutil
from pathlib import Path

def cleanup_root_directory():
    """清理根目录，将文件分类到适当的目录"""

    root_dir = Path("/Users/liangmo/Documents/GitHub/data-diff-n8n")

    # 创建目标目录
    directories = {
        "debug_scripts": root_dir / "debug_scripts",
        "test_scripts": root_dir / "test_scripts",
        "docs_archive": root_dir / "docs_archive",
        "legacy_files": root_dir / "legacy_files"
    }

    for dir_path in directories.values():
        dir_path.mkdir(exist_ok=True)
        print(f"Created directory: {dir_path}")

    # 定义文件分类规则
    file_patterns = {
        "debug_scripts": [
            "debug_*.py",
            "clickzetta_connection_diagnosis.py",
            "dev_helper.py",
            "schema_comparison_summary.py",
            "demo_complete.py",
            "final_*.py"
        ],
        "test_scripts": [
            "test_*.py",
            "test_*.js",
            "test_*.sh"
        ],
        "docs_archive": [
            "*_REPORT.md",
            "*_SUMMARY.md",
            "*_COMPLETION_REPORT.md",
            "*_ANALYSIS_REPORT.md",
            "CLICKZETTA_EXECUTE_QUERY_FEATURE.md",
            "DATABASE_CONNECTOR_*.md",
            "DATA_COMPARISON_*.md",
            "DOCKER_TEST_*.md",
            "MOCK_REMOVAL_*.md",
            "NODE_NAMING_*.md",
            "REFACTORING_*.md",
            "SCHEMA_*.md"
        ],
        "legacy_files": [
            "cleanup-old-scripts.sh",
            "cleanup_project.py",
            "docker-start.sh",
            "start_docker_test.sh",
            "docker-test-db-query.yml"
        ]
    }

    # 移动文件
    moved_files = []

    for category, patterns in file_patterns.items():
        target_dir = directories[category]

        for pattern in patterns:
            for file_path in root_dir.glob(pattern):
                if file_path.is_file():
                    target_path = target_dir / file_path.name
                    try:
                        shutil.move(str(file_path), str(target_path))
                        moved_files.append(f"{category}: {file_path.name}")
                        print(f"Moved {file_path.name} to {category}/")
                    except Exception as e:
                        print(f"Error moving {file_path.name}: {e}")

    # 创建清理报告
    report_path = root_dir / "ROOT_CLEANUP_REPORT.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# 根目录清理报告\n\n")
        f.write(f"清理时间: {os.popen('date').read().strip()}\n\n")
        f.write("## 清理统计\n\n")

        for category, target_dir in directories.items():
            files_in_category = list(target_dir.glob("*"))
            f.write(f"### {category}\n")
            f.write(f"- 目录: `{target_dir.relative_to(root_dir)}/`\n")
            f.write(f"- 文件数量: {len(files_in_category)}\n")
            if files_in_category:
                f.write("- 文件列表:\n")
                for file in sorted(files_in_category):
                    f.write(f"  - {file.name}\n")
            f.write("\n")

        f.write("## 移动的文件\n\n")
        for moved_file in sorted(moved_files):
            f.write(f"- {moved_file}\n")

        f.write("\n## 保留的核心文件\n\n")
        remaining_files = [f for f in root_dir.iterdir()
                         if f.is_file() and not f.name.startswith('.')
                         and f.name not in ['ROOT_CLEANUP_REPORT.md', 'cleanup_root_directory.py']]

        for file in sorted(remaining_files):
            f.write(f"- {file.name}\n")

    print(f"\n清理完成！生成报告: {report_path}")
    print(f"总共移动了 {len(moved_files)} 个文件")

    # 显示根目录结构
    print("\n清理后的根目录结构:")
    for item in sorted(root_dir.iterdir()):
        if item.is_dir() and not item.name.startswith('.'):
            print(f"📁 {item.name}/")
        elif item.is_file() and not item.name.startswith('.'):
            print(f"📄 {item.name}")

if __name__ == "__main__":
    cleanup_root_directory()
