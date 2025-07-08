#!/bin/bash

# 项目重命名脚本：data-diff-qiliang -> data-diff-n8n
# 使用前请确保已在 GitHub 上创建了新仓库 yunqiqiliang/data-diff-n8n

set -e  # 遇到错误时停止执行

echo "🚀 开始重命名项目为 data-diff-n8n..."

# 检查当前目录
CURRENT_DIR=$(pwd)
if [[ ! "$CURRENT_DIR" == *"data-diff-qiliang"* ]]; then
    echo "❌ 错误：请在 data-diff-qiliang 目录中运行此脚本"
    exit 1
fi

echo "📁 当前目录: $CURRENT_DIR"

# 1. 更新 Git 远程仓库
echo "🔄 更新 Git 远程仓库地址..."
git remote set-url origin https://github.com/yunqiqiliang/data-diff-n8n.git

# 验证远程仓库
echo "📋 当前远程仓库配置:"
git remote -v

# 2. 提交所有更改
echo "💾 提交所有更改..."
git add .
git commit -m "Rename project to data-diff-n8n

- Updated project name in pyproject.toml
- Updated repository URLs in all documentation
- Updated GitHub links in README and contributing guides"

# 3. 推送到新仓库
echo "⬆️ 推送到新仓库..."
git push -u origin main

echo "✅ Git 操作完成！"

# 4. 重命名本地文件夹
echo "📂 准备重命名本地文件夹..."
PARENT_DIR=$(dirname "$CURRENT_DIR")
NEW_DIR="$PARENT_DIR/data-diff-n8n"

echo "即将将文件夹从:"
echo "  $CURRENT_DIR"
echo "重命名为:"
echo "  $NEW_DIR"

read -p "是否继续重命名本地文件夹? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd "$PARENT_DIR"
    mv "data-diff-qiliang" "data-diff-n8n"
    echo "✅ 文件夹重命名完成！"
    echo "📍 新路径: $NEW_DIR"
    echo ""
    echo "⚠️  请注意："
    echo "   1. 需要在编辑器/IDE中重新打开新路径下的项目"
    echo "   2. 如果有其他终端窗口，需要 cd 到新路径"
    echo ""
    echo "🎉 项目重命名完成！"
    echo "🌐 新仓库地址: https://github.com/yunqiqiliang/data-diff-n8n"
else
    echo "⏭️  跳过文件夹重命名，你可以稍后手动执行:"
    echo "   cd $PARENT_DIR"
    echo "   mv data-diff-qiliang data-diff-n8n"
fi

echo ""
echo "✨ 重命名流程完成！"
