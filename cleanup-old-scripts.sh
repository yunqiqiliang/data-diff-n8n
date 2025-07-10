#!/bin/bash
# 清理旧的启动脚本

echo "🧹 清理旧的启动脚本..."

# 备份旧脚本（如果需要的话）
if [ "$1" = "--backup" ]; then
    echo "📦 备份旧脚本到 backup-scripts/ 目录..."
    mkdir -p backup-scripts
    cp quick-start.sh backup-scripts/ 2>/dev/null || true
    cp start-dev-env.sh backup-scripts/ 2>/dev/null || true
    cp start-dev.sh backup-scripts/ 2>/dev/null || true
fi

# 删除旧脚本
echo "🗑️ 删除旧的启动脚本..."
rm -f quick-start.sh
rm -f start-dev-env.sh
rm -f start-dev.sh

echo "✅ 清理完成！"
echo ""
echo "📋 现在使用统一的启动脚本："
echo "  ./start.sh           # 快速启动"
echo "  ./start.sh --rebuild # 重新构建"
echo "  ./start.sh --help    # 查看帮助"
