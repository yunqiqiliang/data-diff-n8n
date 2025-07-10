#!/bin/bash
# Docker 容器内 API 服务启动脚本
# 简化版本，专门用于容器内启动

set -e

# 设置环境变量
export PYTHONPATH=/app
export PYTHONUNBUFFERED=1

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_info "🚀 Starting Data-Diff API service..."

# 检查必要目录
if [ ! -d "/app/data_diff" ]; then
    echo "❌ ERROR: data_diff directory not found"
    exit 1
fi

if [ ! -d "/app/n8n" ]; then
    echo "❌ ERROR: n8n directory not found"
    exit 1
fi

# 创建日志目录
mkdir -p /app/logs

print_info "📋 Environment check:"
echo "  - Python version: $(python --version)"
echo "  - Working directory: $(pwd)"
echo "  - Python path: $PYTHONPATH"

# 启动 API 服务
print_success "✅ Starting uvicorn server..."
exec uvicorn n8n.api.main:app --host 0.0.0.0 --port 8000 --log-level info
