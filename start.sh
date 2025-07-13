#!/bin/bash
# Data-Diff N8N 统一启动脚本
# 合并快速启动和完整启动功能

set -e

# 设置代理以解决网络问题
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890

# 默认参数
REBUILD=false
FORCE_REBUILD=false
SKIP_SYSTEM_PRUNE=false
QUICK_MODE=false
VERBOSE=false

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印彩色信息
print_info() {
    echo -e "${BLUE}$1${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_warning() {
    echo -e "${YELLOW}$1${NC}"
}

print_error() {
    echo -e "${RED}$1${NC}"
}

# 显示帮助信息
show_help() {
    cat << EOF
🚀 Data-Diff N8N 统一启动脚本

用法: $0 [选项]

启动模式：
  -q, --quick         快速启动模式（默认，不重新构建）
  -r, --rebuild       重新构建后启动
  -f, --force-rebuild 强制重新构建所有镜像

其他选项：
  --skip-prune        跳过系统清理
  -v, --verbose       详细输出
  -h, --help          显示帮助信息

启动示例：
  $0                  # 快速启动（推荐日常使用）
  $0 --quick          # 快速启动（同上）
  $0 --rebuild        # 代码修改后重新构建
  $0 --force-rebuild  # 完全重新构建
  $0 --verbose        # 详细输出模式

自动化功能：
  ✅ 自动检测和构建 N8N TypeScript 节点
  ✅ 自动生成 SSL 证书（非快速模式）
  ✅ 智能判断是否需要重新构建节点
  ✅ 自动安装 npm 依赖

时间预估：
  快速启动:    15-30 秒
  重新构建:    3-5 分钟
  强制重建:    5-10 分钟

EOF
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -q|--quick)
            QUICK_MODE=true
            shift
            ;;
        -r|--rebuild)
            REBUILD=true
            shift
            ;;
        -f|--force-rebuild)
            FORCE_REBUILD=true
            shift
            ;;
        --skip-prune)
            SKIP_SYSTEM_PRUNE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_error "未知选项: $1"
            echo "使用 $0 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 如果没有指定构建选项，默认使用快速模式
if [ "$REBUILD" = false ] && [ "$FORCE_REBUILD" = false ]; then
    QUICK_MODE=true
fi

# 显示启动信息
if [ "$QUICK_MODE" = true ]; then
    print_info "⚡ 快速启动 Data-Diff N8N 开发环境..."
elif [ "$FORCE_REBUILD" = true ]; then
    print_info "🚀 强制重新构建 Data-Diff N8N 开发环境..."
elif [ "$REBUILD" = true ]; then
    print_info "🔨 重新构建 Data-Diff N8N 开发环境..."
else
    print_info "🚀 启动 Data-Diff N8N 开发环境..."
fi

# 基础文件检查
print_info "📋 检查必要的文件..."
if [ ! -f "docker-compose.dev.yml" ]; then
    print_error "❌ 错误: docker-compose.dev.yml 文件不存在"
    exit 1
fi

# 检查和生成 SSL 证书
check_and_generate_ssl() {
    if [ "$QUICK_MODE" = true ]; then
        return 0  # 快速模式不需要 SSL 证书
    fi

    if [ ! -f "nginx/certs/localhost.crt" ] || [ ! -f "nginx/certs/localhost.key" ]; then
        print_info "🔐 SSL 证书不存在，自动生成..."

        # 创建证书目录
        mkdir -p nginx/certs

        # 生成自签名证书
        if [ "$VERBOSE" = true ]; then
            openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
                -keyout nginx/certs/localhost.key \
                -out nginx/certs/localhost.crt \
                -subj "/C=CN/ST=Shanghai/L=Shanghai/O=DataDiff/CN=localhost"
        else
            openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
                -keyout nginx/certs/localhost.key \
                -out nginx/certs/localhost.crt \
                -subj "/C=CN/ST=Shanghai/L=Shanghai/O=DataDiff/CN=localhost" \
                > /dev/null 2>&1
        fi

        if [ $? -eq 0 ]; then
            print_success "✅ SSL 证书生成完成"
        else
            print_error "❌ SSL 证书生成失败"
            return 1
        fi
    else
        print_info "🔐 SSL 证书已存在"
    fi

    return 0
}

# 完整模式的额外检查
if [ "$QUICK_MODE" = false ]; then
    if ! check_and_generate_ssl; then
        print_error "❌ SSL 证书准备失败，退出启动"
        exit 1
    fi
fi

# 检查和构建 N8N 节点
check_and_build_n8n_nodes() {
    if [ ! -d "n8n" ]; then
        print_error "❌ 错误: n8n 目录不存在"
        return 1
    fi

    if [ ! -f "n8n/package.json" ]; then
        print_error "❌ 错误: n8n/package.json 不存在"
        return 1
    fi

    # 检查是否需要构建 N8N 节点
    need_build=false

    if [ ! -d "n8n/dist" ]; then
        print_info "📦 N8N 节点未构建，需要构建..."
        need_build=true
    elif [ "$REBUILD" = true ] || [ "$FORCE_REBUILD" = true ]; then
        print_info "🔄 重新构建模式，需要重新构建 N8N 节点..."
        need_build=true
    elif [ -n "$(find n8n/src -name "*.ts" -newer n8n/dist/index.js 2>/dev/null)" ]; then
        print_info "🔄 发现 TypeScript 源码更新，需要重新构建 N8N 节点..."
        need_build=true
    fi

    if [ "$need_build" = true ]; then
        print_info "🔨 构建 N8N TypeScript 节点..."
        cd n8n

        # 检查 node_modules
        if [ ! -d "node_modules" ] || [ "$FORCE_REBUILD" = true ]; then
            print_info "📥 安装 N8N 节点依赖..."
            if [ "$VERBOSE" = true ]; then
                npm install
            else
                npm install > /dev/null 2>&1
            fi
        fi

        # 构建节点
        print_info "⚙️ 编译 TypeScript 节点..."
        if [ "$VERBOSE" = true ]; then
            npm run build
        else
            npm run build > /dev/null 2>&1
        fi

        build_result=$?
        cd ..

        if [ $build_result -ne 0 ]; then
            print_error "❌ N8N 节点构建失败"
            return 1
        fi

        print_success "✅ N8N 节点构建完成"
    else
        print_info "⚡ N8N 节点已是最新，跳过构建"
    fi

    return 0
}

# 执行 N8N 节点检查和构建
if ! check_and_build_n8n_nodes; then
    print_error "❌ N8N 节点构建失败，退出启动"
    exit 1
fi

# 停止现有容器
print_info "🛑 停止现有容器..."
if [ "$VERBOSE" = true ]; then
    docker-compose -f docker-compose.dev.yml down
else
    docker-compose -f docker-compose.dev.yml down > /dev/null 2>&1
fi

# 根据模式决定构建策略
if [ "$FORCE_REBUILD" = true ]; then
    print_info "🧹 强制清理旧的镜像..."
    docker system prune -f
    print_info "🔨 强制重新构建 Docker 镜像..."
    docker-compose -f docker-compose.dev.yml build --no-cache \
        --build-arg HTTP_PROXY=$HTTP_PROXY \
        --build-arg HTTPS_PROXY=$HTTPS_PROXY
elif [ "$REBUILD" = true ]; then
    if [ "$SKIP_SYSTEM_PRUNE" = false ]; then
        print_info "🧹 清理旧的镜像..."
        docker system prune -f
    fi
    print_info "🔨 重新构建 Docker 镜像..."
    docker-compose -f docker-compose.dev.yml build \
        --build-arg HTTP_PROXY=$HTTP_PROXY \
        --build-arg HTTPS_PROXY=$HTTPS_PROXY
else
    print_info "⚡ 使用现有镜像快速启动..."
    if [ "$VERBOSE" = true ]; then
        print_info "   💡 如需重新构建，请使用 --rebuild 或 --force-rebuild 选项"
    fi
fi

# 启动所有服务
print_info "🚀 启动所有服务..."
if [ "$VERBOSE" = true ]; then
    docker-compose -f docker-compose.dev.yml up -d
else
    docker-compose -f docker-compose.dev.yml up -d > /dev/null 2>&1
fi

# 等待服务启动
if [ "$QUICK_MODE" = true ]; then
    print_info "⏱️ 等待服务启动..."
    sleep 15
else
    print_info "⏱️ 等待服务启动..."
    sleep 30
fi

# 检查服务状态
print_info "🔍 检查服务状态..."
docker-compose -f docker-compose.dev.yml ps

echo ""
print_success "✅ 开发环境启动完成！"
echo ""
print_info "🔗 服务访问地址："
echo "  - Data-Diff API: http://localhost:8000"
echo "  - N8N 工作流: http://localhost:5678"
echo "  - Grafana 监控: http://localhost:3000"
echo "  - Prometheus: http://localhost:9091"
if [ "$QUICK_MODE" = false ]; then
    echo "  - Nginx (HTTP): http://localhost:80"
    echo "  - Nginx (HTTPS): https://localhost:443"
fi
echo ""
print_info "🔐 默认登录信息："
echo "  - N8N: admin/admin123"
echo "  - Grafana: admin/admin123"
echo ""
print_info "💡 使用提示："
echo "  - 健康检查: ./health-check.sh"
echo "  - 快速启动: $0 --quick"
echo "  - 重新构建: $0 --rebuild"
echo "  - 强制重建: $0 --force-rebuild"
echo "  - 查看日志: docker-compose -f docker-compose.dev.yml logs -f [service_name]"
echo "  - 停止服务: docker-compose -f docker-compose.dev.yml down"
