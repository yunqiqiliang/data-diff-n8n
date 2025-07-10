#!/bin/bash

# Data-Diff N8N 项目部署脚本
# 用于快速部署整个系统环境

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查必要的工具
check_prerequisites() {
    log_info "检查系统依赖..."

    # 检查 Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装，请先安装 Docker"
        exit 1
    fi

    # 检查 Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose 未安装，请先安装 Docker Compose"
        exit 1
    fi

    # 检查 Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 未安装，请先安装 Python 3"
        exit 1
    fi

    # 检查 Poetry（可选）
    if ! command -v poetry &> /dev/null; then
        log_warning "Poetry 未安装，将使用 pip 安装依赖"
    fi

    log_success "系统依赖检查完成"
}

# 设置环境变量
setup_environment() {
    log_info "设置环境变量..."

    # 创建 .env 文件
    cat > .env << EOF
# N8N 配置
N8N_BASIC_AUTH_ACTIVE=true
N8N_BASIC_AUTH_USER=admin
N8N_BASIC_AUTH_PASSWORD=admin123
N8N_PORT=5678

# 数据库配置
POSTGRES_DB=n8n
POSTGRES_USER=n8n
POSTGRES_PASSWORD=n8n_password
POSTGRES_PORT=5432

# Clickzetta 配置
CLICKZETTA_HOST=localhost
CLICKZETTA_PORT=8080
CLICKZETTA_USER=admin
CLICKZETTA_PASSWORD=admin123

# API 配置
API_PORT=8000
API_LOG_LEVEL=info

# Redis 配置
REDIS_HOST=localhost
REDIS_PORT=6379

# 监控配置
GRAFANA_PORT=3000
GRAFANA_PASSWORD=grafana123
PROMETHEUS_PORT=9090
EOF

    log_success "环境变量设置完成"
}

# 构建 Docker 镜像
build_images() {
    log_info "构建 Docker 镜像..."

    # 构建 API 镜像
    docker build -f Dockerfile.api -t data-diff-api:latest .

    log_success "Docker 镜像构建完成"
}

# 启动服务
start_services() {
    log_info "启动服务..."

    # 使用 Docker Compose 启动所有服务
    docker-compose -f docker-compose.full.yml up -d

    log_info "等待服务启动..."
    sleep 30

    # 检查服务状态
    check_services_health
}

# 检查服务健康状态
check_services_health() {
    log_info "检查服务健康状态..."

    # 检查 N8N
    if curl -s http://localhost:5678 > /dev/null; then
        log_success "N8N 服务正常"
    else
        log_error "N8N 服务启动失败"
    fi

    # 检查 API
    if curl -s http://localhost:8000/health > /dev/null; then
        log_success "API 服务正常"
    else
        log_error "API 服务启动失败"
    fi

    # 检查 Grafana
    if curl -s http://localhost:3000 > /dev/null; then
        log_success "Grafana 服务正常"
    else
        log_warning "Grafana 服务可能未启动"
    fi
}

# 初始化数据
initialize_data() {
    log_info "初始化测试数据..."

    # 等待数据库完全启动
    sleep 10

    # 运行初始化脚本
    python3 scripts/init_test_data.py

    log_success "测试数据初始化完成"
}

# 安装 N8N 自定义节点
install_n8n_nodes() {
    log_info "安装 N8N 自定义节点..."

    # 进入 N8N 目录
    cd n8n

    # 安装依赖
    if command -v npm &> /dev/null; then
        npm install
        npm run build
    else
        log_warning "npm 未安装，跳过 N8N 节点构建"
    fi

    cd ..

    log_success "N8N 自定义节点安装完成"
}

# 显示访问信息
show_access_info() {
    log_success "部署完成！"
    echo ""
    log_info "服务访问信息："
    echo "  - N8N 工作流编辑器: http://localhost:5678"
    echo "    用户名: admin"
    echo "    密码: admin123"
    echo ""
    echo "  - API 服务: http://localhost:8000"
    echo "    文档: http://localhost:8000/docs"
    echo ""
    echo "  - Grafana 监控: http://localhost:3000"
    echo "    用户名: admin"
    echo "    密码: grafana123"
    echo ""
    echo "  - Prometheus 指标: http://localhost:9090"
    echo ""
    log_info "配置文件："
    echo "  - 环境变量: .env"
    echo "  - Docker Compose: docker-compose.full.yml"
    echo ""
    log_info "常用命令："
    echo "  - 查看日志: docker-compose -f docker-compose.full.yml logs -f"
    echo "  - 停止服务: docker-compose -f docker-compose.full.yml down"
    echo "  - 重启服务: docker-compose -f docker-compose.full.yml restart"
}

# 清理函数
cleanup() {
    log_info "清理环境..."
    docker-compose -f docker-compose.full.yml down
    docker system prune -f
    log_success "清理完成"
}

# 主函数
main() {
    echo "=========================================="
    echo "  Data-Diff N8N 系统部署脚本"
    echo "=========================================="
    echo ""

    case "${1:-deploy}" in
        "deploy")
            check_prerequisites
            setup_environment
            build_images
            install_n8n_nodes
            start_services
            initialize_data
            show_access_info
            ;;
        "start")
            log_info "启动服务..."
            docker-compose -f docker-compose.full.yml up -d
            check_services_health
            ;;
        "stop")
            log_info "停止服务..."
            docker-compose -f docker-compose.full.yml down
            log_success "服务已停止"
            ;;
        "restart")
            log_info "重启服务..."
            docker-compose -f docker-compose.full.yml restart
            check_services_health
            ;;
        "logs")
            docker-compose -f docker-compose.full.yml logs -f
            ;;
        "status")
            docker-compose -f docker-compose.full.yml ps
            ;;
        "cleanup")
            cleanup
            ;;
        "help")
            echo "用法: $0 [命令]"
            echo ""
            echo "命令:"
            echo "  deploy   - 完整部署系统（默认）"
            echo "  start    - 启动服务"
            echo "  stop     - 停止服务"
            echo "  restart  - 重启服务"
            echo "  logs     - 查看日志"
            echo "  status   - 查看服务状态"
            echo "  cleanup  - 清理环境"
            echo "  help     - 显示此帮助信息"
            ;;
        *)
            log_error "未知命令: $1"
            log_info "使用 '$0 help' 查看可用命令"
            exit 1
            ;;
    esac
}

# 捕获 Ctrl+C
trap 'log_warning "部署被中断"; exit 1' INT

# 执行主函数
main "$@"
