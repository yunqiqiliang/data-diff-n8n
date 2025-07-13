#!/bin/bash
# Data-Diff N8N 生产部署脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 版本号
VERSION=${VERSION:-"latest"}

# 打印函数
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 显示帮助
show_help() {
    cat << EOF
🚀 Data-Diff N8N 生产部署脚本

用法: $0 [命令] [选项]

命令:
  setup     初始化部署环境
  deploy    部署应用
  start     启动服务
  stop      停止服务
  restart   重启服务
  status    查看服务状态
  logs      查看日志
  backup    备份数据
  restore   恢复数据
  upgrade   升级版本

选项:
  -h, --help       显示帮助信息
  -v, --version    指定版本号（默认: latest）
  -f, --file       指定 docker-compose 文件（默认: docker-compose.prod.yml）

示例:
  $0 setup         # 初始化环境
  $0 deploy        # 部署应用
  $0 start         # 启动服务
  $0 logs api      # 查看 API 日志

EOF
}

# 检查 Docker 环境
check_docker() {
    print_info "检查 Docker 环境..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker 未安装！请先安装 Docker Desktop"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose 未安装！"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker 服务未运行！请启动 Docker Desktop"
        exit 1
    fi
    
    print_success "Docker 环境检查通过"
}

# 初始化环境
setup_environment() {
    print_info "初始化部署环境..."
    
    # 创建必要的目录
    mkdir -p {nginx/ssl,nginx/static,monitoring/grafana-config,monitoring/dashboards,init,n8n-nodes}
    
    # 复制配置文件
    if [ ! -f .env ]; then
        if [ -f .env.example.prod ]; then
            cp .env.example.prod .env
            print_warning "已创建 .env 文件，请编辑配置后再继续部署"
            print_info "使用以下命令编辑配置："
            echo "  nano .env"
            exit 0
        else
            print_error ".env.example.prod 文件不存在"
            exit 1
        fi
    fi
    
    # 复制 Grafana 配置
    if [ -d monitoring/grafana ]; then
        cp -r monitoring/grafana/* monitoring/grafana-config/ 2>/dev/null || true
    fi
    
    # 复制仪表板文件
    if [ -d monitoring/grafana-dashboards ]; then
        cp monitoring/grafana-dashboards/*.json monitoring/dashboards/ 2>/dev/null || true
    fi
    
    # 复制 Nginx 静态文件
    if [ -f nginx/index.html ]; then
        cp nginx/index.html nginx/static/
    fi
    
    # 创建初始化 SQL
    cat > init/init-databases.sql << 'EOF'
-- 创建 data_diff_results schema
CREATE SCHEMA IF NOT EXISTS data_diff_results;

-- 设置权限
GRANT ALL PRIVILEGES ON SCHEMA data_diff_results TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_diff_results TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_diff_results TO postgres;

-- 创建 n8n 数据库（如果不存在）
SELECT 'CREATE DATABASE n8n'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'n8n')\gexec
EOF
    
    print_success "环境初始化完成"
}

# 构建镜像
build_images() {
    print_info "构建 Docker 镜像..."
    
    # 使用 build-push.sh 脚本构建镜像
    if [ -f build-push.sh ]; then
        print_info "使用 build-push.sh 构建镜像..."
        ./build-push.sh -v ${VERSION}
        if [ $? -eq 0 ]; then
            print_success "所有镜像构建完成"
        else
            print_error "镜像构建失败"
            exit 1
        fi
    else
        print_error "build-push.sh 脚本不存在"
        exit 1
    fi
}

# 部署应用
deploy_app() {
    check_docker
    
    if [ ! -f .env ]; then
        print_error "未找到 .env 文件，请先运行 setup 命令"
        exit 1
    fi
    
    print_info "开始部署 Data-Diff N8N..."
    
    # 拉取或构建镜像
    if [ "$BUILD_LOCAL" = "true" ]; then
        build_images
    else
        print_info "拉取 Docker 镜像..."
        docker-compose -f docker-compose.prod.yml pull
    fi
    
    # 启动服务
    print_info "启动服务..."
    docker-compose -f docker-compose.prod.yml up -d
    
    # 等待服务启动
    print_info "等待服务启动..."
    sleep 30
    
    # 检查服务状态
    check_services
    
    print_success "部署完成！"
    show_access_info
}

# 启动服务
start_services() {
    print_info "启动服务..."
    docker-compose -f docker-compose.prod.yml up -d
    print_success "服务已启动"
}

# 停止服务
stop_services() {
    print_info "停止服务..."
    docker-compose -f docker-compose.prod.yml down
    print_success "服务已停止"
}

# 重启服务
restart_services() {
    stop_services
    start_services
}

# 查看服务状态
check_services() {
    print_info "检查服务状态..."
    docker-compose -f docker-compose.prod.yml ps
    
    # 检查各服务健康状态
    services=("postgres" "data-diff-api" "n8n" "grafana" "prometheus" "nginx")
    all_healthy=true
    
    for service in "${services[@]}"; do
        if docker-compose -f docker-compose.prod.yml ps | grep -q "${service}.*Up.*healthy"; then
            print_success "${service} 运行正常"
        else
            print_warning "${service} 状态异常"
            all_healthy=false
        fi
    done
    
    if [ "$all_healthy" = true ]; then
        print_success "所有服务运行正常"
    else
        print_warning "部分服务状态异常，请检查日志"
    fi
}

# 查看日志
view_logs() {
    service=$1
    if [ -z "$service" ]; then
        docker-compose -f docker-compose.prod.yml logs -f --tail=100
    else
        docker-compose -f docker-compose.prod.yml logs -f --tail=100 $service
    fi
}

# 备份数据
backup_data() {
    timestamp=$(date +%Y%m%d_%H%M%S)
    backup_dir="backups/backup_${timestamp}"
    
    print_info "开始备份数据到 ${backup_dir}..."
    mkdir -p ${backup_dir}
    
    # 备份数据库
    print_info "备份 PostgreSQL 数据库..."
    docker-compose -f docker-compose.prod.yml exec -T postgres pg_dumpall -U postgres > ${backup_dir}/postgres_backup.sql
    
    # 备份配置文件
    print_info "备份配置文件..."
    cp .env ${backup_dir}/
    cp docker-compose.prod.yml ${backup_dir}/
    
    # 备份 Docker volumes
    print_info "备份 Docker volumes..."
    docker run --rm -v datadiff-n8n_postgres_data:/data -v $(pwd)/${backup_dir}:/backup alpine tar czf /backup/postgres_data.tar.gz -C /data .
    docker run --rm -v datadiff-n8n_n8n_data:/data -v $(pwd)/${backup_dir}:/backup alpine tar czf /backup/n8n_data.tar.gz -C /data .
    docker run --rm -v datadiff-n8n_grafana_data:/data -v $(pwd)/${backup_dir}:/backup alpine tar czf /backup/grafana_data.tar.gz -C /data .
    docker run --rm -v datadiff-n8n_prometheus_data:/data -v $(pwd)/${backup_dir}:/backup alpine tar czf /backup/prometheus_data.tar.gz -C /data .
    
    print_success "备份完成！备份文件保存在: ${backup_dir}"
}

# 显示访问信息
show_access_info() {
    echo ""
    print_info "🔗 服务访问地址："
    echo "  - 主页: http://localhost"
    echo "  - Data-Diff API: http://localhost:8000"
    echo "  - N8N 工作流: http://localhost:5678"
    echo "  - Grafana 监控: http://localhost:3000"
    echo "  - Prometheus: http://localhost:9090"
    echo ""
    print_info "🔐 默认登录信息："
    echo "  - N8N: 查看 .env 文件中的 N8N_BASIC_AUTH_USER 和 N8N_BASIC_AUTH_PASSWORD"
    echo "  - Grafana: 查看 .env 文件中的 GRAFANA_USER 和 GRAFANA_PASSWORD"
    echo ""
}

# 主函数
main() {
    case "$1" in
        setup)
            setup_environment
            ;;
        deploy)
            deploy_app
            ;;
        start)
            start_services
            ;;
        stop)
            stop_services
            ;;
        restart)
            restart_services
            ;;
        status)
            check_services
            ;;
        logs)
            view_logs $2
            ;;
        backup)
            backup_data
            ;;
        -h|--help|help)
            show_help
            ;;
        *)
            print_error "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"