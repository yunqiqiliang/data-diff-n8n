#!/bin/bash
# 构建并推送 Docker 镜像到 Docker Hub

set -e

# 配置
DOCKER_HUB_USER="czqiliang"
VERSION=${VERSION:-"latest"}
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 打印函数
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 显示帮助
show_help() {
    cat << EOF
构建并推送 Docker 镜像

用法: $0 [选项]

选项:
  -h, --help       显示帮助信息
  -v, --version    指定版本号（默认: latest）
  -p, --push       构建后推送到 Docker Hub
  --api-only       只构建 API 镜像
  --n8n-only       只构建 N8N 镜像

示例:
  $0                    # 构建所有镜像
  $0 -p                 # 构建并推送所有镜像
  $0 -v 1.0.0 -p       # 构建并推送指定版本
  $0 --api-only        # 只构建 API 镜像

EOF
}

# 解析参数
PUSH=false
BUILD_API=true
BUILD_N8N=true

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -p|--push)
            PUSH=true
            shift
            ;;
        --api-only)
            BUILD_API=true
            BUILD_N8N=false
            shift
            ;;
        --n8n-only)
            BUILD_API=false
            BUILD_N8N=true
            shift
            ;;
        *)
            print_error "未知参数: $1"
            show_help
            exit 1
            ;;
    esac
done

# 检查 Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker 未安装！"
    exit 1
fi

# 登录 Docker Hub（如果需要推送）
if [ "$PUSH" = true ]; then
    print_info "登录 Docker Hub..."
    docker login -u $DOCKER_HUB_USER
    if [ $? -ne 0 ]; then
        print_error "Docker Hub 登录失败！"
        exit 1
    fi
fi

# 构建 API 镜像
if [ "$BUILD_API" = true ]; then
    print_info "构建 Data-Diff API 镜像..."
    
    docker build \
        -f Dockerfile.api \
        -t $DOCKER_HUB_USER/datadiff-api:$VERSION \
        -t $DOCKER_HUB_USER/datadiff-api:latest \
        --build-arg BUILD_DATE=$BUILD_DATE \
        --build-arg VERSION=$VERSION \
        --build-arg VCS_REF=$GIT_COMMIT \
        .
    
    if [ $? -eq 0 ]; then
        print_success "API 镜像构建成功！"
        
        if [ "$PUSH" = true ]; then
            print_info "推送 API 镜像到 Docker Hub..."
            docker push $DOCKER_HUB_USER/datadiff-api:$VERSION
            docker push $DOCKER_HUB_USER/datadiff-api:latest
            print_success "API 镜像推送成功！"
        fi
    else
        print_error "API 镜像构建失败！"
        exit 1
    fi
fi

# 构建 N8N 镜像
if [ "$BUILD_N8N" = true ]; then
    print_info "构建 N8N with Data-Diff nodes 镜像..."
    
    # 确保 N8N 节点已构建
    if [ ! -d "n8n/dist" ]; then
        print_info "构建 N8N 节点..."
        cd n8n
        npm install
        npm run build
        cd ..
    fi
    
    docker build \
        -f Dockerfile.n8n \
        -t $DOCKER_HUB_USER/datadiff-n8n:$VERSION \
        -t $DOCKER_HUB_USER/datadiff-n8n:latest \
        --build-arg BUILD_DATE=$BUILD_DATE \
        --build-arg VERSION=$VERSION \
        --build-arg VCS_REF=$GIT_COMMIT \
        .
    
    if [ $? -eq 0 ]; then
        print_success "N8N 镜像构建成功！"
        
        if [ "$PUSH" = true ]; then
            print_info "推送 N8N 镜像到 Docker Hub..."
            docker push $DOCKER_HUB_USER/datadiff-n8n:$VERSION
            docker push $DOCKER_HUB_USER/datadiff-n8n:latest
            print_success "N8N 镜像推送成功！"
        fi
    else
        print_error "N8N 镜像构建失败！"
        exit 1
    fi
fi

# 显示镜像信息
print_info "已构建的镜像："
docker images | grep $DOCKER_HUB_USER/datadiff

if [ "$PUSH" = true ]; then
    echo ""
    print_success "所有镜像已推送到 Docker Hub！"
    echo ""
    print_info "用户可以使用以下命令拉取镜像："
    echo "  docker pull $DOCKER_HUB_USER/datadiff-api:$VERSION"
    echo "  docker pull $DOCKER_HUB_USER/datadiff-n8n:$VERSION"
else
    echo ""
    print_info "镜像已在本地构建完成。使用 -p 参数推送到 Docker Hub。"
fi