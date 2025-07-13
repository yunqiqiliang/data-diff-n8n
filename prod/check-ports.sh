#!/bin/bash
# 检查端口占用情况

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 默认端口
DEFAULT_PORTS=(
    "8000:API服务"
    "5678:N8N工作流"
    "3000:Grafana监控"
    "9090:Prometheus"
    "5432:PostgreSQL"
    "80:HTTP"
    "443:HTTPS"
)

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}    端口占用检查工具${NC}"
    echo -e "${BLUE}================================${NC}"
    echo ""
}

check_port() {
    local port=$1
    local service=$2
    
    if command -v lsof &> /dev/null; then
        # macOS/Linux
        if lsof -i :$port > /dev/null 2>&1; then
            echo -e "${RED}✗ 端口 $port ($service) 已被占用${NC}"
            echo -e "  占用进程："
            lsof -i :$port | grep LISTEN | head -1
            echo ""
            return 1
        else
            echo -e "${GREEN}✓ 端口 $port ($service) 可用${NC}"
            return 0
        fi
    elif command -v netstat &> /dev/null; then
        # Windows/Linux fallback
        if netstat -an | grep -E "[:.]$port.*LISTEN" > /dev/null 2>&1; then
            echo -e "${RED}✗ 端口 $port ($service) 已被占用${NC}"
            return 1
        else
            echo -e "${GREEN}✓ 端口 $port ($service) 可用${NC}"
            return 0
        fi
    else
        echo -e "${YELLOW}⚠ 无法检查端口 $port ($service) - 缺少必要工具${NC}"
        return 2
    fi
}

suggest_alternative_port() {
    local base_port=$1
    local max_attempts=10
    local port=$base_port
    
    for i in $(seq 1 $max_attempts); do
        port=$((base_port + i))
        if ! lsof -i :$port > /dev/null 2>&1; then
            echo $port
            return 0
        fi
    done
    
    echo $((base_port + 1000))
}

main() {
    print_header
    
    # 检查是否有 .env 文件
    if [ -f .env ]; then
        echo -e "${BLUE}检测到 .env 文件，读取配置的端口...${NC}"
        source .env
        echo ""
    fi
    
    local has_conflict=false
    local suggestions=""
    
    # 检查每个端口
    for port_info in "${DEFAULT_PORTS[@]}"; do
        IFS=':' read -r port service <<< "$port_info"
        
        # 检查环境变量中的端口配置
        case $service in
            "API服务")
                port=${API_PORT:-$port}
                ;;
            "N8N工作流")
                port=${N8N_PORT:-$port}
                ;;
            "Grafana监控")
                port=${GRAFANA_PORT:-$port}
                ;;
            "Prometheus")
                port=${PROMETHEUS_PORT:-$port}
                ;;
            "PostgreSQL")
                port=${POSTGRES_PORT:-$port}
                ;;
            "HTTP")
                port=${HTTP_PORT:-$port}
                ;;
            "HTTPS")
                port=${HTTPS_PORT:-$port}
                ;;
        esac
        
        if ! check_port $port "$service"; then
            has_conflict=true
            alt_port=$(suggest_alternative_port $port)
            
            case $service in
                "API服务")
                    suggestions="${suggestions}API_PORT=$alt_port\n"
                    ;;
                "N8N工作流")
                    suggestions="${suggestions}N8N_PORT=$alt_port\n"
                    ;;
                "Grafana监控")
                    suggestions="${suggestions}GRAFANA_PORT=$alt_port\n"
                    ;;
                "Prometheus")
                    suggestions="${suggestions}PROMETHEUS_PORT=$alt_port\n"
                    ;;
                "PostgreSQL")
                    suggestions="${suggestions}POSTGRES_PORT=$alt_port\n"
                    ;;
                "HTTP")
                    suggestions="${suggestions}HTTP_PORT=$alt_port\n"
                    ;;
                "HTTPS")
                    suggestions="${suggestions}HTTPS_PORT=$alt_port\n"
                    ;;
            esac
        fi
    done
    
    echo ""
    echo -e "${BLUE}================================${NC}"
    
    if [ "$has_conflict" = true ]; then
        echo -e "${YELLOW}发现端口冲突！${NC}"
        echo ""
        echo -e "${YELLOW}建议在 .env 文件中添加以下配置：${NC}"
        echo ""
        echo -e "$suggestions"
        echo -e "${YELLOW}然后运行 ./deploy.sh restart 重启服务${NC}"
    else
        echo -e "${GREEN}所有端口都可用！${NC}"
        echo -e "${GREEN}可以运行 ./deploy.sh deploy 开始部署${NC}"
    fi
    
    echo -e "${BLUE}================================${NC}"
}

main "$@"