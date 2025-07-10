#!/bin/bash
# 健康检查脚本 - 检查所有服务是否正常运行

set -e

echo "🔍 检查 Data-Diff N8N 服务健康状态..."

# 检查服务状态
check_service() {
    local name=$1
    local url=$2

    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|302"; then
        echo "✅ $name: 正常"
        return 0
    else
        echo "❌ $name: 异常"
        return 1
    fi
}

# 显示 Docker 容器状态
echo "📋 检查 Docker 容器状态..."
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Command}}\t{{.CreatedAt}}\t{{.Status}}\t{{.Ports}}"
echo

# 检查各个服务
echo "🔍 检查服务端点..."
services_ok=true

check_service "Data-Diff API" "http://localhost:8000/health" || services_ok=false
check_service "N8N" "http://localhost:5678/" || services_ok=false
check_service "Grafana" "http://localhost:3000/api/health" || services_ok=false
check_service "Prometheus" "http://localhost:9091/-/healthy" || services_ok=false
check_service "Jupyter" "http://localhost:8889/" || services_ok=false
check_service "Mailhog" "http://localhost:8025/" || services_ok=false

echo

if $services_ok; then
    echo "✅ 所有服务运行正常！"
    echo
    echo "🔗 服务访问地址："
    echo "  - Data-Diff API: http://localhost:8000"
    echo "  - N8N 工作流: http://localhost:5678"
    echo "  - Grafana 监控: http://localhost:3000"
    echo "  - Prometheus: http://localhost:9091"
    echo "  - Jupyter Lab: http://localhost:8889"
    echo "  - Mailhog: http://localhost:8025"
    exit 0
else
    echo "❌ 某些服务异常，请检查日志"
    echo "📋 查看日志命令: docker-compose -f docker-compose.dev.yml logs <service_name>"
    exit 1
fi
for service in "${!services[@]}"; do
    if ! check_service "$service" "${services[$service]}"; then
        all_healthy=false
    fi
done

echo ""
if $all_healthy; then
    echo "✅ 所有服务运行正常！"
    echo ""
    echo "🔗 服务访问地址："
    echo "  - Data-Diff API: http://localhost:8000"
    echo "  - N8N 工作流: http://localhost:5678"
    echo "  - Grafana 监控: http://localhost:3000"
    echo "  - Prometheus: http://localhost:9091"
    echo "  - Jupyter Lab: http://localhost:8889"
    echo "  - Mailhog: http://localhost:8025"
    exit 0
else
    echo "❌ 部分服务异常，请检查日志"
    echo "查看日志: docker-compose -f docker-compose.dev.yml logs -f [service_name]"
    exit 1
fi
