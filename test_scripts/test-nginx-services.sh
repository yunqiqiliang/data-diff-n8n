#!/bin/bash

echo "测试所有服务的统一入口访问..."
echo "================================"

# 测试函数
test_service() {
    local name=$1
    local url=$2
    local expected=$3
    
    echo -n "测试 $name ($url): "
    
    # 获取HTTP状态码
    status=$(curl -s -o /dev/null -w "%{http_code}" -L "$url")
    
    if [ "$status" = "$expected" ]; then
        echo "✅ 成功 (HTTP $status)"
    else
        echo "❌ 失败 (HTTP $status, 期望 $expected)"
    fi
}

# 测试所有服务
test_service "主页" "http://localhost/" "200"
test_service "Grafana" "http://localhost/grafana/" "200"
test_service "Prometheus" "http://localhost/prometheus/graph" "200"
test_service "AlertManager" "http://localhost/alertmanager/" "200"
test_service "N8N" "http://localhost/n8n/" "200"
test_service "API 文档" "http://localhost/api/docs" "200"

echo ""
echo "直接端口访问测试..."
echo "==================="
test_service "Grafana (端口)" "http://localhost:3000" "302"
test_service "Prometheus (端口)" "http://localhost:9091" "302"
test_service "AlertManager (端口)" "http://localhost:9093" "200"

echo ""
echo "测试完成！"