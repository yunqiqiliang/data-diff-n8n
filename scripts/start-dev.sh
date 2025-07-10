#!/bin/bash

# Data-Diff N8N 开发环境启动脚本

set -e

echo "🚀 启动 Data-Diff N8N 开发环境..."

# 检查 Docker 和 Docker Compose 是否安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装 Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装 Docker Compose"
    exit 1
fi

# 检查端口是否被占用
echo "🔍 检查端口占用情况..."
PORTS=(80 443 3000 5432 5678 6379 8000 8025 8123 8888 9000 9090)
for port in "${PORTS[@]}"; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "⚠️  端口 $port 已被占用，请检查并释放该端口"
        echo "   可以使用: lsof -i :$port 查看占用进程"
        read -p "   是否继续启动? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
done

# 创建必要的目录
echo "📁 创建必要的目录..."
mkdir -p exports
mkdir -p monitoring/grafana/dashboards
mkdir -p monitoring/grafana/datasources
mkdir -p monitoring/prometheus
mkdir -p nginx
mkdir -p notebooks
mkdir -p dev

# 创建 Nginx 配置
echo "⚙️  创建 Nginx 配置..."
cat > nginx/nginx.conf << 'EOF'
events {
    worker_connections 1024;
}

http {
    upstream data-diff-api {
        server data-diff-api:8000;
    }

    upstream n8n {
        server n8n:5678;
    }

    upstream grafana {
        server grafana:3000;
    }

    server {
        listen 80;
        server_name localhost;

        # Data-Diff API
        location /api/ {
            proxy_pass http://data-diff-api;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }

        # N8N 工作流引擎
        location /n8n/ {
            proxy_pass http://n8n/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header Connection "upgrade";
            proxy_set_header Upgrade $http_upgrade;
        }

        # Grafana 监控
        location /grafana/ {
            proxy_pass http://grafana/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }

        # 静态文件和文档
        location / {
            root /usr/share/nginx/html;
            index index.html;
        }
    }
}
EOF

# 创建 Prometheus 配置
echo "📊 创建 Prometheus 配置..."
cat > monitoring/prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'data-diff-api'
    static_configs:
      - targets: ['data-diff-api:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres:5432']

  - job_name: 'redis'
    static_configs:
      - targets: ['redis:6379']

  - job_name: 'clickhouse'
    static_configs:
      - targets: ['clickzetta:8123']
EOF

# 创建 ClickHouse 配置
echo "🔧 创建 ClickHouse 配置..."
cat > dev/clickhouse-config.xml << 'EOF'
<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>trace</level>
        <console>true</console>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>

    <listen_host>0.0.0.0</listen_host>

    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>

    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
EOF

# 创建 Grafana 数据源配置
echo "📈 创建 Grafana 配置..."
cat > monitoring/grafana/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOF

# 停止并清理现有容器（如果有）
echo "🧹 清理现有容器..."
docker-compose -f docker-compose.dev.yml down -v --remove-orphans 2>/dev/null || true

# 构建和启动服务
echo "🏗️  构建和启动服务..."
docker-compose -f docker-compose.dev.yml up -d --build

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 30

# 检查服务状态
echo "✅ 检查服务状态..."
docker-compose -f docker-compose.dev.yml ps

echo ""
echo "🎉 开发环境启动完成！"
echo ""
echo "📋 服务访问地址："
echo "   🌐 Data-Diff API:     http://localhost:8000"
echo "   🔧 API 文档:          http://localhost:8000/docs"
echo "   🔄 N8N 工作流:        http://localhost:5678 (admin/admin123)"
echo "   📊 Grafana 监控:      http://localhost:3000 (admin/admin123)"
echo "   📈 Prometheus:        http://localhost:9090"
echo "   🐘 PostgreSQL:        localhost:5432 (postgres/password)"
echo "   🗄️  ClickHouse:        http://localhost:8123"
echo "   📧 MailHog:           http://localhost:8025"
echo "   📓 Jupyter Lab:       http://localhost:8888 (token: datadiff123)"
echo "   🔄 Redis:             localhost:6379"
echo ""
echo "🔧 管理命令："
echo "   停止服务: docker-compose -f docker-compose.dev.yml down"
echo "   查看日志: docker-compose -f docker-compose.dev.yml logs -f [service_name]"
echo "   重启服务: docker-compose -f docker-compose.dev.yml restart [service_name]"
echo ""
echo "📚 快速开始："
echo "   1. 访问 API 文档了解接口: http://localhost:8000/docs"
echo "   2. 登录 N8N 创建工作流: http://localhost:5678"
echo "   3. 查看监控仪表板: http://localhost:3000"
echo "   4. 使用 Jupyter 进行数据分析: http://localhost:8888"
echo ""

# 可选：运行健康检查
read -p "是否运行健康检查? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🔍 运行健康检查..."

    # 检查 API 健康状态
    if curl -s http://localhost:8000/health > /dev/null; then
        echo "✅ Data-Diff API 运行正常"
    else
        echo "❌ Data-Diff API 健康检查失败"
    fi

    # 检查数据库连接
    if docker-compose -f docker-compose.dev.yml exec -T postgres pg_isready -U postgres > /dev/null; then
        echo "✅ PostgreSQL 连接正常"
    else
        echo "❌ PostgreSQL 连接失败"
    fi

    # 检查 Redis 连接
    if docker-compose -f docker-compose.dev.yml exec -T redis redis-cli ping | grep -q PONG; then
        echo "✅ Redis 连接正常"
    else
        echo "❌ Redis 连接失败"
    fi

    echo "✅ 健康检查完成"
fi

echo ""
echo "🚀 开发环境已就绪，开始开发吧！"
