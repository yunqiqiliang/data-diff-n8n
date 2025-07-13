# Data-Diff N8N 生产部署指南

## 概述

本指南介绍如何在生产环境中部署 Data-Diff N8N 系统。整个部署过程已经过优化，用户只需要安装 Docker Desktop 即可完成部署。

## 系统要求

### 最低配置
- CPU: 4 核心
- 内存: 8GB RAM
- 存储: 50GB 可用空间
- 操作系统: Windows 10/11, macOS, Linux

### 推荐配置
- CPU: 8 核心或更多
- 内存: 16GB RAM 或更多
- 存储: 100GB SSD
- 网络: 稳定的互联网连接

### 软件要求
- Docker Desktop（必需）
- 文本编辑器（用于修改配置文件）

## 快速部署

### 1. 安装 Docker Desktop

根据您的操作系统下载并安装 Docker Desktop：
- [Windows](https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe)
- [macOS](https://desktop.docker.com/mac/main/amd64/Docker.dmg)
- [Linux](https://docs.docker.com/desktop/linux/install/)

### 2. 下载项目

```bash
# 方法1：使用 Git（如果已安装）
git clone https://github.com/yunqiqiliang/data-diff-n8n.git
cd data-diff-n8n

# 方法2：直接下载 ZIP
# 访问 https://github.com/yunqiqiliang/data-diff-n8n
# 点击 "Code" -> "Download ZIP"
# 解压到指定目录
```

### 3. 初始化环境

```bash
# Windows
deploy.bat setup

# macOS/Linux
./deploy.sh setup
```

### 4. 配置环境变量

编辑生成的 `.env` 文件，修改关键配置：

```env
# 必须修改的密码配置
POSTGRES_PASSWORD=your_secure_password_here
N8N_BASIC_AUTH_PASSWORD=your_secure_password_here
GRAFANA_PASSWORD=your_secure_password_here

# 端口配置（如果端口冲突，请修改这些端口号）
API_PORT=8000           # API 服务端口
N8N_PORT=5678          # N8N 工作流端口
GRAFANA_PORT=3000      # Grafana 监控端口
PROMETHEUS_PORT=9090   # Prometheus 端口
POSTGRES_PORT=5432     # PostgreSQL 端口
HTTP_PORT=80           # HTTP 端口
HTTPS_PORT=443         # HTTPS 端口

# 示例：修改端口避免冲突
# API_PORT=8001
# N8N_PORT=5679
# GRAFANA_PORT=3001
```

#### 端口冲突解决

如果遇到端口冲突错误，如：
```
Error: bind: address already in use
```

请按以下步骤解决：

1. **查看占用端口的进程**：
   ```bash
   # macOS/Linux
   lsof -i :8000
   
   # Windows
   netstat -ano | findstr :8000
   ```

2. **修改 .env 文件中的端口**：
   ```env
   # 将冲突的端口改为其他未使用的端口
   API_PORT=8001
   N8N_PORT=5679
   ```

3. **重新部署**：
   ```bash
   ./deploy.sh restart
   ```

### 5. 部署应用

```bash
# Windows
deploy.bat deploy

# macOS/Linux
./deploy.sh deploy
```

部署过程大约需要 5-10 分钟，具体时间取决于网络速度。

### 6. 访问服务

部署完成后，可以通过以下地址访问各服务：

- **主页**: http://localhost
- **API 文档**: http://localhost:8000/docs
- **N8N 工作流**: http://localhost:5678
- **Grafana 监控**: http://localhost:3000
- **Prometheus**: http://localhost:9090

## 详细配置说明

### 数据库配置

系统默认使用内置的 PostgreSQL 数据库。如需使用外部数据库：

```env
# 使用外部数据库
EXTERNAL_DATABASE_URL=postgresql://user:password@host:5432/database
```

### N8N 配置

```env
# 基本认证
N8N_BASIC_AUTH_ACTIVE=true
N8N_BASIC_AUTH_USER=admin
N8N_BASIC_AUTH_PASSWORD=your_password

# Webhook URL（用于外部访问）
N8N_WEBHOOK_URL=http://your-domain.com/webhook
```

### SSL/HTTPS 配置

1. 准备 SSL 证书文件：
   - `nginx/ssl/cert.pem`
   - `nginx/ssl/key.pem`

2. 修改 `nginx/nginx.prod.conf`，取消 HTTPS 部分的注释

3. 更新 `.env` 文件中的域名配置

### 性能优化

```env
# API 工作进程数（建议设置为 CPU 核心数）
API_WORKERS=4

# 日志级别（生产环境建议使用 INFO 或 WARNING）
LOG_LEVEL=INFO
```

## 服务管理

### 启动服务
```bash
./deploy.sh start
```

### 停止服务
```bash
./deploy.sh stop
```

### 重启服务
```bash
./deploy.sh restart
```

### 查看服务状态
```bash
./deploy.sh status
```

### 查看日志
```bash
# 查看所有服务日志
./deploy.sh logs

# 查看特定服务日志
./deploy.sh logs api
./deploy.sh logs n8n
./deploy.sh logs grafana
```

## 数据备份与恢复

### 备份数据
```bash
./deploy.sh backup
```

备份包括：
- PostgreSQL 数据库
- N8N 工作流和凭据
- Grafana 仪表板和设置
- Prometheus 历史数据
- 配置文件

### 恢复数据
```bash
./deploy.sh restore backups/backup_20240113_120000
```

## 监控和告警

### Grafana 仪表板

系统预配置了三个仪表板：
1. **Data-Diff Overview** - 系统概览
2. **System Metrics** - 系统资源监控
3. **Business Metrics** - 业务指标监控

### 配置告警

1. 登录 Grafana (http://localhost:3000)
2. 导航到 Alerting > Alert rules
3. 创建新的告警规则
4. 配置通知渠道（Email, Slack, Webhook 等）

## 故障排除

### 服务无法启动

```bash
# 检查 Docker 状态
docker info

# 查看服务日志
docker-compose -f docker-compose.prod.yml logs [service_name]

# 检查端口占用
netstat -an | grep [port_number]
```

### 数据库连接失败

1. 检查 PostgreSQL 容器状态
2. 验证数据库凭据
3. 确认网络连接

### N8N 工作流执行失败

1. 检查 N8N 日志
2. 验证 API 服务可用性
3. 检查网络连接

## 安全建议

1. **修改默认密码**：部署后立即修改所有默认密码
2. **限制访问**：使用防火墙限制服务端口访问
3. **启用 HTTPS**：在生产环境中必须启用 HTTPS
4. **定期备份**：建立自动备份策略
5. **监控告警**：配置系统和业务监控告警
6. **更新维护**：定期更新系统和依赖

## 升级指南

```bash
# 备份当前版本
./deploy.sh backup

# 拉取最新代码
git pull

# 升级服务
./deploy.sh upgrade
```

## 技术支持

- 项目主页：https://github.com/yunqiqiliang/data-diff-n8n
- 问题反馈：https://github.com/yunqiqiliang/data-diff-n8n/issues
- 使用文档：https://github.com/yunqiqiliang/data-diff-n8n/wiki