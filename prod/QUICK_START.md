# 🚀 Data-Diff N8N 快速启动指南

只需 **3 步** 即可启动 Data-Diff N8N！

## 前提条件

- 安装 [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- 至少 8GB 内存

## 快速启动

### 步骤 1：下载项目

```bash
git clone https://github.com/yunqiqiliang/data-diff-n8n.git
cd data-diff-n8n
```

或者 [下载 ZIP 文件](https://github.com/yunqiqiliang/data-diff-n8n/archive/refs/heads/master.zip)

### 步骤 2：初始化并配置

**macOS/Linux:**
```bash
./deploy.sh setup
```

**Windows:**
```cmd
deploy.bat setup
```

编辑生成的 `.env` 文件：

1. **修改密码**（必须）：
```env
POSTGRES_PASSWORD=your_password_here
N8N_BASIC_AUTH_PASSWORD=your_password_here
GRAFANA_PASSWORD=your_password_here
```

2. **修改端口**（如有冲突）：
```env
# 默认端口
API_PORT=8000
N8N_PORT=5678
GRAFANA_PORT=3000

# 如果端口被占用，改为其他端口
# API_PORT=8001
# N8N_PORT=5679
# GRAFANA_PORT=3001
```

### 步骤 3：启动服务

**macOS/Linux:**
```bash
./deploy.sh deploy
```

**Windows:**
```cmd
deploy.bat deploy
```

等待 5-10 分钟，直到所有服务启动完成。

## 🎉 开始使用

访问以下地址开始使用：

| 服务 | 地址 | 用户名 | 密码 |
|------|------|--------|------|
| 主页 | http://localhost | - | - |
| N8N 工作流 | http://localhost:5678 | admin | 查看 .env |
| API 文档 | http://localhost:8000/docs | - | - |
| Grafana | http://localhost:3000 | admin | 查看 .env |

## 常用命令

```bash
# 查看服务状态
./deploy.sh status

# 查看日志
./deploy.sh logs

# 停止服务
./deploy.sh stop

# 重启服务
./deploy.sh restart
```

## 需要帮助？

- 📖 [完整部署文档](docs/PRODUCTION_DEPLOYMENT.md)
- 🐛 [报告问题](https://github.com/yunqiqiliang/data-diff-n8n/issues)
- 💬 [讨论区](https://github.com/yunqiqiliang/data-diff-n8n/discussions)