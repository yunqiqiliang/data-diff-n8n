# 🚀 一键启动脚本：start.sh

本项目推荐使用 `start.sh` 脚本进行开发环境的启动和管理。它集成了常用的构建、重启、依赖检查、SSL 证书生成等自动化流程，极大简化了本地开发和测试。

### 常用用法

```bash
# 快速启动（推荐日常开发）
./start.sh

# 重新构建（代码有变动时）
./start.sh --rebuild

# 强制重建所有镜像（依赖或环境大变动时）
./start.sh --force-rebuild

# 查看详细帮助
./start.sh --help
```

### 主要功能

- 自动检测并构建 N8N TypeScript 节点
- 自动安装 npm 依赖
- 自动生成 SSL 证书（非快速模式）
- 智能判断是否需要重新构建
- 支持 Docker 镜像的快速启动、重建、强制重建
- 启动后自动输出所有服务访问地址和默认账号

### 启动参数说明

- `--quick` / `-q`：快速启动（默认）
- `--rebuild` / `-r`：重新构建后启动
- `--force-rebuild` / `-f`：强制重建所有镜像
- `--skip-prune`：跳过系统清理
- `--verbose` / `-v`：详细输出
- `--help` / `-h`：显示帮助信息

### 启动后访问

  - Data-Diff API: http://localhost:8000
  - N8N 工作流: http://localhost:5678
  - Grafana 监控: http://localhost:3000
  - Prometheus: http://localhost:9091
  - Jupyter Lab: http://localhost:8889
  - Mailhog: http://localhost:8025
  - Nginx (HTTP): http://localhost:80
  - Nginx (HTTPS): https://localhost:443

---


## 🔗 项目结构供跨数据库的数据比较和工作流自动化功能。

## 🚀 快速开始

### 环境要求
- Docker & Docker Compose
- Node.js 18+ (用于构建 N8N 节点)
- Python 3.11+ (可选，用于本地开发)

### 一键启动
```bash
# 克隆项目
git clone https://github.com/yunqiqiliang/data-diff-n8n.git
cd data-diff-n8n

# 构建 N8N 节点
cd n8n && rm -rf dist && npm run build && cd ..


# 启动所有服务
docker-compose -f docker-compose.dev.yml up -d
```


## 🌟 功能特性

### 核心功能
- **多数据库支持**: 支持 PostgreSQL, ClickZetta, MySQL, SQLite, Oracle, MS SQL
- **数据比较**: 表数据比较、模式比较、实时差异检测
- **工作流自动化**: 基于 N8N 的可视化工作流
- **参数自动填充**: 智能从上游节点获取连接信息和表列表
- **表达式引用**: 支持 N8N 表达式语法引用上游数据

### 技术特性
- **异步处理**: 后台任务处理大数据量比较
- **错误处理**: 完善的错误信息和调试支持
- **RESTful API**: 完整的 API 接口
- **健康监控**: 集成 Grafana 和 Prometheus
- **容器化部署**: 完整的 Docker 部署方案


## � 项目结构

```
data-diff-n8n/
├── n8n/                      # N8N 节点和 API 实现
│   ├── src/nodes/           # 自定义节点 (ClickzettaConnector, DatabaseConnector)
│   ├── src/credentials/     # 节点凭证定义
│   └── api/                 # FastAPI 后端服务
├── data_diff/               # 核心数据比对模块
├── debug_scripts/           # 调试脚本集合
├── test_scripts/            # 测试脚本集合
├── docs_archive/            # 历史文档归档
├── legacy_files/            # 遗留文件归档
├── monitoring/              # Prometheus + Grafana 监控
├── scripts/                 # 工具脚本
└── tests/                   # 正式测试套件
```

详细结构说明请查看 [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

## �📖 文档

- [启动指南](docs/STARTUP_GUIDE.md) - 详细的启动和使用说明
- [部署检查清单](docs/DEPLOYMENT_CHECKLIST.md) - 部署前准备工作
- [自动化功能说明](docs/AUTOMATION_FEATURES.md) - 启动脚本的自动化功能
- [数据库注册表重构总结](docs/DATABASE_REGISTRY_REFACTOR_SUMMARY.md) - 架构说明
- [本地开发指南](docs/LOCAL_DEV.md) - 本地开发环境设置
- [Clickzetta 修复总结](docs/CLICKZETTA_FIX_SUMMARY.md) - Clickzetta 相关修复
- [Clickzetta 测试指南](docs/CLICKZETTA_TESTING_GUIDE.md) - Clickzetta 测试说明

## ⚠️ 关于原项目

As of May 17, 2024, Datafold is no longer actively supporting or developing open source data-diff. We're grateful to everyone who made contributions along the way. Please see [our blog post](https://www.datafold.com/blog/sunsetting-open-source-data-diff) for additional context on this decision.

---️ As of May 17, 2024, Datafold is no longer actively supporting or developing open source data-diff. We’re grateful to everyone who made contributions along the way. Please see [our blog post](https://www.datafold.com/blog/sunsetting-open-source_data-diff) for additional context on this decision.

---

# data-diff-n8n: Compare datasets fast, within or across SQL databases

## Contributors

<a href="https://github.com/yunqiqiliang/data-diff-n8n/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=yunqiqiliang/data-diff-n8n" />
</a>

## License

This project is licensed under the terms of the [MIT License](https://github.com/yunqiqiliang/data-diff-n8n/blob/master/LICENSE).
