# data-diff-n8n: Compare datasets fast, within or across SQL databases

一个基于 data-diff 的 N8N 集成解决方案，提供跨数据库的数据比较和工作流自动化功能。

## 🚀 快速开始

### 环境要求
- Docker & Docker Compose
- Node.js 18+ (用于构建 N8N 节点)
- Python 3.11+ (可选，用于本地开发)

### 重新构建 N8N 节点包
```bash
cd n8n && npm run build
```
### 一键启动
```bash
# 克隆项目
git clone https://github.com/yunqiqiliang/data-diff-n8n.git
cd data-diff-n8n

# 一键启动（自动处理所有构建和配置）
./start.sh --force-rebuild
```

> 🎉 **全自动化**: 脚本会自动构建 N8N 节点、生成 SSL 证书、安装依赖等！

### 启动选项
```bash
./start.sh                  # 快速启动（推荐日常使用）
./start.sh --rebuild        # 代码修改后重新构建
./start.sh --force-rebuild  # 完全重新构建
./start.sh --help           # 查看帮助
```

## 🌟 功能特性

- **多数据库支持**: 支持 14 种数据库类型（PostgreSQL, MySQL, Clickzetta, ClickHouse, Snowflake 等）
- **N8N 集成**: 提供自定义节点，轻松创建数据比较工作流
- **实时监控**: 集成 Grafana 和 Prometheus 监控
- **RESTful API**: 提供完整的 API 接口
- **可视化分析**: 内置 Jupyter Lab 用于数据分析

## 🔗 服务访问

启动成功后，您可以访问：

- **N8N 工作流**: http://localhost:5678 (admin/admin123)
- **Data-Diff API**: http://localhost:8000
- **Grafana 监控**: http://localhost:3000 (admin/admin123)
- **Jupyter Lab**: http://localhost:8889 (token=datadiff123)

## 📖 文档

- [启动指南](docs/STARTUP_GUIDE.md) - 详细的启动和使用说明
- [部署检查清单](docs/DEPLOYMENT_CHECKLIST.md) - 部署前准备工作
- [自动化功能说明](docs/AUTOMATION_FEATURES.md) - 启动脚本的自动化功能
- [数据库注册表重构总结](docs/DATABASE_REGISTRY_REFACTOR_SUMMARY.md) - 架构说明
- [本地开发指南](docs/LOCAL_DEV.md) - 本地开发环境设置
- [Clickzetta 修复总结](docs/CLICKZETTA_FIX_SUMMARY.md) - Clickzetta 相关修复
- [Clickzetta 测试指南](docs/CLICKZETTA_TESTING_GUIDE.md) - Clickzetta 测试说明

## ⚠️ 关于原项目

As of May 17, 2024, Datafold is no longer actively supporting or developing open source data-diff. We're grateful to everyone who made contributions along the way. Please see [our blog post](https://www.datafold.com/blog/sunsetting-open-source-data-diff) for additional context on this decision.

---️ As of May 17, 2024, Datafold is no longer actively supporting or developing open source data-diff. We’re grateful to everyone who made contributions along the way. Please see [our blog post](https://www.datafold.com/blog/sunsetting-open-source-data-diff) for additional context on this decision.

---

# data-diff-n8n: Compare datasets fast, within or across SQL databases

## Contributors

<a href="https://github.com/yunqiqiliang/data-diff-n8n/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=yunqiqiliang/data-diff-n8n" />
</a>

## License

This project is licensed under the terms of the [MIT License](https://github.com/yunqiqiliang/data-diff-n8n/blob/master/LICENSE).
