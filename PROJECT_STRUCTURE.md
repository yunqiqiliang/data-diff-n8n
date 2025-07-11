# 项目结构文档

## 概述
Data-Diff N8N 是一个基于 N8N 工作流引擎的跨数据库数据比对平台，支持 ClickZetta、PostgreSQL、MySQL 等多种数据库类型。

## 根目录结构

```
data-diff-n8n/
├── 📁 核心代码模块
│   ├── data_diff/                    # 核心数据比对模块
│   ├── n8n/                          # N8N 节点和 API 实现
│   │   ├── src/                      # N8N 自定义节点源码
│   │   │   ├── nodes/                # 自定义节点实现
│   │   │   │   ├── ClickzettaConnector/
│   │   │   │   └── DatabaseConnector/
│   │   │   └── credentials/          # 节点凭证定义
│   │   └── api/                      # 后端 API 服务
│   │       └── main.py               # FastAPI 主应用
│   └── scripts/                      # 工具脚本
│
├── 📁 开发和测试
│   ├── dev/                          # 开发环境配置
│   ├── tests/                        # 正式测试套件
│   ├── debug_scripts/                # 调试脚本集合
│   ├── test_scripts/                 # 测试脚本集合
│   └── notebooks/                    # Jupyter 笔记本
│
├── 📁 部署和运维
│   ├── monitoring/                   # 监控配置
│   ├── nginx/                        # Nginx 配置
│   ├── exports/                      # 导出文件目录
│   └── cleanup_archive/              # 历史清理归档
│
├── 📁 文档和配置
│   ├── docs/                         # 项目文档
│   ├── docs_archive/                 # 历史文档归档
│   ├── legacy_files/                 # 遗留文件归档
│   └── ROOT_CLEANUP_REPORT.md        # 根目录清理报告
│
├── 📄 核心配置文件
│   ├── docker-compose.dev.yml        # Docker 开发环境
│   ├── Dockerfile.api                # API 服务镜像
│   ├── pyproject.toml                # Python 项目配置
│   ├── poetry.lock                   # 依赖锁定文件
│   ├── ruff.toml                     # 代码格式化配置
│   ├── readthedocs.yml               # 文档构建配置
│   └── data-diff-n8n.code-workspace  # VS Code 工作区
│
├── 📄 项目文档
│   ├── README.md                     # 项目说明
│   ├── CODE_OF_CONDUCT.md            # 行为准则
│   ├── CONTRIBUTING.md               # 贡献指南
│   └── LICENSE                       # 许可证
│
└── 📄 运行脚本
    ├── start.sh                      # 项目启动脚本
    └── health-check.sh               # 健康检查脚本
```

## 核心模块说明

### N8N 节点实现
- **ClickzettaConnector**: 专门用于 ClickZetta 数据库的连接器节点
- **DatabaseConnector**: 通用数据库连接器节点，支持多种数据库类型

### API 服务
- **连接测试**: `/api/v1/connections/test`
- **查询执行**: `/api/v1/query/execute`
- **表列表**: `/api/v1/tables/list`
- **数据比对**: `/api/v1/comparison/create`

### 支持的数据库类型
- ClickZetta
- PostgreSQL
- MySQL
- ClickHouse
- Snowflake
- BigQuery
- Redshift
- Oracle
- SQL Server
- DuckDB
- Databricks
- Trino/Presto
- Vertica

## 开发环境

### 启动开发环境
```bash
# 启动所有服务
docker-compose -f docker-compose.dev.yml up -d

# 或使用启动脚本
./start.sh
```

### 服务端口
- N8N 工作流引擎: http://localhost:5678
- API 服务: http://localhost:8000
- PostgreSQL: localhost:5432
- Grafana 监控: http://localhost:3000
- Prometheus: http://localhost:9091
- Jupyter Lab: http://localhost:8889

## 清理归档

### 已归档的文件类型
1. **debug_scripts/** (21 个文件)
   - 调试脚本和开发辅助工具
   - 连接诊断脚本
   - 参数解析调试工具

2. **test_scripts/** (60 个文件)
   - 功能测试脚本
   - API 测试脚本
   - 数据库连接测试

3. **docs_archive/** (10 个文件)
   - 历史完成报告
   - 功能说明文档
   - 重构总结报告

4. **legacy_files/** (5 个文件)
   - 遗留的启动脚本
   - 旧版清理工具
   - 历史配置文件

## 最佳实践

### 开发流程
1. 功能开发在 `data_diff/` 和 `n8n/src/` 目录下进行
2. 测试脚本放在 `test_scripts/` 目录
3. 调试脚本放在 `debug_scripts/` 目录
4. 文档更新在 `docs/` 目录

### 代码规范
- 使用 `ruff` 进行代码格式化
- 遵循 Python PEP 8 规范
- TypeScript 使用 ESLint 配置

### 部署
- 使用 Docker Compose 进行多服务部署
- 监控使用 Prometheus + Grafana
- 反向代理使用 Nginx

## 维护说明

### 定期清理
- 定期将调试脚本移动到 `debug_scripts/`
- 测试脚本移动到 `test_scripts/`
- 完成的文档归档到 `docs_archive/`

### 版本管理
- 使用 Git 进行版本控制
- 重要节点打标签
- 维护 CHANGELOG.md

### 监控和日志
- 使用 Prometheus 收集指标
- Grafana 可视化监控数据
- 日志统一输出到标准输出

---

*最后更新时间: 2025年7月11日*
*清理统计: 移动了 96 个文件，保持根目录整洁*
