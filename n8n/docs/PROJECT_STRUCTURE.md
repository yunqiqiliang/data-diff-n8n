# Data-Diff N8N 项目结构

## 核心目录

- **.git/**: 项目相关文件
- **.github/**: 项目相关文件
- **.pytest_cache/**: 项目相关文件
- **__pycache__/**: 项目相关文件
- **data_diff/**: 数据比对核心库
- **dev/**: 开发工具和配置
- **docs/**: 项目文档
- **exports/**: 导出数据和报告
- **monitoring/**: 监控和日志配置
- **n8n/**: N8N 节点和 API 核心代码
- **nginx/**: Nginx 反向代理配置
- **notebooks/**: Jupyter 笔记本和分析
- **scripts/**: 部署和维护脚本
- **tests/**: 单元测试和集成测试

## 核心配置文件

- **pyproject.toml**: Python 项目配置和依赖
- **docker-compose.dev.yml**: Docker 开发环境配置
- **Dockerfile.api**: API 服务 Docker 镜像
- **README.md**: 项目说明文档
- **.env.dev**: 开发环境变量
- **start.sh**: 项目启动脚本

## 清理说明

清理时间: 2025-07-11 11:48:49,456
- 调试和测试文件已移动到 `cleanup_archive/` 目录
- 过时的文档和临时文件已归档
- 核心功能代码和配置文件已保留
- 如需恢复文件，请查看 `cleanup_archive/` 目录
