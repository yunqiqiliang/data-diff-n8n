# 本地开发指南

这个目录包含了用于本地开发和测试的脚本，可以避免每次都重新构建 Docker 镜像。

## 前提条件

确保您已经激活了 conda 的 `n8n` 环境：

```bash
conda activate n8n
```

## 安装依赖

在项目根目录下安装 Python 依赖：

```bash
# 使用 pip 安装项目依赖
pip install -e .

# 或者使用 poetry（如果您使用 poetry）
poetry install
```

## 本地测试脚本

### 1. 基础功能测试

快速测试所有核心模块是否能正常导入和工作：

```bash
python test_local.py
```

这个脚本会测试：
- 模块导入
- 连接管理器
- 工作流构建器
- 模板管理器
- 调度器

### 2. 启动本地 API 服务器

启动本地 FastAPI 服务器进行开发和测试：

```bash
python start_local_api.py
```

服务启动后，您可以访问：
- API 文档: http://localhost:8000/docs
- 交互式文档: http://localhost:8000/redoc
- 健康检查: http://localhost:8000/health

### 3. 开发助手工具

这个工具提供了多种开发和测试功能：

```bash
# 运行所有测试
python dev_helper.py --test

# 启动 API 服务器
python dev_helper.py --api

# 测试特定的连接字符串
python dev_helper.py --connection "postgresql://user:pass@localhost:5432/db"
```

## 本地开发工作流

1. **修改代码** - 在 `n8n/` 目录下修改您的代码
2. **快速测试** - 运行 `python test_local.py` 验证基础功能
3. **启动 API** - 运行 `python start_local_api.py` 测试 API 功能
4. **调试问题** - 使用 `python dev_helper.py --test` 进行详细测试
5. **确认无误** - 再提交到 Docker 环境进行最终验证

## 环境变量

本地测试时会使用以下默认环境变量：

- `DATABASE_URL`: `sqlite:///./test.db` (本地 SQLite 数据库)
- `REDIS_URL`: `redis://localhost:6379` (需要本地 Redis)
- `LOG_LEVEL`: `DEBUG`

您可以根据需要修改这些变量。

## 常见问题

### 1. 模块导入错误

确保您在项目根目录下运行脚本，并且已经安装了所有依赖。

### 2. Redis 连接错误

如果您没有本地 Redis，可以：
- 安装并启动 Redis: `brew install redis && brew services start redis`
- 或者修改环境变量使用 Docker 中的 Redis

### 3. 数据库连接错误

本地测试默认使用 SQLite，如果需要测试其他数据库，请修改 `DATABASE_URL` 环境变量。

## 提示

- 使用 `--reload` 参数启动 API 服务器时，代码修改会自动重启服务器
- 测试脚本会在项目根目录创建 `test.db` SQLite 文件
- 所有日志级别设置为 `DEBUG`，便于调试

## 下一步

当本地测试通过后，您可以：

1. 提交代码到 git
2. 重新构建 Docker 镜像: `docker-compose -f docker-compose.dev.yml up -d --build`
3. 在完整的 Docker 环境中进行最终测试
