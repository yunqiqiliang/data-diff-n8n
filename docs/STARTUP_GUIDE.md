# 🚀 Data-Diff N8N 启动指南

## 🎯 统一启动脚本

现在只需要一个脚本 `start.sh` 就可以处理所有启动场景！

### 基本用法

```bash
# 快速启动（推荐日常使用）
./start.sh

# 显示帮助信息
./start.sh --help
```

### 启动选项

#### 1. ⚡ 快速启动（默认）
```bash
./start.sh
# 或
./start.sh --quick
```
- **用途**: 日常开发使用
- **特点**: 不重新构建镜像，直接启动现有容器
- **时间**: ~15-30 秒
- **适用**: 代码未修改，只需要启动服务

#### 2. 🔨 重新构建启动
```bash
./start.sh --rebuild
```
- **用途**: 代码有更新时使用
- **特点**: 重新构建镜像后启动
- **时间**: ~3-5 分钟
- **适用**:
  - Python 代码修改
  - N8N 节点代码修改
  - 依赖包更新

#### 3. 🚀 强制重新构建
```bash
./start.sh --force-rebuild
```
- **用途**: 完全重新构建
- **特点**: 清理缓存，完全重新构建
- **时间**: ~5-10 分钟
- **适用**:
  - Docker 镜像问题
  - 依赖冲突
  - 初次部署

#### 4. 🔍 详细输出模式
```bash
./start.sh --verbose
./start.sh --rebuild --verbose
```
- **用途**: 调试和排错
- **特点**: 显示详细的构建和启动信息

## 🤖 自动化功能

### 智能构建检测
- ✅ **自动检测 N8N 节点变化**: 比较 TypeScript 源码和构建产物的时间戳
- ✅ **自动安装依赖**: 检测 `node_modules` 是否存在，自动运行 `npm install`
- ✅ **智能重新构建**: 根据源码变化自动决定是否需要重新构建

### 自动环境准备
- ✅ **SSL 证书自动生成**: 完整模式下自动生成自签名证书
- ✅ **目录自动创建**: 自动创建必要的目录结构
- ✅ **依赖检查**: 自动检查 Docker 和其他必要组件

### 构建优化
- ✅ **增量构建**: 只在必要时重新构建 N8N 节点
- ✅ **缓存利用**: 充分利用 npm 和 Docker 构建缓存
- ✅ **并行处理**: 同时进行多个准备工作

## 使用建议

### 📅 日常开发流程：
1. **早上开始工作**: `./start.sh`
2. **代码修改后**: `./start.sh --rebuild`
3. **遇到问题时**: `./start.sh --force-rebuild`

### 🔧 什么时候需要重新构建？

**自动触发重新构建的情况：**
- ✅ `n8n/src/` 目录下的 TypeScript 文件有更新
- ✅ `n8n/package.json` 或 `n8n/package-lock.json` 有变化
- ✅ 使用 `--rebuild` 或 `--force-rebuild` 选项
- ✅ `n8n/dist` 目录不存在

**需要重新构建 Docker 镜像的情况：**
- ✅ 修改了 Python 代码（`data_diff/`, `n8n/` 目录）
- ✅ 修改了 `requirements.txt` 或 `pyproject.toml`
- ✅ 修改了 `Dockerfile.api`
- ✅ 添加了新的 Python 依赖

**不需要重新构建的情况：**
- ❌ 只修改了配置文件（`nginx.conf`, `prometheus.yml` 等）
- ❌ 只修改了 `docker-compose.dev.yml`
- ❌ 只查看日志或监控
- ❌ 重启服务

### 💡 智能构建特性

脚本现在具有以下智能功能：

1. **自动检测源码变化**: 比较 TypeScript 源码和构建产物的修改时间
2. **按需构建 N8N 节点**: 只在必要时重新编译 TypeScript
3. **自动安装依赖**: 检测并安装缺失的 npm 包
4. **自动生成证书**: 完整模式下自动创建 SSL 证书
5. **增量构建**: 充分利用缓存，减少不必要的构建时间

### 📊 性能对比

| 启动方式 | 首次启动 | 后续启动 | 适用场景 |
|---------|---------|---------|---------|
| `./start.sh` | - | 15-30秒 | 日常使用 |
| `./start.sh --rebuild` | 3-5分钟 | 3-5分钟 | 代码更新 |
| `./start.sh --force-rebuild` | 5-10分钟 | 5-10分钟 | 问题修复 |

### 🔍 健康检查
```bash
./health-check.sh
```
- 检查所有服务是否正常运行
- 验证 API 端点是否可访问
- 建议在启动后运行

### 🛑 停止服务
```bash
docker-compose -f docker-compose.dev.yml down
```

### 📋 查看日志
```bash
# 查看所有服务日志
docker-compose -f docker-compose.dev.yml logs -f

# 查看特定服务日志
docker-compose -f docker-compose.dev.yml logs -f data-diff-api
docker-compose -f docker-compose.dev.yml logs -f n8n
```

## 故障排除

### 常见问题：
1. **端口被占用**: 检查其他服务是否在使用相同端口
2. **镜像构建失败**: 使用 `--force-rebuild` 选项
3. **服务启动失败**: 查看服务日志排查问题
4. **N8N 节点未加载**: 确保 `n8n/dist` 目录存在且有内容

### 调试技巧：
1. 使用 `./health-check.sh` 快速检查服务状态
2. 使用 `docker-compose ps` 查看容器状态
3. 使用 `docker-compose logs -f [service]` 查看特定服务日志
4. 使用 `docker system df` 检查磁盘空间使用情况
