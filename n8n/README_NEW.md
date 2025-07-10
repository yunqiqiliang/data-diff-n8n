# N8N 自定义节点开发指南

## 概述

本项目已成功实现 N8N 自定义节点的开发环境配置，包含两个核心节点：
- **ClickzettaConnector**: Clickzetta 数据库连接器
- **DataComparison**: 数据比对执行器

## 🚀 快速开始

### 1. 一键启动开发环境

```bash
./start-dev.sh
```

### 2. 访问 N8N 界面

打开浏览器访问：`http://localhost/n8n/`

**登录信息**：
- 用户名：`admin`
- 密码：`admin123`

### 3. 查找自定义节点

在 N8N 界面左侧节点面板中，可以找到：
- **Clickzetta Connector** (位于 Transform 分组)
- **Data Comparison** (位于 Transform 分组)

## 📦 项目结构

```
n8n/
├── src/                              # TypeScript 源码
│   ├── nodes/                        # 自定义节点
│   │   ├── ClickzettaConnector/      # Clickzetta 连接器
│   │   └── DataComparison/           # 数据比对器
│   ├── credentials/                  # 认证配置
│   │   ├── ClickzettaApi.credentials.ts
│   │   └── DataDiffConfig.credentials.ts
│   └── index.ts                      # 导出入口
├── dist/                             # 编译输出 (自动生成)
├── package.json                      # 节点包配置
├── tsconfig.json                     # TypeScript 配置
└── README.md                         # 本文档
```

## 🛠️ 开发工作流

### 1. 修改节点代码

编辑 `n8n/src/nodes/` 下的 TypeScript 文件

### 2. 构建节点

```bash
cd n8n
npm install
npm run build
```

### 3. 重启容器

```bash
docker-compose -f docker-compose.dev.yml restart n8n
```

### 4. 验证节点

运行测试脚本：
```bash
./test-nodes.sh
```

## 🔧 节点配置详情

### ClickzettaConnector 节点

**功能**：
- 测试 Clickzetta 数据库连接
- 获取数据库 schema 信息
- 列出数据库表

**输入参数**：
- 操作类型 (测试连接/获取Schema/列表表)
- Schema 名称 (可选)

**输出**：
- 连接状态和元数据信息

### DataComparison 节点

**功能**：
- 表对表数据比对
- Schema 结构比对

**输入参数**：
- 源数据库连接字符串
- 目标数据库连接字符串
- 源表名 / 目标表名
- 主键列 (用于比对)

**输出**：
- 比对结果统计
- 差异详情

## 🔑 认证配置

### ClickzettaApi 凭据

包含以下字段：
- Host (主机地址)
- Port (端口，默认 8443)
- Database (数据库名)
- Username / Password
- Schema (默认 public)
- SSL (默认启用)

### DataDiffConfig 凭据

包含数据比对配置：
- 比对方法 (HashDiff/JoinDiff)
- 采样配置
- 性能优化设置

## 🐳 Docker 部署

### 自动化部署流程

项目使用统一的 `start.sh` 脚本来自动化构建和部署：

```bash
# 启动完整的开发环境
./start.sh

# 强制重新构建
./start.sh --rebuild

# 查看详细日志
./start.sh --verbose
```

### 部署架构

- **N8N 服务**：使用官方镜像 `n8nio/n8n:latest`
- **自定义节点**：通过 volume 挂载方式加载（`./n8n/dist:/home/node/.n8n/custom-extensions:ro`）
- **API 服务**：使用 `Dockerfile.api` 构建
- **数据库服务**：使用官方镜像（PostgreSQL, MySQL, ClickHouse）

### 数据持久化

- N8N 数据：`n8n_data` 卷
- PostgreSQL 数据：`postgres_data` 卷
- 工作流和连接不会因容器重启丢失

## 🔍 调试与测试

### 查看容器日志

```bash
# 查看 N8N 日志
docker-compose -f docker-compose.dev.yml logs -f n8n

# 查看所有服务日志
docker-compose -f docker-compose.dev.yml logs -f
```

### 进入容器调试

```bash
# 进入 N8N 容器
docker-compose -f docker-compose.dev.yml exec n8n sh

# 检查自定义节点安装
ls -la /home/node/.n8n/custom-package/dist/
```

### 验证节点注册

```bash
# 在 N8N 容器内执行
n8n list:packages
```

## 🚧 已知问题与解决方案

### 1. 节点不显示

**问题**：自定义节点在 N8N 界面中不显示

**解决方案**：
1. 检查 TypeScript 编译是否成功：`npm run build`
2. 验证 package.json 中的 n8n 配置
3. 重启 N8N 容器
4. 检查容器日志中的错误信息

### 2. 编译错误

**问题**：TypeScript 编译失败

**解决方案**：
1. 检查 tsconfig.json 配置
2. 确保所有依赖已安装：`npm install`
3. 修复代码中的类型错误

### 3. 容器启动失败

**问题**：N8N 容器无法启动

**解决方案**：
1. 检查 start.sh 脚本的构建过程
2. 确保 package.json 格式正确
3. 检查磁盘空间和 Docker 资源
4. 运行 health-check.sh 验证服务状态

## 📝 开发最佳实践

### 1. 代码结构

- 每个节点一个文件夹
- 遵循 N8N 节点开发规范
- 使用 TypeScript 严格模式

### 2. 错误处理

- 使用 try-catch 包装异步操作
- 提供友好的错误消息
- 记录调试信息

### 3. 测试

- 使用 Mock 数据进行单元测试
- 集成测试验证完整流程
- 性能测试确保大数据量处理能力

## 🎯 下一步计划

### 短期目标

1. ✅ 完成基础节点开发和构建
2. ✅ 实现 Docker 容器集成
3. ✅ 建立开发工作流
4. 🔄 实现真实的 data-diff 集成
5. 🔄 添加更多配置选项和错误处理

### 长期目标

1. 📋 实现完整的工作流模板
2. 📋 添加结果可视化功能
3. 📋 集成监控和告警
4. 📋 发布到 N8N 社区

## 🤝 贡献指南

### 代码提交

1. Fork 项目
2. 创建功能分支
3. 提交更改
4. 创建 Pull Request

### 报告问题

使用 GitHub Issues 报告 Bug 和功能请求

## 📞 技术支持

如有问题，请查看：
1. 本文档的故障排除部分
2. N8N 官方文档
3. 项目 GitHub Issues

---

**🎉 恭喜！** 你已经成功搭建了完整的 N8N 自定义节点开发环境，可以开始创建强大的数据比对工作流了！
