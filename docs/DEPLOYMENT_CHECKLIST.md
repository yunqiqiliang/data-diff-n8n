# Data-Diff N8N 部署前检查清单

## 📋 必要的准备工作

### ✅ 已完成的工作：
- [x] 构建 N8N TypeScript 节点 (`npm run build`)
- [x] 修复 Prometheus 配置文件路径
- [x] 修复 docker-compose.dev.yml 挂载路径
- [x] 生成 SSL 证书用于 Nginx HTTPS
- [x] 创建启动脚本 (`start-dev-env.sh`)
- [x] 创建环境变量文件 (`.env.dev`)
- [x] 创建健康检查脚本 (`health-check.sh`)
- [x] 验证 API 健康检查端点

### 🔍 部署前最终检查：

1. **Docker 环境检查**
   ```bash
   docker --version
   docker-compose --version
   ```

2. **端口占用检查**
   ```bash
   lsof -i :80,443,5432,5678,6379,8000,3000,9091,8889,8025
   ```

3. **文件权限检查**
   ```bash
   ls -la start-dev-env.sh health-check.sh
   ```

4. **N8N 节点构建验证**
   ```bash
   ls -la n8n/dist/
   ```

5. **SSL 证书验证**
   ```bash
   ls -la nginx/certs/
   ```

## 🚀 部署步骤

### 1. 首次部署（完整构建）
```bash
./start.sh --force-rebuild
```

### 2. 日常启动（快速启动）
```bash
./start.sh
```

### 3. 代码更新后启动
```bash
./start.sh --rebuild
```

### 4. 验证服务状态
```bash
./health-check.sh
```

### 3. 访问服务
- **Data-Diff API**: http://localhost:8000
- **N8N 工作流**: http://localhost:5678 (admin/admin123)
- **Grafana 监控**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9091
- **Jupyter Lab**: http://localhost:8889 (token=datadiff123)
- **Mailhog**: http://localhost:8025

### 4. 测试核心功能
1. 访问 N8N 并登录
2. 检查自定义节点是否可用
3. 测试 Clickzetta 连接
4. 验证数据比较功能
5. 查看监控面板

## 🔧 故障排除

### 常见问题：
1. **端口冲突**: 检查端口占用，修改 docker-compose.dev.yml 中的端口映射
2. **SSL 证书问题**: 重新生成证书或使用 HTTP 访问
3. **N8N 节点未加载**: 检查 n8n/dist 目录和构建状态
4. **数据库连接失败**: 检查 PostgreSQL 容器状态和环境变量

### 查看日志：
```bash
# 查看所有服务日志
docker-compose -f docker-compose.dev.yml logs -f

# 查看特定服务日志
docker-compose -f docker-compose.dev.yml logs -f data-diff-api
docker-compose -f docker-compose.dev.yml logs -f n8n
```

### 重启服务：
```bash
# 重启所有服务
docker-compose -f docker-compose.dev.yml restart

# 重启特定服务
docker-compose -f docker-compose.dev.yml restart data-diff-api
```

## 📊 监控和维护

- 使用 Grafana 监控系统性能
- 使用 Prometheus 收集指标
- 定期检查日志文件
- 监控数据库性能

## 🔒 安全注意事项

- 生产环境需要使用强密码
- 配置适当的防火墙规则
- 定期更新 SSL 证书
- 审查和更新依赖项
