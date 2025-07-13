# Nginx 集成说明

## 概述

Data-Diff N8N 项目集成了 Nginx 作为统一的反向代理服务器，提供所有服务的统一访问入口。

## 架构优势

1. **统一入口**: 所有服务通过同一个域名/端口访问
2. **路径管理**: 通过 URL 路径区分不同服务
3. **安全增强**: 可配置 HTTPS、访问控制、限流等
4. **负载均衡**: 未来可扩展支持多实例负载均衡
5. **便于部署**: 生产环境只需暴露 80/443 端口

## 服务路由配置

| 服务 | 直接访问地址 | Nginx 路径 | 说明 |
|------|-------------|-----------|------|
| Data-Diff API | http://localhost:8000 | http://localhost/api/ | REST API 服务 |
| N8N 工作流 | http://localhost:5678 | http://localhost/n8n/ | 工作流编排界面 |
| Grafana 监控 | http://localhost:3000 | http://localhost/grafana/ | 监控仪表板 |
| Prometheus | http://localhost:9091 | http://localhost/prometheus/ | 指标查询界面 |
| AlertManager | http://localhost:9093 | http://localhost/alertmanager/ | 告警管理界面 |
| Jupyter Lab | http://localhost:8889 | http://localhost/jupyter/ | 数据分析环境 |

## 使用方式

### 1. 统一访问（推荐）

通过 Nginx 访问所有服务：

```bash
# 启动所有服务
./start.sh

# 访问服务
浏览器打开 http://localhost
```

### 2. 服务访问示例

- **监控面板**: http://localhost/grafana/
- **API 文档**: http://localhost/api/docs
- **工作流设计**: http://localhost/n8n/

### 3. HTTPS 配置（可选）

如需启用 HTTPS：

1. 准备证书文件放入 `nginx/certs/` 目录
2. 更新 `nginx/nginx.conf` 添加 HTTPS 服务器块
3. 重启 Nginx 服务

## 技术细节

### WebSocket 支持

N8N 和 Jupyter 需要 WebSocket 支持，已配置：

```nginx
proxy_http_version 1.1;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "upgrade";
```

### 子路径配置

各服务已配置支持子路径访问：

- **Grafana**: 通过环境变量 `GF_SERVER_ROOT_URL` 和 `GF_SERVER_SERVE_FROM_SUB_PATH`
- **Prometheus**: 通过命令行参数 `--web.external-url`
- **AlertManager**: 通过命令行参数 `--web.external-url`

### 请求头传递

所有代理配置都传递必要的请求头：

```nginx
proxy_set_header Host $host;
proxy_set_header X-Real-IP $remote_addr;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;
```

## 故障排查

### 1. 服务无法通过 Nginx 访问

- 检查服务是否正常运行：`docker-compose ps`
- 查看 Nginx 日志：`docker-compose logs nginx`
- 确认网络连接：服务都在同一个 Docker 网络中

### 2. 页面样式或功能异常

- 检查浏览器控制台是否有资源加载错误
- 确认服务的子路径配置正确
- 清除浏览器缓存重试

### 3. WebSocket 连接失败

- 确认 Nginx 配置包含 WebSocket 支持
- 检查防火墙或代理设置

## 扩展配置

### 添加新服务

在 `nginx/nginx.conf` 中添加新的 location 块：

```nginx
location /newservice/ {
    proxy_pass http://service-name:port/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

### 配置访问控制

可以添加基本认证或 IP 限制：

```nginx
location /admin/ {
    auth_basic "Admin Area";
    auth_basic_user_file /etc/nginx/.htpasswd;
    allow 192.168.1.0/24;
    deny all;
    proxy_pass http://admin-service:8080/;
}
```

## 最佳实践

1. **生产环境**: 始终使用 HTTPS，配置适当的安全头
2. **日志管理**: 配置访问日志和错误日志轮转
3. **性能优化**: 启用 gzip 压缩，配置缓存策略
4. **监控集成**: 可以添加 Nginx 指标到 Prometheus