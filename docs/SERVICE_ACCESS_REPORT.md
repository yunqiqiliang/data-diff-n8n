# 服务访问测试报告

## 测试时间
2025-07-13

## 测试结果总览

### ✅ 可正常访问的服务

1. **主页导航**: http://localhost/
   - 状态：正常
   - 提供所有服务的快速链接

2. **N8N 工作流**: http://localhost/n8n/
   - 状态：正常
   - 登录：admin/admin123

3. **API 文档**: http://localhost/api/docs
   - 状态：正常
   - Swagger UI 界面

4. **Prometheus**: http://localhost/prometheus/
   - 状态：正常
   - 自动重定向到 /prometheus/graph

5. **AlertManager**: http://localhost/alertmanager/
   - 状态：正常

### ⚠️ 需要直接访问的服务

1. **Grafana 监控**
   - 推荐访问：http://localhost:3000
   - 登录：admin/admin123
   - 说明：子路径配置存在问题，建议直接通过端口访问

## 访问方式总结

### 方式一：通过 Nginx 统一入口
适用于大部分服务：
- 主页：http://localhost/
- N8N：http://localhost/n8n/
- API：http://localhost/api/docs
- Prometheus：http://localhost/prometheus/
- AlertManager：http://localhost/alertmanager/

### 方式二：直接端口访问
适用于特定服务：
- Grafana：http://localhost:3000
- 也可用于所有服务的备用访问

## 已知问题

1. **Grafana 子路径访问**
   - 问题：通过 /grafana/ 访问时出现重定向循环
   - 解决方案：使用端口 3000 直接访问
   - 原因：Grafana 的子路径配置较为复杂，与反向代理配合时容易出现问题

2. **Prometheus 端口访问**
   - 通过端口 9091 访问时返回 404
   - 这是因为配置了外部 URL，建议通过 Nginx 访问

## 建议

1. 对于日常使用，推荐通过主页 (http://localhost/) 访问各服务
2. Grafana 建议收藏 http://localhost:3000 直接访问
3. 其他服务都可以通过 Nginx 统一入口正常访问

## 服务健康状态

所有核心服务运行正常：
- ✅ Data-Diff API
- ✅ N8N 工作流引擎
- ✅ PostgreSQL 数据库
- ✅ Redis 缓存
- ✅ Prometheus 监控
- ✅ Grafana 仪表板
- ✅ AlertManager 告警
- ✅ Nginx 反向代理