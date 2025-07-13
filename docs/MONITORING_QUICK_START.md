# 监控系统快速启动指南

## 概述

Data-Diff N8N 集成了完整的监控解决方案，基于 Prometheus + Grafana 构建，可以实时监控系统性能和业务指标。

## 快速启动

### 1. 启动服务

使用项目提供的统一启动脚本，一键启动所有服务（包括监控）：

```bash
# 在项目根目录执行
./start.sh

# 脚本会自动完成：
# ✓ 环境检查
# ✓ 镜像构建
# ✓ 服务启动（包括监控组件）
# ✓ 健康检查
# ✓ 显示所有服务的访问地址
```

### 2. 访问监控界面

推荐通过 Nginx 统一入口访问：

- **Grafana 仪表板**: http://localhost/grafana/
  - 用户名: `admin`
  - 密码: `admin123`
  
- **Prometheus 查询界面**: http://localhost/prometheus/

- **AlertManager 告警管理**: http://localhost/alertmanager/

> 提示：也可以直接访问服务端口（Grafana: 3000, Prometheus: 9091, AlertManager: 9093）

### 3. 查看监控数据

1. 登录 Grafana
2. 导航到 "Dashboards" → "Data Diff Monitoring Dashboard"
3. 查看各项指标：
   - API 性能指标
   - 比对任务统计
   - 资源使用情况
   - 业务指标趋势

## 关键指标说明

### 性能指标
- **API 响应时间**: 各接口的 P50、P90、P95 响应时间
- **请求速率**: 每秒处理的请求数 (QPS)
- **错误率**: 4xx 和 5xx 错误的比例

### 业务指标
- **比对任务数**: 成功、失败、运行中的任务统计
- **差异率**: 数据差异的平均比例
- **功能使用情况**: 各功能的使用频率统计

### 资源指标
- **内存使用**: API 服务的内存占用
- **CPU 使用率**: 处理器使用情况
- **数据库连接**: 活跃连接数

## 常用查询

在 Prometheus 中使用这些查询来获取特定指标：

```promql
# API 响应时间 P95
histogram_quantile(0.95, sum(rate(datadiff_api_request_duration_seconds_bucket[5m])) by (le))

# 比对失败率
sum(rate(datadiff_comparison_total{status="failed"}[5m])) / sum(rate(datadiff_comparison_total[5m]))

# 平均差异率
avg(datadiff_difference_rate)

# 内存使用（MB）
datadiff_memory_usage_bytes / (1024 * 1024)
```

## 告警配置

系统预配置了以下告警：

- **高 API 响应时间**: P95 > 5秒
- **高内存使用**: > 8GB
- **高 CPU 使用**: > 80%
- **高差异率**: > 10%
- **高失败率**: > 10%

## 故障排查

### 监控数据不显示

1. 检查 API 服务是否正常运行
2. 访问 http://localhost:8000/metrics 确认指标暴露正常
3. 在 Prometheus 中检查 target 状态

### Grafana 无法连接 Prometheus

1. 检查 Prometheus 服务状态
2. 确认网络连接正常
3. 检查数据源配置

## 更多信息

- 详细的监控设计方案：[监控设计文档](monitoring-design.md)
- 完整的部署和配置指南：[监控部署指南](../monitoring/README.md)