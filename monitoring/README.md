# 数据比对系统监控部署指南

## 概述

本监控系统基于 Prometheus + Grafana 构建，提供全面的性能监控和业务指标监控。

## 监控指标分类

### 1. 性能指标
- **API响应时间**: P50、P90、P95、P99分位数
- **请求吞吐量**: QPS (每秒请求数)
- **比对执行时间**: 表比对和Schema比对的执行耗时
- **资源使用**: CPU使用率、内存使用量
- **数据库连接池**: 活跃连接数、空闲连接数

### 2. 业务指标
- **差异率**: 发现差异的比例
- **差异类型分布**: 新增、删除、修改记录的分布
- **数据质量指标**: 空值率、数据完整性
- **使用情况**: 功能使用频率、热门比对对象

### 3. 可用性指标
- **服务健康状态**: API可用性
- **错误率**: 4xx和5xx错误比例
- **比对失败率**: 失败任务占比

## 部署步骤

### 1. 前置条件
- Docker和Docker Compose已安装
- 主应用（data-diff-n8n）已部署运行
- 确保API服务已启用Prometheus指标导出

### 2. 启动监控栈

监控服务已集成在主系统中，使用统一启动脚本：

```bash
# 启动所有服务（包括监控）
./start.sh

# 启动脚本会自动：
# 1. 检查环境依赖
# 2. 构建必要的镜像
# 3. 启动所有服务（包括 Prometheus、Grafana、AlertManager）
# 4. 等待服务就绪
# 5. 显示访问地址
```

如果需要单独管理监控服务：

```bash
# 查看监控服务状态
docker-compose -f docker-compose.dev.yml ps prometheus grafana alertmanager

# 重启监控服务
docker-compose -f docker-compose.dev.yml restart prometheus grafana alertmanager

# 查看监控服务日志
docker-compose -f docker-compose.dev.yml logs -f prometheus grafana alertmanager
```

### 3. 访问监控服务

启动脚本执行完成后会自动显示所有服务的访问地址。您可以通过以下方式访问监控服务：

#### 服务访问方式

**通过 Nginx 统一入口访问：**
- **主页导航**: http://localhost/
- **N8N 工作流**: http://localhost/n8n/
- **API 文档**: http://localhost/api/
- **Prometheus**: http://localhost/prometheus/
- **AlertManager**: http://localhost/alertmanager/

**需要直接端口访问的服务：**
- **Grafana**: http://localhost:3000 (默认账号: admin/admin123)
  - 注意：访问 /grafana/ 会自动重定向到端口 3000

**备用直接端口访问：**
- Prometheus: http://localhost:9091
- AlertManager: http://localhost:9093
- N8N: http://localhost:5678
- API: http://localhost:8000

您也可以随时运行 `docker-compose -f docker-compose.dev.yml ps` 查看服务状态。

### 4. 配置告警通知

编辑 `alertmanager.yml`，配置告警接收器：

```yaml
receivers:
  - name: 'critical'
    webhook_configs:
      - url: 'http://your-webhook-url/critical'
    email_configs:
      - to: 'alerts@example.com'
        from: 'alertmanager@example.com'
        smarthost: 'smtp.example.com:587'
        auth_username: 'alertmanager@example.com'
        auth_password: 'password'
```

## 使用指南

### 1. 查看仪表板

1. 登录 Grafana (http://localhost:3000)
2. 导航到 "Dashboards" -> "Browse"
3. 选择以下预置仪表板之一：
   - **Data-Diff Overview** - 系统总览
   - **API Performance** - API 性能监控
   - **System Metrics** - 系统资源监控
   - **Business Metrics** - 业务指标追踪

详细的仪表板使用说明请参考 [仪表板使用指南](DASHBOARDS_GUIDE.md)

### 2. 自定义告警规则

编辑 `alerting-rules.yml` 添加新的告警规则：

```yaml
- alert: CustomAlert
  expr: your_metric_expression > threshold
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "告警摘要"
    description: "详细描述"
```

### 3. 查询指标

在Prometheus中使用PromQL查询指标：

```promql
# 查看API响应时间P95
histogram_quantile(0.95, sum(rate(datadiff_api_request_duration_seconds_bucket[5m])) by (le))

# 查看比对失败率
sum(rate(datadiff_comparison_total{status="failed"}[5m])) / sum(rate(datadiff_comparison_total[5m]))

# 查看差异率趋势
datadiff_difference_rate
```

## 监控指标详解

### API性能指标

| 指标名称 | 描述 | 单位 |
|---------|------|------|
| datadiff_api_request_duration_seconds | API请求耗时 | 秒 |
| datadiff_api_request_total | API请求总数 | 计数 |
| datadiff_comparison_duration_seconds | 比对任务耗时 | 秒 |

### 业务指标

| 指标名称 | 描述 | 单位 |
|---------|------|------|
| datadiff_comparison_differences_total | 发现的差异数量 | 计数 |
| datadiff_difference_rate | 差异率 | 比例(0-1) |
| datadiff_rows_compared_total | 比对的行数 | 计数 |
| datadiff_column_null_rate | 列空值率 | 比例(0-1) |

### 资源指标

| 指标名称 | 描述 | 单位 |
|---------|------|------|
| datadiff_memory_usage_bytes | 内存使用量 | 字节 |
| datadiff_cpu_usage_percent | CPU使用率 | 百分比 |
| datadiff_db_connections_active | 活跃数据库连接数 | 计数 |

## 故障排查

### 1. Prometheus无法抓取指标
- 检查API服务是否正常运行
- 验证 `/metrics` 端点是否可访问：`curl http://localhost:8000/metrics`
- 检查网络连接和防火墙设置
- 确保 `psutil` 等依赖已安装（检查 pyproject.toml）

### 2. Grafana无法显示数据
- 检查Prometheus数据源配置
- 验证PromQL查询语法
- 检查时间范围设置

### 3. 告警不触发
- 检查告警规则表达式
- 验证AlertManager配置
- 查看Prometheus告警状态页面

## 性能优化建议

1. **数据保留策略**: 根据需求调整Prometheus数据保留时间
2. **抓取间隔**: 根据实际需求调整metrics抓取频率
3. **仪表板优化**: 避免在单个仪表板中放置过多图表
4. **查询优化**: 使用recording rules预计算复杂查询

## 扩展监控

### 添加自定义指标

在应用代码中添加新的指标：

```python
from prometheus_client import Counter, Histogram

# 定义新指标
custom_metric = Counter(
    'datadiff_custom_metric_total',
    'Custom metric description',
    ['label1', 'label2']
)

# 记录指标
custom_metric.labels(label1='value1', label2='value2').inc()
```

### 集成其他监控工具

可以将指标导出到其他监控系统：
- Datadog
- New Relic
- CloudWatch
- Azure Monitor

## 维护建议

1. **定期备份**: 备份Grafana仪表板和Prometheus数据
2. **更新版本**: 定期更新监控组件版本
3. **清理数据**: 定期清理过期的监控数据
4. **审查告警**: 定期审查和优化告警规则