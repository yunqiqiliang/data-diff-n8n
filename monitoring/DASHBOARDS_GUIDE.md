# Grafana Dashboard 使用指南

## 概述

根据实际可用的 Prometheus 指标，我们重新设计了监控仪表板，提供更准确和实用的监控视图。

## 仪表板列表

### 1. Data-Diff Overview（总览仪表板）
**文件**: `overview.json`  
**用途**: 提供系统整体运行状况的快速概览

**关键指标**:
- **服务状态**: API 服务是否正常运行
- **资源使用**: CPU 使用率、内存使用量
- **业务统计**: 总比对次数、总比对行数、发现差异数、数据质量分数
- **性能趋势**: API 请求速率、比对执行速率
- **表比对汇总**: 每个表的比对统计详情

### 2. Business Metrics（业务指标仪表板）
**文件**: `business-metrics.json`  
**用途**: 深入分析业务相关的指标

**关键指标**:
- **统计卡片**: 
  - 总比对次数
  - 发现的差异总数
  - 比对的总行数
  - 平均数据质量分数
- **趋势图表**:
  - 比对速率（每分钟）
  - 比对执行时间分布（P50、P95）
- **详细表格**:
  - 表比对详情（包含差异数、比对行数、差异率）
- **可视化**:
  - 列空值率热力图
  - 功能使用分布饼图
  - 每小时比对行数趋势

### 3. System Metrics（系统指标仪表板）
**文件**: `system-metrics.json`  
**用途**: 监控系统资源和性能

**关键指标**:
- **仪表盘**:
  - CPU 使用率
  - 内存使用量
  - API 请求速率
- **趋势图表**:
  - CPU 使用率趋势
  - 内存使用趋势
  - API 响应时间分位数（P50、P90、P95）
- **API 端点统计表**:
  - 各端点的请求数
  - 平均响应时间

## 实际可用的指标

### 系统指标
- `datadiff_cpu_usage_percent` - CPU 使用率
- `datadiff_memory_usage_bytes` - 内存使用量（字节）
- `datadiff_system_info` - 系统信息（Python版本等）

### API 指标
- `datadiff_api_request_total` - API 请求总数
- `datadiff_api_request_duration_seconds` - API 请求耗时
- `api_request_duration_seconds` - API 请求耗时（兼容）

### 业务指标
- `datadiff_comparison_total` - 比对任务总数
- `datadiff_comparison_duration_seconds` - 比对执行耗时
- `datadiff_comparison_differences_total` - 发现的差异数量
- `datadiff_rows_compared_total` - 比对的行数
- `datadiff_difference_rate` - 差异率
- `datadiff_column_null_rate` - 列空值率
- `datadiff_data_quality_score` - 数据质量分数
- `datadiff_feature_usage_total` - 功能使用统计

## 使用技巧

### 1. 时间范围选择
- Overview：默认显示最近 3 小时
- Business Metrics：默认显示最近 6 小时
- System Metrics：默认显示最近 1 小时

### 2. 数据刷新
- 建议设置 30 秒自动刷新
- 业务指标每分钟从数据库更新一次

### 3. 告警设置建议
- CPU 使用率 > 80% 时告警
- 内存使用量 > 1GB 时告警
- API 响应时间 P95 > 1秒时告警
- 数据质量分数 < 70% 时告警

## 注意事项

1. **业务指标需要历史数据**
   - 需要有成功的比对任务才能看到业务指标
   - 指标从数据库中的历史记录生成
   - 每分钟自动更新一次

2. **差异率显示**
   - 某些情况下可能显示负值（当 match_rate > 100 时）
   - 这通常是由于数据格式问题，需要检查原始数据

3. **性能考虑**
   - 避免选择过长的时间范围
   - 表格类面板建议限制显示行数

## 故障排查

### 业务指标无数据
1. 检查是否有成功的比对任务
2. 查看 API 日志确认指标更新任务是否正常运行
3. 确认数据库连接正常

### 系统指标异常
1. 检查 psutil 模块是否正确安装
2. 确认 API 服务正常运行
3. 查看 Prometheus targets 页面

### Dashboard 不显示
1. 确认文件已正确放置在 `/monitoring/grafana-dashboards/` 目录
2. 重启 Grafana 容器
3. 检查 Grafana 日志