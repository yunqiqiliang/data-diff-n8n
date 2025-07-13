# 比对结果持久化与获取机制

## 概述

系统支持多层次的结果存储和获取机制，确保比对结果不会丢失：

1. **内存缓存**：快速访问最近的结果
2. **物化表存储**：长期持久化存储
3. **自动降级**：内存没有时自动从数据库获取

## 结果存储流程

```
比对完成
  ↓
保存到内存缓存（临时）
  ↓
物化到 PostgreSQL 表（持久）
  ↓
发送 Webhook 回调（如果启用）
```

## 结果获取优先级

当通过 API 或 n8n 节点获取结果时：

1. **首先检查内存**：最快，适合刚完成的任务
2. **其次查询物化表**：适合历史任务和内存已清理的情况
3. **都没有则返回 404**：任务不存在

## 物化表结构

结果存储在以下表中：

- `comparison_summary`：比对汇总信息
- `difference_details`：差异详情（限制 1000 条）
- `column_statistics`：列级统计信息
- `timeline_analysis`：时间线分析结果
- `performance_metrics`：性能指标

## API 响应中的标识

从物化表获取的结果会包含特殊标识：

```json
{
  "comparison_id": "xxx",
  "status": "completed",
  "result": {
    // ... 结果数据
  },
  "materialized_from_db": true  // 标识从数据库获取
}
```

## 使用场景

### 1. 实时获取（内存）

适用于：
- 刚完成的比对任务
- 需要快速响应的场景
- 短期内多次查询同一结果

### 2. 历史查询（物化表）

适用于：
- 查询几小时或几天前的结果
- 服务重启后的结果恢复
- 审计和历史分析

### 3. 批量分析（直接查询物化表）

```sql
-- 查询最近 7 天的所有比对结果
SELECT 
    comparison_id,
    source_table,
    target_table,
    match_rate,
    rows_different,
    start_time
FROM data_diff_results.comparison_summary
WHERE start_time >= NOW() - INTERVAL '7 days'
ORDER BY start_time DESC;

-- 查询特定表的历史比对趋势
SELECT 
    DATE(start_time) as comparison_date,
    AVG(match_rate) as avg_match_rate,
    SUM(rows_different) as total_differences,
    COUNT(*) as comparison_count
FROM data_diff_results.comparison_summary
WHERE source_table = 'public.orders'
GROUP BY DATE(start_time)
ORDER BY comparison_date DESC;
```

## 清理策略

### 内存清理

- 默认保留最近 1000 个结果
- 超过限制时自动清理最旧的结果
- 服务重启时清空

### 物化表清理

建议定期清理旧数据：

```sql
-- 清理 30 天前的数据
DELETE FROM data_diff_results.comparison_summary
WHERE start_time < NOW() - INTERVAL '30 days';

-- 或使用分区表自动管理
```

## 性能考虑

1. **内存获取**：毫秒级响应
2. **物化表获取**：
   - 有索引：10-50ms
   - 包含差异详情：50-200ms
   - 大量数据：可能需要优化查询

## 最佳实践

1. **立即处理**：对于重要结果，在收到回调后立即处理
2. **定期归档**：将重要结果导出到数据仓库
3. **监控存储**：定期检查物化表大小
4. **优化查询**：为常用查询创建适当的索引

## 故障恢复

如果内存和物化表都没有结果：

1. 检查 comparison_id 是否正确
2. 确认任务是否成功完成
3. 查看 API 日志了解任务状态
4. 如果任务失败，需要重新执行比对

## 配置选项

在比对请求中控制物化行为：

```json
{
  "comparison_config": {
    "materialize_results": true,  // 是否物化结果
    "store_difference_details": true,  // 存储差异详情
    "store_column_stats": true,  // 存储列统计
    "store_timeline_analysis": true  // 存储时间线分析
  }
}
```