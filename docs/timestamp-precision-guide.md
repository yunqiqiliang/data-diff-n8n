# 时间戳精度处理功能指南

## 概述

时间戳精度处理功能允许在比对时间戳数据时设置不同的精度级别，忽略小于指定精度的时间差异。这对于处理跨系统的时间同步问题特别有用。

## 重要：时区问题

### ⚠️ ClickZetta 时区限制

**ClickZetta 不支持时区功能！**在使用 ClickZetta 进行时间戳比对时，必须确保：

1. **所有时间戳数据必须预先转换为 UTC**
2. ClickZetta 无法执行会话级时区设置
3. 与其他支持时区的数据库比对时可能产生误差

### 时区兼容性矩阵

| 数据库 | 时区支持 | SET TIMEZONE | 带时区类型 | 注意事项 |
|--------|----------|--------------|------------|----------|
| PostgreSQL | ✅ 完整 | ✅ | TIMESTAMP WITH TIME ZONE | 推荐使用 |
| MySQL | ✅ 部分 | ✅ | ❌ | 只有会话时区 |
| ClickZetta | ❌ 无 | ❌ | ❌ | **必须预转换为 UTC** |
| ClickHouse | ❌ 无 | ❌ | ❌ | 内部使用 UTC |
| BigQuery | ❌ 无 | ❌ | ❌ | 总是 UTC |
| Snowflake | ✅ 完整 | ✅ | TIMESTAMP_TZ | 三种时间戳类型 |
| Oracle | ✅ 完整 | ✅ | TIMESTAMP WITH TIME ZONE | 完整支持 |

## 功能特点

### 1. 支持的时间类型
- **TIMESTAMP / TIMESTAMP WITHOUT TIME ZONE**: 不带时区的时间戳
- **TIMESTAMP WITH TIME ZONE / TIMESTAMPTZ**: 带时区的时间戳
- **DATETIME**: 日期时间类型
- **DATE**: 日期类型
- **TIME**: 时间类型

### 2. 精度级别

| 精度 | 值 | 说明 | 示例差异 |
|------|-----|------|----------|
| Microsecond | `microsecond` | 微秒级（0.000001秒） | 2024-01-15 14:30:45.123456 vs .123457 |
| Millisecond | `millisecond` | 毫秒级（0.001秒） | 2024-01-15 14:30:45.123 vs .124 |
| Second | `second` | 秒级（1秒） | 2024-01-15 14:30:45 vs 14:30:46 |
| Minute | `minute` | 分钟级（60秒） | 2024-01-15 14:30:00 vs 14:31:00 |
| Hour | `hour` | 小时级（3600秒） | 2024-01-15 14:00:00 vs 15:00:00 |
| Day | `day` | 天级 | 2024-01-15 vs 2024-01-16 |

### 3. 处理逻辑

根据选择的精度级别，时间戳会被截断到相应精度后再进行比较：

```sql
-- 原始时间戳
2024-01-15 14:30:45.123456

-- 不同精度下的截断结果
microsecond: 2024-01-15 14:30:45.123456  (不截断)
millisecond: 2024-01-15 14:30:45.123000
second:      2024-01-15 14:30:45.000000
minute:      2024-01-15 14:30:00.000000
hour:        2024-01-15 14:00:00.000000
day:         2024-01-15 00:00:00.000000
```

## 在 n8n 中使用

### 配置参数

在 Data Comparison 节点的 Advanced Options 中：

```
Timestamp Precision: Second
```

**参数说明**：
- **默认值**: microsecond（最高精度）
- **选项**: microsecond, millisecond, second, minute, hour, day

## 使用示例

### 示例 1：跨系统数据同步验证

**场景**：验证两个系统的订单数据，允许秒级的时间差异

```javascript
{
  "operation": "compareTable",
  "sourceTable": "orders_source",
  "targetTable": "orders_target",
  "keyColumns": "order_id",
  "timestampPrecision": "second",
  "columnsToCompare": "order_id,customer_id,order_time,update_time"
}
```

**结果**：
- 14:30:45.123 vs 14:30:45.999 → 相等（亚秒差异被忽略）
- 14:30:45 vs 14:30:46 → 不同（超过1秒差异）

### 示例 2：ClickZetta 时间戳比对

**场景**：ClickZetta 与 PostgreSQL 的数据比对

```javascript
{
  "operation": "compareTable",
  "sourceTable": "clickzetta_events",  // ClickZetta
  "targetTable": "postgres_events",     // PostgreSQL
  "keyColumns": "event_id",
  "timestampPrecision": "millisecond",  // 使用毫秒精度容忍小差异
  "columnsToCompare": "event_id,user_id,event_time"
}
```

**重要提示**：
1. 确保 ClickZetta 中的所有时间戳都是 UTC
2. PostgreSQL 应设置为 UTC 时区
3. 考虑使用较低精度（如秒或分钟）来容忍时区转换误差

### 示例 3：日志数据分析

**场景**：比对不同来源的日志，只关心分钟级别的差异

```javascript
{
  "operation": "compareTable",
  "sourceTable": "app_logs",
  "targetTable": "aggregated_logs",
  "keyColumns": "log_id",
  "timestampPrecision": "minute",
  "columnsToCompare": "log_id,level,message,timestamp"
}
```

### 示例 4：财务日报对账

**场景**：比对每日财务报表，只需要日期级别的精度

```javascript
{
  "operation": "compareTable",
  "sourceTable": "daily_transactions",
  "targetTable": "financial_reports",
  "keyColumns": "transaction_date,account_id",
  "timestampPrecision": "day",
  "columnsToCompare": "amount,balance,last_updated"
}
```

## 最佳实践

### 1. 时区处理建议

#### 使用 ClickZetta 时：
```sql
-- 在导入数据前转换为 UTC
-- PostgreSQL 示例
INSERT INTO clickzetta_table 
SELECT 
    id,
    created_at AT TIME ZONE 'UTC' as created_at_utc
FROM source_table;
```

#### 跨数据库比对前：
```sql
-- 设置会话时区（支持的数据库）
SET TIME ZONE 'UTC';  -- PostgreSQL
SET @@session.time_zone='+00:00';  -- MySQL
ALTER SESSION SET TIMEZONE = 'UTC';  -- Oracle
```

### 2. 精度选择指南

| 使用场景 | 推荐精度 | 原因 |
|----------|----------|------|
| 实时交易系统 | microsecond/millisecond | 需要高精度 |
| 日常业务数据 | second | 平衡精度和容错 |
| 批处理系统 | minute | 容忍处理延迟 |
| 统计报表 | hour/day | 只关心趋势 |
| ClickZetta 比对 | second/minute | 容忍时区问题 |

### 3. 性能优化

- **高精度比对**：适合小数据集或关键数据
- **低精度比对**：适合大数据集或容错要求高的场景
- **配合采样**：大数据集建议同时启用采样功能

```javascript
{
  "timestampPrecision": "minute",   // 降低精度
  "enableSampling": true,           // 启用采样
  "samplingPercent": 10             // 采样10%
}
```

## 故障排查

### 问题 1：ClickZetta 比对总是显示差异

**原因**：
- ClickZetta 数据不是 UTC
- 其他数据库未设置正确时区
- 精度设置过高

**解决方案**：
1. 确认所有 ClickZetta 数据都是 UTC
2. 降低精度到秒或分钟级别
3. 检查数据导入时的时区转换

### 问题 2：带时区和不带时区的列比对失败

**原因**：
- TIMESTAMP WITH TIME ZONE vs TIMESTAMP WITHOUT TIME ZONE
- 数据库处理方式不同

**解决方案**：
- 统一使用不带时区的时间戳
- 或在比对前转换为相同类型

### 问题 3：日期边界问题

**原因**：
- 不同时区导致日期不同
- 例如：UTC 2024-01-15 23:00 = PST 2024-01-15 15:00

**解决方案**：
- 使用 day 精度时确保时区一致
- 或使用较低精度（如 hour）

## SQL 实现示例

### PostgreSQL
```sql
-- 截断到秒
date_trunc('second', timestamp_column)

-- 截断到分钟
date_trunc('minute', timestamp_column)
```

### MySQL
```sql
-- 截断到秒
DATE_FORMAT(timestamp_column, '%Y-%m-%d %H:%i:%s.000000')

-- 截断到分钟
DATE_FORMAT(timestamp_column, '%Y-%m-%d %H:%i:00.000000')
```

### ClickZetta
```sql
-- 截断到秒
date_format(timestamp_column, 'yyyy-MM-dd HH:mm:ss.000000')

-- 截断到分钟
date_format(timestamp_column, 'yyyy-MM-dd HH:mm:00.000000')
```

## 限制和注意事项

### 1. 当前限制
- 只支持 JOINDIFF 算法（同数据库比对）
- 所有时间戳列使用相同的精度级别
- ClickZetta 完全不支持时区

### 2. 未来改进
- 支持列级别的精度配置
- 支持 HASHDIFF 算法
- 自动时区检测和警告

### 3. 注意事项
- 时区问题是跨数据库比对的主要挑战
- ClickZetta 用户必须特别注意数据预处理
- 定期验证时间戳数据的一致性