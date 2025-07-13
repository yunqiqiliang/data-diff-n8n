# 指标计算口径说明

## 问题分析

### 1. Histogram 类型指标的误用
Prometheus 的 Histogram 类型会自动生成以下后缀：
- `_count`: 观测次数
- `_sum`: 所有观测值的累计和
- `_bucket`: 分桶计数

当前问题：
- 使用 `datadiff_comparison_differences_total_count` 统计比对次数是错误的，这是差异数的观测次数
- 使用 `datadiff_rows_compared_total_sum` 会累计所有历史值，导致数值异常大

### 2. 正确的指标类型选择

#### Gauge（仪表盘）- 用于可增可减的值
- `datadiff_table_comparisons_total` - 表比对总数（从数据库查询）
- `datadiff_schema_comparisons_total` - Schema比对总数（从数据库查询）
- `datadiff_difference_rate` - 差异率
- `datadiff_column_null_rate` - 空值率

#### Counter（计数器）- 只增不减的累计值
- `datadiff_comparison_total` - 比对执行次数（实时统计）
- `datadiff_api_request_total` - API请求次数

#### Histogram（直方图）- 用于分布统计
- `datadiff_comparison_differences_total` - 差异数分布
- `datadiff_rows_compared_total` - 比对行数分布
- `datadiff_comparison_duration_seconds` - 执行时间分布

## 正确的计算口径

### 1. 比对总次数
```promql
# 从数据库统计的准确值
datadiff_table_comparisons_total + datadiff_schema_comparisons_total
```

### 2. 表比对详情
```promql
# 获取每个表的最新差异数（不是累计）
max by (table_name) (datadiff_comparison_differences_total_sum)

# 获取每个表的最新比对行数
max by (table_name) (datadiff_rows_compared_total_sum)

# 计算差异率
max by (table_name) (datadiff_difference_rate)
```

### 3. 比对速率（每分钟）
```promql
# 使用 Counter 类型的 rate 函数
rate(datadiff_comparison_total[5m]) * 60
```

### 4. 执行时间分位数
```promql
# P95 响应时间
histogram_quantile(0.95, sum(rate(datadiff_comparison_duration_seconds_bucket[5m])) by (le))

# P50 响应时间
histogram_quantile(0.50, sum(rate(datadiff_comparison_duration_seconds_bucket[5m])) by (le))
```

### 5. 总差异数
```promql
# 所有表的差异数总和（取最新值，不是累计）
sum(max by (table_name) (datadiff_comparison_differences_total_sum))
```

### 6. 总比对行数
```promql
# 所有表的比对行数总和（取最新值，不是累计）
sum(max by (table_name) (datadiff_rows_compared_total_sum))
```

## 注意事项

1. **避免使用 Histogram 的 _sum 直接求和**
   - 错误：`sum(datadiff_rows_compared_total_sum)` 
   - 正确：`sum(max by (table_name) (datadiff_rows_compared_total_sum))`

2. **使用正确的聚合函数**
   - 对于 Gauge：使用 `avg`, `max`, `min`
   - 对于 Counter：使用 `rate`, `increase`
   - 对于 Histogram：使用 `histogram_quantile`

3. **时间窗口选择**
   - rate 计算建议使用 5m 窗口
   - increase 计算根据需要选择 1h, 24h 等

4. **标签匹配**
   - 确保查询时使用正确的标签
   - 避免标签不匹配导致的空结果

## 修正后的关键指标

| 指标名称 | 计算方法 | 说明 |
|---------|---------|------|
| 总比对次数 | `datadiff_table_comparisons_total + datadiff_schema_comparisons_total` | 从数据库查询的准确值 |
| 表比对次数 | `datadiff_table_comparisons_total` | 仅表比对 |
| Schema比对次数 | `datadiff_schema_comparisons_total` | 仅Schema比对 |
| 总差异数 | `sum(max by (table_name) (datadiff_comparison_differences_total_sum))` | 所有表的差异总和 |
| 总比对行数 | `sum(max by (table_name) (datadiff_rows_compared_total_sum))` | 所有表的行数总和 |
| 比对速率 | `rate(datadiff_comparison_total[5m]) * 60` | 每分钟比对次数 |
| 平均差异率 | `avg(datadiff_difference_rate)` | 所有表的平均差异率 |