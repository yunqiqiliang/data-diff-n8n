# Data-Diff 采样功能使用指南

## 概述

采样功能允许在处理大数据量时，只比较数据的一个代表性子集，从而大幅提升比对性能。

## 采样方法

### 1. DETERMINISTIC（确定性采样）- 推荐

确定性采样使用模运算确保两个数据库采样相同的行，适合跨数据库比对。

**工作原理**：
- 单主键：`WHERE (CAST(id AS BIGINT) % 100) = 0`（采样1%）
- 复合主键：使用哈希函数确保一致性

**优点**：
- 保证两边采样相同的行
- 比对结果准确
- 适合跨数据库场景

### 2. Statistical（统计采样）

使用数据库原生采样函数，速度快但可能采样不同的行。

**支持的数据库**：
- PostgreSQL: `TABLESAMPLE SYSTEM/BERNOULLI`
- ClickHouse: `SAMPLE`
- MySQL: `ORDER BY RAND() LIMIT`
- Snowflake: `SAMPLE`
- BigQuery: `TABLESAMPLE SYSTEM`
- 等等...

## 在 n8n 中使用

### 通过 DataComparison 节点

1. 在"高级选项"中启用采样：
   ```json
   {
     "enable_sampling": true,
     "sample_size": 10000,
     "sampling_method": "DETERMINISTIC"
   }
   ```

2. 或使用百分比采样：
   ```json
   {
     "enable_sampling": true,
     "sampling_percent": 10.0,  // 采样10%
     "sampling_method": "DETERMINISTIC"
   }
   ```

### 通过 API 调用

```python
import requests

comparison_config = {
    "source_config": {
        "database_type": "postgresql",
        "connection_params": {...}
    },
    "target_config": {
        "database_type": "clickhouse",
        "connection_params": {...}
    },
    "comparison_options": {
        "source_table": "large_table",
        "target_table": "large_table_copy",
        "key_columns": ["id"],
        "enable_sampling": True,
        "sample_size": 100000,  # 采样10万行
        "sampling_method": "DETERMINISTIC"  # 确保跨数据库一致性
    }
}

response = requests.post("http://localhost:8000/api/compare", json=comparison_config)
```

## 最佳实践

### 1. 选择采样方法

- **跨数据库比对**：始终使用 `DETERMINISTIC`
- **同一数据库比对**：可以使用 `SYSTEM` 或 `BERNOULLI` 获得更好性能
- **小数据集**：不建议采样（< 100,000 行）

### 2. 采样大小

- **固定大小**：适合已知数据量的场景
  ```json
  {
    "sample_size": 50000  // 采样5万行
  }
  ```

- **百分比**：适合数据量变化的场景
  ```json
  {
    "sampling_percent": 5.0  // 采样5%
  }
  ```

### 3. 性能优化

- 大表（> 1亿行）：使用 1-5% 采样
- 中表（100万-1亿行）：使用 5-10% 采样
- 确保主键列有索引（DETERMINISTIC 采样需要）

## 示例场景

### 场景 1：跨数据库数据迁移验证

```javascript
// PostgreSQL -> ClickHouse 迁移验证
{
  "operation": "compareTable",
  "source_table": "orders",
  "target_table": "orders_migrated",
  "key_columns": ["order_id"],
  "enable_sampling": true,
  "sampling_percent": 2.0,  // 2% 采样
  "sampling_method": "DETERMINISTIC"  // 确保一致性
}
```

### 场景 2：日常数据质量检查

```javascript
// 同一数据库内的快速检查
{
  "operation": "compareTable",
  "source_table": "daily_sales",
  "target_table": "daily_sales_backup",
  "key_columns": ["sale_id"],
  "enable_sampling": true,
  "sample_size": 10000,  // 固定采样1万行
  "sampling_method": "SYSTEM"  // 使用快速系统采样
}
```

### 场景 3：大数据量全量比对

```javascript
// 10亿行级别的表比对
{
  "operation": "compareTable",
  "source_table": "events",
  "target_table": "events_replica",
  "key_columns": ["event_id", "timestamp"],
  "enable_sampling": true,
  "sampling_percent": 0.1,  // 0.1% 采样（100万行）
  "sampling_method": "DETERMINISTIC",
  "extra_columns": ["user_id", "event_type"]
}
```

## 注意事项

1. **DETERMINISTIC 采样要求**：
   - 主键列必须可以转换为数字类型
   - 复合主键使用哈希函数（可能因数据库而异）

2. **统计采样的局限性**：
   - 不同数据库可能采样不同的行
   - 结果可能有统计误差

3. **性能考虑**：
   - DETERMINISTIC 采样需要计算模运算，可能稍慢
   - Statistical 采样使用数据库原生功能，通常更快

4. **结果解释**：
   - 采样比对只能发现采样数据中的差异
   - 使用统计推断估算总体差异数

## 故障排除

### 错误：数据库不支持采样

```
NotImplementedError: Database 'xxx' does not support sampling
```

**解决方案**：
- 检查数据库版本是否支持采样功能
- 考虑使用 DETERMINISTIC 方法
- 对于不支持的数据库，考虑在应用层实现采样

### 错误：采样结果不一致

**可能原因**：
- 使用了 Statistical 采样方法
- 主键列包含 NULL 值
- 数据在采样过程中发生变化

**解决方案**：
- 切换到 DETERMINISTIC 采样
- 确保主键列没有 NULL 值
- 在数据稳定时进行比对