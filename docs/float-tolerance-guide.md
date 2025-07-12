# 浮点数容差比较功能指南

## 概述

浮点数容差比较功能允许在比对数值类型数据时设置一个容差范围，在该范围内的差异将被视为相等。这对于处理浮点数精度问题特别有用。

## 功能特点

### 1. 支持的数据类型
- **FLOAT / DOUBLE PRECISION**: 浮点数类型
- **DECIMAL / NUMERIC**: 定点小数类型
- **REAL**: 单精度浮点数

### 2. 比较逻辑

使用容差比较时，两个值被认为相等的条件是：
```
ABS(value1 - value2) <= tolerance
```

例如：
- 容差 = 0.01
- 值1 = 10.001，值2 = 10.009
- 差异 = |10.001 - 10.009| = 0.008
- 0.008 <= 0.01，所以这两个值被认为相等

### 3. NULL 值处理
- 如果任一值为 NULL，将使用标准的 `IS DISTINCT FROM` 比较
- NULL 值不参与容差比较

## 在 n8n 中使用

### 配置参数

在 Data Comparison 节点的 Advanced Options 中：

```
Float Tolerance: 0.0001
```

**参数说明**：
- **默认值**: 0.0001
- **范围**: >= 0
- **0 值**: 表示精确比较（不使用容差）

### 常见配置值

| 场景 | 推荐值 | 说明 |
|------|--------|------|
| 高精度科学计算 | 0.0000001 | 7位小数精度 |
| 一般科学计算 | 0.0001 | 4位小数精度（默认） |
| 财务数据（美分） | 0.01 | 2位小数精度 |
| 财务数据（元） | 0.1 | 1位小数精度 |
| 一般业务数据 | 0.1 - 1.0 | 根据业务需求 |
| 百分比数据 | 0.01 - 0.1 | 1-10%的差异 |

## 使用示例

### 示例 1：财务数据比对

**场景**：比对两个系统的订单金额，允许美分级别的舍入误差

```javascript
{
  "operation": "compareTable",
  "sourceTable": "orders_system_a",
  "targetTable": "orders_system_b",
  "keyColumns": "order_id",
  "floatTolerance": 0.01,  // 允许1美分的误差
  "columnsToCompare": "order_id,customer_id,total_amount,tax_amount"
}
```

**结果**：
- 10.00 vs 10.01 → 相等（差异0.01 = 容差）
- 10.00 vs 10.02 → 不同（差异0.02 > 容差）

### 示例 2：科学数据比对

**场景**：比对实验测量数据，允许仪器精度误差

```javascript
{
  "operation": "compareTable",
  "sourceTable": "measurements_lab1",
  "targetTable": "measurements_lab2",
  "keyColumns": "measurement_id",
  "floatTolerance": 0.0001,  // 允许0.01%的误差
  "columnsToCompare": "temperature,pressure,humidity"
}
```

### 示例 3：百分比数据比对

**场景**：比对计算的百分比值，允许舍入误差

```javascript
{
  "operation": "compareTable",
  "sourceTable": "analytics_old",
  "targetTable": "analytics_new",
  "keyColumns": "metric_id",
  "floatTolerance": 0.1,  // 允许0.1%的差异
  "columnsToCompare": "conversion_rate,bounce_rate,engagement_rate"
}
```

## 实现原理

### SQL 生成示例

当启用浮点数容差时，比较逻辑会生成如下 SQL：

```sql
-- 标准比较（无容差）
a.price IS DISTINCT FROM b.price

-- 容差比较（tolerance = 0.01）
CASE 
    WHEN (a.price IS NULL OR b.price IS NULL) 
    THEN a.price IS DISTINCT FROM b.price
    ELSE ABS(a.price - b.price) > 0.01
END
```

### 性能考虑

1. **索引影响**：容差比较无法使用索引优化，可能影响大表性能
2. **计算开销**：每行都需要计算 ABS 和比较，增加 CPU 开销
3. **建议**：对大表使用采样功能配合容差比较

## 最佳实践

### 1. 选择合适的容差值

- **太小**：可能因浮点数精度问题产生误报
- **太大**：可能漏掉真正的数据差异
- **建议**：从小值开始测试，逐步调整

### 2. 配合其他功能使用

```javascript
{
  "operation": "compareTable",
  "floatTolerance": 0.01,      // 浮点数容差
  "enableSampling": true,       // 启用采样
  "samplingPercent": 10,        // 采样10%
  "samplingMethod": "DETERMINISTIC"  // 确定性采样
}
```

### 3. 监控和调优

- 记录不同容差值下的差异数量
- 分析差异分布，优化容差设置
- 对关键列使用更严格的容差

## 限制和注意事项

### 1. 当前限制
- 只支持 JOINDIFF 算法（同数据库比对）
- HASHDIFF 算法（跨数据库）暂不支持容差比较
- 所有数值列使用相同的容差值

### 2. 未来改进
- 支持列级别的容差配置
- 支持相对容差（百分比）
- 支持 HASHDIFF 算法

### 3. 注意事项
- 容差比较会增加查询复杂度
- 大表建议配合采样使用
- 定期验证容差设置的合理性

## 故障排查

### 问题 1：设置容差后仍报告大量差异

**可能原因**：
- 容差值设置过小
- 数据类型不是数值类型
- 使用了 HASHDIFF 算法

**解决方案**：
- 增大容差值测试
- 确认列类型为 FLOAT/DECIMAL
- 确保使用 JOINDIFF 算法

### 问题 2：性能下降明显

**可能原因**：
- 表数据量过大
- 没有使用采样

**解决方案**：
- 启用采样功能
- 减少比对的列数
- 考虑分批比对

## 测试建议

1. **单元测试**：创建小表测试不同容差值
2. **集成测试**：在实际数据的子集上测试
3. **性能测试**：评估不同数据量的执行时间
4. **边界测试**：测试容差边界值（等于、略小于、略大于）