# n8n Data Comparison 节点采样功能使用指南

## 功能概述

Data Comparison 节点现在支持智能采样功能，可以在处理大数据量时显著提升性能。

## 配置界面说明

### 基本配置

1. **Operation**: 选择 "Compare Table"
2. **Source Table**: 选择或输入源表名
3. **Target Table**: 选择或输入目标表名
4. **Key Columns**: 主键列，用于唯一标识行（默认: id）

### 高级选项 - 采样配置

点击 "Add Option" 并选择相关采样选项：

#### 1. Enable Sampling（启用采样）
- **类型**: Boolean
- **默认值**: false  
- **说明**: 开启后将对大数据集进行采样比对

#### 2. Sampling Type（采样类型）
- **Fixed Size**: 固定行数采样
- **Percentage**: 百分比采样

#### 3. Sample Size（采样大小）
- **显示条件**: 当 Sampling Type = "Fixed Size"
- **默认值**: 10000
- **说明**: 要采样的行数

#### 4. Sample Percentage（采样百分比）
- **显示条件**: 当 Sampling Type = "Percentage"  
- **默认值**: 10
- **范围**: 0.1-100
- **说明**: 要采样的数据百分比

#### 5. Sampling Method（采样方法）
- **Deterministic (Recommended)**: 确定性采样，确保两边采样相同的行
- **System**: 系统级快速采样，可能导致两边采样不同的行
- **Bernoulli**: 行级随机采样，更准确但可能导致两边采样不同

#### 6. Auto Sample Threshold（自动采样阈值）
- **默认值**: 100000
- **说明**: 当表行数超过此值时自动建议采样

#### 7. Sampling Confidence（置信度）
- **选项**: 90%, 95%, 99%
- **默认值**: 95%
- **说明**: 统计采样的置信水平

#### 8. Sampling Margin of Error（误差容限）
- **选项**: 0.1%, 1%, 5%
- **默认值**: 1%
- **说明**: 可接受的采样误差范围

## 使用场景示例

### 场景 1: 小规模精确比对
```json
{
  "operation": "compareTable",
  "sourceTable": "users",
  "targetTable": "users_backup",
  "keyColumns": "user_id",
  "enableSampling": false
}
```
适用于：< 10万行的表

### 场景 2: 中等规模固定采样
```json
{
  "operation": "compareTable",
  "sourceTable": "orders",
  "targetTable": "orders_replica",
  "keyColumns": "order_id",
  "enableSampling": true,
  "samplingType": "size",
  "sampleSize": 50000,
  "samplingMethod": "DETERMINISTIC"
}
```
适用于：10万-1000万行的表

### 场景 3: 大规模百分比采样
```json
{
  "operation": "compareTable",
  "sourceTable": "events",
  "targetTable": "events_archive",
  "keyColumns": "event_id,timestamp",
  "enableSampling": true,
  "samplingType": "percent",
  "samplePercent": 1.0,
  "samplingMethod": "DETERMINISTIC",
  "samplingConfidence": 0.99
}
```
适用于：> 1000万行的表

### 场景 4: 跨数据库迁移验证
```json
{
  "operation": "compareTable",
  "sourceTable": "products",
  "targetTable": "products_migrated",
  "keyColumns": "product_id",
  "enableSampling": true,
  "samplingType": "percent",
  "samplePercent": 5.0,
  "samplingMethod": "DETERMINISTIC",
  "extraColumns": "name,price,category"
}
```
**重要**: 跨数据库比对必须使用 DETERMINISTIC 方法

## 最佳实践

### 1. 何时使用采样

| 数据量 | 建议 |
|--------|------|
| < 10万行 | 不需要采样 |
| 10万-100万行 | 可选采样，建议 10-20% |
| 100万-1000万行 | 建议采样，5-10% |
| > 1000万行 | 强烈建议采样，1-5% |

### 2. 采样方法选择

- **同数据库比对**: 可以使用任何方法
- **跨数据库比对**: 必须使用 DETERMINISTIC
- **需要快速结果**: 使用 SYSTEM
- **需要准确结果**: 使用 BERNOULLI 或 DETERMINISTIC

### 3. 性能优化技巧

1. **使用索引**: 确保主键列有索引
2. **选择合适的采样率**: 平衡准确性和性能
3. **监控内存使用**: 大数据集可能需要更多内存
4. **分批处理**: 对于超大数据集，考虑分批比对

## 结果解释

### 采样状态信息
比对结果中会包含采样信息：
```json
{
  "sampling_applied": true,
  "sampling_method": "DETERMINISTIC",
  "sample_size": 50000,
  "confidence_level": 0.95,
  "margin_of_error": 0.01,
  "estimated_total_differences": 1250,
  "confidence_interval": [1200, 1300]
}
```

### 差异统计推断
- **estimated_total_differences**: 基于采样推断的总差异数
- **confidence_interval**: 置信区间，真实差异数有95%概率在此范围内

## 故障排除

### 1. "Database does not support sampling"
**原因**: 数据库不支持指定的采样方法
**解决**: 切换到 DETERMINISTIC 方法

### 2. "Sample size too large"
**原因**: 采样大小超过表总行数
**解决**: 减小采样大小或使用百分比采样

### 3. "Sampling results inconsistent"
**原因**: 使用了非确定性采样方法
**解决**: 切换到 DETERMINISTIC 方法

### 4. 性能未改善
**可能原因**:
- 采样率过高
- 主键列没有索引
- 网络延迟高

## 示例工作流

### 基础比对工作流
```
1. Database Connector (Source)
   - Operation: Prepare for Comparison
   - Table: orders
   
2. Database Connector (Target)  
   - Operation: Prepare for Comparison
   - Table: orders_backup
   
3. Data Comparison
   - Operation: Compare Table
   - Enable Sampling: true
   - Sampling Type: Percentage
   - Sample Percentage: 10
   - Sampling Method: DETERMINISTIC
```

### 高级监控工作流
```
1. Schedule Trigger
   - Every hour
   
2. Database Connector (Source)
   - Prepare for Comparison
   
3. Database Connector (Target)
   - Prepare for Comparison
   
4. Data Comparison
   - Compare Table with sampling
   
5. IF (differences > threshold)
   - Send Alert Email
   - Log to monitoring system
```

## 注意事项

1. **数据一致性**: DETERMINISTIC 采样确保可重复的结果
2. **统计准确性**: 采样结果是统计估算，不是精确值
3. **资源使用**: 采样仍需要扫描部分数据，确保有足够资源
4. **实时性**: 采样期间的数据变化可能影响结果