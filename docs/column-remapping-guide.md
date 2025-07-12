# 列重映射功能指南

## 概述

列重映射功能允许您比较具有不同列名的表。这在以下场景特别有用：
- 数据迁移后列名发生变化
- 不同系统使用不同的命名约定
- 比较来自不同供应商的相似数据

## 功能特点

### 1. 灵活的映射方式
- **显式映射**：手动指定列名对应关系
- **自动匹配**：未映射的同名列自动匹配
- **大小写选项**：支持大小写敏感/不敏感匹配

### 2. 映射语法
使用简单的冒号分隔格式：
```
source_column:target_column
```

多个映射用逗号分隔：
```
user_id:customer_id,created_at:creation_date,updated_at:modification_date
```

## 在 n8n 中使用

### 配置参数

在 Data Comparison 节点的 Advanced Options 中：

1. **Column Remapping**：
   - 格式：`source1:target1,source2:target2`
   - 示例：`user_id:customer_id,created_at:creation_date`

2. **Case-Sensitive Column Remapping**：
   - 默认：true（大小写敏感）
   - 设为 false 时，`USER_ID` 可以匹配 `user_id`

## 使用示例

### 示例 1：数据库迁移验证

**场景**：从旧系统迁移到新系统，列名发生了变化

旧系统表结构：
```sql
CREATE TABLE users_old (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(100),
    created_at TIMESTAMP,
    modified_at TIMESTAMP
);
```

新系统表结构：
```sql
CREATE TABLE customers_new (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    creation_date TIMESTAMP,
    last_modified TIMESTAMP
);
```

配置：
```javascript
{
  "operation": "compareTable",
  "sourceTable": "users_old",
  "targetTable": "customers_new",
  "keyColumns": "user_id",  // 源表的主键
  "columnRemapping": "user_id:customer_id,user_name:customer_name,created_at:creation_date,modified_at:last_modified",
  "caseSensitiveRemapping": true
}
```

### 示例 2：跨系统数据同步验证

**场景**：验证 CRM 和 ERP 系统中的客户数据

CRM 系统：
```sql
-- 使用驼峰命名
CREATE TABLE crmCustomers (
    customerId INT,
    firstName VARCHAR(50),
    lastName VARCHAR(50),
    emailAddress VARCHAR(100),
    phoneNumber VARCHAR(20)
);
```

ERP 系统：
```sql
-- 使用下划线命名
CREATE TABLE erp_customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email_address VARCHAR(100),
    phone_number VARCHAR(20)
);
```

配置：
```javascript
{
  "operation": "compareTable",
  "sourceTable": "crmCustomers",
  "targetTable": "erp_customers",
  "keyColumns": "customerId",
  "columnRemapping": "customerId:customer_id,firstName:first_name,lastName:last_name,emailAddress:email_address,phoneNumber:phone_number",
  "caseSensitiveRemapping": true
}
```

### 示例 3：大小写不敏感匹配

**场景**：比较使用不同大小写约定的表

```javascript
{
  "operation": "compareTable",
  "sourceTable": "ORACLE_CUSTOMERS",  // Oracle 通常使用大写
  "targetTable": "postgres_customers",  // PostgreSQL 通常使用小写
  "keyColumns": "CUSTOMER_ID",
  "columnRemapping": "",  // 不需要显式映射
  "caseSensitiveRemapping": false  // 自动匹配不同大小写
}
```

### 示例 4：部分列映射

**场景**：只映射名称不同的列，相同名称的列自动匹配

源表：
```sql
CREATE TABLE source_table (
    id INT,
    name VARCHAR(100),
    email VARCHAR(100),
    created_date TIMESTAMP,  -- 需要映射
    status VARCHAR(20)
);
```

目标表：
```sql
CREATE TABLE target_table (
    id INT,
    name VARCHAR(100),
    email VARCHAR(100),
    creation_time TIMESTAMP,  -- 不同的列名
    status VARCHAR(20)
);
```

配置：
```javascript
{
  "columnRemapping": "created_date:creation_time",  // 只映射不同的列
  // id, name, email, status 会自动匹配
}
```

## 工作原理

### 1. 映射处理流程

```
1. 解析映射配置
   ↓
2. 验证映射的列是否存在
   ↓
3. 应用显式映射
   ↓
4. 自动匹配未映射的同名列
   ↓
5. 执行比对
```

### 2. 列匹配优先级

1. **显式映射**：最高优先级
2. **精确匹配**：列名完全相同
3. **大小写不敏感匹配**：仅在 `caseSensitiveRemapping=false` 时

### 3. 验证和警告

系统会验证：
- 映射中的源列是否存在于源表
- 映射中的目标列是否存在于目标表
- 是否有重复映射

出现问题时会显示警告但不会中断执行。

## 高级用法

### 1. 使用 API 进行映射

```python
from data_diff import diff_tables, connect_to_table

# 定义映射
column_mappings = {
    "user_id": "customer_id",
    "created_at": "creation_date",
    "updated_at": "modification_date"
}

# 执行比对
diff_result = diff_tables(
    table1,
    table2,
    column_remapping=column_mappings,
    case_sensitive_remapping=False
)
```

### 2. 自动建议映射

```python
from data_diff.column_remapping import ColumnRemapper

# 获取列名
source_columns = ["user_id", "user_name", "created_at"]
target_columns = ["customer_id", "customer_name", "creation_date"]

# 自动建议映射（基于相似度）
suggestions = ColumnRemapper.suggest_mappings(
    source_columns,
    target_columns,
    similarity_threshold=0.8  # 80% 相似度阈值
)
# 结果: {"user_name": "customer_name"}
```

### 3. 结合其他高级功能

列重映射可以与其他功能组合使用：

```javascript
{
  "columnRemapping": "price:product_price,qty:quantity",
  "floatTolerance": 0.01,  // 价格比较允许 0.01 的误差
  "timestampPrecision": "second",  // 时间戳精度
  "jsonComparisonMode": "normalized"  // JSON 比较模式
}
```

## 最佳实践

### 1. 命名约定转换

创建标准的映射模板：

```javascript
// 驼峰转下划线
const camelToSnake = {
  "userId": "user_id",
  "userName": "user_name",
  "createdAt": "created_at",
  "updatedAt": "updated_at"
};

// 下划线转驼峰
const snakeToCamel = {
  "user_id": "userId",
  "user_name": "userName",
  "created_at": "createdAt",
  "updated_at": "updatedAt"
};
```

### 2. 文档化映射关系

维护映射文档：

```yaml
# column_mappings.yml
old_system_to_new:
  users:
    user_id: customer_id
    user_name: customer_name
    created_at: creation_date
    modified_at: last_modified
  
  orders:
    order_id: purchase_id
    user_id: customer_id
    order_date: purchase_date
```

### 3. 验证映射完整性

确保所有重要列都被映射：

```python
# 检查是否有未映射的列
unmapped_source = set(source_columns) - set(mappings.keys())
unmapped_target = set(target_columns) - set(mappings.values())

if unmapped_source:
    print(f"Warning: Unmapped source columns: {unmapped_source}")
if unmapped_target:
    print(f"Warning: Unmapped target columns: {unmapped_target}")
```

## 限制和注意事项

### 1. 当前限制
- 只支持 1:1 列映射（不支持一对多或多对一）
- 不支持列值转换（只映射名称）
- 不支持动态映射（基于条件的映射）

### 2. 性能影响
- 列映射本身对性能影响极小
- 主要性能取决于比对的数据量

### 3. 兼容性
- 支持所有数据库类型
- 与所有其他功能兼容（采样、容差等）

## 故障排查

### 问题 1：映射的列未找到

**错误信息**：
```
Column mapping warning: Source column 'user_id' in mapping not found in source table
```

**解决方案**：
1. 检查列名拼写
2. 确认列是否存在于表中
3. 检查大小写设置

### 问题 2：列数不匹配

**错误信息**：
```
The provided columns are of a different count
```

**解决方案**：
- 使用列映射时，这个错误不应出现
- 如果出现，检查是否正确配置了 `columnRemapping`

### 问题 3：重复映射

**症状**：
同一个目标列被多个源列映射

**解决方案**：
检查映射配置，确保每个目标列只被映射一次

## SQL 示例

### 验证映射的查询

```sql
-- 检查源表列
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'source_table'
ORDER BY ordinal_position;

-- 检查目标表列
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'target_table'
ORDER BY ordinal_position;
```

### 手动验证映射结果

```sql
-- 使用映射关系手动比对
SELECT 
    s.user_id AS source_id,
    t.customer_id AS target_id,
    s.user_name AS source_name,
    t.customer_name AS target_name,
    CASE 
        WHEN s.user_id = t.customer_id 
         AND s.user_name = t.customer_name 
        THEN 'MATCH'
        ELSE 'DIFF'
    END AS comparison_result
FROM source_table s
FULL OUTER JOIN target_table t 
    ON s.user_id = t.customer_id;
```

## 未来改进计划

1. **多对一映射**：支持多个源列映射到一个目标列
2. **值转换**：支持简单的值转换规则
3. **映射模板**：预定义的常用映射模板
4. **智能建议**：基于 AI 的列映射建议
5. **可视化映射**：图形化的映射配置界面