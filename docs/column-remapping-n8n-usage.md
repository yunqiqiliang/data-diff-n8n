# 列重映射功能 - n8n 使用指南

## 快速开始

### 场景：比较两个列名不同的表

假设你需要比较旧系统和新系统的用户数据，但它们的列名不同。

## 使用步骤

### 1. 创建工作流

```
[Database Connector 1] ─┐
                        ├─> [Data Comparison] ─> [输出结果]
[Database Connector 2] ─┘
```

### 2. 配置 Data Comparison 节点

#### 基本配置
- **Operation**: Compare Table
- **Source Table**: users_old
- **Target Table**: customers_new
- **Key Columns**: user_id（源表的主键列名）

#### 高级选项（Advanced Options）

找到 **Column Remapping** 字段并输入：

```
user_id:customer_id,user_name:customer_name,created_at:creation_date
```

### 3. 配置解释

| 配置项 | 值 | 说明 |
|--------|-----|------|
| Column Remapping | `user_id:customer_id` | 将源表的 user_id 映射到目标表的 customer_id |
| Case-Sensitive Remapping | true/false | 是否区分大小写 |

## 常见使用场景

### 场景 1：数据库迁移验证

**问题**：从 MySQL 迁移到 PostgreSQL，列名从驼峰改为下划线

**源表（MySQL）**：
- userId
- userName
- createdAt

**目标表（PostgreSQL）**：
- user_id
- user_name
- created_at

**配置**：
```
userId:user_id,userName:user_name,createdAt:created_at
```

### 场景 2：跨系统数据同步

**问题**：CRM 和 ERP 系统的列名不同

**CRM 系统**：
- customerId
- customerName
- contactEmail

**ERP 系统**：
- cust_id
- cust_name
- email

**配置**：
```
customerId:cust_id,customerName:cust_name,contactEmail:email
```

### 场景 3：大小写不敏感匹配

**问题**：Oracle（大写）vs PostgreSQL（小写）

**Oracle**：
- USER_ID
- USER_NAME
- CREATED_DATE

**PostgreSQL**：
- user_id
- user_name
- created_date

**配置**：
- Column Remapping: 留空
- Case-Sensitive Remapping: **false**（关闭大小写敏感）

系统会自动匹配对应的列。

## 高级技巧

### 1. 部分映射

你不需要映射所有列。相同名称的列会自动匹配：

```
# 只映射不同的列
created_at:creation_date,modified_at:last_modified

# email、status 等相同名称的列会自动匹配
```

### 2. 验证映射

运行比对前，系统会：
1. 检查映射的列是否存在
2. 显示警告（如果有列不存在）
3. 显示实际会比对的列对

### 3. 调试技巧

如果映射不工作：
1. 检查列名拼写
2. 确认大小写设置
3. 查看执行日志中的映射信息

## 完整示例工作流

### 输入数据

**源表数据**：
```json
[
  {
    "user_id": 1,
    "user_name": "John Doe",
    "created_at": "2024-01-01"
  }
]
```

**目标表数据**：
```json
[
  {
    "customer_id": 1,
    "customer_name": "John Doe",
    "creation_date": "2024-01-01"
  }
]
```

### 配置

```json
{
  "operation": "compareTable",
  "sourceTable": "users",
  "targetTable": "customers",
  "keyColumns": "user_id",
  "columnRemapping": "user_id:customer_id,user_name:customer_name,created_at:creation_date"
}
```

### 预期结果

系统会正确比对这些列，即使它们的名称不同。如果数据一致，将显示"无差异"。

## 注意事项

1. **映射格式**：必须使用冒号（:）分隔，逗号（,）连接多个映射
2. **空格处理**：系统会自动去除前后空格
3. **重复映射**：避免将多个源列映射到同一个目标列
4. **性能影响**：列映射本身不影响性能

## 故障排查

### 错误：列未找到
```
Column mapping warning: Source column 'user_id' in mapping not found in source table
```
**解决**：检查列名拼写和大小写

### 错误：列数不匹配
如果没有使用列映射，可能会看到：
```
The provided columns are of a different count
```
**解决**：使用列映射功能

## API 使用示例

如果通过 API 调用：

```bash
curl -X POST http://localhost:5000/api/compare \
  -H "Content-Type: application/json" \
  -d '{
    "source": {
      "table": "users_old",
      "connection_id": "source_db"
    },
    "target": {
      "table": "customers_new",
      "connection_id": "target_db"
    },
    "comparison": {
      "key_columns": "user_id",
      "column_remapping": "user_id:customer_id,user_name:customer_name"
    }
  }'
```

## 总结

列重映射功能让你能够：
- ✅ 比较列名不同的表
- ✅ 处理不同的命名约定
- ✅ 简化数据迁移验证
- ✅ 支持跨系统数据比对

无需修改原始表结构，即可进行准确的数据比对！