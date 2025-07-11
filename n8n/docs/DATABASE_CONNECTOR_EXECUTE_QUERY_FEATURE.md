# DatabaseConnector Execute Query 功能说明

## 功能概述

为 DatabaseConnector 节点添加了 "Execute Query" 操作，允许用户执行自定义 SQL 查询并获取结果。该功能与 ClickzettaConnector 节点保持一致，支持多种数据库类型。

## 新增功能

### 1. Execute Query 操作
- **功能**: 执行用户输入的 SQL 查询
- **输入**: 用户提供的 SQL 语句
- **输出**: 查询结果数据
- **支持数据库**: PostgreSQL, MySQL, Clickzetta, SQL Server, Oracle, Trino/Presto, DuckDB 等

### 2. 新增参数

#### SQL Query (sqlQuery)
- **类型**: 多行文本输入
- **默认值**: 空
- **占位符**: `SELECT * FROM table_name LIMIT 10`
- **描述**: 要执行的 SQL 查询语句
- **显示条件**: 仅在选择 "Execute Query" 操作时显示

#### Return Raw Results (returnRawResults)
- **类型**: 布尔值
- **默认值**: false
- **描述**: 是否返回原始查询结果或格式化数据
- **显示条件**: 仅在选择 "Execute Query" 操作时显示

## 输出格式

### 原始结果模式 (returnRawResults = true)
返回单个 JSON 对象，包含完整的查询结果：
```json
{
  "operation": "executeQuery",
  "databaseType": "postgresql",
  "success": true,
  "query": "SELECT * FROM users LIMIT 10",
  "data": [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
  ],
  "timestamp": "2025-07-11T12:00:00.000Z"
}
```

### 格式化结果模式 (returnRawResults = false)
返回多个 JSON 对象，每行数据一个对象：
```json
[
  {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "_meta": {
      "operation": "executeQuery",
      "databaseType": "postgresql",
      "query": "SELECT * FROM users LIMIT 10",
      "timestamp": "2025-07-11T12:00:00.000Z"
    }
  },
  {
    "id": 2,
    "name": "Bob",
    "email": "bob@example.com",
    "_meta": {
      "operation": "executeQuery",
      "databaseType": "postgresql",
      "query": "SELECT * FROM users LIMIT 10",
      "timestamp": "2025-07-11T12:00:00.000Z"
    }
  }
]
```

## API 集成

### 查询执行 API
- **默认端点**: `http://data-diff-api:8000/api/v1/query/execute`
- **环境变量**: `DATABASE_QUERY_API_URL`
- **方法**: POST
- **请求体**:
```json
{
  "connection": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "username": "user",
    "password": "pass",
    "database": "mydb",
    "schema": "public"
  },
  "query": "SELECT * FROM table_name"
}
```

### 支持的数据库类型

#### PostgreSQL
```json
{
  "type": "postgresql",
  "host": "localhost",
  "port": 5432,
  "username": "user",
  "password": "pass",
  "database": "mydb",
  "schema": "public"
}
```

#### MySQL
```json
{
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "username": "user",
  "password": "pass",
  "database": "mydb"
}
```

#### Clickzetta
```json
{
  "type": "clickzetta",
  "username": "user",
  "password": "pass",
  "instance": "instance",
  "service": "service",
  "workspace": "workspace",
  "vcluster": "vcluster",
  "schema": "public"
}
```

#### SQL Server
```json
{
  "type": "sqlserver",
  "host": "localhost",
  "port": 1433,
  "username": "user",
  "password": "pass",
  "database": "mydb"
}
```

## 使用示例

### 示例 1: 基本查询
```sql
SELECT * FROM users LIMIT 10
```

### 示例 2: 聚合查询
```sql
SELECT COUNT(*) as total_users, 
       AVG(age) as avg_age 
FROM users 
WHERE status = 'active'
```

### 示例 3: 跨表查询
```sql
SELECT u.name, o.total_amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.created_at > '2024-01-01'
```

### 示例 4: 数据修改（谨慎使用）
```sql
UPDATE users 
SET last_login = NOW() 
WHERE id = 123
```

## 错误处理

### 输入验证
- 检查 SQL 查询是否为空
- 如果查询为空，返回错误: "SQL query is required"

### API 错误处理
- 网络请求失败
- API 返回错误状态码
- 查询执行失败

### 错误输出格式
```json
{
  "operation": "executeQuery",
  "databaseType": "postgresql",
  "success": false,
  "error": "具体错误信息",
  "timestamp": "2025-07-11T12:00:00.000Z"
}
```

## 实现细节

### 新增方法

#### executeQueryMethod
- **功能**: 执行 SQL 查询的核心方法
- **参数**: config (数据库配置), sqlQuery (SQL查询)
- **返回**: 查询结果或错误

#### formatQueryResults
- **功能**: 格式化查询结果为 N8N 兼容格式
- **参数**: result (查询结果)
- **返回**: 格式化后的结果数组

### 与 ClickzettaConnector 的区别

1. **数据库类型标识**: DatabaseConnector 在输出中包含 `databaseType` 字段
2. **连接配置**: 支持更多数据库类型的连接配置
3. **通用性**: 可以连接任何支持的数据库类型

## 配置要求

### 环境变量
- `DATABASE_QUERY_API_URL`: 查询 API 端点 URL（可选）

### 依赖服务
- 后端 API 服务必须支持 `/api/v1/query/execute` 端点
- 对应的数据库驱动必须正确安装

## 测试建议

1. **连接测试**: 首先使用 "Test Connection" 验证数据库连接
2. **简单查询**: 测试基本的 SELECT 语句
3. **复杂查询**: 测试 JOIN、聚合等复杂查询
4. **不同数据库**: 测试不同数据库类型的查询
5. **错误处理**: 测试无效 SQL 语句的错误处理
6. **输出格式**: 验证两种输出格式的差异

## 安全注意事项

1. **SQL 注入**: 用户输入的 SQL 需要在后端进行适当的验证和清理
2. **权限控制**: 确保用户只能执行被授权的查询
3. **数据修改**: 谨慎使用 INSERT、UPDATE、DELETE 等修改操作
4. **资源限制**: 考虑添加查询超时和结果集大小限制

## 最佳实践

1. **查询优化**: 使用适当的 WHERE 条件和 LIMIT 限制结果集
2. **索引使用**: 确保查询使用适当的索引
3. **事务处理**: 对于修改操作，考虑使用事务
4. **监控**: 监控查询性能和资源使用情况

## 后续改进建议

1. **查询模板**: 提供常用查询模板供用户选择
2. **参数化查询**: 支持参数化 SQL 查询
3. **查询历史**: 保存和重用查询历史
4. **结果导出**: 支持将查询结果导出为 CSV、JSON 等格式
5. **查询计划**: 提供查询执行计划分析
6. **连接池**: 实现数据库连接池以提高性能
