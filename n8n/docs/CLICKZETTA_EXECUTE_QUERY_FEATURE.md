# ClickzettaConnector Execute Query 功能说明

## 功能概述

为 ClickzettaConnector 节点添加了 "Execute Query" 操作，允许用户执行自定义 SQL 查询并获取结果。

## 新增功能

### 1. Execute Query 操作
- **功能**: 执行用户输入的 SQL 查询
- **输入**: 用户提供的 SQL 语句
- **输出**: 查询结果数据

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
      "query": "SELECT * FROM users LIMIT 10",
      "timestamp": "2025-07-11T12:00:00.000Z"
    }
  }
]
```

## API 集成

### 查询执行 API
- **默认端点**: `http://data-diff-api:8000/api/v1/query/execute`
- **环境变量**: `CLICKZETTA_QUERY_API_URL`
- **方法**: POST
- **请求体**:
```json
{
  "connection": {
    "username": "user",
    "password": "pass",
    "instance": "instance",
    "service": "service",
    "workspace": "workspace",
    "vcluster": "vcluster",
    "schema": "schema",
    "type": "clickzetta"
  },
  "query": "SELECT * FROM table_name"
}
```

### 预期响应
```json
{
  "success": true,
  "result": [
    {"column1": "value1", "column2": "value2"},
    {"column1": "value3", "column2": "value4"}
  ]
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

### 示例 3: 表信息查询
```sql
SHOW TABLES
```

### 示例 4: 表结构查询
```sql
DESCRIBE users
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
  "success": false,
  "error": "具体错误信息",
  "timestamp": "2025-07-11T12:00:00.000Z"
}
```

## 实现细节

### 新增方法

#### executeQueryMethod
- **功能**: 执行 SQL 查询的核心方法
- **参数**: credentials, sqlQuery
- **返回**: 查询结果或错误

#### formatQueryResults
- **功能**: 格式化查询结果为 N8N 兼容格式
- **参数**: result (查询结果)
- **返回**: 格式化后的结果数组

## 配置要求

### 环境变量
- `CLICKZETTA_QUERY_API_URL`: 查询 API 端点 URL（可选）

### 依赖服务
- 后端 API 服务必须支持 `/api/v1/query/execute` 端点
- Clickzetta 数据库连接必须正常

## 测试建议

1. **连接测试**: 首先使用 "Test Connection" 验证数据库连接
2. **简单查询**: 测试基本的 SELECT 语句
3. **复杂查询**: 测试 JOIN、聚合等复杂查询
4. **错误处理**: 测试无效 SQL 语句的错误处理
5. **输出格式**: 验证两种输出格式的差异

## 安全注意事项

1. **SQL 注入**: 用户输入的 SQL 需要在后端进行适当的验证和清理
2. **权限控制**: 确保用户只能执行被授权的查询
3. **资源限制**: 考虑添加查询超时和结果集大小限制

## 后续改进建议

1. **查询模板**: 提供常用查询模板供用户选择
2. **参数化查询**: 支持参数化 SQL 查询
3. **查询历史**: 保存和重用查询历史
4. **结果缓存**: 缓存查询结果以提高性能
5. **查询分析**: 提供查询执行计划和性能分析
