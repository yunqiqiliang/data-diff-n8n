# 数据库连接器节点输出增强

## 概述
此次更新为 `DatabaseConnector` 和 `ClickzettaConnector` 节点添加了以下功能：

1. **Test Connection 操作输出连接 URL**
2. **List Tables 操作输出格式化的表列表**
3. **Data Comparison 节点支持从上游节点自动获取连接信息**

## 功能详情

### 1. DatabaseConnector 节点增强

#### Test Connection 操作
- **新增输出**: `connectionUrl` - 完整的数据库连接 URL
- **新增输出**: `connectionConfig` - 连接配置对象
- **支持的数据库类型**: PostgreSQL, MySQL, ClickHouse, Clickzetta, Snowflake, BigQuery, Redshift, Oracle, SQL Server, DuckDB, Databricks, Trino, Presto, Vertica

#### List Tables 操作
- **新增输出**: `tables` - 格式化的表列表数组
- **表项格式**:
  ```json
  {
    "name": "schema.table_name",
    "value": "schema.table_name",
    "description": "Table: table_name"
  }
  ```

#### 连接 URL 格式示例
```
// PostgreSQL
postgresql://user:pass@host:5432/database?sslmode=require

// Clickzetta
clickzetta://user:pass@instance/service?workspace=ws&vcluster=vc&schema=public

// MySQL
mysql://user:pass@host:3306/database?ssl=true
```

### 2. ClickzettaConnector 节点增强

#### Test Connection 操作
- **新增输出**: `connectionUrl` - Clickzetta 连接 URL
- **新增输出**: `connectionConfig` - 包含 type: 'clickzetta' 的配置对象

#### List Tables 操作
- **新增输出**: `tables` - 格式化的 Clickzetta 表列表
- **表项格式**:
  ```json
  {
    "name": "schema.table_name",
    "value": "schema.table_name",
    "description": "Clickzetta table: table_name"
  }
  ```

### 3. DataComparison 节点增强

#### 新增功能
- **自动填充上游数据**: 新增 `autoFillFromUpstream` 参数
- **智能连接获取**: 自动从上游节点获取连接 URL
- **智能表选择**: 自动从上游节点获取表列表

#### 上游数据处理逻辑
1. 如果启用了 `autoFillFromUpstream`，会从输入数据中提取：
   - 连接 URL (`connectionUrl`)
   - 表列表 (`tables`)
2. 如果用户未填写连接字符串，会自动使用上游数据
3. 如果用户未填写表名，会自动使用上游数据

#### 输出增强
- **新增输出**: `upstreamData` - 从上游节点提取的数据
- **新增输出**: `availableConnections` - 可用连接列表
- **新增输出**: `availableTables` - 可用表列表

## 使用流程

### 场景 1: 连接测试 → 数据比较
```
[DatabaseConnector] → [DataComparison]
    ↓ Test Connection        ↓ 自动获取连接 URL
    connectionUrl        →   sourceConnection/targetConnection
```

### 场景 2: 表列表 → 数据比较
```
[DatabaseConnector] → [DataComparison]
    ↓ List Tables           ↓ 自动获取表列表
    tables             →    sourceTable/targetTable
```

### 场景 3: 完整工作流
```
[DatabaseConnector A] → [DatabaseConnector B] → [DataComparison]
    ↓ Test Connection       ↓ Test Connection       ↓ 自动获取两个连接
    connectionUrl      →    connectionUrl      →    source + target

[DatabaseConnector A] → [DataComparison]
    ↓ List Tables           ↓ 自动获取表列表
    tables             →    sourceTable/targetTable
```

## 技术实现

### 核心方法

#### DatabaseConnector
- `buildConnectionUrl(config)` - 构建各种数据库的连接 URL
- `formatTablesForSelection(result, config)` - 格式化表列表为选择项

#### ClickzettaConnector
- `buildConnectionUrl(credentials)` - 构建 Clickzetta 连接 URL
- `formatTablesForSelection(result, credentials)` - 格式化 Clickzetta 表列表

#### DataComparison
- `extractUpstreamData(items, autoFill)` - 从上游节点提取数据
- 修改的 `compareTables()` 和 `compareSchemas()` 方法支持上游数据

### 数据流格式

#### Test Connection 输出
```json
{
  "operation": "testConnection",
  "databaseType": "postgresql",
  "success": true,
  "connectionUrl": "postgresql://user:pass@host:5432/db",
  "connectionConfig": { ... },
  "data": { ... },
  "timestamp": "2025-07-09T..."
}
```

#### List Tables 输出
```json
{
  "operation": "listTables",
  "databaseType": "postgresql",
  "success": true,
  "tables": [
    {
      "name": "public.users",
      "value": "public.users",
      "description": "Table: users"
    }
  ],
  "data": { ... },
  "timestamp": "2025-07-09T..."
}
```

## 向后兼容性
- 所有现有功能保持不变
- 新增的输出字段不会影响现有工作流
- DataComparison 节点仍支持手动输入连接字符串和表名

## 配置建议
1. 在 DataComparison 节点中启用 `autoFillFromUpstream`
2. 将 DatabaseConnector/ClickzettaConnector 节点的输出直接连接到 DataComparison 节点
3. 可以串联多个连接器节点来比较不同数据库
