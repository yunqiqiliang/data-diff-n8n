# 使用 Prepare for Comparison 操作的最佳实践

## 概述

`Prepare for Comparison` 是专门为数据比对设计的操作，它在一个输出中包含了所有必要的信息：
- 连接配置
- 可用表列表
- 数据库元信息
- 示例数据（可选）

## 输出格式

### DatabaseConnector 的 prepareComparison 输出
```json
{
  "operation": "prepareComparison",
  "databaseType": "postgresql",
  "success": true,
  "connectionUrl": "postgresql://user:pass@host:5432/db",
  "connectionConfig": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "username": "user",
    "password": "pass",
    "database": "mydb",
    "schema": "public"
  },
  "tables": [
    {
      "name": "public.users",
      "value": "public.users",
      "description": "Table: users"
    },
    {
      "name": "public.orders",
      "value": "public.orders", 
      "description": "Table: orders"
    }
  ],
  "schema": "public",
  "statistics": {
    "totalTables": 15,
    "schema": "public",
    "databaseType": "postgresql",
    "databaseVersion": "PostgreSQL 14.2"
  },
  "sampleData": {
    "public.users": {
      "rows": [...],
      "rowCount": 5,
      "columns": ["id", "name", "email", "created_at"]
    }
  },
  "metadata": {
    "totalTables": 15,
    "includedSampleData": true,
    "preparedAt": "2025-07-12T10:00:00.000Z"
  },
  "comparisonReady": true,
  "comparisonConfig": {
    "source_config": {...},
    "available_tables": [...],
    "database_type": "postgresql"
  }
}
```

### ClickzettaConnector 的 prepareComparison 输出
```json
{
  "operation": "prepareComparison",
  "success": true,
  "connectionUrl": "clickzetta://user:pass@instance.service/workspace?vcluster=vc",
  "connectionConfig": {
    "type": "clickzetta",
    "username": "user",
    "password": "pass",
    "instance": "instance",
    "service": "service",
    "workspace": "workspace",
    "vcluster": "vcluster",
    "schema": "default"
  },
  "tables": [
    {
      "name": "default.users",
      "value": "default.users",
      "description": "Clickzetta table: users"
    }
  ],
  "statistics": {
    "totalTables": 10,
    "schema": "default",
    "databaseType": "clickzetta",
    "workspace": "workspace",
    "vcluster": "vcluster",
    "clickzettaVersion": "1.0.0"
  },
  "sampleData": {
    "default.users": {
      "rows": [...],
      "rowCount": 5,
      "columns": ["id", "name", "email"],
      "samplingMethod": "TABLESAMPLE"
    }
  },
  "comparisonReady": true,
  "clickzettaSpecific": {
    "supportsTablesample": true,
    "supportedSamplingMethods": ["TABLESAMPLE", "SYSTEM", "ROW"]
  }
}
```

## 使用场景

### 1. 基本比对流程
```
[DatabaseConnector: Prepare for Comparison] ────┐
                                                 ├──> [Data Comparison]
[ClickzettaConnector: Prepare for Comparison] ──┘
```

### 2. 带过滤的比对
配置 Prepare for Comparison：
- Table Filter: `user%` (只包含以 user 开头的表)
- Include Sample Data: true
- Sample Size: 10

### 3. 多环境比对
```
[Dev DB: Prepare for Comparison] ──┐
                                   ├──> [Data Comparison] ──> [Report Differences]
[Prod DB: Prepare for Comparison] ─┘
```

## 数据比对节点的智能识别

当数据比对节点检测到输入包含 `comparisonReady: true` 时，会自动：
1. 提取 `connectionConfig` 作为数据库连接
2. 从 `tables` 数组中提供表选择
3. 使用 `statistics` 显示数据库信息
4. 利用 `sampleData` 进行预览

## 配置示例

### DatabaseConnector 配置
```yaml
Operation: Prepare for Comparison
Schema Name: public
Include Sample Data: true
Sample Size: 5
Table Filter: order_%  # 只包含订单相关的表
```

### ClickzettaConnector 配置
```yaml
Operation: Prepare for Comparison
Schema Name: dw
Include Sample Data: true
Sample Size: 10
Table Filter: %_fact, %_dim  # 只包含事实表和维度表
```

## 性能优化

1. **表过滤**: 使用 Table Filter 减少不必要的表扫描
2. **示例数据**: 只在需要预览时启用
3. **采样大小**: 保持较小的 Sample Size (5-10 行)
4. **缓存**: prepareComparison 的结果可以被多个比对重用

## 错误处理

如果 prepareComparison 失败，输出会包含：
```json
{
  "operation": "prepareComparison",
  "success": false,
  "error": "Failed to connect to database: connection timeout",
  "timestamp": "2025-07-12T10:00:00.000Z"
}
```

## 最佳实践

1. **一次准备，多次使用**: prepareComparison 的输出可以连接到多个数据比对节点
2. **使用 Set 节点保存配置**: 将 prepareComparison 的输出保存，在工作流中重用
3. **条件过滤**: 根据 `comparisonReady` 字段判断是否可以进行比对
4. **错误重试**: 使用 IF 节点检查 `success` 字段，失败时重试

这种设计大大简化了数据比对的配置流程，让用户只需要一步操作就能准备好所有必要的信息。