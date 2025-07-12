# 数据比对节点最优集成指南

## 概述

新的双输入数据比对节点专门设计用于与 DatabaseConnector 和 ClickzettaConnector 无缝集成，提供智能的自动配置检测。

## 核心特性

### 1. 智能配置检测

节点会自动从上游连接器提取：
- **连接配置**：`connectionConfig`、`connectionUrl`
- **数据库类型**：`databaseType`、`type`
- **表信息**：`tableName`、`tables`、从 SQL 查询提取
- **元数据**：`_meta` 字段中的信息

### 2. 灵活的表名检测策略

```javascript
// 检测优先级
1. 直接字段：tableName, table, table_name
2. 元数据：_meta.tableName, _meta.query
3. SQL 解析：FROM 子句提取
4. 表列表：tables[0]
```

## 最佳实践工作流

### 1. 基础比对流程
```
[DatabaseConnector: List Tables] ──┐
                                   ├──> [Data Comparison] ──> [Email Report]
[ClickzettaConnector: List Tables] ┘
```

### 2. 查询结果比对
```
[DatabaseConnector: Execute Query] ──┐
                                     ├──> [Data Comparison] ──> [Slack Alert]
[ClickzettaConnector: Execute Query] ┘
```

### 3. 多表批量比对
```
[Get Table List] ──> [Loop Over Tables] ──┐
                                          ├──> [DatabaseConnector] ──┐
                                          │                          ├──> [Data Comparison] ──> [Aggregate Results]
                                          └──> [ClickzettaConnector] ┘
```

## 连接器输出格式兼容性

### DatabaseConnector 输出
```json
{
  "operation": "testConnection",
  "databaseType": "postgresql",
  "connectionUrl": "postgresql://user:pass@host:5432/db",
  "connectionConfig": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "mydb"
  }
}
```

### ClickzettaConnector 输出
```json
{
  "operation": "listTables",
  "tables": [
    {
      "name": "users",
      "schema": "public",
      "type": "TABLE"
    }
  ],
  "connectionUrl": "clickzetta://user:pass@host:9000/db",
  "connectionConfig": {
    "type": "clickzetta",
    "host": "clickzetta.example.com"
  }
}
```

## 配置模式

### 1. 自动检测模式（推荐）
```json
{
  "configMode": "auto",
  "tableSelection": "auto",
  "keyColumns": "",  // 自动检测主键
  "advancedOptions": {
    "algorithm": "auto",
    "enableSampling": true
  }
}
```

### 2. 半自动模式
```json
{
  "configMode": "auto",
  "tableSelection": "specified",
  "sourceTable": "public.users",
  "targetTable": "dw.users",
  "keyColumns": "id,created_at"
}
```

### 3. 手动模式
```json
{
  "configMode": "manual",
  "sourceConfig": {
    "connectionString": "postgresql://...",
    "tableName": "users"
  },
  "targetConfig": {
    "connectionString": "clickzetta://...",
    "tableName": "users_copy"
  }
}
```

## 高级功能集成

### 1. 采样优化
- **ClickZetta**：自动使用 `TABLESAMPLE` 语法
- **PostgreSQL**：使用 `TABLESAMPLE BERNOULLI`
- **MySQL**：回退到 `RAND()` 方法

### 2. 算法自动选择
```javascript
// 自动选择逻辑
if (sourceType === targetType) {
  // 同类型数据库：使用 JoinDiff
  algorithm = 'joindiff';
} else {
  // 跨数据库：使用 HashDiff
  algorithm = 'hashdiff';
}
```

### 3. 错误处理
- 连接失败时的友好提示
- 表名未找到时的建议
- 权限不足时的说明

## 性能优化建议

1. **大表比对**
   - 启用智能采样
   - 使用 SYSTEM 采样方法
   - 设置合理的线程数

2. **跨地域比对**
   - 使用 HashDiff 算法
   - 增加 bisection_factor
   - 考虑时间窗口过滤

3. **实时监控**
   - 使用较小的采样比例
   - 设置定时触发
   - 配置阈值告警

## 故障排除

### 常见问题

1. **"Cannot detect table name"**
   - 确保上游节点执行了 listTables 或包含表名信息
   - 手动指定表名

2. **"Connection configuration missing"**
   - 检查上游节点是否正确配置了凭据
   - 确保节点之间正确连接

3. **"Sampling not supported"**
   - 某些数据库不支持原生采样
   - 系统会自动回退到其他方法

## 示例工作流模板

### 数据质量检查
```yaml
nodes:
  - name: Source DB
    type: DatabaseConnector
    operation: listTables
    
  - name: Target DB  
    type: ClickzettaConnector
    operation: listTables
    
  - name: Compare
    type: DataComparisonDualInput
    configMode: auto
    advancedOptions:
      enableSampling: true
      samplingConfidence: 0.95
      
  - name: Quality Report
    type: EmailSend
    condition: "{{$json.comparison_result.statistics.match_rate < 0.99}}"
```

这种设计充分利用了 n8n 的数据流特性，让数据比对变得简单而强大。