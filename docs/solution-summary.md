# 解决方案总结：智能获取前序节点参数

## 问题分析

原始问题：
- DatabaseConnector 和 ClickzettaConnector 的不同操作返回不同的数据结构
- `testConnection` 有连接信息但没有表信息
- `listTables` 有表信息但没有连接信息
- `executeQuery` 只有查询结果，既没有连接信息也没有表列表
- 数据比对节点无法可靠地自动获取所需的所有参数

## 解决方案：专用的 prepareComparison 操作

### 核心设计
为 DatabaseConnector 和 ClickzettaConnector 添加了专门的 `Prepare for Comparison` 操作，一次性提供所有必要信息。

### 输出结构
```json
{
  "comparisonReady": true,  // 标识符，表示这是为比对准备的数据
  "connectionUrl": "...",
  "connectionConfig": {...},
  "tables": [...],
  "comparisonConfig": {
    "source_config": {...},
    "available_tables": [...],
    "database_type": "..."
  }
}
```

### 智能识别机制

#### 1. DataComparisonDualInput 的 extractNodeConfig 方法
```typescript
// 优先检查 prepareComparison 的输出
if (json.comparisonReady === true && json.comparisonConfig) {
    return {
        connectionUrl: json.connectionUrl,
        connectionConfig: compConfig.source_config,
        type: compConfig.database_type,
        isPrepared: true,
        availableTables: compConfig.available_tables
    };
}
// 然后回退到其他检测逻辑...
```

#### 2. detectTableName 方法增强
```typescript
// 检查 prepareComparison 的表列表
if (json.comparisonReady === true && json.tables) {
    if (json.tables.length > 0) {
        return json.tables[0].value;
    }
}
// 其他检测逻辑...
```

#### 3. convertToApiConfig 方法优化
```typescript
// 如果来自 prepareComparison，直接使用准备好的配置
if (config.isPrepared && config.connectionConfig) {
    return config.connectionConfig;
}
// 其他转换逻辑...
```

## 实现细节

### DatabaseConnector 的 prepareComparison
1. 获取表列表（带过滤）
2. 获取数据库版本信息
3. 可选：获取示例数据
4. 返回标准化的比对配置

### ClickzettaConnector 的 prepareComparison
1. 获取表列表（支持 workspace/vcluster）
2. 使用 TABLESAMPLE 高效采样
3. 包含 Clickzetta 特定的元数据
4. 返回优化的比对配置

## 使用优势

### 1. 简化的工作流
之前：
```
testConnection ──> Merge ──┐
                           ├──> Data Comparison (复杂的参数提取)
listTables ────────────────┘
```

现在：
```
prepareComparison ──> Data Comparison (自动识别所有参数)
```

### 2. 可靠的参数检测
- 使用 `comparisonReady` 标识符确保数据来源正确
- 所有必要信息在一个输出中，避免了复杂的查找逻辑
- 保持向后兼容性（仍支持其他操作的输出）

### 3. 性能优化
- 一次操作获取所有信息，减少数据库查询
- 支持表过滤，避免不必要的元数据查询
- 可选的示例数据，按需获取

### 4. 扩展性
- 易于添加新的元数据（如索引信息、统计信息等）
- 数据库特定的优化（如 ClickZetta 的 TABLESAMPLE）
- 标准化的输出格式，便于未来扩展

## 错误处理

### 优雅降级
1. 如果 prepareComparison 不可用，回退到原有的检测逻辑
2. 清晰的错误提示，指导用户使用正确的操作
3. 保持与旧版工作流的兼容性

### 调试支持
- 在输出中包含 `metadata` 字段，记录准备过程的详细信息
- 保留原始的连接配置，便于调试
- 时间戳记录，方便追踪数据准备时间

## 最佳实践

1. **推荐使用 prepareComparison**：新建工作流时优先使用此操作
2. **缓存结果**：prepareComparison 的输出可以缓存，避免重复查询
3. **批量处理**：一次准备可以供多个比对任务使用
4. **条件过滤**：使用 Table Filter 减少不必要的表扫描

## 总结

通过添加专门的 `prepareComparison` 操作，我们彻底解决了"从前序节点获取合适参数"的核心问题。这个解决方案：

- ✅ 提供了可靠的自动参数检测
- ✅ 简化了用户配置
- ✅ 保持了向后兼容性
- ✅ 提升了性能和用户体验
- ✅ 为未来扩展留下了空间

这是一个优雅的解决方案，将复杂的参数获取逻辑封装在了数据准备阶段，让数据比对节点可以专注于其核心功能。