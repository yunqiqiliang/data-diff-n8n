# JSON 列智能比对功能指南

## 概述

JSON 列智能比对功能允许在比对包含 JSON/JSONB 数据类型的表时，使用不同的比对策略来处理 JSON 数据的特殊性。这对于处理非结构化或半结构化数据特别有用。

## 功能特点

### 1. 支持的 JSON 类型

- **PostgreSQL**: `json`, `jsonb`
- **MySQL**: `json` (5.7+)
- **SQL Server**: `json` (通过 nvarchar 存储)
- **BigQuery**: `JSON` 
- **Snowflake**: `VARIANT`/`OBJECT`
- **其他数据库**: 作为文本字符串比较

### 2. 比对模式

#### Exact（精确匹配）
- JSON 必须完全相同，包括：
  - 空格和换行
  - 键的顺序
  - 格式化方式
- 适用场景：需要确保 JSON 完全未被修改

```json
// 这两个会被认为不同
{"name": "John", "age": 30}
{"age": 30, "name": "John"}
```

#### Normalized（标准化）- 推荐
- 忽略格式差异：
  - 空格、换行、缩进
  - 尾随逗号
- 保留键顺序敏感性
- 适用场景：大多数业务场景

```json
// 这两个会被认为相同
{"name": "John", "age": 30}
{
  "name": "John",
  "age": 30
}
```

#### Semantic（语义比对）
- 完全语义等价比较：
  - 忽略键顺序
  - 忽略数组元素顺序（可配置）
  - 递归处理嵌套结构
- 数据库支持情况：
  - ✅ PostgreSQL (jsonb)
  - ✅ Snowflake
  - ⚠️ MySQL (部分支持)
  - ⚠️ BigQuery (部分支持)
  - ❌ 其他数据库（降级为 Normalized）

```json
// 这两个会被认为相同
{"name": "John", "age": 30, "hobbies": ["reading", "gaming"]}
{"age": 30, "hobbies": ["gaming", "reading"], "name": "John"}
```

#### Keys Only（仅比较键）
- 只比较 JSON 结构（键），忽略值
- 用于检测模式变化
- 适用场景：数据迁移验证、API 版本兼容性检查

```json
// 这两个会被认为相同（因为键相同）
{"name": "John", "age": 30}
{"name": "Jane", "age": 25}

// 这两个会被认为不同（键不同）
{"name": "John", "age": 30}
{"name": "John", "age": 30, "email": "john@example.com"}
```

## 在 n8n 中使用

### 配置参数

在 Data Comparison 节点的 Advanced Options 中：

```
JSON Comparison Mode: Normalized
```

**参数说明**：
- **默认值**: normalized（推荐）
- **选项**: exact, normalized, semantic, keys_only

## 使用示例

### 示例 1：API 响应验证

**场景**：验证两个系统的 API 响应是否一致

```javascript
{
  "operation": "compareTable",
  "sourceTable": "api_responses_system_a",
  "targetTable": "api_responses_system_b",
  "keyColumns": "request_id",
  "jsonComparisonMode": "semantic",
  "columnsToCompare": "request_id,endpoint,response_body,timestamp"
}
```

**说明**：使用 semantic 模式忽略 JSON 响应中的键顺序差异

### 示例 2：配置文件同步验证

**场景**：确保配置文件在多个环境中保持一致

```javascript
{
  "operation": "compareTable",
  "sourceTable": "config_prod",
  "targetTable": "config_staging",
  "keyColumns": "config_name",
  "jsonComparisonMode": "exact",
  "columnsToCompare": "config_name,config_data,last_updated"
}
```

**说明**：使用 exact 模式确保配置完全相同

### 示例 3：数据模式演化检测

**场景**：检测 JSON 数据的模式是否发生变化

```javascript
{
  "operation": "compareTable",
  "sourceTable": "events_v1",
  "targetTable": "events_v2",
  "keyColumns": "event_id",
  "jsonComparisonMode": "keys_only",
  "columnsToCompare": "event_id,event_data"
}
```

**说明**：使用 keys_only 模式只关注结构变化

### 示例 4：ClickZetta JSON 比对

**场景**：ClickZetta 存储的 JSON 数据比对

```javascript
{
  "operation": "compareTable",
  "sourceTable": "clickzetta_json_table",
  "targetTable": "postgres_json_table",
  "keyColumns": "id",
  "jsonComparisonMode": "normalized",  // ClickZetta 不支持语义比对
  "columnsToCompare": "id,json_data"
}
```

**重要提示**：
- ClickZetta 将 JSON 存储为字符串
- 不支持原生 JSON 操作
- 建议使用 normalized 模式

## 数据库特定行为

### PostgreSQL
- **最佳支持**：原生 jsonb 类型支持所有比对模式
- **性能**：jsonb 比 json 更快
- **建议**：优先使用 jsonb 类型

### MySQL
- **版本要求**：5.7+
- **限制**：不支持深度嵌套的语义比对
- **建议**：使用 normalized 模式

### BigQuery
- **特殊处理**：JSON 自动转换为规范形式
- **限制**：keys_only 模式支持有限
- **建议**：使用 semantic 或 normalized 模式

### Snowflake
- **类型**：使用 VARIANT 类型
- **优势**：完整的语义比对支持
- **建议**：可以使用任何模式

### ClickZetta
- **类型**：JSON 存储为 STRING
- **限制**：只支持文本比较
- **建议**：使用 exact 或 normalized 模式

## 性能考虑

### 模式性能对比

| 模式 | 相对性能 | 说明 |
|------|----------|------|
| exact | 最快 | 简单字符串比较 |
| normalized | 快 | 需要格式化处理 |
| semantic | 较慢 | 需要解析和递归比较 |
| keys_only | 中等 | 需要提取键集合 |

### 优化建议

1. **大型 JSON**：
   - 考虑使用采样功能
   - 避免使用 semantic 模式
   - 考虑将 JSON 展开为列

2. **索引优化**：
   ```sql
   -- PostgreSQL
   CREATE INDEX idx_jsonb_data ON table_name USING gin (json_column);
   
   -- MySQL
   ALTER TABLE table_name ADD INDEX idx_json_data ((CAST(json_column AS CHAR(255))));
   ```

3. **预处理**：
   - 在插入时标准化 JSON
   - 使用计算列存储 JSON 摘要

## 最佳实践

### 1. 选择合适的模式

```
┌─────────────────────────────────────────┐
│ 需要检测任何变化？                      │
│ (包括格式)                              │
└────────────┬────────────────────────────┘
             │ 是 → Exact
             │ 否 ↓
┌─────────────────────────────────────────┐
│ 只关心结构变化？                        │
└────────────┬────────────────────────────┘
             │ 是 → Keys Only
             │ 否 ↓
┌─────────────────────────────────────────┐
│ 需要忽略键顺序？                        │
└────────────┬────────────────────────────┘
             │ 是 → Semantic
             │ 否 → Normalized (推荐)
```

### 2. 处理 NULL 值

JSON null 与数据库 NULL 的区别：
```json
{"key": null}    // JSON null
{"key": "null"}  // 字符串 "null"
{}               // 缺少 key
NULL             // 数据库 NULL
```

### 3. 处理大型 JSON

对于超过 1MB 的 JSON：
1. 考虑拆分为多个列
2. 使用 keys_only 模式快速检查
3. 启用采样功能

### 4. 跨数据库比对

不同数据库的 JSON 实现差异：
```javascript
// 配置建议
{
  "jsonComparisonMode": "normalized",  // 最兼容
  "caseSensitive": false,              // 忽略大小写
  "strictTypeChecking": false          // 宽松类型检查
}
```

## 故障排查

### 问题 1：Semantic 模式返回太多差异

**原因**：
- 数据库不支持语义比对
- 自动降级到 normalized 模式

**解决方案**：
1. 检查数据库支持情况
2. 显式使用 normalized 模式
3. 查看日志中的降级警告

### 问题 2：JSON 被识别为普通文本

**原因**：
- 列类型不是 JSON
- 数据库不支持 JSON 类型

**解决方案**：
```sql
-- 转换列类型
ALTER TABLE table_name 
ALTER COLUMN json_column TYPE jsonb USING json_column::jsonb;
```

### 问题 3：性能问题

**症状**：
- JSON 比对非常慢
- 内存使用过高

**解决方案**：
1. 使用更简单的比对模式
2. 添加 JSON 索引
3. 启用采样功能
4. 考虑预处理 JSON

## 限制和注意事项

### 1. 当前限制
- 只支持 JOINDIFF 算法
- 不支持 JSON 数组的部分匹配
- 不支持 JSONPath 表达式过滤

### 2. 类型转换
某些数据库可能改变 JSON 中的数据类型：
- 数字精度变化
- 布尔值表示（true/1, false/0）
- 日期格式差异

### 3. 编码问题
- 确保 UTF-8 编码一致性
- 注意特殊字符转义
- 处理 Unicode 规范化

## SQL 实现示例

### PostgreSQL 语义比对
```sql
SELECT 
    id,
    json_column1::jsonb = json_column2::jsonb AS is_equal
FROM comparison_table;
```

### MySQL 标准化比对
```sql
SELECT 
    id,
    JSON_COMPACT(json_column1) = JSON_COMPACT(json_column2) AS is_equal
FROM comparison_table;
```

### 提取 JSON 键（PostgreSQL）
```sql
SELECT 
    id,
    array_agg(DISTINCT jsonb_object_keys(json_column)) AS keys
FROM comparison_table
GROUP BY id;
```

## 未来改进计划

1. **JSONPath 支持**：允许比对 JSON 的特定部分
2. **自定义比对规则**：忽略特定键或值
3. **数组比对选项**：有序/无序数组比对
4. **性能优化**：并行 JSON 解析
5. **可视化差异**：JSON 差异的树形展示