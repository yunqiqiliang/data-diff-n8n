# 使用 Data Comparison Result 节点

## 概述

Data Comparison Result 节点用于获取数据比对任务的结果。它支持两种方式获取 comparison ID：

1. **自动获取**：从上游节点的输出中提取
2. **手动输入**：在节点配置中指定

## 使用方式

### 方式 1：自动获取 comparison ID（推荐）

最常见的用法是将此节点连接到 Data Comparison 节点之后：

```
[Data Comparison] --> [Data Comparison Result]
```

Data Comparison Result 节点会自动从输入数据中查找以下字段：
- `comparison_id`
- `comparisonId` 
- `id`

### 方式 2：使用表达式

如果 comparison ID 在其他字段中，可以使用 n8n 表达式：

```javascript
{{ $json.data.comparison_id }}
{{ $json["comparison_id"] }}
{{ $node["Store IDs"].json.comparison_id }}
```

### 方式 3：手动输入

直接在 Comparison ID 字段中输入具体的 ID 值：
```
02f29186-e0c9-464c-8e7e-7ec66ac7c24d
```

## 工作流示例

### 基础工作流
```
[Database Source] ─┐
                   ├─> [Data Comparison] -> [Data Comparison Result] -> [Process Results]
[Database Target] ─┘
```

### 带等待的工作流
```
[Data Comparison] -> [Wait 30s] -> [Data Comparison Result] -> [Send Report]
```

### 存储并稍后获取
```
主工作流：
[Data Comparison] -> [Store to Database]

独立工作流：
[Get IDs from Database] -> [Loop] -> [Data Comparison Result]
```

## 配置选项

- **Comparison ID**：留空则自动从输入获取，或手动指定
- **Wait for Completion**：是否等待比对完成
- **Max Wait Time**：最长等待时间（秒）
- **Poll Interval**：轮询间隔（秒）

## 注意事项

1. 如果输入数据中没有 comparison ID 字段，且没有手动提供，节点会报错
2. 支持获取表比对和模式比对的结果
3. 结果中会包含 `comparison_type` 字段，标识是 table 还是 schema 比对