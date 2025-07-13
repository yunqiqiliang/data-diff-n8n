# 节点输入方式说明

## n8n 中的两种输入方式

### 1. 连接输入（Connection Inputs）
通过连接线从其他节点传入数据：
- Data Comparison (Dual Input) 需要两个连接输入：Source 和 Target
- 这些是数据流的输入

### 2. 参数输入（Parameter Inputs）
通过节点配置界面输入：
- Data Comparison Result 需要在配置中输入 Comparison ID
- 这是配置参数，不是数据流

## Data Comparison Result 节点

```yaml
节点配置:
  - Comparison ID: "uuid-xxx"  # 手动输入或使用表达式
  - Wait for Completion: false
  - Max Wait Time: 300
  - Poll Interval: 5
```

### 使用表达式动态获取 ID

虽然不需要连接线，但可以使用 n8n 表达式从之前的节点获取 ID：

```javascript
// 从上一个节点获取
{{ $node["Data Comparison"].json.comparison_id }}

// 从特定节点获取
{{ $node["Store IDs"].json.comparison_id }}

// 从变量获取
{{ $vars.comparisonId }}
```

## 典型使用场景

### 场景 1：立即获取结果
```
[Data Comparison] -> [Function (存储ID)] -> [Wait] -> [Data Comparison Result]
                                                           ↑
                                              使用表达式获取 comparison_id
```

### 场景 2：批量处理
```
主工作流：
[Data Comparison] -> [PostgreSQL (存储ID到数据库)]

独立工作流：
[PostgreSQL (读取待处理ID)] -> [Loop] -> [Data Comparison Result (使用当前循环项的ID)]
```

### 场景 3：手动检查
```
[Manual Trigger] -> [Data Comparison Result (手动输入ID)]
```

## 设计理由

为什么 Data Comparison Result 不需要连接输入？

1. **灵活性**：可以在任何工作流中独立使用
2. **解耦**：不依赖于发起比对的节点
3. **批量处理**：易于循环处理多个 ID
4. **调试友好**：可以手动输入 ID 进行测试

## 总结

- **连接输入** = 数据流（需要连接线）
- **参数输入** = 配置值（在节点设置中）
- Data Comparison Result 使用参数输入，更灵活
- 可以通过表达式动态获取 comparison ID