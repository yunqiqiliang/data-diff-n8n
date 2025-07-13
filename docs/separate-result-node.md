# 使用独立的 Data Comparison Result 节点

## 架构改进

为了更好地符合 n8n 的设计理念，我们将获取比对结果的功能分离到独立的节点：

- **Data Comparison (Dual Input)** - 需要两个输入（Source 和 Target），用于发起比对
- **Data Comparison Result** - 不需要输入，仅根据 comparison ID 获取结果

## 使用方式

### 1. 发起比对

使用 Data Comparison 节点发起异步比对：

```
[Database Source] ─┐
                   ├─> [Data Comparison] -> [Store Comparison ID]
[Database Target] ─┘
```

返回数据：
```json
{
  "comparison_id": "uuid-xxx",
  "status": "started",
  "message": "Comparison task started. Use \"Data Comparison Result\" node to retrieve results.",
  "next_step": "Add a \"Data Comparison Result\" node with the comparison_id to get results"
}
```

### 2. 获取结果

使用独立的 Data Comparison Result 节点：

```
[Trigger/Schedule] -> [Data Comparison Result] -> [Process Result]
```

配置：
- **Comparison ID**: 输入从第一步获得的 ID
- **Wait for Completion**: 是否等待完成（可选）
  - 如果启用，会轮询直到获得结果
  - 如果禁用，立即返回当前状态

### 工作流示例

#### 批量处理模式
```
主工作流：
[Daily Trigger] -> [Get Tables] -> [Loop] -> [Data Comparison] -> [Store IDs]

结果处理工作流：
[Hourly Trigger] -> [Get Pending IDs] -> [Loop] -> [Data Comparison Result] -> [Process]
```

#### 即时处理模式
```
[Manual Trigger] -> [Data Comparison] -> [Wait 30s] -> [Data Comparison Result] -> [Send Report]
```

## 优势

1. **清晰的职责分离**
   - 比对节点专注于发起任务
   - 结果节点专注于获取结果

2. **灵活的工作流设计**
   - 可以在不同工作流中获取结果
   - 支持批量处理多个比对任务

3. **符合 n8n 设计理念**
   - 每个节点有明确的输入输出
   - 避免了动态输入数量的复杂性

## 注意事项

- Comparison ID 有效期取决于 API 配置
- 建议及时处理结果，避免数据过期
- 可以将 ID 存储在数据库中进行管理