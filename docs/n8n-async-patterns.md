# n8n 中的异步处理模式

## 问题
- Webhook 节点只能作为触发器（工作流的第一个节点）
- 不能在同一个工作流中间使用 Webhook 接收回调
- 轮询会阻塞工作流执行

## 解决方案

### 方案1：Split In Batches + Wait 节点
使用 n8n 的 Split In Batches 节点配合 Wait 节点实现非阻塞等待：

```
[Data Comparison] 
    ↓ (启动异步任务)
[Split In Batches] 
    ↓
[其他任务...]  
    ↓
[Wait Node] (等待 X 秒)
    ↓
[Get Comparison Result]
    ↓
[Merge]
```

### 方案2：使用 Code 节点实现事件驱动
```javascript
// 在 Code 节点中
const comparisonId = $input.first().json.comparison_id;

// 将任务 ID 存储到外部系统（Redis/数据库）
await storeTaskId(comparisonId);

// 继续执行其他任务
return [{
  json: {
    taskQueued: true,
    comparisonId: comparisonId,
    message: "Task queued, use separate workflow to process results"
  }
}];
```

### 方案3：使用 n8n 的 Execute Workflow 节点
主工作流：
```
[Data Comparison] → [Execute Workflow (触发子工作流)] → [继续其他任务]
```

子工作流（独立运行）：
```
[Execute Workflow Trigger] → [Wait/Poll] → [Get Result] → [Process]
```

### 方案4：两阶段处理（推荐）

**阶段1 - 启动工作流：**
```
[Trigger] → [Data Comparison (async)] → [Store Task ID] → [Send Notification]
```

**阶段2 - 结果处理工作流：**
```
[Webhook/Schedule Trigger] → [Get Pending Tasks] → [Get Results] → [Process]
```

## 最佳实践建议

### 如果你需要在同一个工作流中处理结果：
1. 使用 Split In Batches + Wait 的模式
2. 在 Wait 期间可以执行其他任务
3. 定期检查结果状态

### 如果你可以使用多个工作流：
1. 主工作流负责启动任务
2. 使用 Webhook 工作流处理回调
3. 或使用定时工作流批量处理结果

### 示例：批量比对处理
```javascript
// 工作流1：批量启动比对
const comparisons = [];
for (const table of tables) {
  const result = await startComparison(table);
  comparisons.push({
    id: result.comparison_id,
    table: table.name,
    startTime: new Date()
  });
}

// 存储到数据库
await storePendingComparisons(comparisons);

// 工作流2：定时检查结果（每5分钟）
const pending = await getPendingComparisons();
for (const task of pending) {
  const result = await getComparisonResult(task.id);
  if (result.status === 'completed') {
    await processResult(result);
    await markAsProcessed(task.id);
  }
}
```

## 结论
在 n8n 中实现真正的异步处理需要：
1. 使用多个工作流协作
2. 或接受一定程度的等待（使用 Wait 节点）
3. 利用外部存储管理任务状态

单一工作流内无法实现完全的异步非阻塞处理。