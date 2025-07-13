# Webhook 回调机制说明

## 概述

当启用异步比对和回调功能时，系统会在比对任务完成后向指定的 webhook URL 发送 HTTP POST 请求，通知任务结果。

## 回调数据结构

### 成功完成时的回调数据

```json
{
  "comparison_id": "02f29186-e0c9-464c-8e7e-7ec66ac7c24d",
  "status": "completed",
  "result": {
    // 完整的比对结果数据
    "summary": {
      "total_rows": 10000,
      "rows_matched": 9990,
      "rows_different": 10,
      "match_rate": 99.9
    },
    "differences": [...],
    // 其他结果字段...
  }
}
```

### 失败时的回调数据

```json
{
  "comparison_id": "02f29186-e0c9-464c-8e7e-7ec66ac7c24d",
  "status": "failed",
  "error": "Connection timeout to source database"
}
```

## 如何区分不同的比对任务

每个回调请求都包含唯一的 `comparison_id` 字段，这是系统在发起比对时生成的 UUID。通过这个 ID 可以：

1. **识别具体的比对任务**
2. **关联到原始的比对请求**
3. **在 n8n 工作流中进行后续处理**

## n8n 工作流设计模式

### 方式 1：使用单个 Webhook 节点处理所有回调

```
[Webhook (path: comparison-callback)]
    ↓
[Switch - 根据 comparison_id 路由]
    ├─> [处理任务 A 的结果]
    ├─> [处理任务 B 的结果]
    └─> [处理未知任务]
```

### 方式 2：使用不同的 Webhook 路径

在发起比对时，为不同类型的任务指定不同的 webhook 路径：

- 日常数据质量检查：`/webhook/daily-quality-check`
- 实时数据同步验证：`/webhook/sync-validation`
- 数据迁移验证：`/webhook/migration-check`

### 方式 3：在回调头中添加自定义标识

在 Data Comparison 节点配置中添加自定义回调头：

```javascript
// Callback Headers 配置
{
  "X-Task-Type": "daily-check",
  "X-Department": "finance",
  "X-Priority": "high"
}
```

## 实现示例

### n8n Webhook 节点处理

```javascript
// 在 Function 节点中处理回调数据
const comparisonId = $json.comparison_id;
const status = $json.status;

if (status === 'completed') {
  // 处理成功的结果
  const result = $json.result;
  
  // 可以基于 comparison_id 查询原始任务信息
  // 或者直接处理结果数据
  
  return {
    json: {
      comparison_id: comparisonId,
      total_differences: result.summary.rows_different,
      match_rate: result.summary.match_rate,
      alert_needed: result.summary.match_rate < 95
    }
  };
} else {
  // 处理失败情况
  return {
    json: {
      comparison_id: comparisonId,
      error: $json.error,
      alert_type: 'comparison_failed'
    }
  };
}
```

### 使用数据库存储映射关系

如果需要更复杂的任务管理，可以：

1. **发起比对时**：将 comparison_id 和任务元数据存储到数据库
2. **收到回调时**：根据 comparison_id 查询任务信息
3. **执行相应处理**：基于任务类型和配置执行不同的处理逻辑

```sql
-- 任务映射表
CREATE TABLE comparison_tasks (
  comparison_id UUID PRIMARY KEY,
  task_name VARCHAR(255),
  task_type VARCHAR(50),
  source_table VARCHAR(255),
  target_table VARCHAR(255),
  created_at TIMESTAMP,
  workflow_id VARCHAR(100),
  metadata JSONB
);
```

## 最佳实践

1. **使用有意义的 Webhook 路径**：根据业务场景设置不同的路径
2. **添加认证头**：在回调头中添加认证信息，确保安全性
3. **实现幂等性**：确保重复的回调不会造成问题
4. **设置超时重试**：API 会在回调失败时进行有限次重试
5. **记录所有回调**：在 n8n 中记录所有收到的回调，便于问题排查

## 故障处理

如果 webhook 调用失败：

1. API 会记录错误日志
2. 比对结果仍然会保存在系统中
3. 可以通过 Data Comparison Result 节点主动查询结果
4. 建议实现备用的轮询机制作为 fallback