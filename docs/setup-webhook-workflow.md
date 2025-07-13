# 设置 Webhook 接收工作流

## 概述

当启用回调功能后，需要在 n8n 中创建一个 Webhook 节点来接收比对结果的回调通知。

## 步骤 1：创建接收工作流

### 1.1 创建新工作流

在 n8n 中创建一个新的工作流，命名为 "数据比对结果处理"。

### 1.2 添加 Webhook 节点

1. 添加一个 **Webhook** 节点
2. 配置如下：
   - **Webhook URLs**: 选择 "Production" 和 "Test"
   - **HTTP Method**: `POST`
   - **Path**: `comparison-callback`（与 Data Comparison 节点中配置的路径一致）
   - **Response Mode**: `On Received`
   - **Response Code**: `200`
   - **Response Data**: `{"status": "received"}`

### 1.3 保存并激活工作流

**重要**：必须激活工作流，否则 webhook 不会生效！

## 步骤 2：处理回调数据

在 Webhook 节点后添加处理节点：

### 2.1 添加 Switch 节点

根据比对状态路由到不同的处理分支：

```javascript
// Switch 条件
{
  "rules": [
    {
      "field": "={{$json.status}}",
      "operation": "equals",
      "value": "completed"
    },
    {
      "field": "={{$json.status}}",
      "operation": "equals", 
      "value": "failed"
    }
  ]
}
```

### 2.2 处理成功结果

添加 Function 节点处理成功的比对结果：

```javascript
// 提取关键信息
const comparisonId = $json.comparison_id;
const result = $json.result;

// 计算差异统计
const summary = result.summary || {};
const totalDifferences = summary.rows_different || 0;
const matchRate = summary.match_rate || 0;

// 判断是否需要告警
const needsAlert = matchRate < 95; // 匹配率低于95%时告警

return {
  json: {
    comparison_id: comparisonId,
    total_rows: summary.total_rows,
    differences: totalDifferences,
    match_rate: matchRate,
    needs_alert: needsAlert,
    timestamp: new Date().toISOString()
  }
};
```

### 2.3 处理失败结果

添加另一个 Function 节点处理失败情况：

```javascript
// 记录错误信息
const comparisonId = $json.comparison_id;
const error = $json.error;

return {
  json: {
    comparison_id: comparisonId,
    status: 'failed',
    error: error,
    alert_type: 'comparison_failed',
    timestamp: new Date().toISOString()
  }
};
```

## 步骤 3：后续动作

根据业务需求，可以添加以下后续动作：

### 3.1 发送通知

- **Email**: 发送邮件通知相关人员
- **Slack/钉钉**: 发送即时消息
- **短信**: 紧急情况下发送短信告警

### 3.2 存储结果

- **PostgreSQL**: 将结果存储到数据库
- **Google Sheets**: 记录到在线表格
- **S3/OSS**: 存储详细的差异报告

### 3.3 触发其他工作流

- 使用 **Execute Workflow** 节点触发数据修复工作流
- 触发数据质量报告生成工作流

## 完整示例工作流

```
[Webhook (comparison-callback)]
    ↓
[Switch - 根据状态分流]
    ├─> [成功] -> [Function - 处理结果] -> [PostgreSQL - 存储] -> [Slack - 通知]
    └─> [失败] -> [Function - 记录错误] -> [Email - 告警]
```

## 注意事项

1. **Webhook Path 必须匹配**：确保 Webhook 节点的 path 与 Data Comparison 节点中配置的 webhookPath 完全一致
2. **工作流必须激活**：只有激活的工作流才能接收 webhook 请求
3. **测试模式**：可以使用 n8n 的测试 webhook URL 进行调试
4. **错误处理**：建议添加 Error Trigger 节点处理异常情况
5. **幂等性**：确保重复处理同一个 comparison_id 不会造成问题

## 调试技巧

1. **查看 Webhook URL**：
   - 在 Webhook 节点中点击 "Webhook URLs"
   - 复制显示的 URL（如 `http://localhost:5678/webhook/comparison-callback`）

2. **手动测试**：
   使用 curl 或 Postman 测试 webhook：
   ```bash
   curl -X POST http://localhost:5678/webhook/comparison-callback \
     -H "Content-Type: application/json" \
     -d '{
       "comparison_id": "test-123",
       "status": "completed",
       "result": {
         "summary": {
           "total_rows": 1000,
           "rows_different": 10,
           "match_rate": 99.0
         }
       }
     }'
   ```

3. **查看执行历史**：
   - 在 n8n 的 "Executions" 页面查看工作流执行历史
   - 检查每个节点的输入输出数据

## 高级配置

### 使用环境变量

如果在不同环境中使用不同的 webhook 路径：

```javascript
// 在 Data Comparison 节点中
webhookPath: process.env.COMPARISON_WEBHOOK_PATH || 'comparison-callback'
```

### 添加认证

为了安全，可以在回调中添加认证头：

1. 在 Data Comparison 节点的 Callback Headers 中添加：
   ```json
   {
     "X-Webhook-Secret": "your-secret-key"
   }
   ```

2. 在 Webhook 节点后添加 IF 节点验证：
   ```javascript
   {{$headers['x-webhook-secret'] === 'your-secret-key'}}
   ```

### 处理大量回调

如果有大量并发回调，考虑：

1. 使用队列系统（如 RabbitMQ）
2. 限制并发处理数量
3. 实现重试机制