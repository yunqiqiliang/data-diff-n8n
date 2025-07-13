# 自动回调功能使用指南

## 概述
在 docker-compose 环境中，n8n 节点可以自动生成内部回调 URL，简化异步比对的配置。

## 配置步骤

### 1. 启用自动回调
在 Data Comparison 节点中：

```yaml
Callback Settings:
  - Use Auto Callback: true  # 启用自动回调
  - Webhook Path: comparison-callback  # 默认值，可自定义
```

这会自动生成回调 URL：`http://n8n:5678/webhook/comparison-callback`

### 2. 创建 Webhook 接收节点

在同一个或另一个 n8n 工作流中创建 Webhook 节点：

```yaml
Webhook Node:
  - Path: comparison-callback  # 必须与上面的 Webhook Path 一致
  - HTTP Method: POST
  - Response Mode: Response Using 'Respond to Webhook' Node
```

### 3. 处理回调数据

```javascript
// Function 节点示例
const result = items[0].json;

if (result.status === 'completed') {
  // 处理成功的比对结果
  const summary = result.result.summary;
  const differences = result.result.differences || [];
  
  return [{
    json: {
      comparison_id: result.comparison_id,
      has_differences: summary.rows_different > 0,
      match_rate: summary.match_rate,
      differences_count: summary.rows_different,
      // 添加时间戳便于追踪
      received_at: new Date().toISOString()
    }
  }];
} else if (result.status === 'failed') {
  // 处理失败情况
  throw new Error(`Comparison failed: ${result.error}`);
}
```

## 完整工作流示例

### 主工作流（发起比对）

```json
{
  "nodes": [
    {
      "name": "Database Source",
      "type": "n8n-nodes-base.postgres",
      "position": [250, 300]
    },
    {
      "name": "Database Target", 
      "type": "n8n-nodes-base.postgres",
      "position": [250, 500]
    },
    {
      "name": "Data Comparison",
      "type": "dataComparisonDualInput",
      "parameters": {
        "operation": "compareTable",
        "callbackSettings": {
          "useAutoCallback": true,
          "webhookPath": "comparison-callback",
          "asyncMode": true
        }
      },
      "position": [450, 400]
    }
  ]
}
```

### 回调处理工作流

```json
{
  "nodes": [
    {
      "name": "Webhook",
      "type": "n8n-nodes-base.webhook",
      "parameters": {
        "path": "comparison-callback",
        "httpMethod": "POST",
        "responseMode": "responseNode"
      },
      "position": [250, 300]
    },
    {
      "name": "Process Result",
      "type": "n8n-nodes-base.function",
      "parameters": {
        "functionCode": "// 上面的处理代码"
      },
      "position": [450, 300]
    },
    {
      "name": "Send Notification",
      "type": "n8n-nodes-base.emailSend",
      "position": [650, 300]
    }
  ]
}
```

## 返回的信息示例

当启用自动回调时，Data Comparison 节点会返回：

```json
{
  "comparison_id": "uuid-xxx",
  "status": "started",
  "message": "Comparison started. Result will be sent to callback URL: http://n8n:5678/webhook/comparison-callback",
  "callback_url": "http://n8n:5678/webhook/comparison-callback",
  "async": true,
  "next_step": "Create a Webhook node in n8n with path: comparison-callback"
}
```

## 优势

1. **零配置**：不需要手动输入回调 URL
2. **内部通信**：使用 Docker 网络内部地址，更安全高效
3. **提示友好**：返回信息包含下一步操作建议
4. **灵活切换**：可以随时切换到自定义 URL

## 注意事项

1. **网络隔离**：确保 n8n 和 data-diff-api 在同一个 Docker 网络中
2. **Webhook 路径唯一**：避免多个工作流使用相同的 webhook 路径
3. **错误处理**：在回调处理工作流中添加适当的错误处理
4. **超时设置**：考虑为大数据量比对设置合理的超时时间

## 故障排查

如果回调没有收到：
1. 检查 Docker 网络配置
2. 查看 API 容器日志：`docker logs data-diff-api`
3. 验证 Webhook 路径是否正确
4. 确认 n8n Webhook 节点已激活