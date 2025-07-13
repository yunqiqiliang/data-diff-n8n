# n8n 回调工作流示例

## 概述
使用回调机制实现真正的异步数据比对，避免工作流阻塞。

## 工作流设计

### 1. 主工作流
负责启动比对任务并继续执行其他任务。

```yaml
# 主工作流节点配置
nodes:
  - id: "database_source"
    type: "DatabaseConnector"
    parameters:
      # 源数据库配置
      
  - id: "database_target"
    type: "DatabaseConnector"
    parameters:
      # 目标数据库配置
      
  - id: "data_comparison"
    type: "DataComparisonDualInput"
    parameters:
      operation: "compareTable"
      callbackSettings:
        callbackUrl: "https://your-n8n.com/webhook/comparison-callback"
        callbackHeaders:
          - name: "X-Workflow-ID"
            value: "{{$workflow.id}}"
          - name: "Authorization"
            value: "Bearer your-webhook-token"
        asyncMode: true
      # 其他比对配置...
      
  - id: "continue_other_tasks"
    type: "Function"
    parameters:
      # 继续执行其他任务，不等待比对结果
```

### 2. 回调处理工作流
独立的工作流，专门处理比对完成的回调。

```yaml
# 回调处理工作流
nodes:
  - id: "webhook_receiver"
    type: "Webhook"
    parameters:
      path: "comparison-callback"
      httpMethod: "POST"
      responseMode: "responseNode"
      authentication: "headerAuth"
      headerAuth:
        name: "Authorization"
        value: "Bearer your-webhook-token"
        
  - id: "extract_result"
    type: "Function"
    parameters:
      functionCode: |
        const result = items[0].json;
        
        if (result.status === 'completed') {
          return [{
            json: {
              comparison_id: result.comparison_id,
              summary: result.result.summary,
              differences_count: result.result.summary.rows_different,
              match_rate: result.result.summary.match_rate,
              has_differences: result.result.summary.rows_different > 0
            }
          }];
        } else if (result.status === 'failed') {
          throw new Error(`Comparison failed: ${result.error}`);
        }
        
  - id: "check_differences"
    type: "If"
    parameters:
      conditions:
        - value1: "{{$json.has_differences}}"
          value2: true
          
  - id: "send_alert"
    type: "EmailSend"
    parameters:
      # 发送差异警报
      
  - id: "store_result"
    type: "PostgreSQL"
    parameters:
      operation: "insert"
      table: "comparison_history"
      # 存储比对历史
```

## 使用步骤

### 1. 设置 Webhook
1. 创建回调处理工作流
2. 添加 Webhook 节点，获取回调 URL
3. 设置认证（推荐使用 Header Auth）

### 2. 配置主工作流
1. 在 Data Comparison 节点启用回调设置
2. 填入回调 URL
3. 设置必要的回调头（认证、追踪信息等）

### 3. 处理回调数据
回调数据格式：
```json
{
  "comparison_id": "uuid",
  "status": "completed",
  "result": {
    "summary": {
      "total_rows": 10000,
      "rows_matched": 9990,
      "rows_different": 10,
      "match_rate": 99.9
    },
    "differences": [...],
    "column_statistics": {...},
    "timeline_analysis": {...}
  }
}
```

## 高级用法

### 1. 批量比对
启动多个比对任务，通过回调统一处理结果。

### 2. 进度通知
如果 API 支持，可以接收多次回调获取进度更新。

### 3. 错误重试
在回调处理工作流中实现错误重试逻辑。

## 优势
- **非阻塞执行**：主工作流不需要等待比对完成
- **并行处理**：可以同时运行多个比对任务
- **灵活处理**：回调工作流可以根据结果执行不同逻辑
- **错误隔离**：比对失败不会影响主工作流

## 注意事项
1. 确保回调 URL 可以从 API 服务器访问
2. 设置合适的超时时间
3. 实现幂等性，避免重复处理
4. 考虑安全性，使用认证保护 Webhook