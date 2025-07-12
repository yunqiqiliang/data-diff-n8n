# n8n 回调机制设计方案

## 概述
替代当前的同步等待机制，实现真正的异步回调，让 n8n 工作流能够继续执行其他任务，而不是阻塞等待比对完成。

## 方案设计

### 1. Webhook 回调方案（推荐）

#### API 端改造
```python
# main.py - 添加回调支持
@app.post("/api/v1/compare/tables/nested")
async def compare_tables_nested_endpoint(
    request: NestedComparisonRequest,
    background_tasks: BackgroundTasks
):
    comparison_id = str(uuid.uuid4())
    
    # 提取回调配置
    callback_url = request.callback_url
    callback_headers = request.callback_headers or {}
    
    # 立即返回，使用真正的异步任务
    asyncio.create_task(
        execute_comparison_with_callback(
            comparison_id,
            source_config,
            target_config,
            comparison_config,
            callback_url,
            callback_headers
        )
    )
    
    return {
        "comparison_id": comparison_id,
        "status": "started",
        "callback_url": callback_url
    }

async def execute_comparison_with_callback(
    comparison_id: str,
    source_config: dict,
    target_config: dict,
    comparison_config: dict,
    callback_url: str,
    callback_headers: dict
):
    """执行比对并回调"""
    try:
        # 执行比对
        result = await execute_comparison_async(
            comparison_id,
            source_config,
            target_config,
            comparison_config
        )
        
        # 回调通知
        if callback_url:
            await send_callback(
                callback_url,
                callback_headers,
                {
                    "comparison_id": comparison_id,
                    "status": "completed",
                    "result": result
                }
            )
    except Exception as e:
        if callback_url:
            await send_callback(
                callback_url,
                callback_headers,
                {
                    "comparison_id": comparison_id,
                    "status": "failed",
                    "error": str(e)
                }
            )

async def send_callback(url: str, headers: dict, data: dict):
    """发送回调请求"""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                url,
                json=data,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                logger.info(f"Callback sent to {url}, status: {response.status}")
        except Exception as e:
            logger.error(f"Failed to send callback: {e}")
```

#### n8n 节点端改造
```typescript
// DataComparisonDualInput.node.ts
properties: [
    // ... 现有属性
    {
        displayName: 'Enable Callback',
        name: 'enableCallback',
        type: 'boolean',
        default: false,
        description: 'Use webhook callback instead of waiting for result',
        displayOptions: {
            show: {
                operation: ['compareTable'],
            },
        },
    },
    {
        displayName: 'Callback URL',
        name: 'callbackUrl',
        type: 'string',
        default: '',
        placeholder: 'https://your-n8n-instance.com/webhook/comparison-callback',
        description: 'URL to receive the comparison result callback',
        displayOptions: {
            show: {
                operation: ['compareTable'],
                enableCallback: [true],
            },
        },
        required: true,
    },
]

async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const enableCallback = this.getNodeParameter('enableCallback', 0, false) as boolean;
    
    if (enableCallback) {
        const callbackUrl = this.getNodeParameter('callbackUrl', 0) as string;
        
        // 添加回调URL到请求
        requestData.callback_url = callbackUrl;
        requestData.callback_headers = {
            'X-Comparison-ID': comparison_id,
            'Authorization': 'Bearer ' + this.getCredentials('n8nApi')?.apiKey
        };
        
        // 发起异步比对
        const startResult = await DataComparisonDualInput.callComparisonAPI(requestData);
        
        // 立即返回，不等待
        return [this.helpers.returnJsonArray([{
            comparison_id: startResult.comparison_id,
            status: 'started',
            message: 'Comparison started. Result will be sent to callback URL.',
            callback_url: callbackUrl
        }])];
    }
    
    // 原有的同步逻辑...
}
```

### 2. n8n Webhook 节点配置

创建一个单独的 Webhook 节点来接收回调：

```yaml
# n8n 工作流配置示例
nodes:
  - name: "Comparison Callback Webhook"
    type: "n8n-nodes-base.webhook"
    parameters:
      path: "comparison-callback"
      responseMode: "responseNode"
      httpMethod: "POST"
    
  - name: "Process Comparison Result"
    type: "n8n-nodes-base.function"
    parameters:
      functionCode: |
        const result = items[0].json;
        
        if (result.status === 'completed') {
          // 处理成功的比对结果
          return [{
            json: {
              comparison_id: result.comparison_id,
              summary: result.result.summary,
              differences: result.result.differences
            }
          }];
        } else {
          // 处理失败
          throw new Error(`Comparison failed: ${result.error}`);
        }
```

### 3. 事件驱动架构（高级方案）

使用消息队列（如 RabbitMQ、Redis Pub/Sub）：

```python
# 使用 Redis Pub/Sub
import redis
import json

redis_client = redis.Redis(host='redis', port=6379)

async def execute_comparison_with_pubsub(
    comparison_id: str,
    source_config: dict,
    target_config: dict,
    comparison_config: dict
):
    """执行比对并发布事件"""
    try:
        result = await execute_comparison_async(
            comparison_id,
            source_config,
            target_config,
            comparison_config
        )
        
        # 发布完成事件
        redis_client.publish(
            f'comparison:completed:{comparison_id}',
            json.dumps({
                "comparison_id": comparison_id,
                "status": "completed",
                "result": result
            })
        )
    except Exception as e:
        # 发布失败事件
        redis_client.publish(
            f'comparison:failed:{comparison_id}',
            json.dumps({
                "comparison_id": comparison_id,
                "status": "failed",
                "error": str(e)
            })
        )
```

### 4. SSE（Server-Sent Events）方案

实时推送比对进度：

```python
# SSE endpoint
@app.get("/api/v1/compare/stream/{comparison_id}")
async def stream_comparison_progress(comparison_id: str):
    """流式返回比对进度"""
    async def event_generator():
        # 订阅进度更新
        pubsub = redis_client.pubsub()
        pubsub.subscribe(f'comparison:progress:{comparison_id}')
        
        try:
            while True:
                message = pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    data = json.loads(message['data'])
                    yield f"data: {json.dumps(data)}\n\n"
                    
                    if data['status'] in ['completed', 'failed']:
                        break
                        
                await asyncio.sleep(0.1)
        finally:
            pubsub.unsubscribe()
            pubsub.close()
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )
```

## 优势对比

| 方案 | 优势 | 劣势 |
|------|------|------|
| Webhook 回调 | 简单可靠，n8n 原生支持 | 需要暴露公网 URL |
| 消息队列 | 高可靠性，支持重试 | 需要额外基础设施 |
| SSE | 实时进度更新 | 连接管理复杂 |

## 推荐实现步骤

1. **先实现 Webhook 回调**
   - 最简单，与 n8n 集成最好
   - 可以使用 n8n 内置的 Webhook 节点

2. **添加进度通知**
   - 在比对过程中多次回调
   - 报告当前进度百分比

3. **支持批量回调**
   - 一次回调多个比对结果
   - 减少网络开销

## 使用示例

### n8n 工作流配置
1. 创建 Webhook 节点监听回调
2. Data Comparison 节点启用回调模式
3. 继续执行其他任务
4. Webhook 收到结果后触发后续处理

### 优点
- 不阻塞工作流执行
- 支持并行处理多个比对任务
- 更好的错误处理和重试机制
- 可以实现进度通知