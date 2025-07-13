# 真正的异步实现方案

## 问题分析
FastAPI 的 `background_tasks` 会导致 HTTP 连接保持到任务完成，这使得客户端（n8n）等待整个过程完成。

## 解决方案

### 方案1：使用真正的异步任务队列（推荐）

```python
# 使用 Celery 或 RQ
from celery import Celery

celery_app = Celery('data-diff', broker='redis://redis:6379')

@celery_app.task
def execute_comparison_task(comparison_id, source_config, target_config, comparison_config):
    """在独立进程中执行比对"""
    # 执行比对逻辑
    pass

@app.post("/api/v1/compare/tables/nested")
async def compare_tables_nested_endpoint(request: NestedComparisonRequest):
    comparison_id = str(uuid.uuid4())
    
    # 立即返回，任务在独立进程中执行
    execute_comparison_task.delay(
        comparison_id,
        source_config,
        target_config,
        comparison_config
    )
    
    return {
        "comparison_id": comparison_id,
        "status": "started"
    }
```

### 方案2：使用 asyncio.create_task（不等待）

```python
import asyncio

@app.post("/api/v1/compare/tables/nested")
async def compare_tables_nested_endpoint(request: NestedComparisonRequest):
    comparison_id = str(uuid.uuid4())
    
    # 创建任务但不等待
    asyncio.create_task(
        execute_comparison_async(
            comparison_id,
            source_config,
            target_config,
            comparison_config
        )
    )
    
    # 立即返回
    return {
        "comparison_id": comparison_id,
        "status": "started"
    }
```

### 方案3：使用线程池

```python
from concurrent.futures import ThreadPoolExecutor
import threading

executor = ThreadPoolExecutor(max_workers=10)

def execute_comparison_sync(comparison_id, source_config, target_config, comparison_config):
    """在独立线程中执行比对"""
    # 创建新的事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            execute_comparison_async(comparison_id, source_config, target_config, comparison_config)
        )
    finally:
        loop.close()

@app.post("/api/v1/compare/tables/nested")
async def compare_tables_nested_endpoint(request: NestedComparisonRequest):
    comparison_id = str(uuid.uuid4())
    
    # 在线程池中执行
    executor.submit(
        execute_comparison_sync,
        comparison_id,
        source_config,
        target_config,
        comparison_config
    )
    
    return {
        "comparison_id": comparison_id,
        "status": "started"
    }
```

### 方案4：修改 n8n 节点支持流式响应

```typescript
// 在 n8n 节点中添加超时控制
const controller = new AbortController();
const timeoutId = setTimeout(() => controller.abort(), 5000); // 5秒超时

try {
    const response = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestData),
        signal: controller.signal
    });
    
    // 立即读取响应，不等待连接关闭
    const data = await response.json();
    clearTimeout(timeoutId);
    
    return data;
} catch (error) {
    if (error.name === 'AbortError') {
        // 超时但可能已经收到响应
        console.log('Request timed out but may have been processed');
    }
    throw error;
}
```

## 推荐方案

最佳实践是使用方案1（Celery）或方案2（asyncio.create_task），这样可以：
1. 真正实现异步处理
2. 避免阻塞 HTTP 连接
3. 提供更好的可扩展性
4. 支持任务状态跟踪和重试