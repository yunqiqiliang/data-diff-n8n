# 统一的异步比对设计

## 概述

为了保持用户体验的一致性，现在表比对（compareTable）和模式比对（compareSchema）都支持：
- 异步执行
- 回调通知
- 使用同一个 Data Comparison Result 节点获取结果

## 功能特性

### 1. Data Comparison (Dual Input) 节点
- **Compare Table**: 比对两个表的数据差异
- **Compare Schema**: 比对两个数据库的结构差异

两种操作都支持：
- ✅ Enable Callback - 启用回调通知
- ✅ Callback Mode - 自动或自定义回调 URL
- ✅ Callback Headers - 自定义回调请求头

### 2. Data Comparison Result 节点
- 不需要输入连接
- 根据 comparison ID 获取结果
- 自动识别比对类型（table/schema）
- 支持轮询等待结果完成

## 使用示例

### 表数据比对
```
工作流1 - 发起比对：
[PostgreSQL Source] ─┐
                     ├─> [Data Comparison (compareTable)] -> [Store ID]
[PostgreSQL Target] ─┘

工作流2 - 处理结果：
[Webhook] -> [Process Table Differences] -> [Send Alert]
```

### 数据库结构比对
```
工作流1 - 发起比对：
[MySQL Dev DB] ─┐
                 ├─> [Data Comparison (compareSchema)] -> [Store ID]
[MySQL Prod DB] ─┘

工作流2 - 处理结果：
[Webhook] -> [Process Schema Changes] -> [Generate Migration Script]
```

### 批量处理模式
```
[Schedule Trigger] -> [Get DB List] -> [Loop] -> [Data Comparison] -> [Store IDs]

[Timer Trigger] -> [Get Pending IDs] -> [Loop] -> [Data Comparison Result] -> [Report]
```

## 返回数据格式

### 启动异步任务时
```json
{
  "comparison_id": "uuid-xxx",
  "status": "started",
  "message": "Comparison started. Result will be sent to callback URL: http://n8n:5678/webhook/comparison-callback",
  "callback_url": "http://n8n:5678/webhook/comparison-callback",
  "async": true,
  "next_step": "Create a Webhook node in n8n with path: comparison-callback",
  "operation_type": "table" // 或 "schema"
}
```

### 获取结果时
```json
{
  "comparison_id": "uuid-xxx",
  "status": "completed",
  "comparison_type": "table", // 或 "schema"
  "result": {
    // 表比对结果
    "summary": {
      "total_rows": 10000,
      "rows_matched": 9990,
      "rows_different": 10,
      "match_rate": 99.9
    },
    "differences": [...],
    
    // 或模式比对结果
    "schemas": {
      "source": {...},
      "target": {...}
    },
    "differences": {
      "missing_in_target": [...],
      "missing_in_source": [...],
      "type_mismatches": [...]
    }
  }
}
```

## 优势

1. **一致的用户体验**
   - 表比对和模式比对使用相同的异步模式
   - 统一的回调配置界面
   - 相同的结果获取方式

2. **灵活的工作流设计**
   - 可以混合处理不同类型的比对
   - 批量管理所有比对任务
   - 统一的错误处理

3. **更好的性能**
   - 大数据量表比对不会超时
   - 复杂模式比对可以后台运行
   - 支持并发执行多个比对

## 迁移指南

如果你之前使用 "Get Comparison Result" 操作：
1. 添加新的 "Data Comparison Result" 节点
2. 输入 comparison ID
3. 删除原来的操作调用

新设计更清晰、更灵活！