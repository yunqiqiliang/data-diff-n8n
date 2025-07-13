# 简化的回调功能使用指南

## 新的 UI 设计

现在回调功能更加直观，不需要点击 "Add Callback Setting"：

### 1. Enable Callback 开关
直接在节点配置中可见，一键启用回调功能。

### 2. Callback Mode 选择
- **Auto (Docker Compose)**：自动生成内部 webhook URL
- **Custom URL**：手动指定回调地址

### 3. 配置示例

#### 自动模式（推荐）
```yaml
Data Comparison 节点配置:
  - Operation: Compare Table
  - Enable Callback: ✓
  - Callback Mode: Auto (Docker Compose)
  - Webhook Path: comparison-callback  # 默认值
```

自动生成的回调 URL：`http://n8n:5678/webhook/comparison-callback`

#### 自定义模式
```yaml
Data Comparison 节点配置:
  - Operation: Compare Table  
  - Enable Callback: ✓
  - Callback Mode: Custom URL
  - Callback URL: https://your-server.com/webhook
  - Callback Headers: 
    - Authorization: Bearer your-token
```

## 工作流配置

### 主工作流（发起比对）
```
[Database Source] ─┐
                   ├─> [Data Comparison] -> [Continue Other Tasks...]
[Database Target] ─┘
```

### 回调工作流（处理结果）
```
[Webhook] -> [Process Result] -> [Send Notification]
```

## 返回数据示例

启用回调后，Data Comparison 节点会立即返回：

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

1. **更直观的 UI**：不需要进入嵌套的 collection 设置
2. **智能默认值**：自动模式适用于大多数 docker-compose 场景
3. **灵活切换**：可以轻松在自动和自定义模式之间切换
4. **清晰的提示**：返回信息包含具体的下一步操作指导

## 快速开始

1. 在 Data Comparison 节点中开启 "Enable Callback"
2. 选择 "Auto (Docker Compose)"
3. 创建新工作流，添加 Webhook 节点，路径设为 "comparison-callback"
4. 运行主工作流，比对结果会自动发送到 Webhook

就这么简单！