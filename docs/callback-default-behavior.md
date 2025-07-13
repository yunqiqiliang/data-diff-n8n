# 回调功能默认行为说明

## 当前默认行为
- **Callback Settings 默认关闭**：`default: {}`
- 用户需要主动点击 "Add Callback Setting" 来配置
- 不配置回调时，使用 FastAPI background_tasks（连接保持）

## 如何修改默认行为

### 选项1：默认启用异步模式
```typescript
{
  displayName: 'Callback Settings',
  name: 'callbackSettings',
  type: 'collection',
  default: {
    asyncMode: true  // 默认启用异步模式
  },
  // ...
}
```

### 选项2：简化为独立的异步模式开关
```typescript
{
  displayName: 'Async Mode',
  name: 'asyncMode',
  type: 'boolean',
  default: true,  // 默认启用
  description: 'Execute comparison asynchronously and return immediately',
  displayOptions: {
    show: {
      operation: ['compareTable'],
    },
  },
}
```

### 选项3：根据数据量自动决定
```typescript
// 在执行逻辑中
const rowCount = await estimateRowCount();
if (rowCount > 100000 && !callbackSettings.callbackUrl) {
  // 自动启用异步模式
  requestData.async_mode = true;
}
```

## 推荐方案
保持当前设计（默认关闭），因为：
1. 向后兼容性好
2. 用户可以根据需要选择同步或异步
3. 避免意外的行为变化