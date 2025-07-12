# 使用 Prepare for Comparison 的示例工作流

## 简单比对工作流

### 步骤 1: 配置源数据库连接器
```yaml
节点: DatabaseConnector
操作: Prepare for Comparison
配置:
  - Schema Name: public
  - Include Sample Data: false  # 不需要示例数据
  - Table Filter: ""  # 包含所有表
```

### 步骤 2: 配置目标数据库连接器
```yaml
节点: ClickzettaConnector  
操作: Prepare for Comparison
配置:
  - Schema Name: dw
  - Include Sample Data: false
  - Table Filter: ""
```

### 步骤 3: 连接到数据比对节点
```yaml
节点: Data Comparison (Dual Input)
配置:
  - Operation: Compare Tables
  - Configuration Mode: Auto-detect from Connected Nodes
  - Table Selection: Auto-detect from Query/Operation
  - Key Columns: ""  # 自动检测
  - Advanced Options:
    - Algorithm: Auto
    - Enable Smart Sampling: true
```

## 工作流 JSON 示例

```json
{
  "nodes": [
    {
      "parameters": {
        "operation": "prepareComparison",
        "schemaName": "public",
        "includeSampleData": false,
        "tableFilter": ""
      },
      "name": "Source DB",
      "type": "n8n-nodes-data-diff-clickzetta.databaseConnector",
      "typeVersion": 1,
      "position": [250, 300]
    },
    {
      "parameters": {
        "operation": "prepareComparison",
        "schemaName": "dw",
        "includeSampleData": false,
        "tableFilter": ""
      },
      "name": "Target DB",
      "type": "n8n-nodes-data-diff-clickzetta.clickzettaConnector",
      "typeVersion": 1,
      "position": [250, 500]
    },
    {
      "parameters": {
        "operation": "compareTables",
        "configMode": "auto",
        "tableSelection": "auto",
        "keyColumns": "",
        "advancedOptions": {
          "algorithm": "auto",
          "enableSampling": true,
          "samplingConfidence": 0.95
        }
      },
      "name": "Compare Data",
      "type": "n8n-nodes-data-diff-clickzetta.dataComparisonDualInput",
      "typeVersion": 2,
      "position": [500, 400]
    }
  ],
  "connections": {
    "Source DB": {
      "main": [
        [
          {
            "node": "Compare Data",
            "type": "main",
            "index": 0
          }
        ]
      ]
    },
    "Target DB": {
      "main": [
        [
          {
            "node": "Compare Data",
            "type": "main",
            "index": 1
          }
        ]
      ]
    }
  }
}
```

## 高级示例：批量表比对

### 使用 Loop 节点批量比对多个表

```yaml
工作流结构:
1. Source DB (prepareComparison) ──┐
                                   ├──> Extract Tables ──> Loop Over Tables
2. Target DB (prepareComparison) ──┘                           │
                                                               ▼
                                                        Set Table Names
                                                               │
                                                               ▼
                                                    ┌──> Source Query ──┐
                                                    │                    ├──> Compare
                                                    └──> Target Query ──┘        │
                                                                                  ▼
                                                                           Aggregate Results
```

### 关键节点配置

#### Extract Tables (Code 节点)
```javascript
// 从两个输入中提取共同的表
const sourceInput = $input.first();
const targetInput = $input.last();

const sourceTables = sourceInput.json.tables || [];
const targetTables = targetInput.json.tables || [];

// 找出两边都存在的表
const commonTables = sourceTables.filter(st => 
  targetTables.some(tt => tt.name === st.name)
);

return commonTables.map(table => ({
  json: {
    tableName: table.name,
    sourceConfig: sourceInput.json.connectionConfig,
    targetConfig: targetInput.json.connectionConfig
  }
}));
```

## 实时监控工作流

### 定时比对关键表

```yaml
触发器: Schedule Trigger (每小时)
         │
         ▼
    Prepare Source ──┐
                     ├──> Compare Critical Tables ──> IF (差异 > 阈值)
    Prepare Target ──┘                                    │
                                                         ▼
                                                    Send Alert
```

### 关键表配置
```javascript
// 在 Compare 节点中指定关键表
{
  "sourceTable": "public.orders",
  "targetTable": "dw.orders_fact",
  "keyColumns": "order_id,created_date",
  "advancedOptions": {
    "enableSampling": false,  // 关键表不采样
    "whereCondition": "created_date >= CURRENT_DATE - INTERVAL '1 day'"
  }
}
```

## 性能优化技巧

### 1. 缓存 Prepare 结果
使用 Redis 节点缓存 prepareComparison 的结果，避免重复查询元数据：

```yaml
Check Cache (Redis Get) ──> IF (not cached) ──> Prepare Comparison ──> Store in Cache
                             │                                               │
                             └──> Use Cached Result <──────────────────────┘
```

### 2. 并行比对
使用 Split In Batches 节点并行处理多个表：

```yaml
Prepare Comparison ──> Split Tables (批次大小: 5) ──> Parallel Compare ──> Merge Results
```

### 3. 增量比对
只比对最近更改的数据：

```javascript
// 在 Advanced Options 中设置
{
  "whereCondition": "updated_at >= '{{ $workflow.lastSuccessfulRun }}'"
}
```

## 错误处理模式

### 重试机制
```yaml
Prepare Comparison ──> IF (success) ──> Compare
                      │
                      └─> Wait (30s) ──> Retry (最多3次)
```

### 降级策略
```yaml
Try Full Compare ──> IF (timeout) ──> Enable Sampling ──> Compare with Sample
```

## 总结

使用 `Prepare for Comparison` 操作的优势：
1. **一次准备，多次使用**：连接信息和表列表可以重用
2. **智能检测**：数据比对节点自动识别所有必要信息
3. **减少配置**：不需要手动输入连接字符串
4. **更好的错误处理**：在准备阶段就能发现连接问题

这种模式特别适合：
- 定期的数据质量检查
- 多环境数据同步验证
- 批量表比对
- 复杂的数据迁移验证