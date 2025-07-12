# 双输入数据比对节点设计

## 概述

改进后的 DataComparison V2 节点支持两个输入，更符合 n8n 的设计理念和数据比对的实际需求。

## 主要改进

### 1. 双输入设计
```typescript
inputs: [
    {
        displayName: 'Source',
        type: NodeConnectionType.Main,
    },
    {
        displayName: 'Target', 
        type: NodeConnectionType.Main,
    }
]
```

### 2. 智能参数获取
- **自动模式**：从上游节点自动提取连接信息和表名
- **手动模式**：支持手动输入连接参数

### 3. 使用场景

#### 场景 1：数据库到数据库比对
```
[PostgreSQL Node] ──┐
                    ├──> [Data Comparison V2] ──> [Results]
[ClickZetta Node] ──┘
```

#### 场景 2：查询结果比对
```
[SQL Query Node 1] ──┐
                     ├──> [Data Comparison V2] ──> [Alert if Different]
[SQL Query Node 2] ──┘
```

#### 场景 3：ETL 验证
```
[Source Data] ──> [Transform] ──┐
                                ├──> [Data Comparison V2] ──> [Quality Report]
[Target Data] ──────────────────┘
```

## 优势

1. **更直观**：两个输入清晰表示源和目标
2. **更灵活**：可以从不同类型的节点获取数据
3. **更智能**：自动检测连接和表信息
4. **更符合 n8n 理念**：数据流驱动的设计

## 节点配置示例

### 自动模式（推荐）
```json
{
    "dataSourceMode": "fromNodes",
    "tableSelectionMode": "auto",
    "keyColumns": "", // 自动检测
    "advancedOptions": {
        "algorithm": "auto",
        "enableSampling": true,
        "samplingMethod": "SYSTEM"
    }
}
```

### 手动模式
```json
{
    "dataSourceMode": "manual",
    "sourceConnection": "postgresql://user:pass@host/db",
    "targetConnection": "clickzetta://user:pass@host/db",
    "sourceTable": "public.users",
    "targetTable": "dw.users",
    "keyColumns": "id,created_at"
}
```

## 数据流示例

### 输入数据结构（从数据库节点）
```json
// Source Input (Input 0)
{
    "connectionString": "postgresql://...",
    "tableName": "users",
    "rowCount": 10000,
    "columns": ["id", "name", "email"]
}

// Target Input (Input 1)
{
    "connectionString": "clickzetta://...",
    "tableName": "users_copy",
    "rowCount": 9999,
    "columns": ["id", "name", "email"]
}
```

### 输出数据结构
```json
{
    "success": true,
    "data": {
        "job_id": "comp-123",
        "status": "completed",
        "statistics": {
            "source_rows": 10000,
            "target_rows": 9999,
            "matched": 9999,
            "missing_in_target": 1,
            "match_rate": 0.9999
        },
        "differences": [
            {
                "type": "missing_in_target",
                "key": {"id": 10000},
                "source_row": {"id": 10000, "name": "John", "email": "john@example.com"}
            }
        ]
    },
    "metadata": {
        "sourceTable": "users",
        "targetTable": "users_copy",
        "algorithm": "hashdiff",
        "samplingUsed": false
    }
}
```

## 实现要点

1. **连接信息提取**：支持多种字段名格式
2. **表名智能检测**：从查询语句或元数据中提取
3. **参数继承**：上游节点的配置可以自动继承
4. **错误处理**：清晰的错误提示

这种设计更符合 n8n 的哲学：
- 数据流驱动
- 可视化连接
- 灵活组合
- 智能推断