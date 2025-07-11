# 数据类型支持和警告功能

## ⚠️ 重要警告

Data-Diff-N8N 检测到不支持的数据类型时，**这些字段将被完全忽略**，导致比对结果不完整且可能产生误导。这不是"部分影响"，而是**完全排除**这些字段的比对。

## 风险说明

### 不支持类型的实际影响
- **字段被完全忽略**：不支持的字段（如 PostgreSQL 的 `money` 类型）不会参与任何比对
- **比对结果不完整**：即使这些字段的值完全不同，比对仍可能显示"100% 匹配"
- **用户可能被误导**：表面上看起来数据一致，实际上重要字段被跳过了

### 示例场景
```sql
-- 表结构
CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    amount MONEY,        -- 这个字段会被忽略！
    description TEXT
);

-- 即使 amount 字段值完全不同，比对仍会显示匹配
-- 源表: amount = '$100.00'
-- 目标表: amount = '$999.99'
-- 结果: 显示 100% 匹配 (因为只比对了 id 和 description)
```

## 功能特性

### 1. 自动检测不支持的数据类型
系统会自动检测以下不支持的数据类型：
- PostgreSQL: `money`, `uuid`, `inet`, `macaddr`
- 其他数据库的类似特殊类型

### 2. 两种处理模式

#### 默认模式 (strict_type_checking: false)
- 检测到不支持类型时显示**严重警告**
- 继续执行比对，但结果**不完整且可能误导**
- 数据质量评分强制设置为 "Poor"
- **不推荐用于生产环境**

#### 严格模式 (strict_type_checking: true) ⭐ **推荐**
- 检测到不支持类型时立即报错
- 比对任务失败，避免产生误导性结果
- **推荐用于生产环境**

### 3. 详细的警告信息
比对结果包含严重警告信息：
```json
{
  "warnings": {
    "unsupported_types": [
      "检测到不支持的数据类型，这些列将被完全忽略，导致比对结果不完整: 源表 public.test_table.payment (data_type: money)"
    ],
    "message": "检测到不支持的数据类型，这些字段被完全忽略，比对结果不完整且可能误导！",
    "severity": "critical",
    "recommendation": "建议启用严格类型检查模式或预处理数据，将不支持的类型转换为支持的类型"
  },
  "summary": {
    "data_quality_score": "Poor",
    "incomplete_comparison": true,
    "ignored_columns": 1
  }
}
```

## 使用方法

### 在 N8N 节点中
1. 在 Data Comparison 节点中找到 "Strict Type Checking" 选项
2. **强烈建议设置为 `true`（严格模式）**
3. 如果必须使用默认模式，请仔细检查警告信息

### 通过 API
```bash
curl -X POST http://localhost:8000/api/v1/compare/tables \
  -H "Content-Type: application/json" \
  -d '{
    "source_connection": "postgresql://user:pass@host/db",
    "target_connection": "postgresql://user:pass@host/db",
    "source_table": "table1",
    "target_table": "table2",
    "key_columns": ["id"],
    "strict_type_checking": true
  }'
```

## 最佳实践

### 1. 优先使用严格模式
```json
{
  "strict_type_checking": true
}
```

### 2. 预处理数据
在比对前将不支持的类型转换为支持的类型：

#### PostgreSQL Money 类型
```sql
-- 方案1: 转换为 DECIMAL
CREATE VIEW payments_view AS
SELECT
  id,
  payment::decimal AS payment_decimal,
  description
FROM payments;

-- 方案2: 转换为字符串
CREATE VIEW payments_view AS
SELECT
  id,
  payment::text AS payment_text,
  description
FROM payments;
```

#### UUID 类型
```sql
-- 转换为字符串
CREATE VIEW users_view AS
SELECT
  id,
  uuid_field::text AS uuid_text,
  name
FROM users;
```

### 3. 验证比对范围
- 检查表结构，确认所有重要字段都被支持
- 使用严格模式验证，确保没有字段被忽略
- 记录哪些字段参与了比对

## 生产环境建议

1. **始终启用严格类型检查** - 避免误导性结果
2. **数据预处理** - 建立视图或转换不支持的类型
3. **监控和告警** - 当检测到不支持类型时及时处理
4. **文档记录** - 记录哪些字段因类型问题被排除

这个功能确保了用户能够明确了解比对的真实范围，避免因不支持的数据类型而产生危险的误导性结论。
