# DataComparison 节点功能对比

## 功能完整性对比

| 功能 | 原始节点 (单输入) | 双输入节点 | 说明 |
|------|----------------|-----------|------|
| **操作类型** |
| Compare Table | ✅ | ✅ | 表数据比对 |
| Compare Schema | ✅ | ✅ | 模式结构比对 |
| Get Comparison Result | ✅ | ✅ | 获取异步比对结果 |
| **连接配置** |
| 手动输入连接字符串 | ✅ | ✅ | 支持手动配置 |
| 从上游节点自动获取 | ✅ (单输入) | ✅ (双输入) | 更直观的双输入设计 |
| **表配置** |
| 手动指定表名 | ✅ | ✅ | 支持手动输入 |
| 自动检测表名 | ✅ | ✅ | 从查询/操作中提取 |
| **比对参数** |
| Key Columns | ✅ | ✅ | 主键列配置 |
| Columns to Compare | ✅ | ✅ | 指定比对列 |
| Where Condition | ✅ | ✅ | SQL 过滤条件 |
| **算法选择** |
| Auto | ✅ | ✅ | 自动选择最优算法 |
| JoinDiff | ✅ | ✅ | 同库优化 |
| HashDiff | ✅ | ✅ | 跨库优化 |
| **采样功能** |
| Enable Smart Sampling | ✅ | ✅ | 智能采样开关 |
| Sample Size | ✅ | ✅ | 采样大小 |
| Auto Sample Threshold | ✅ | ✅ | 自动采样阈值 |
| Sampling Method | ❌ | ✅ | ROW/SYSTEM 选择 |
| Sampling Confidence | ✅ | ✅ | 置信度配置 |
| Sampling Tolerance | ✅ | ✅ | 容差配置 |
| **性能参数** |
| Thread Count | ✅ | ✅ | 并行线程数 |
| Bisection Factor | ✅ | ✅ | HashDiff 分段因子 |
| Bisection Threshold | ✅ | ✅ | HashDiff 分段阈值 |
| **数据类型处理** |
| Float Tolerance | ❌ | ✅ | 浮点数容差 |
| Timestamp Precision | ❌ | ✅ | 时间戳精度 |
| Case Sensitive | ✅ | ✅ | 大小写敏感 |
| Strict Type Checking | ✅ | ✅ | 严格类型检查 |

## 双输入节点的独特优势

### 1. 更直观的连接方式
- **视觉化数据流**：源和目标通过独立的连接线表示
- **清晰的语义**：左侧输入为源，右侧输入为目标

### 2. 增强的自动检测
- **智能配置提取**：自动识别 DatabaseConnector 和 ClickzettaConnector 的输出
- **多级表名检测**：支持从多个字段和元数据中提取表名

### 3. 改进的采样配置
- **采样方法选择**：支持 ROW（精确）和 SYSTEM（快速）
- **数据库特定优化**：针对 ClickZetta 使用 TABLESAMPLE 语法

### 4. 新增的数据类型处理
- **浮点数容差**：配置浮点数比较的精度
- **时间戳精度**：支持微秒到分钟级别的时间比较

## 迁移建议

### 从单输入到双输入
1. **简单场景**：直接替换节点，连接两个数据源
2. **复杂场景**：利用自动检测功能减少手动配置

### 保留单输入节点的场景
1. 需要在单个流程中串行处理多个比对
2. 使用旧版工作流且不想修改

## 总结

双输入节点（DataComparisonDualInput）完全实现了原始节点的所有功能，并且：
- ✅ 提供了更好的用户体验
- ✅ 增加了更多高级功能
- ✅ 保持了向后兼容性
- ✅ 符合 n8n 的设计理念

建议新建工作流时优先使用双输入版本。