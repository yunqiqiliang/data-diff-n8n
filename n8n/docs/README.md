# N8N Clickzetta 数据比对集成文档

这个目录包含了 data-diff-n8n 项目的完整文档，专注于 **Clickzetta 与其他主流数据库的数据比对解决方案**。

## 文档列表

### 核心设计文档
- **[REQUIREMENTS_AND_DESIGN.md](REQUIREMENTS_AND_DESIGN.md)** - 需求分析与系统设计文档
  - Clickzetta 数据比对核心场景
  - 数据迁移、同步验证专业方案
  - 支持与 PostgreSQL、MySQL、Oracle、Snowflake 等主流数据库的双向比对
  - 系统架构设计和 N8N 工作流集成方案
  - Docker Compose 一键部署
  - 企业级性能优化和技术实现

## 🎯 核心场景聚焦

### 主要应用场景
- **🔄 数据迁移验证**：Oracle/MySQL/PostgreSQL → Clickzetta
- **☁️ 云数仓整合**：Snowflake/BigQuery/Redshift ↔ Clickzetta
- **📊 分析平台升级**：ClickHouse/Vertica → Clickzetta
- **⚡ 实时同步监控**：业务库 ↔ Clickzetta 分析库
- **🔧 ETL 流程验证**：多源数据 → ETL → Clickzetta
- **✅ 数据质量监控**：生产环境 ↔ Clickzetta 测试/备份环境

### 🚀 核心优势
- **专业化**：专门针对 Clickzetta 数据比对场景优化
- **高性能**：智能采样（2x-20x 提速）、并行处理、二分法优化
- **全兼容**：支持与主流数据库的无缝数据类型转换和精度保持
- **企业级**：基于 N8N 工作流的可视化编排、进度监控、异常恢复

### 🏗️ 支持的数据库对比
| 数据库类型 | 与 Clickzetta 比对支持 | 主要场景 |
|-----------|---------------------|----------|
| PostgreSQL | ✅ 完全支持 | 业务库迁移、实时同步 |
| MySQL | ✅ 完全支持 | 业务库迁移、数据整合 |
| Oracle | ✅ 完全支持 | 遗留系统迁移 |
| Snowflake | ✅ 完全支持 | 云数仓整合 |
| BigQuery | ✅ 完全支持 | 多云架构 |
| ClickHouse | ✅ 完全支持 | 分析平台升级 |
| Redshift | ✅ 完全支持 | AWS 云迁移 |
| SQL Server | ✅ 完全支持 | 企业系统迁移 |

### 📊 技术亮点
- 基于开源 data-diff 核心算法 + Clickzetta 专业驱动
- 严格遵循 Datafold 官方最佳实践，专注 Clickzetta 场景
- HASHDIFF（跨库）/ JOINDIFF（同库）智能算法选择
- 统计学采样配置（95%-99% 置信度，0.001%-5% 容错率）
- N8N 可视化工作流编排，一键 Docker Compose 部署

---

**这是专门为 Clickzetta 数据比对需求设计的企业级开源解决方案**
