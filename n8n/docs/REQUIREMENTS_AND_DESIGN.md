# N8N 跨数据库数据比对集成 - 需求分析与设计文档

## 1. **C. 比对执行与优化**
- 并行查询执行
- 智能采样策略（置信度、容错率配置）
- 数据类型自动识别和适配
- 索引优化建议目标

### 1.1 项目背景
基于 data-diff 项目，结合 N8N 工作流引擎，实现一个完整的跨数据库数据比对解决方案。该解决方案将提供：
- 自动化的跨数据库数据比对工作流
- 可视化的比对结果展示
- 灵活的比对配置和调度
- 企业级的部署和运维能力

### 1.2 项目目标
1. **核心目标**：实现 Clickzetta 与其他主流数据库的数据比对功能，专注于数据迁移、同步验证等核心场景
2. **集成目标**：与 N8N 深度集成，提供可视化工作流编排能力
3. **部署目标**：通过 Docker Compose 实现一键部署和运维
4. **性能目标**：支持大规模数据比对，具备采样、并行处理等优化能力
5. **兼容目标**：确保与 Datafold 官方最佳实践和 data-diff 实际能力完全对齐

## 2. 需求分析

### 2.1 功能需求

#### 2.1.0 数据库支持范围与核心场景
本解决方案专注于 **Clickzetta 与其他主流数据库的数据比对**，支持以下核心场景：

**核心比对场景**：
- **🎯 Clickzetta ← 其他数据库**：数据迁移到 Clickzetta 的验证场景
- **🎯 Clickzetta → 其他数据库**：从 Clickzetta 导出数据的验证场景
- **🎯 Clickzetta ↔ 其他数据库**：双向数据同步验证场景

**支持的对比数据库类型**：
- **云数据仓库**：Snowflake、BigQuery、Redshift、Databricks
- **传统关系型数据库**：PostgreSQL、MySQL、Oracle、SQL Server
- **分析型数据库**：ClickHouse、Vertica
- **大数据查询引擎**：Presto、Trino
- **轻量级数据库**：DuckDB

**典型业务场景**：
- **数据迁移验证**：
  - Oracle/MySQL/PostgreSQL → Clickzetta（传统数据库迁移到分析平台）
  - Snowflake/BigQuery → Clickzetta（云数仓数据迁移验证）
  - ClickHouse/Vertica → Clickzetta（分析平台数据迁移）
- **数据同步验证**：
  - 业务库（MySQL/PostgreSQL）↔ Clickzetta（实时/准实时数据同步）
  - 数据仓库（Snowflake/Redshift）↔ Clickzetta（数据仓库间同步）
- **ETL 结果验证**：
  - 源数据库 → ETL 处理 → Clickzetta（数据处理流程验证）

**数据类型兼容性**：
- **自动类型转换**：智能处理不同数据库间的数据类型差异
- **精度保持**：数值类型精度自动适配，避免精度丢失导致的误报
- **时间格式统一**：自动处理时间戳、日期等格式差异
- **字符编码**：支持 UTF-8、GBK 等多种字符编码比对

#### 2.1.1 核心数据比对功能
基于 Datafold 官方文档和 data-diff 实际能力，需要实现以下核心功能：

**A. 数据源配置**
- **核心支持**：Clickzetta 与 PostgreSQL、MySQL、Oracle、Snowflake、BigQuery、ClickHouse、Redshift 等主流数据库的双向比对
- **专业场景**：针对 Clickzetta 数据迁移、同步验证等场景优化的配置模板
- **灵活查询支持**：支持表对表比对和查询对查询比对
- **智能数据过滤**：支持 WHERE 条件和时间范围筛选（min-age/max-age）
- **列映射处理**：支持列重映射（相同数据类型但不同列名的比对）
- **字符处理**：支持大小写敏感/不敏感的列名处理

**B. 比对策略配置**
- 主键配置：支持单列或多列主键，支持复合主键
- 列选择：可选择要比对的具体列（include/exclude patterns）
- 采样配置：支持统计采样（置信度 95%-99%，容错率 0.001%-5%）
- 限制设置：差异行数限制（limit）、每列差异限制、表写入限制
- 算法选择：HASHDIFF（跨库）vs JOINDIFF（同库）自动选择

**C. 比对执行与优化**
- 并行查询执行（最大64并发连接）
- 智能二分法分段比对（bisection-factor/threshold）
- 数据类型自动转换和精度处理
- 索引优化建议和性能估算
- 断点续传和异常恢复

**D. 结果处理与展示**
- 概览视图：比对状态、运行时间、总体统计（表行数、差异百分比）
- 列级分析：每列的差异数量/百分比和数据类型匹配状态
- 主键分析：独有行识别（exclusive rows）和重复键检测
- 值级分析：具体差异行的详细信息，支持字符级高亮
- 结果物化：将比对结果保存到数据表（materialize_to_table）
- 多格式输出：JSON、JSONL、统计摘要、详细差异行

#### 2.1.2 N8N 工作流集成功能

**A. N8N 节点开发**
- Clickzetta 连接器节点（支持连接测试和元数据获取）
- 数据比对配置节点（智能策略建议和参数验证）
- 比对执行节点（支持进度监控和中断恢复）
- 结果分析节点（多格式输出和可视化处理）
- 通知报告节点（邮件、Slack、Webhook 多渠道）
- 性能监控节点（执行时间、资源使用情况）

**B. 工作流模板**
- **🎯 数据迁移验证模板**：
  - Oracle/MySQL/PostgreSQL → Clickzetta（传统数据库迁移）
  - Snowflake/BigQuery/Redshift → Clickzetta（云数仓迁移）
  - ClickHouse/Vertica → Clickzetta（分析平台迁移）
- **🎯 数据同步验证模板**：
  - 业务库（MySQL/PostgreSQL）↔ Clickzetta（实时同步验证）
  - 数据仓库（Snowflake/Redshift）↔ Clickzetta（定期同步验证）
- **🎯 ETL 流程验证模板**：
  - 源数据库 → dbt/Spark → Clickzetta（数据处理流程验证）
  - 多源数据库 → 数据集成 → Clickzetta（数据整合验证）
- **🎯 质量监控模板**：
  - 生产环境 ↔ Clickzetta 备份环境（数据一致性监控）
  - 历史数据 ↔ Clickzetta 当前数据（数据完整性检查）

**C. 自动化调度**
- 基于 cron 的定时比对任务
- 数据变更事件触发比对（webhook triggers）
- 条件式比对执行（threshold-based triggers）
- 失败重试机制（exponential backoff）
- 依赖关系管理（upstream task completion）

### 2.2 非功能需求

#### 2.2.1 性能需求
- 支持千万级数据量的比对（通过采样和分段处理）
- 采样模式下提供 2x-20x 性能提升（基于 Datafold 基准测试）
- 并发连接数可配置（最大64个连接）
- 支持流式处理，减少内存占用
- 二分法优化：bisection-factor（默认32）和 threshold（默认16K）
- 智能索引利用和查询优化

#### 2.2.2 可靠性需求
- 比对过程异常恢复
- 数据库连接断开重连
- 结果数据持久化存储
- 操作日志完整记录

#### 2.2.3 可用性需求
- Web 界面直观易用
- 比对进度实时显示
- 错误信息清晰准确
- 支持比对任务暂停/恢复

#### 2.2.4 可维护性需求
- 模块化架构设计
- 标准化日志格式
- 性能监控指标
- 配置管理集中化

## 3. 系统架构设计

### 3.1 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose 部署环境                    │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    N8N      │  │   Redis     │  │    PostgreSQL       │  │
│  │  (工作流引擎) │  │  (队列缓存)   │  │  (元数据存储)        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              Data-Diff-N8N 核心服务                     │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │  │
│  │  │   N8N 节点   │  │  比对引擎    │  │   结果处理器     │  │  │
│  │  │   集成层     │  │  (data-diff) │  │   (报告生成)     │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              核心场景：Clickzetta 数据比对               │  │
│  │  ┌─────────────┐           ↕️           ┌─────────────┐  │  │
│  │  │ Clickzetta  │  ← 数据迁移验证 →    │ PostgreSQL  │  │  │
│  │  │   (分析库)   │  ← 数据同步验证 →    │  (业务库)    │  │  │
│  │  └─────────────┘           ↕️           └─────────────┘  │  │
│  │  ┌─────────────┐  ← ETL流程验证 →    ┌─────────────┐  │  │
│  │  │   MySQL     │           ↕️           │   Oracle    │  │  │
│  │  │  (源系统)    │  ← 质量监控验证 →    │  (遗留系统)  │  │  │
│  │  └─────────────┘           ↕️           └─────────────┘  │  │
│  │  ┌─────────────┐           ↕️           ┌─────────────┐  │  │
│  │  │ Snowflake   │  ← 云迁移验证 →     │  ClickHouse │  │  │
│  │  │  (云数仓)    │           ↕️           │  (时序库)    │  │  │
│  │  └─────────────┘    (与Clickzetta)    └─────────────┘  │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 核心组件设计

#### 3.2.1 N8N 自定义节点

**A. Clickzetta 连接器节点 (clickzetta-connector)**
- 功能：建立和管理 Clickzetta 数据库连接
- 输入：连接配置参数
- 输出：数据库连接实例和元数据

**B. 数据比对配置节点 (data-diff-config)**
- 功能：配置比对参数和策略
- 输入：源/目标数据源、比对选项
- 输出：比对任务配置

**C. 数据比对执行节点 (data-diff-executor)**
- 功能：执行数据比对任务
- 输入：比对配置、数据源连接
- 输出：比对结果和统计信息

**D. 结果分析节点 (diff-result-analyzer)**
- 功能：分析和格式化比对结果
- 输入：原始比对结果
- 输出：格式化的分析报告

**E. 通知报告节点 (diff-notification)**
- 功能：发送比对结果通知
- 输入：分析报告
- 输出：邮件/webhook/Slack 通知

#### 3.2.2 数据比对引擎

基于现有的 `data_diff` 核心库，扩展以下功能：

**A. 比对策略管理器**
```python
class DiffStrategy:
    def __init__(self):
        self.sampling_config = SamplingConfig()
        self.column_config = ColumnConfig()
        self.performance_config = PerformanceConfig()

    def optimize_for_dataset_size(self, row_count: int):
        """根据数据集大小自动优化策略"""
        pass

    def estimate_runtime(self, source_meta, target_meta):
        """估算比对运行时间"""
        pass
```

**B. 结果处理器**
```python
class DiffResultProcessor:
    def generate_summary(self, diff_result):
        """生成比对摘要"""
        pass

    def format_for_n8n(self, diff_result):
        """格式化为 N8N 兼容格式"""
        pass

    def materialize_to_table(self, diff_result, target_db):
        """将结果物化到数据表"""
        pass
```

### 3.3 数据流设计

#### 3.3.1 标准比对工作流
```
数据源配置 → 连接验证 → 比对配置 → 执行比对 → 结果分析 → 报告生成 → 通知发送
     ↓           ↓           ↓           ↓           ↓           ↓           ↓
  参数验证    连接测试    策略优化    进度监控    异常处理    格式转换    多渠道推送
```

#### 3.3.2 高级比对工作流
```
                    ┌→ 采样策略选择 → 快速预比对 → 策略调整 ┐
数据源分析 → 数据量评估 ┤                                    ├→ 正式比对
                    └→ 索引检查 → 性能优化建议 → 用户确认 ┘
                                    ↓
                    结果处理 → 物化存储 → 趋势分析 → 智能告警
```

## 4. 技术实现方案

### 4.1 技术栈选择

**核心框架**
- **N8N**: 工作流引擎和可视化编排
- **data-diff**: 数据比对核心算法
- **FastAPI**: API 服务框架
- **SQLAlchemy**: 数据库 ORM

**数据存储**
- **PostgreSQL**: 元数据和配置存储
- **Redis**: 任务队列和缓存
- **ClickHouse**: 比对结果存储（可选）

**容器化部署**
- **Docker**: 容器化打包
- **Docker Compose**: 多服务编排
- **Nginx**: 反向代理和负载均衡

### 4.2 N8N 节点开发

#### 4.2.1 节点开发框架
基于 N8N 的节点开发标准，创建自定义节点包：

```typescript
// package.json
{
  "name": "n8n-nodes-data-diff",
  "version": "1.0.0",
  "description": "Data-diff integration nodes for N8N",
  "main": "index.js",
  "n8n": {
    "nodes": [
      "dist/nodes/ClickzettaConnector.node.js",
      "dist/nodes/DataDiffConfig.node.js",
      "dist/nodes/DataDiffExecutor.node.js",
      "dist/nodes/DiffResultAnalyzer.node.js"
    ]
  }
}
```

#### 4.2.2 关键节点实现示例

**Clickzetta 连接器节点**
```typescript
export class ClickzettaConnector implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Clickzetta Connector',
    name: 'clickzettaConnector',
    group: ['input'],
    version: 1,
    description: 'Connect to Clickzetta database',
    defaults: {
      name: 'Clickzetta Connector',
    },
    inputs: ['main'],
    outputs: ['main'],
    properties: [
      {
        displayName: 'Connection',
        name: 'connection',
        type: 'credentials',
        default: '',
        required: true,
      },
      // 其他配置属性...
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    // 实现连接逻辑
  }
}
```

### 4.3 Docker Compose 配置

#### 4.3.1 服务定义
```yaml
version: '3.8'

services:
  # N8N 工作流引擎
  n8n:
    image: n8nio/n8n:latest
    ports:
      - "5678:5678"
    environment:
      - N8N_BASIC_AUTH_ACTIVE=true
      - N8N_BASIC_AUTH_USER=admin
      - N8N_BASIC_AUTH_PASSWORD=changeme
      - DB_TYPE=postgresdb
      - DB_POSTGRESDB_HOST=postgres
      - DB_POSTGRESDB_DATABASE=n8n
      - DB_POSTGRESDB_USER=n8n
      - DB_POSTGRESDB_PASSWORD=n8n
    volumes:
      - n8n_data:/home/node/.n8n
      - ./custom-nodes:/home/node/.n8n/custom
    depends_on:
      - postgres
      - redis

  # PostgreSQL 数据库
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_DB=n8n
      - POSTGRES_USER=n8n
      - POSTGRES_PASSWORD=n8n
    volumes:
      - postgres_data:/var/lib/postgresql/data

  # Redis 缓存
  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

  # Data-Diff 核心服务
  data-diff-service:
    build:
      context: .
      dockerfile: Dockerfile.data-diff
    environment:
      - DATABASE_URL=postgresql://n8n:n8n@postgres:5432/n8n
      - REDIS_URL=redis://redis:6379
    volumes:
      - ./configs:/app/configs
    depends_on:
      - postgres
      - redis

  # Nginx 反向代理
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - n8n

volumes:
  n8n_data:
  postgres_data:
  redis_data:
```

#### 4.3.2 自定义服务 Dockerfile
```dockerfile
# Dockerfile.data-diff
FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 安装 data-diff 项目
RUN pip install -e .

# 设置环境变量
ENV PYTHONPATH=/app

# 启动命令
CMD ["python", "-m", "data_diff_n8n.server"]
```

## 5. 核心功能实现

### 5.1 比对配置管理

#### 5.1.1 配置数据模型
```python
@dataclass
class DiffConfig:
    """数据比对配置 - 基于 data-diff 实际参数"""
    id: str
    name: str
    source_connection: ConnectionConfig
    target_connection: ConnectionConfig
    source_query: str  # 支持表名或 SQL 查询
    target_query: str

    # 核心比对参数
    key_columns: List[str]  # 主键列
    update_column: Optional[str] = None  # 更新时间列
    extra_columns: List[str] = None  # 额外比对列
    columns_pattern: List[str] = None  # 列名模式匹配
    where: Optional[str] = None  # WHERE 过滤条件

    # 时间范围过滤
    min_age: Optional[str] = None  # 最小年龄（如 "5min"）
    max_age: Optional[str] = None  # 最大年龄
    min_update: Optional[str] = None  # 最小更新时间
    max_update: Optional[str] = None  # 最大更新时间

    # 采样配置
    sampling_enabled: bool = False
    sampling_confidence: float = 0.95  # 95% 置信度
    sampling_tolerance: float = 0.001  # 0.001% 容错率
    sampling_threshold: int = 100000   # 采样阈值

    # 性能配置
    algorithm: str = "AUTO"  # AUTO, HASHDIFF, JOINDIFF
    threaded: bool = True
    max_threadpool_size: int = 1
    bisection_factor: int = 32
    bisection_threshold: int = 16384

    # 输出配置
    limit: Optional[int] = None  # 最大差异行数
    materialize_to_table: Optional[str] = None
    materialize_all_rows: bool = False
    table_write_limit: int = 200000

    # 验证选项
    case_sensitive: bool = False
    assume_unique_key: bool = False
    sample_exclusive_rows: bool = False
    skip_null_keys: bool = False

    # 通知配置
    notification_config: NotificationConfig = None

@dataclass
class DiffResult:
    """比对结果数据结构 - 基于 data-diff 输出格式"""
    status: str  # "success" | "failed"
    result: str  # "identical" | "different"

    # 概览统计
    total_rows_source: int
    total_rows_target: int
    rows_identical: int
    rows_different: int
    rows_exclusive_source: int  # 仅源表存在
    rows_exclusive_target: int  # 仅目标表存在

    # 执行信息
    runtime_seconds: float
    algorithm_used: str
    rows_downloaded: int

    # 列级分析
    columns_analysis: Dict[str, ColumnDiffStats]
    columns_added: List[str]
    columns_removed: List[str]
    columns_type_changed: List[str]

    # 具体差异数据
    diff_rows: List[Dict[str, Any]]
    exclusive_rows_source: List[Dict[str, Any]]
    exclusive_rows_target: List[Dict[str, Any]]

    # 元数据
    source_info: Dict[str, Any]
    target_info: Dict[str, Any]
    diff_schema: List[Tuple[str, type]]
```

#### 5.1.2 智能配置建议器
```python
class ConfigurationAdvisor:
    """配置建议器 - 基于 Datafold 最佳实践"""

    def analyze_tables(self, source_meta, target_meta) -> List[Dict]:
        """分析表结构并提供配置建议"""
        suggestions = []

        # 分析数据量并建议采样
        max_rows = max(source_meta.row_count, target_meta.row_count)
        if max_rows > 1000000:
            speedup_estimate = min(20, max_rows / 100000)  # 基于 Datafold 基准
            suggestions.append({
                'type': 'sampling',
                'priority': 'high',
                'message': f'建议启用采样以提高性能，预计提速 {speedup_estimate:.1f}x',
                'config': {
                    'sampling_enabled': True,
                    'sampling_confidence': 0.95,
                    'sampling_tolerance': 0.001
                }
            })

        # 分析列数量并建议优化
        if len(source_meta.columns) > 30:
            key_columns = self._identify_key_columns(source_meta)
            important_columns = self._identify_important_columns(source_meta)
            suggestions.append({
                'type': 'column_selection',
                'priority': 'medium',
                'message': '表列数较多，建议选择关键列进行比对',
                'config': {
                    'recommended_columns': key_columns + important_columns,
                    'hybrid_approach': True
                }
            })

        # 分析主键分布并建议索引优化
        pk_gaps = self._analyze_primary_key_distribution(source_meta)
        if pk_gaps > 0.1:  # 主键分布稀疏
            suggestions.append({
                'type': 'indexing',
                'priority': 'medium',
                'message': '主键分布存在较大间隙，建议优化索引结构',
                'config': {'bisection_threshold': 32768}  # 增加阈值
            })

        # 分析数据类型兼容性
        type_issues = self._check_data_type_compatibility(source_meta, target_meta)
        if type_issues:
            suggestions.append({
                'type': 'data_types',
                'priority': 'high',
                'message': '发现数据类型不匹配，需要处理',
                'config': {'type_casting_needed': type_issues}
            })

        return suggestions

    def estimate_performance(self, config: DiffConfig) -> Dict:
        """估算性能指标"""
        # 基于表大小、列数、采样配置等估算
        base_time = self._estimate_base_time(config)

        # 采样加速系数
        sampling_factor = 1
        if config.sampling_enabled:
            sampling_factor = self._calculate_sampling_speedup(config)

        # 并发加速系数
        thread_factor = min(config.max_threadpool_size, 4)  # 实际加速有限

        estimated_time = base_time / (sampling_factor * thread_factor)

        return {
            'estimated_runtime_seconds': estimated_time,
            'estimated_rows_processed': self._estimate_rows_processed(config),
            'memory_usage_mb': self._estimate_memory_usage(config),
            'network_transfer_mb': self._estimate_network_transfer(config),
            'cost_optimization_tips': self._generate_cost_tips(config)
        }

    def _identify_key_columns(self, meta) -> List[str]:
        """识别关键列（主键、外键、索引列）"""
        # 实现基于表元数据的关键列识别
        pass

    def _identify_important_columns(self, meta) -> List[str]:
        """识别重要业务列（金额、状态等）"""
        # 基于列名模式和数据类型识别
        important_patterns = [
            r'.*amount.*', r'.*price.*', r'.*total.*',
            r'.*status.*', r'.*state.*', r'.*flag.*',
            r'.*created.*', r'.*updated.*'
        ]
        # 实现模式匹配逻辑
        pass
```

### 5.2 比对执行引擎

#### 5.2.1 任务编排器
```python
class DiffOrchestrator:
    """比对任务编排器 - 基于 data-diff 核心 API"""

    async def execute_diff(self, config: DiffConfig, callback=None) -> DiffResult:
        """执行比对任务"""
        try:
            # 1. 预检查和验证
            await self._pre_check(config)

            # 2. 建立数据库连接
            source_table = self._create_table_segment(
                config.source_connection,
                config.source_query,
                config.key_columns,
                config.update_column,
                config.extra_columns,
                where=config.where,
                min_age=config.min_age,
                max_age=config.max_age
            )

            target_table = self._create_table_segment(
                config.target_connection,
                config.target_query,
                config.key_columns,
                config.update_column,
                config.extra_columns,
                where=config.where,
                min_age=config.min_age,
                max_age=config.max_age
            )

            # 3. 执行核心比对
            diff_result = await self._execute_core_diff(
                source_table, target_table, config
            )

            # 4. 处理和格式化结果
            processed_result = await self._post_process_result(diff_result, config)

            # 5. 可选的结果物化
            if config.materialize_to_table:
                await self._materialize_results(processed_result, config)

            # 6. 回调通知
            if callback:
                await callback(processed_result)

            return processed_result

        except Exception as e:
            logger.error(f"Diff execution failed: {e}")
            raise DiffExecutionError(str(e)) from e

    def _create_table_segment(self, connection_config, query, key_columns,
                            update_column=None, extra_columns=None, **kwargs):
        """创建 TableSegment 实例"""
        from data_diff import connect_to_table

        # 构建连接字符串或字典
        db_info = self._build_connection_info(connection_config)

        return connect_to_table(
            db_info=db_info,
            table_name=query,  # 可以是表名或查询
            key_columns=tuple(key_columns),
            update_column=update_column,
            extra_columns=tuple(extra_columns) if extra_columns else None,
            **kwargs
        )

    async def _execute_core_diff(self, source_table, target_table, config):
        """执行核心比对逻辑"""
        from data_diff import diff_tables, Algorithm

        # 设置算法
        algorithm = Algorithm.AUTO
        if config.algorithm == "HASHDIFF":
            algorithm = Algorithm.HASHDIFF
        elif config.algorithm == "JOINDIFF":
            algorithm = Algorithm.JOINDIFF

        # 执行比对
        return await asyncio.to_thread(
            diff_tables,
            source_table,
            target_table,
            algorithm=algorithm,
            threaded=config.threaded,
            max_threadpool_size=config.max_threadpool_size,
            bisection_factor=config.bisection_factor,
            bisection_threshold=config.bisection_threshold,
            materialize_to_table=config.materialize_to_table,
            materialize_all_rows=config.materialize_all_rows,
            table_write_limit=config.table_write_limit,
            validate_unique_key=not config.assume_unique_key,
            sample_exclusive_rows=config.sample_exclusive_rows,
            skip_null_keys=config.skip_null_keys
        )

    async def _post_process_result(self, diff_result, config) -> DiffResult:
        """后处理比对结果"""
        # 转换为标准化的 DiffResult 格式
        stats = diff_result.get_stats_dict(is_dbt=False)

        return DiffResult(
            status="success",
            result="different" if stats['total'] > 0 else "identical",
            total_rows_source=stats['rows_A'],
            total_rows_target=stats['rows_B'],
            rows_identical=stats['unchanged'],
            rows_different=stats['updated'],
            rows_exclusive_source=stats['exclusive_A'],
            rows_exclusive_target=stats['exclusive_B'],
            runtime_seconds=getattr(diff_result, 'runtime', 0),
            algorithm_used=config.algorithm,
            rows_downloaded=diff_result.stats.get('rows_downloaded', 0),
            # ... 其他字段的转换
        )
```

#### 5.2.2 进度监控
```python
class DiffProgressMonitor:
    """比对进度监控"""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def update_progress(self, task_id: str, progress: float, stage: str):
        """更新任务进度"""
        progress_data = {
            'progress': progress,
            'stage': stage,
            'timestamp': datetime.utcnow().isoformat()
        }
        await self.redis.hset(f"diff_progress:{task_id}", mapping=progress_data)

    async def get_progress(self, task_id: str):
        """获取任务进度"""
        return await self.redis.hgetall(f"diff_progress:{task_id}")
```

### 5.3 结果处理与展示

#### 5.3.1 结果格式化器
```python
class DiffResultFormatter:
    """比对结果格式化器 - 基于 data-diff 输出格式"""

    def format_summary(self, diff_result: DiffResult) -> Dict:
        """生成比对摘要 - 兼容 Datafold UI 格式"""
        return {
            'overview': {
                'status': diff_result.status,
                'result': diff_result.result,
                'total_rows_source': diff_result.total_rows_source,
                'total_rows_target': diff_result.total_rows_target,
                'rows_identical': diff_result.rows_identical,
                'rows_different': diff_result.rows_different,
                'rows_exclusive_source': diff_result.rows_exclusive_source,
                'rows_exclusive_target': diff_result.rows_exclusive_target,
                'runtime_seconds': diff_result.runtime_seconds,
                'algorithm_used': diff_result.algorithm_used,
                'difference_percentage': self._calculate_diff_percentage(diff_result)
            },
            'columns': {
                'analysis': diff_result.columns_analysis,
                'added': diff_result.columns_added,
                'removed': diff_result.columns_removed,
                'type_changed': diff_result.columns_type_changed,
                'primary_keys': diff_result.source_info.get('primary_keys', [])
            },
            'performance': {
                'rows_downloaded': diff_result.rows_downloaded,
                'sampling_used': diff_result.source_info.get('sampling_enabled', False),
                'network_transfer_optimized': diff_result.algorithm_used == 'HASHDIFF'
            }
        }

    def format_for_n8n(self, diff_result: DiffResult) -> List[Dict]:
        """格式化为 N8N 输出格式"""
        base_output = {
            'json': self.format_summary(diff_result),
            'metadata': {
                'source': diff_result.source_info,
                'target': diff_result.target_info,
                'execution_time': diff_result.runtime_seconds,
                'data_diff_version': '1.1.0'
            }
        }

        # 如果有具体差异数据，分批输出
        output_items = [base_output]

        if diff_result.diff_rows:
            # 按批次分割差异行，避免单个输出过大
            batch_size = 1000
            for i in range(0, len(diff_result.diff_rows), batch_size):
                batch = diff_result.diff_rows[i:i+batch_size]
                output_items.append({
                    'json': {
                        'type': 'diff_rows',
                        'batch_index': i // batch_size,
                        'rows': batch
                    }
                })

        if diff_result.exclusive_rows_source:
            output_items.append({
                'json': {
                    'type': 'exclusive_source',
                    'rows': diff_result.exclusive_rows_source[:1000]  # 限制输出
                }
            })

        if diff_result.exclusive_rows_target:
            output_items.append({
                'json': {
                    'type': 'exclusive_target',
                    'rows': diff_result.exclusive_rows_target[:1000]  # 限制输出
                }
            })

        return output_items

    def format_for_datafold_ui(self, diff_result: DiffResult) -> Dict:
        """格式化为与 Datafold UI 兼容的格式"""
        return {
            'version': '1.1.0',
            'status': 'success',
            'result': diff_result.result,
            'dataset1': diff_result.source_info.get('table_path', []),
            'dataset2': diff_result.target_info.get('table_path', []),
            'rows': {
                'exclusive': {
                    'dataset1': self._format_exclusive_rows(diff_result.exclusive_rows_source),
                    'dataset2': self._format_exclusive_rows(diff_result.exclusive_rows_target)
                },
                'diff': self._format_diff_rows(diff_result.diff_rows)
            },
            'columns': {
                'primaryKey': diff_result.source_info.get('primary_keys', []),
                'dataset1': self._format_column_info(diff_result.source_info.get('columns', [])),
                'dataset2': self._format_column_info(diff_result.target_info.get('columns', [])),
                'exclusive': {
                    'dataset1': diff_result.columns_removed,
                    'dataset2': diff_result.columns_added
                },
                'typeChanged': diff_result.columns_type_changed
            },
            'summary': {
                'rows': {
                    'total': {
                        'dataset1': diff_result.total_rows_source,
                        'dataset2': diff_result.total_rows_target
                    },
                    'exclusive': {
                        'dataset1': diff_result.rows_exclusive_source,
                        'dataset2': diff_result.rows_exclusive_target
                    },
                    'updated': diff_result.rows_different,
                    'unchanged': diff_result.rows_identical
                },
                'stats': {
                    'diffCounts': diff_result.columns_analysis
                }
            }
        }

    def _calculate_diff_percentage(self, diff_result: DiffResult) -> float:
        """计算差异百分比"""
        total_rows = max(diff_result.total_rows_source, diff_result.total_rows_target)
        if total_rows == 0:
            return 0.0

        different_rows = (diff_result.rows_different +
                         diff_result.rows_exclusive_source +
                         diff_result.rows_exclusive_target)

        return round((different_rows / total_rows) * 100, 2)
```

#### 5.3.2 报告生成器
```python
class DiffReportGenerator:
    """比对报告生成器 - 支持多种输出格式"""

    def generate_html_report(self, diff_result: DiffResult) -> str:
        """生成 HTML 报告 - 模仿 Datafold UI 样式"""
        template = self._load_template('diff_report.html')

        charts_config = self._generate_charts_config(diff_result)
        summary_stats = self.formatter.format_summary(diff_result)

        return template.render(
            summary=summary_stats,
            charts=charts_config,
            diff_rows=diff_result.diff_rows[:100],  # 限制显示行数
            exclusive_rows_source=diff_result.exclusive_rows_source[:50],
            exclusive_rows_target=diff_result.exclusive_rows_target[:50],
            timestamp=datetime.utcnow(),
            performance_stats={
                'algorithm': diff_result.algorithm_used,
                'runtime': diff_result.runtime_seconds,
                'rows_processed': diff_result.total_rows_source + diff_result.total_rows_target,
                'network_optimized': diff_result.algorithm_used == 'HASHDIFF'
            }
        )

    def generate_excel_report(self, diff_result: DiffResult) -> bytes:
        """生成 Excel 报告 - 包含多个工作表"""
        import pandas as pd
        from io import BytesIO

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 概览工作表
            overview_df = pd.DataFrame([{
                '指标': '源表行数', '值': diff_result.total_rows_source
            }, {
                '指标': '目标表行数', '值': diff_result.total_rows_target
            }, {
                '指标': '相同行数', '值': diff_result.rows_identical
            }, {
                '指标': '不同行数', '值': diff_result.rows_different
            }, {
                '指标': '仅源表存在', '值': diff_result.rows_exclusive_source
            }, {
                '指标': '仅目标表存在', '值': diff_result.rows_exclusive_target
            }, {
                '指标': '运行时间(秒)', '值': diff_result.runtime_seconds
            }])
            overview_df.to_excel(writer, sheet_name='概览', index=False)

            # 差异行工作表
            if diff_result.diff_rows:
                diff_df = pd.DataFrame(diff_result.diff_rows)
                diff_df.to_excel(writer, sheet_name='差异行', index=False)

            # 独有行工作表
            if diff_result.exclusive_rows_source:
                exclusive_source_df = pd.DataFrame(diff_result.exclusive_rows_source)
                exclusive_source_df.to_excel(writer, sheet_name='仅源表存在', index=False)

            if diff_result.exclusive_rows_target:
                exclusive_target_df = pd.DataFrame(diff_result.exclusive_rows_target)
                exclusive_target_df.to_excel(writer, sheet_name='仅目标表存在', index=False)

            # 列分析工作表
            if diff_result.columns_analysis:
                columns_df = pd.DataFrame([
                    {'列名': col, '差异数量': stats}
                    for col, stats in diff_result.columns_analysis.items()
                ])
                columns_df.to_excel(writer, sheet_name='列级分析', index=False)

        output.seek(0)
        return output.read()

    def generate_slack_message(self, diff_result: DiffResult) -> Dict:
        """生成 Slack 消息格式"""
        color = "good" if diff_result.result == "identical" else "warning"

        # 构建主要统计信息
        stats_text = f"""
        • 源表: {diff_result.total_rows_source:,} 行
        • 目标表: {diff_result.total_rows_target:,} 行
        • 相同: {diff_result.rows_identical:,} 行
        • 不同: {diff_result.rows_different:,} 行
        • 仅源表: {diff_result.rows_exclusive_source:,} 行
        • 仅目标表: {diff_result.rows_exclusive_target:,} 行
        • 运行时间: {diff_result.runtime_seconds:.2f} 秒
        """

        return {
            "attachments": [{
                "color": color,
                "title": f"数据比对结果: {diff_result.result.upper()}",
                "text": stats_text,
                "fields": [
                    {
                        "title": "算法",
                        "value": diff_result.algorithm_used,
                        "short": True
                    },
                    {
                        "title": "差异百分比",
                        "value": f"{self.formatter._calculate_diff_percentage(diff_result):.2f}%",
                        "short": True
                    }
                ],
                "footer": "Data-Diff N8N Integration",
                "ts": int(time.time())
            }]
        }

    def _generate_charts_config(self, diff_result: DiffResult) -> Dict:
        """生成图表配置 - 用于前端展示"""
        return {
            'row_distribution': {
                'type': 'pie',
                'data': {
                    'labels': ['相同', '不同', '仅源表', '仅目标表'],
                    'datasets': [{
                        'data': [
                            diff_result.rows_identical,
                            diff_result.rows_different,
                            diff_result.rows_exclusive_source,
                            diff_result.rows_exclusive_target
                        ],
                        'backgroundColor': ['#28a745', '#ffc107', '#dc3545', '#17a2b8']
                    }]
                }
            },
            'column_differences': {
                'type': 'bar',
                'data': {
                    'labels': list(diff_result.columns_analysis.keys())[:10],  # 前10列
                    'datasets': [{
                        'label': '差异数量',
                        'data': list(diff_result.columns_analysis.values())[:10],
                        'backgroundColor': '#007bff'
                    }]
                }
            }
        }
```

## 6. 部署与运维

### 6.1 部署方案

#### 6.1.1 开发环境部署
```bash
# 克隆项目
git clone https://github.com/yunqiqiliang/data-diff-n8n.git
cd data-diff-n8n

# 启动开发环境
docker-compose -f docker-compose.dev.yml up -d

# 安装 N8N 自定义节点
docker exec -it data-diff-n8n_n8n_1 npm install n8n-nodes-data-diff
```

#### 6.1.2 生产环境部署
```bash
# 生产环境配置
cp .env.example .env
# 编辑 .env 文件配置生产参数

# 启动生产环境
docker-compose -f docker-compose.prod.yml up -d

# 设置 SSL 证书
docker exec -it nginx certbot --nginx -d your-domain.com
```

### 6.2 监控与日志

#### 6.2.1 性能监控
```python
# 集成 Prometheus 监控
from prometheus_client import Counter, Histogram, Gauge

diff_executions_total = Counter('diff_executions_total', 'Total diff executions')
diff_duration_seconds = Histogram('diff_duration_seconds', 'Diff execution duration')
active_connections = Gauge('active_database_connections', 'Active database connections')
```

#### 6.2.2 日志管理
```yaml
# docker-compose.yml 日志配置
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

### 6.3 备份与恢复

#### 6.3.1 数据备份策略
```bash
# 自动备份脚本
#!/bin/bash
# backup.sh

# 备份 PostgreSQL
docker exec postgres pg_dump -U n8n n8n > backup/postgres_$(date +%Y%m%d_%H%M%S).sql

# 备份 N8N 工作流
docker exec n8n n8n export:workflow --all --output=backup/workflows_$(date +%Y%m%d_%H%M%S).json

# 备份配置文件
tar -czf backup/configs_$(date +%Y%m%d_%H%M%S).tar.gz configs/
```

## 7. 测试策略

### 7.1 单元测试
- 数据库连接器测试（连接、查询、元数据获取）
- 比对算法核心逻辑测试（HASHDIFF vs JOINDIFF）
- N8N 节点功能测试（输入验证、输出格式）
- 配置验证测试（参数组合、边界条件）
- 采样算法测试（置信度、容错率验证）
- 数据类型转换测试（跨数据库兼容性）

### 7.2 集成测试
- 端到端工作流测试（完整 N8N 流程）
- 跨数据库比对测试（各种数据库组合）
- 性能基准测试（基于 Datafold 基准数据）
- 故障恢复测试（网络中断、数据库连接断开）
- 大数据量测试（百万级、千万级数据）
- 采样效果验证（性能提升vs准确性）

### 7.3 性能测试
- 大数据量比对测试（基于 Datafold 性能基准）
- 并发执行测试（多任务同时运行）
- 内存使用测试（内存泄漏、峰值监控）
- 网络传输优化测试（HASHDIFF vs 传统方法）
- 采样性能测试（2x-20x 提速验证）
- 索引优化效果测试（主键分布影响）

## 8. 项目里程碑

### 8.1 第一阶段（4周）
- ✅ 完成 Clickzetta 数据库支持
- ✅ 实现基础 N8N 集成框架
- 🔄 开发核心 N8N 节点
- 🔄 实现基础比对工作流

### 8.2 第二阶段（4周）
- 开发完整的 Web 界面
- 实现高级比对策略
- 添加结果可视化功能
- 完善错误处理和监控

### 8.3 第三阶段（4周）
- 性能优化和测试
- 完善部署和运维工具
- 编写用户文档
- 准备生产环境发布

## 9. 风险评估与应对

### 9.1 技术风险
- **风险**：大数据量比对性能问题
- **应对**：采样策略（2x-20x提速）、智能二分法、并行处理、索引优化建议

### 9.2 集成风险
- **风险**：N8N 自定义节点兼容性
- **应对**：严格按照 N8N 开发规范、充分测试、版本兼容性检查

### 9.3 运维风险
- **风险**：Docker 容器化部署复杂性
- **应对**：详细的部署文档、自动化脚本、健康检查机制

### 9.4 数据质量风险
- **风险**：采样导致的准确性损失
- **应对**：可配置的置信度和容错率、采样效果验证、关键场景禁用采样

### 9.5 性能风险
- **风险**：高差异百分比场景下性能退化
- **应对**：智能差异检测、提前终止机制、分批处理、用户预警

## 10. 结论

本设计方案基于成熟的 data-diff 核心算法和 N8N 工作流引擎，深度结合 Datafold 官方文档的最佳实践，实现了一个完整的企业级跨数据库数据比对解决方案。该方案具有以下优势：

1. **功能完整**：涵盖从数据源配置到结果展示的完整流程，支持 Datafold 官方的所有核心功能
2. **性能卓越**：通过智能采样（2x-20x提速）、并行处理、二分法优化等技术确保高性能
3. **易于部署**：一键 Docker Compose 部署，降低运维复杂度
4. **扩展性强**：基于 N8N 的插件化架构，便于功能扩展和自定义工作流
5. **企业级**：完善的监控、日志、备份机制，支持大规模生产环境
6. **最佳实践**：严格遵循 Datafold 的配置建议和性能优化策略

**关键技术特性**：
- **🎯 Clickzetta 专业支持**：专门优化的 Clickzetta 与主流数据库（PostgreSQL、MySQL、Oracle、Snowflake、BigQuery、ClickHouse、Redshift 等）的双向比对能力
- **🚀 核心场景优化**：针对数据迁移、同步验证、ETL 结果验证等 Clickzetta 核心应用场景的专业化解决方案
- **⚡ 智能算法选择**：HASHDIFF（跨库）和 JOINDIFF（同库）自动算法选择，针对 Clickzetta 场景优化
- **📈 高性能采样**：智能采样策略，在保证准确性的前提下实现 2x-20x 性能提升，特别适合大数据量迁移验证
- **🔄 完整兼容性处理**：Clickzetta 与其他数据库间的数据类型自动转换、精度保持、字符编码统一
- **📊 统计学基础**：基于统计学的置信度和容错率配置（95%-99% 置信度，0.001%-5% 容错率）
- **🏢 企业级特性**：支持结果物化、进度监控、异常恢复、分布式执行等功能

该方案专门针对 Clickzetta 数据比对需求设计，特别适用于以下核心场景：

**🎯 核心应用场景**：
- **数据迁移项目**：从 Oracle/MySQL/PostgreSQL 迁移到 Clickzetta 时的数据完整性验证
- **云数仓整合**：Snowflake/BigQuery/Redshift 与 Clickzetta 之间的数据迁移和一致性验证
- **分析平台升级**：ClickHouse/Vertica 向 Clickzetta 平台迁移时的数据验证
- **实时同步监控**：业务库（MySQL/PostgreSQL）与 Clickzetta 分析库的数据同步质量保证
- **ETL 流程验证**：多源数据系统经过 ETL 处理后导入 Clickzetta 的结果验证
- **数据质量监控**：生产环境与 Clickzetta 备份/测试环境的数据一致性监控

**💡 独特价值**：
- 专门为 Clickzetta 场景优化的数据比对解决方案
- 基于 data-diff 开源核心 + Clickzetta 专业支持的企业级方案
- 相比商业化 Datafold 解决方案的开源替代，专注 Clickzetta 核心需求
