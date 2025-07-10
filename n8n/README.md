# N8N 集成文档

## 概述

这个项目在原始的 data-diff 基础上增加了两个主要功能：
1. **Clickzetta 数据库支持** - 为 data-diff 添加对 Clickzetta 数据库的完整支持
2. **N8N 工作流集成** - 提供在 N8N 工作流中使用 data-diff 和 Clickzetta 的集成功能

所有增量代码都组织在 `n8n/` 目录下，保持与主项目代码的清晰分离。

## 目录结构

```
# 主项目结构
data_diff/
├── databases/
│   └── clickzetta.py                   # Clickzetta 数据库驱动（主项目）

# N8N 集成模块
n8n/
├── __init__.py                          # N8N 模块初始化
├── docs/                               # 文档目录
│   └── REQUIREMENTS_AND_DESIGN.md     # 需求分析与设计文档
├── integration/                         # N8N 集成模块
│   ├── __init__.py
│   └── workflow.py                     # N8N 工作流集成
└── tests/                              # 测试模块
    ├── __init__.py
    ├── test_clickzetta_connector.py    # Clickzetta 连接器测试
    ├── test_clickzetta_database.py     # Clickzetta 数据库测试
    ├── test_n8n_integration.py         # N8N 集成测试
    ├── test_complete_integration.py    # 完整集成测试
    └── test_n8n_workflow.py           # N8N 工作流测试
```
```

## 核心文档

### 📖 详细设计文档
完整的需求分析、系统架构设计和技术实现方案请参考：
- **[需求分析与设计文档](docs/REQUIREMENTS_AND_DESIGN.md)** - 包含完整的跨数据库数据比对解决方案设计，支持 Clickzetta 与 15+ 主流数据库的任意组合比对

## 核心组件

### 1. Clickzetta 数据库支持 (`data_diff/databases/clickzetta.py`)

- **类**: `Clickzetta`, `Dialect`
- **功能**: 提供完整的 Clickzetta 数据库支持
- **特性**:
  - 支持 clickzetta-connector >= 0.8.51
  - 完整的 SQL 查询支持
  - 数据类型映射和转换
  - 表结构查询和元数据获取
  - 与 data-diff 框架的无缝集成

### 2. N8N 工作流集成 (`n8n/integration/workflow.py`)

#### N8NClickzettaConnector
- **用途**: N8N 工作流中的 Clickzetta 连接器
- **方法**:
  - `connect()`: 建立数据库连接
  - `get_tables()`: 获取表列表
  - `get_table_schema()`: 获取表结构
  - `query_data()`: 执行 SQL 查询

#### N8NDataDiffNode
- **用途**: N8N 工作流中的数据比较节点
- **方法**:
  - `compare_tables()`: 比较两个表的数据差异

#### N8NWorkflowHelper
- **用途**: 工作流辅助工具类
- **方法**:
  - `load_connection_from_file()`: 从配置文件加载连接信息
  - `format_n8n_output()`: 格式化 N8N 节点输出
  - `validate_connection_config()`: 验证连接配置

## 安装和配置

### 1. 环境要求

```bash
# Python 版本
Python >= 3.8.0, < 4.0

# 主要依赖
clickzetta-connector >= 0.8.51
attrs
data-diff (主项目)
```

### 2. 连接配置

在 `~/.clickzetta/connections.json` 中配置 Clickzetta 连接信息：

```json
{
  "connections": [
    {
      "name": "uat",
      "username": "your_username",
      "password": "your_password",
      "service": "your_service",
      "instance": "your_instance",
      "workspace": "your_workspace",
      "vcluster": "your_vcluster",
      "schema": "your_schema"
    }
  ]
}
```

## 使用示例

### 1. 基本 Clickzetta 连接

```python
from data_diff.databases.clickzetta import Clickzetta

# 创建数据库连接
db = Clickzetta(
    thread_count=1,
    username="your_username",
    password="your_password",
    service="your_service",
    instance="your_instance",
    workspace="your_workspace",
    virtualcluster="your_vcluster",
    schema="your_schema"
)

# 执行查询
result = db.query("SELECT * FROM your_table LIMIT 10")
print(result)
```

### 2. N8N 工作流集成

```python
from n8n.integration.workflow import (
    N8NClickzettaConnector,
    N8NWorkflowHelper,
    create_n8n_clickzetta_workflow_example
)

# 加载连接配置
config = N8NWorkflowHelper.load_connection_from_file(
    "~/.clickzetta/connections.json",
    "uat"
)

# 创建连接器
connector = N8NClickzettaConnector(config)
connector.connect()

# 获取表信息
tables = connector.get_tables()
schema = connector.get_table_schema(tables[0])

# 查询数据
data = connector.query_data("SELECT * FROM my_table", limit=100)
```

### 3. 使用 data-diff 进行表比较

```python
from data_diff.table_segment import TableSegment
from data_diff.databases.clickzetta import Clickzetta

# 创建数据库连接
db = Clickzetta(...)

# 创建表段对象
table_segment = TableSegment(
    database=db,
    table_path=("my_table",),
    key_columns=["id"],
    case_sensitive=False
)

# 获取表结构
schema = table_segment.get_schema()
print(f"表结构: {schema}")
```

## 测试

项目提供了完整的测试套件，涵盖所有增量功能：

### 运行所有测试

```bash
# 进入项目目录
cd /path/to/data-diff-n8n

# 运行完整集成测试
python n8n/tests/test_complete_integration.py

# 运行 N8N 工作流测试
python n8n/tests/test_n8n_workflow.py

# 运行单独的组件测试
python n8n/tests/test_clickzetta_connector.py
python n8n/tests/test_clickzetta_database.py
python n8n/tests/test_n8n_integration.py
```

### 测试分类

1. **连接器测试** - 验证 clickzetta-connector 基本功能
2. **数据库测试** - 验证 Clickzetta 数据库驱动功能
3. **集成测试** - 验证与 data-diff 框架的集成
4. **工作流测试** - 验证 N8N 工作流组件功能

## 开发指南

### 添加新功能

1. 在相应的 `n8n/` 子目录下添加代码
2. 在 `n8n/tests/` 中添加对应的测试
3. 更新此文档说明新功能

### 代码规范

- 遵循项目现有的代码风格
- 添加适当的类型注解
- 编写完整的文档字符串
- 确保所有新功能都有对应的测试

### 测试规范

- 使用描述性的测试函数名
- 包含成功和失败场景的测试
- 提供清晰的错误信息和日志
- 确保测试能够独立运行

## 故障排除

### 常见问题

1. **连接失败**
   - 检查 `~/.clickzetta/connections.json` 配置
   - 确认网络连接和服务可用性
   - 验证用户名和密码

2. **导入错误**
   - 确保项目路径正确添加到 Python path
   - 检查依赖包是否正确安装
   - 验证 Python 版本兼容性

3. **测试失败**
   - 确保 Clickzetta 服务可访问
   - 检查连接配置文件格式
   - 查看详细的错误日志

### 调试技巧

- 启用详细日志记录
- 使用 Python 调试器
- 检查网络连接状态
- 验证 SQL 查询语法

## 版本历史

### v1.0.0
- 初始版本
- 添加 Clickzetta 数据库支持
- 实现 N8N 工作流集成基础功能
- 提供完整的测试套件

## 贡献

欢迎贡献代码！请遵循以下步骤：

1. Fork 项目
2. 创建功能分支
3. 添加功能和测试
4. 提交 Pull Request

## 许可证

遵循主项目的许可证条款。
