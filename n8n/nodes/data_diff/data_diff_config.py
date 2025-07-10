"""
Data-Diff 比对配置节点
用于配置数据比对参数和策略
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime

from n8n_sdk import Node, NodePropertyTypes, INodeExecutionData


class DataDiffConfig(Node):
    """
    Data-Diff 比对配置节点
    用于配置数据比对的各种参数和策略
    """

    display_name = "Data-Diff Config"
    description = "Configure data comparison parameters and strategies"
    group = "transform"
    version = 1

    # 节点输入输出配置
    inputs = ["main"]
    outputs = ["main"]

    # 节点属性配置
    properties = [
        # 基础配置
        {
            "displayName": "Source Database",
            "name": "source_database",
            "type": NodePropertyTypes.OPTIONS,
            "default": "clickzetta",
            "options": [
                {"name": "Clickzetta", "value": "clickzetta"},
                {"name": "PostgreSQL", "value": "postgresql"},
                {"name": "MySQL", "value": "mysql"},
                {"name": "Oracle", "value": "oracle"},
                {"name": "SQL Server", "value": "sqlserver"},
                {"name": "Snowflake", "value": "snowflake"},
                {"name": "BigQuery", "value": "bigquery"},
                {"name": "ClickHouse", "value": "clickhouse"},
                {"name": "Redshift", "value": "redshift"},
                {"name": "DuckDB", "value": "duckdb"}
            ],
            "description": "Source database type"
        },
        {
            "displayName": "Target Database",
            "name": "target_database",
            "type": NodePropertyTypes.OPTIONS,
            "default": "postgresql",
            "options": [
                {"name": "Clickzetta", "value": "clickzetta"},
                {"name": "PostgreSQL", "value": "postgresql"},
                {"name": "MySQL", "value": "mysql"},
                {"name": "Oracle", "value": "oracle"},
                {"name": "SQL Server", "value": "sqlserver"},
                {"name": "Snowflake", "value": "snowflake"},
                {"name": "BigQuery", "value": "bigquery"},
                {"name": "ClickHouse", "value": "clickhouse"},
                {"name": "Redshift", "value": "redshift"},
                {"name": "DuckDB", "value": "duckdb"}
            ],
            "description": "Target database type"
        },
        {
            "displayName": "Source Table",
            "name": "source_table",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Source table name (schema.table or table)"
        },
        {
            "displayName": "Target Table",
            "name": "target_table",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Target table name (schema.table or table)"
        },
        # 比对策略配置
        {
            "displayName": "Comparison Algorithm",
            "name": "algorithm",
            "type": NodePropertyTypes.OPTIONS,
            "default": "joindiff",
            "options": [
                {
                    "name": "JoinDiff (Recommended)",
                    "value": "joindiff",
                    "description": "Join-based comparison, suitable for most cases"
                },
                {
                    "name": "HashDiff",
                    "value": "hashdiff",
                    "description": "Hash-based comparison, good for large datasets"
                }
            ],
            "description": "Comparison algorithm to use"
        },
        {
            "displayName": "Key Columns",
            "name": "key_columns",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Primary key columns for comparison (comma-separated)"
        },
        {
            "displayName": "Compare Columns",
            "name": "compare_columns",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "description": "Specific columns to compare (empty = all columns, comma-separated)"
        },
        {
            "displayName": "Exclude Columns",
            "name": "exclude_columns",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "description": "Columns to exclude from comparison (comma-separated)"
        },
        # 采样和性能配置
        {
            "displayName": "Enable Sampling",
            "name": "enable_sampling",
            "type": NodePropertyTypes.BOOLEAN,
            "default": False,
            "description": "Enable data sampling for large datasets"
        },
        {
            "displayName": "Sample Size",
            "name": "sample_size",
            "type": NodePropertyTypes.NUMBER,
            "default": 10000,
            "displayOptions": {
                "show": {
                    "enable_sampling": [True]
                }
            },
            "description": "Number of rows to sample"
        },
        {
            "displayName": "Sampling Method",
            "name": "sampling_method",
            "type": NodePropertyTypes.OPTIONS,
            "default": "random",
            "displayOptions": {
                "show": {
                    "enable_sampling": [True]
                }
            },
            "options": [
                {"name": "Random", "value": "random"},
                {"name": "Systematic", "value": "systematic"},
                {"name": "Stratified", "value": "stratified"}
            ],
            "description": "Sampling method to use"
        },
        {
            "displayName": "Confidence Level",
            "name": "confidence_level",
            "type": NodePropertyTypes.NUMBER,
            "default": 0.95,
            "displayOptions": {
                "show": {
                    "enable_sampling": [True]
                }
            },
            "description": "Statistical confidence level (0.9, 0.95, 0.99)"
        },
        # 过滤和条件
        {
            "displayName": "Source Filter",
            "name": "source_filter",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "description": "WHERE condition for source table"
        },
        {
            "displayName": "Target Filter",
            "name": "target_filter",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "description": "WHERE condition for target table"
        },
        {
            "displayName": "Time Range Filter",
            "name": "time_range_filter",
            "type": NodePropertyTypes.BOOLEAN,
            "default": False,
            "description": "Enable time-based filtering"
        },
        {
            "displayName": "Time Column",
            "name": "time_column",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "displayOptions": {
                "show": {
                    "time_range_filter": [True]
                }
            },
            "description": "Time column for filtering"
        },
        {
            "displayName": "Time Range",
            "name": "time_range",
            "type": NodePropertyTypes.STRING,
            "default": "1 day",
            "displayOptions": {
                "show": {
                    "time_range_filter": [True]
                }
            },
            "description": "Time range (e.g., '1 day', '1 week', '1 month')"
        },
        # 比对选项
        {
            "displayName": "Tolerance",
            "name": "tolerance",
            "type": NodePropertyTypes.NUMBER,
            "default": 0.0,
            "description": "Numerical tolerance for floating point comparisons"
        },
        {
            "displayName": "Case Sensitive",
            "name": "case_sensitive",
            "type": NodePropertyTypes.BOOLEAN,
            "default": True,
            "description": "Case sensitive string comparison"
        },
        {
            "displayName": "Ignore Whitespace",
            "name": "ignore_whitespace",
            "type": NodePropertyTypes.BOOLEAN,
            "default": False,
            "description": "Ignore leading/trailing whitespace in strings"
        },
        # 性能和资源配置
        {
            "displayName": "Max Memory MB",
            "name": "max_memory_mb",
            "type": NodePropertyTypes.NUMBER,
            "default": 1024,
            "description": "Maximum memory usage in MB"
        },
        {
            "displayName": "Parallel Workers",
            "name": "parallel_workers",
            "type": NodePropertyTypes.NUMBER,
            "default": 4,
            "description": "Number of parallel workers"
        },
        {
            "displayName": "Batch Size",
            "name": "batch_size",
            "type": NodePropertyTypes.NUMBER,
            "default": 10000,
            "description": "Batch size for processing"
        }
    ]

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    async def execute(self, items: List[INodeExecutionData]) -> List[INodeExecutionData]:
        """
        执行配置节点逻辑
        """
        results = []

        for item_index, item in enumerate(items):
            try:
                # 构建比对配置
                config = self._build_comparison_config(item_index)

                # 验证配置
                validation_result = self._validate_config(config)

                if not validation_result["valid"]:
                    raise ValueError(f"Invalid configuration: {validation_result['errors']}")

                # 构建输出数据
                output_data = {
                    "config_type": "data_diff_comparison",
                    "timestamp": datetime.now().isoformat(),
                    "config": config,
                    "validation": validation_result
                }

                results.append({
                    "json": output_data
                })

            except Exception as e:
                self.logger.error(f"Error in DataDiffConfig execution: {e}")
                results.append({
                    "json": {
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                })

        return results

    def _build_comparison_config(self, item_index: int) -> Dict[str, Any]:
        """
        构建比对配置字典
        """
        config = {
            # 基础配置
            "source_database": self.get_node_parameter("source_database", item_index),
            "target_database": self.get_node_parameter("target_database", item_index),
            "source_table": self.get_node_parameter("source_table", item_index),
            "target_table": self.get_node_parameter("target_table", item_index),

            # 比对策略
            "algorithm": self.get_node_parameter("algorithm", item_index),
            "key_columns": self._parse_column_list(self.get_node_parameter("key_columns", item_index)),
            "compare_columns": self._parse_column_list(self.get_node_parameter("compare_columns", item_index)),
            "exclude_columns": self._parse_column_list(self.get_node_parameter("exclude_columns", item_index)),

            # 采样配置
            "enable_sampling": self.get_node_parameter("enable_sampling", item_index),
            "sample_size": self.get_node_parameter("sample_size", item_index),
            "sampling_method": self.get_node_parameter("sampling_method", item_index),
            "confidence_level": self.get_node_parameter("confidence_level", item_index),

            # 过滤条件
            "source_filter": self.get_node_parameter("source_filter", item_index),
            "target_filter": self.get_node_parameter("target_filter", item_index),
            "time_range_filter": self.get_node_parameter("time_range_filter", item_index),
            "time_column": self.get_node_parameter("time_column", item_index),
            "time_range": self.get_node_parameter("time_range", item_index),

            # 比对选项
            "tolerance": self.get_node_parameter("tolerance", item_index),
            "case_sensitive": self.get_node_parameter("case_sensitive", item_index),
            "ignore_whitespace": self.get_node_parameter("ignore_whitespace", item_index),

            # 性能配置
            "max_memory_mb": self.get_node_parameter("max_memory_mb", item_index),
            "parallel_workers": self.get_node_parameter("parallel_workers", item_index),
            "batch_size": self.get_node_parameter("batch_size", item_index)
        }

        return config

    def _parse_column_list(self, column_string: str) -> List[str]:
        """
        解析列名列表
        """
        if not column_string:
            return []
        return [col.strip() for col in column_string.split(",") if col.strip()]

    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        验证配置有效性
        """
        errors = []
        warnings = []

        # 验证必填字段
        required_fields = ["source_database", "target_database", "source_table", "target_table", "key_columns"]
        for field in required_fields:
            if not config.get(field):
                errors.append(f"Missing required field: {field}")

        # 验证数据库类型
        supported_databases = ["clickzetta", "postgresql", "mysql", "oracle", "sqlserver",
                             "snowflake", "bigquery", "clickhouse", "redshift", "duckdb"]

        if config.get("source_database") not in supported_databases:
            errors.append(f"Unsupported source database: {config.get('source_database')}")

        if config.get("target_database") not in supported_databases:
            errors.append(f"Unsupported target database: {config.get('target_database')}")

        # 验证采样配置
        if config.get("enable_sampling"):
            if config.get("sample_size", 0) <= 0:
                errors.append("Sample size must be greater than 0 when sampling is enabled")

            confidence_level = config.get("confidence_level", 0)
            if not (0.8 <= confidence_level <= 0.99):
                errors.append("Confidence level must be between 0.8 and 0.99")

        # 验证性能配置
        if config.get("max_memory_mb", 0) < 256:
            warnings.append("Low memory limit may affect performance")

        if config.get("parallel_workers", 0) < 1:
            errors.append("Parallel workers must be at least 1")

        # 验证容差配置
        if config.get("tolerance", 0) < 0:
            errors.append("Tolerance must be non-negative")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
