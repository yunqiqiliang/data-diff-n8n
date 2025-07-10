"""
Clickzetta 数据库适配器
专门针对 Clickzetta 数据库的特殊处理
"""

import logging
from typing import Dict, Any, List, Optional
import json
from .database_registry import database_registry


class ClickzettaAdapter:
    """
    Clickzetta 数据库适配器
    处理 Clickzetta 特有的功能和优化
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def optimize_connection_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        优化 Clickzetta 连接配置

        Args:
            config: 原始连接配置

        Returns:
            优化后的配置
        """
        optimized_config = config.copy()

        # Clickzetta 特有的优化设置
        if config.get("database_type") == "clickzetta":
            # 从数据库注册表获取 Clickzetta 配置
            clickzetta_db_config = database_registry.get_database_config("clickzetta")

            # 设置默认模式
            if "schema" not in optimized_config and clickzetta_db_config.default_schema:
                optimized_config["schema"] = clickzetta_db_config.default_schema

            # 设置默认工作空间
            if "workspace" not in optimized_config:
                default_workspace = clickzetta_db_config.extra_params.get("default_workspace", "default")
                optimized_config["workspace"] = default_workspace

            # 注意：Clickzetta 不使用传统的端口，连接通过 instance.service 格式
            # 如果配置中有端口，移除它以避免混淆
            if "port" in optimized_config:
                self.logger.warning("Removing port from Clickzetta config - Clickzetta uses instance.service format")
                optimized_config.pop("port")

            # 启用压缩
            if "enable_compression" not in optimized_config:
                optimized_config["enable_compression"] = True

            # 设置连接池大小
            if "connection_pool_size" not in optimized_config:
                optimized_config["connection_pool_size"] = 10

            # 设置查询超时
            if "query_timeout" not in optimized_config:
                optimized_config["query_timeout"] = 300

            # 启用异步查询
            if "enable_async" not in optimized_config:
                optimized_config["enable_async"] = True

            self.logger.info("Applied Clickzetta-specific optimizations")

        return optimized_config

    def build_optimized_query(
        self,
        table_name: str,
        columns: List[str],
        where_clause: Optional[str] = None,
        sample_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        构建针对 Clickzetta 优化的查询

        Args:
            table_name: 表名
            columns: 列名列表
            where_clause: WHERE 条件
            sample_config: 采样配置

        Returns:
            优化的 SQL 查询
        """
        # 构建列选择
        if columns:
            column_list = ", ".join(columns)
        else:
            column_list = "*"

        # 基础查询
        query = f"SELECT {column_list} FROM {table_name}"

        # 添加 WHERE 条件
        if where_clause:
            query += f" WHERE {where_clause}"

        # 添加 Clickzetta 特有的采样
        if sample_config and sample_config.get("enable_sampling"):
            sample_size = sample_config.get("sample_size", 10000)
            sampling_method = sample_config.get("sampling_method", "random")

            if sampling_method == "random":
                # 使用 SAMPLE 子句进行随机采样
                sample_ratio = min(sample_size / 1000000, 0.1)  # 假设大表有百万行
                query += f" SAMPLE {sample_ratio}"
            elif sampling_method == "systematic":
                # 使用系统采样
                query += f" ORDER BY rand() LIMIT {sample_size}"
            else:
                # 默认限制行数
                query += f" LIMIT {sample_size}"

        # 添加 Clickzetta 特有的优化提示
        query = self._add_clickzetta_hints(query, sample_config)

        self.logger.debug(f"Generated optimized Clickzetta query: {query}")
        return query

    def _add_clickzetta_hints(self, query: str, config: Optional[Dict[str, Any]] = None) -> str:
        """
        添加 Clickzetta 查询提示
        """
        hints = []

        if config:
            # 并行度提示
            if config.get("parallel_workers", 0) > 1:
                hints.append(f"/*+ PARALLEL({config['parallel_workers']}) */")

            # 内存限制提示
            if config.get("max_memory_mb"):
                hints.append(f"/*+ MAX_MEMORY({config['max_memory_mb']}MB) */")

        if hints:
            hint_str = " ".join(hints)
            query = f"{hint_str}\n{query}"

        return query

    def optimize_comparison_strategy(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        优化 Clickzetta 比对策略

        Args:
            config: 比对配置

        Returns:
            优化后的配置
        """
        optimized_config = config.copy()

        source_db = config.get("source_database")
        target_db = config.get("target_database")

        # 如果涉及 Clickzetta，应用特殊优化
        if source_db == "clickzetta" or target_db == "clickzetta":

            # 对于大表，启用采样
            if not config.get("enable_sampling"):
                optimized_config["enable_sampling"] = True
                optimized_config["sample_size"] = 100000
                optimized_config["sampling_method"] = "random"
                optimized_config["confidence_level"] = 0.95

            # 使用 JoinDiff 算法（对 Clickzetta 更优）
            if not config.get("algorithm"):
                optimized_config["algorithm"] = "joindiff"

            # 增加并行度
            if not config.get("parallel_workers"):
                optimized_config["parallel_workers"] = 8

            # 设置批处理大小
            if not config.get("batch_size"):
                optimized_config["batch_size"] = 50000

            # 针对 Clickzetta 的时间过滤优化
            if config.get("time_range_filter") and config.get("time_column"):
                optimized_config["clickzetta_time_optimization"] = True

            self.logger.info("Applied Clickzetta comparison optimizations")

        return optimized_config

    def get_recommended_indexes(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        获取 Clickzetta 推荐索引

        Args:
            config: 比对配置

        Returns:
            推荐索引列表
        """
        recommendations = []

        key_columns = config.get("key_columns", [])
        time_column = config.get("time_column")

        if key_columns:
            # 推荐主键索引
            recommendations.append({
                "type": "primary_key_index",
                "columns": key_columns,
                "priority": "high",
                "reason": "Primary key columns for efficient joins",
                "sql": f"ALTER TABLE {{table}} ORDER BY ({', '.join(key_columns)})"
            })

        if time_column:
            # 推荐时间列索引
            recommendations.append({
                "type": "time_index",
                "columns": [time_column],
                "priority": "medium",
                "reason": "Time-based filtering optimization",
                "sql": f"ALTER TABLE {{table}} ADD INDEX idx_time ({time_column}) TYPE minmax GRANULARITY 1"
            })

        # 推荐分区策略
        if time_column:
            recommendations.append({
                "type": "partition_strategy",
                "columns": [time_column],
                "priority": "high",
                "reason": "Improved query performance for time-based data",
                "sql": f"ALTER TABLE {{table}} PARTITION BY toYYYYMM({time_column})"
            })

        return recommendations

    def estimate_performance(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        估计 Clickzetta 比对性能

        Args:
            config: 比对配置

        Returns:
            性能估计
        """
        # 基于配置估计性能
        rows_estimated = 1000000  # 假设的行数

        # 根据采样调整
        if config.get("enable_sampling"):
            sample_size = config.get("sample_size", 10000)
            rows_to_process = min(sample_size, rows_estimated)
        else:
            rows_to_process = rows_estimated

        # 根据并行度调整
        parallel_workers = config.get("parallel_workers", 1)
        base_throughput = 10000  # 每秒处理行数
        estimated_throughput = base_throughput * parallel_workers * 0.8  # 并行效率 80%

        estimated_time_seconds = rows_to_process / estimated_throughput

        return {
            "estimated_rows": rows_estimated,
            "rows_to_process": rows_to_process,
            "estimated_time_seconds": estimated_time_seconds,
            "estimated_throughput": estimated_throughput,
            "parallel_workers": parallel_workers,
            "memory_usage_mb": config.get("max_memory_mb", 1024),
            "performance_tier": self._get_performance_tier(estimated_time_seconds)
        }

    def _get_performance_tier(self, time_seconds: float) -> str:
        """
        获取性能等级
        """
        if time_seconds < 60:
            return "fast"
        elif time_seconds < 300:
            return "medium"
        elif time_seconds < 1800:
            return "slow"
        else:
            return "very_slow"

    def get_data_type_mappings(self) -> Dict[str, Dict[str, str]]:
        """
        获取 Clickzetta 与其他数据库的数据类型映射

        Returns:
            数据类型映射字典
        """
        return {
            "postgresql": {
                "UInt64": "BIGINT",
                "Int64": "BIGINT",
                "UInt32": "INTEGER",
                "Int32": "INTEGER",
                "String": "VARCHAR",
                "FixedString": "CHAR",
                "DateTime": "TIMESTAMP",
                "Date": "DATE",
                "Float64": "DOUBLE PRECISION",
                "Float32": "REAL",
                "Decimal": "DECIMAL"
            },
            "mysql": {
                "UInt64": "BIGINT UNSIGNED",
                "Int64": "BIGINT",
                "UInt32": "INT UNSIGNED",
                "Int32": "INT",
                "String": "VARCHAR",
                "FixedString": "CHAR",
                "DateTime": "DATETIME",
                "Date": "DATE",
                "Float64": "DOUBLE",
                "Float32": "FLOAT",
                "Decimal": "DECIMAL"
            },
            "snowflake": {
                "UInt64": "NUMBER",
                "Int64": "NUMBER",
                "UInt32": "NUMBER",
                "Int32": "NUMBER",
                "String": "VARCHAR",
                "FixedString": "CHAR",
                "DateTime": "TIMESTAMP_NTZ",
                "Date": "DATE",
                "Float64": "FLOAT",
                "Float32": "FLOAT",
                "Decimal": "NUMBER"
            }
        }

    def suggest_compatibility_fixes(self, issues: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        建议兼容性修复方案

        Args:
            issues: 兼容性问题列表

        Returns:
            修复建议列表
        """
        suggestions = []

        for issue in issues:
            issue_type = issue.get("type")

            if issue_type == "data_type_mismatch":
                source_type = issue.get("source_type")
                target_type = issue.get("target_type")
                column = issue.get("column")

                suggestions.append({
                    "issue": issue,
                    "suggestion": f"Convert {source_type} to {target_type} for column {column}",
                    "sql": f"CAST({column} AS {target_type})",
                    "priority": "high"
                })

            elif issue_type == "encoding_mismatch":
                suggestions.append({
                    "issue": issue,
                    "suggestion": "Ensure consistent character encoding (UTF-8 recommended)",
                    "action": "Check database encoding settings",
                    "priority": "medium"
                })

            elif issue_type == "timezone_mismatch":
                suggestions.append({
                    "issue": issue,
                    "suggestion": "Normalize timezone handling",
                    "sql": "Convert all timestamps to UTC",
                    "priority": "high"
                })

        return suggestions

    def validate_connection_config(self, config: Dict[str, Any]) -> List[str]:
        """
        验证 Clickzetta 连接配置，使用数据库注册表的验证逻辑

        Args:
            config: 连接配置

        Returns:
            验证错误列表
        """
        errors = []

        # 使用数据库注册表的验证逻辑
        if config.get("database_type") == "clickzetta":
            registry_errors = database_registry.validate_config("clickzetta", config)
            errors.extend(registry_errors)

            # 添加 Clickzetta 特有的验证
            if not config.get("instance"):
                errors.append("Clickzetta requires 'instance' parameter")

            if not config.get("service"):
                errors.append("Clickzetta requires 'service' parameter")

            if not config.get("workspace"):
                errors.append("Clickzetta requires 'workspace' parameter")

        return errors

    def build_connection_string(self, config: Dict[str, Any]) -> str:
        """
        构建 Clickzetta 连接字符串，使用数据库注册表的构建逻辑

        Args:
            config: 连接配置

        Returns:
            连接字符串
        """
        if config.get("database_type") == "clickzetta":
            return database_registry.build_connection_string("clickzetta", config)
        else:
            raise ValueError("This adapter only supports Clickzetta database type")

    def get_connection_help(self) -> str:
        """
        获取 Clickzetta 连接帮助信息

        Returns:
            连接帮助信息
        """
        clickzetta_config = database_registry.get_database_config("clickzetta")
        return clickzetta_config.connect_uri_help if clickzetta_config else ""

    def get_required_params(self) -> List[str]:
        """
        获取 Clickzetta 连接所需的参数

        Returns:
            必需参数列表
        """
        clickzetta_config = database_registry.get_database_config("clickzetta")
        if clickzetta_config:
            return clickzetta_config.connect_uri_params + ["instance", "service", "workspace"]
        return []
