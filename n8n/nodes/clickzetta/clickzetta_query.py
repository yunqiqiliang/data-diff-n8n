"""
Clickzetta 查询节点
用于执行 Clickzetta 数据库查询
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime

from n8n_sdk import Node, NodePropertyTypes, INodeExecutionData


class ClickzettaQuery(Node):
    """
    Clickzetta 查询节点
    支持执行各种 Clickzetta 查询操作
    """

    display_name = "Clickzetta Query"
    description = "Execute queries on Clickzetta database"
    group = "transform"
    version = 1

    # 节点输入输出配置
    inputs = ["main"]
    outputs = ["main"]

    # 节点属性配置
    properties = [
        {
            "displayName": "Connection",
            "name": "connection",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Clickzetta connection ID or connection config"
        },
        {
            "displayName": "Query Type",
            "name": "query_type",
            "type": NodePropertyTypes.OPTIONS,
            "noDataExpression": True,
            "default": "select",
            "options": [
                {
                    "name": "SELECT Query",
                    "value": "select",
                    "description": "Execute a SELECT query"
                },
                {
                    "name": "INSERT Query",
                    "value": "insert",
                    "description": "Execute an INSERT query"
                },
                {
                    "name": "UPDATE Query",
                    "value": "update",
                    "description": "Execute an UPDATE query"
                },
                {
                    "name": "DELETE Query",
                    "value": "delete",
                    "description": "Execute a DELETE query"
                },
                {
                    "name": "DDL Query",
                    "value": "ddl",
                    "description": "Execute DDL (CREATE, ALTER, DROP) queries"
                },
                {
                    "name": "Custom Query",
                    "value": "custom",
                    "description": "Execute custom SQL query"
                }
            ]
        },
        {
            "displayName": "SQL Query",
            "name": "sql_query",
            "type": NodePropertyTypes.STRING,
            "typeOptions": {
                "rows": 5
            },
            "default": "SELECT * FROM table_name LIMIT 10",
            "required": True,
            "description": "SQL query to execute"
        },
        {
            "displayName": "Parameters",
            "name": "parameters",
            "type": NodePropertyTypes.JSON,
            "default": "{}",
            "description": "Query parameters as JSON object"
        },
        {
            "displayName": "Limit Results",
            "name": "limit_results",
            "type": NodePropertyTypes.BOOLEAN,
            "default": True,
            "description": "Limit the number of returned results"
        },
        {
            "displayName": "Max Results",
            "name": "max_results",
            "type": NodePropertyTypes.NUMBER,
            "default": 1000,
            "displayOptions": {
                "show": {
                    "limit_results": [True]
                }
            },
            "description": "Maximum number of results to return"
        },
        {
            "displayName": "Output Format",
            "name": "output_format",
            "type": NodePropertyTypes.OPTIONS,
            "default": "json",
            "options": [
                {
                    "name": "JSON",
                    "value": "json",
                    "description": "Return results as JSON objects"
                },
                {
                    "name": "Array",
                    "value": "array",
                    "description": "Return results as arrays"
                },
                {
                    "name": "CSV",
                    "value": "csv",
                    "description": "Return results as CSV string"
                }
            ]
        },
        # 查询优化选项
        {
            "displayName": "Query Timeout",
            "name": "query_timeout",
            "type": NodePropertyTypes.NUMBER,
            "default": 300,
            "description": "Query timeout in seconds"
        },
        {
            "displayName": "Use Streaming",
            "name": "use_streaming",
            "type": NodePropertyTypes.BOOLEAN,
            "default": False,
            "description": "Use streaming for large result sets"
        },
        {
            "displayName": "Buffer Size",
            "name": "buffer_size",
            "type": NodePropertyTypes.NUMBER,
            "default": 10000,
            "displayOptions": {
                "show": {
                    "use_streaming": [True]
                }
            },
            "description": "Buffer size for streaming queries"
        }
    ]

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    async def execute(self, items: List[INodeExecutionData]) -> List[INodeExecutionData]:
        """
        执行查询节点逻辑
        """
        results = []

        for item_index, item in enumerate(items):
            try:
                # 获取参数
                connection = self.get_node_parameter("connection", item_index)
                query_type = self.get_node_parameter("query_type", item_index)
                sql_query = self.get_node_parameter("sql_query", item_index)
                parameters = self.get_node_parameter("parameters", item_index)
                output_format = self.get_node_parameter("output_format", item_index)

                # 解析参数
                if isinstance(parameters, str):
                    parameters = json.loads(parameters) if parameters else {}

                # 构建查询配置
                query_config = self._build_query_config(item_index)

                # 执行查询
                query_result = await self._execute_query(
                    connection, sql_query, parameters, query_config
                )

                # 格式化输出
                formatted_result = self._format_output(query_result, output_format)

                # 构建输出数据
                output_data = {
                    "query_type": query_type,
                    "sql_query": sql_query,
                    "timestamp": datetime.now().isoformat(),
                    "execution_time_ms": query_result.get("execution_time_ms", 0),
                    "row_count": query_result.get("row_count", 0),
                    "data": formatted_result
                }

                results.append({
                    "json": output_data
                })

            except Exception as e:
                self.logger.error(f"Error in ClickzettaQuery execution: {e}")
                results.append({
                    "json": {
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                })

        return results

    def _build_query_config(self, item_index: int) -> Dict[str, Any]:
        """
        构建查询配置
        """
        return {
            "limit_results": self.get_node_parameter("limit_results", item_index),
            "max_results": self.get_node_parameter("max_results", item_index),
            "query_timeout": self.get_node_parameter("query_timeout", item_index),
            "use_streaming": self.get_node_parameter("use_streaming", item_index),
            "buffer_size": self.get_node_parameter("buffer_size", item_index)
        }

    async def _execute_query(
        self,
        connection: str,
        sql_query: str,
        parameters: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行 SQL 查询
        """
        try:
            # 使用 Clickzetta 适配器优化查询
            from ...core.clickzetta_adapter import ClickzettaAdapter

            adapter = ClickzettaAdapter()

            # 如果是 SELECT 查询，可以使用适配器的查询优化
            if "SELECT" in sql_query.upper():
                # 尝试解析查询以获取表名等信息
                table_name = self._extract_table_name(sql_query)
                columns = self._extract_columns(sql_query)
                where_clause = self._extract_where_clause(sql_query)

                # 构建采样配置
                sample_config = None
                if config.get("use_streaming") and config.get("max_results"):
                    sample_config = {
                        "enable_sampling": True,
                        "sample_size": config.get("max_results", 1000),
                        "sampling_method": "random"
                    }

                # 使用适配器构建优化查询
                if table_name:
                    try:
                        optimized_query = adapter.build_optimized_query(
                            table_name=table_name,
                            columns=columns,
                            where_clause=where_clause,
                            sample_config=sample_config
                        )
                        self.logger.info(f"Using optimized query: {optimized_query}")
                        sql_query = optimized_query
                    except Exception as e:
                        self.logger.warning(f"Failed to optimize query, using original: {e}")

            # 这里实现实际的查询执行逻辑
            # 暂时使用模拟实现

            # 模拟查询执行时间
            execution_time_ms = 250

            # 模拟查询结果
            if "SELECT" in sql_query.upper():
                mock_data = [
                    {"id": 1, "name": "Alice", "email": "alice@example.com", "created_at": "2024-01-01 10:00:00"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com", "created_at": "2024-01-02 11:00:00"},
                    {"id": 3, "name": "Charlie", "email": "charlie@example.com", "created_at": "2024-01-03 12:00:00"}
                ]
                row_count = len(mock_data)
            else:
                mock_data = []
                row_count = 1  # 对于非 SELECT 语句，返回影响的行数

            return {
                "status": "success",
                "execution_time_ms": execution_time_ms,
                "row_count": row_count,
                "data": mock_data,
                "columns": [
                    {"name": "id", "type": "UInt64"},
                    {"name": "name", "type": "String"},
                    {"name": "email", "type": "String"},
                    {"name": "created_at", "type": "DateTime"}
                ] if mock_data else []
            }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Query execution failed: {str(e)}",
                "execution_time_ms": 0,
                "row_count": 0,
                "data": []
            }

    def _format_output(self, query_result: Dict[str, Any], output_format: str) -> Any:
        """
        格式化输出结果
        """
        data = query_result.get("data", [])

        if output_format == "json":
            return data
        elif output_format == "array":
            if not data:
                return []
            # 转换为二维数组
            headers = list(data[0].keys()) if data else []
            rows = [headers]
            for row in data:
                rows.append([row.get(col, None) for col in headers])
            return rows
        elif output_format == "csv":
            if not data:
                return ""
            # 转换为 CSV 字符串
            headers = list(data[0].keys()) if data else []
            csv_lines = [",".join(headers)]
            for row in data:
                csv_lines.append(",".join(str(row.get(col, "")) for col in headers))
            return "\n".join(csv_lines)
        else:
            return data

    def _apply_query_limits(self, data: List[Dict], config: Dict[str, Any]) -> List[Dict]:
        """
        应用查询限制
        """
        if config.get("limit_results") and config.get("max_results"):
            max_results = config["max_results"]
            return data[:max_results]
        return data

    def _extract_table_name(self, sql_query: str) -> Optional[str]:
        """
        从 SQL 查询中提取表名
        """
        try:
            # 简单的表名提取逻辑
            import re

            # 匹配 FROM 后面的表名
            match = re.search(r'FROM\s+([^\s,;]+)', sql_query, re.IGNORECASE)
            if match:
                return match.group(1).strip()

            return None
        except Exception:
            return None

    def _extract_columns(self, sql_query: str) -> List[str]:
        """
        从 SQL 查询中提取列名
        """
        try:
            import re

            # 匹配 SELECT 和 FROM 之间的列名
            match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
            if match:
                columns_str = match.group(1).strip()
                if columns_str == '*':
                    return []  # 返回空列表表示选择所有列

                # 分割列名并清理
                columns = [col.strip() for col in columns_str.split(',')]
                return columns

            return []
        except Exception:
            return []

    def _extract_where_clause(self, sql_query: str) -> Optional[str]:
        """
        从 SQL 查询中提取 WHERE 子句
        """
        try:
            import re

            # 匹配 WHERE 子句
            match = re.search(r'WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+HAVING|\s+LIMIT|$)', sql_query, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()

            return None
        except Exception:
            return None
