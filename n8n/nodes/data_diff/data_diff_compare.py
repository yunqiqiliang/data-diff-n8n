"""
Data-Diff 比对执行节点
用于执行实际的数据比对操作
"""

from typing import Dict, Any, List, Optional
import json
import logging
import asyncio
from datetime import datetime, timedelta
import uuid

from n8n_sdk import Node, NodePropertyTypes, INodeExecutionData


class DataDiffCompare(Node):
    """
    Data-Diff 比对执行节点
    执行数据比对并返回结果
    """

    display_name = "Data-Diff Compare"
    description = "Execute data comparison between databases"
    group = "transform"
    version = 1

    # 节点输入输出配置
    inputs = ["main"]
    outputs = ["main", "differences", "summary"]

    # 节点属性配置
    properties = [
        {
            "displayName": "Source Connection",
            "name": "source_connection",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Source database connection config or ID"
        },
        {
            "displayName": "Target Connection",
            "name": "target_connection",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Target database connection config or ID"
        },
        {
            "displayName": "Comparison Config",
            "name": "comparison_config",
            "type": NodePropertyTypes.JSON,
            "default": "{}",
            "required": True,
            "description": "Comparison configuration from DataDiffConfig node"
        },
        {
            "displayName": "Execution Mode",
            "name": "execution_mode",
            "type": NodePropertyTypes.OPTIONS,
            "default": "sync",
            "options": [
                {
                    "name": "Synchronous",
                    "value": "sync",
                    "description": "Wait for comparison to complete"
                },
                {
                    "name": "Asynchronous",
                    "value": "async",
                    "description": "Start comparison and return job ID"
                }
            ],
            "description": "Execution mode"
        },
        {
            "displayName": "Timeout (seconds)",
            "name": "timeout",
            "type": NodePropertyTypes.NUMBER,
            "default": 3600,
            "description": "Maximum execution time in seconds"
        },
        {
            "displayName": "Report Format",
            "name": "report_format",
            "type": NodePropertyTypes.OPTIONS,
            "default": "json",
            "options": [
                {"name": "JSON", "value": "json"},
                {"name": "CSV", "value": "csv"},
                {"name": "HTML", "value": "html"},
                {"name": "Excel", "value": "excel"}
            ],
            "description": "Output report format"
        },
        {
            "displayName": "Include Sample Data",
            "name": "include_sample_data",
            "type": NodePropertyTypes.BOOLEAN,
            "default": True,
            "description": "Include sample differences in the report"
        },
        {
            "displayName": "Max Sample Size",
            "name": "max_sample_size",
            "type": NodePropertyTypes.NUMBER,
            "default": 100,
            "displayOptions": {
                "show": {
                    "include_sample_data": [True]
                }
            },
            "description": "Maximum number of sample differences to include"
        }
    ]

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    async def execute(self, items: List[INodeExecutionData]) -> List[List[INodeExecutionData]]:
        """
        执行比对节点逻辑
        返回三个输出：主要结果、差异详情、汇总信息
        """
        main_results = []
        difference_results = []
        summary_results = []

        for item_index, item in enumerate(items):
            try:
                # 获取参数
                source_connection = self.get_node_parameter("source_connection", item_index)
                target_connection = self.get_node_parameter("target_connection", item_index)
                comparison_config = self.get_node_parameter("comparison_config", item_index)
                execution_mode = self.get_node_parameter("execution_mode", item_index)
                timeout = self.get_node_parameter("timeout", item_index)

                # 解析配置
                if isinstance(comparison_config, str):
                    comparison_config = json.loads(comparison_config)

                # 生成任务 ID
                job_id = str(uuid.uuid4())

                # 执行比对
                if execution_mode == "sync":
                    result = await self._execute_sync_comparison(
                        source_connection, target_connection, comparison_config, job_id, timeout
                    )
                else:
                    result = await self._execute_async_comparison(
                        source_connection, target_connection, comparison_config, job_id
                    )

                # 构建输出结果
                main_output, diff_output, summary_output = self._build_outputs(result, item_index)

                main_results.append(main_output)
                difference_results.append(diff_output)
                summary_results.append(summary_output)

            except Exception as e:
                self.logger.error(f"Error in DataDiffCompare execution: {e}")
                error_output = {
                    "json": {
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                }
                main_results.append(error_output)
                difference_results.append(error_output)
                summary_results.append(error_output)

        return [main_results, difference_results, summary_results]

    async def _execute_sync_comparison(
        self,
        source_connection: str,
        target_connection: str,
        config: Dict[str, Any],
        job_id: str,
        timeout: int
    ) -> Dict[str, Any]:
        """
        执行同步比对
        """
        start_time = datetime.now()

        try:
            # 这里实现实际的 data-diff 集成逻辑
            # 暂时使用模拟实现

            # 模拟比对执行
            await asyncio.sleep(2)  # 模拟处理时间

            # 模拟比对结果
            result = self._generate_mock_comparison_result(config, job_id, start_time)

            return result

        except asyncio.TimeoutError:
            return {
                "status": "timeout",
                "job_id": job_id,
                "message": f"Comparison timed out after {timeout} seconds",
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "job_id": job_id,
                "message": f"Comparison failed: {str(e)}",
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }

    async def _execute_async_comparison(
        self,
        source_connection: str,
        target_connection: str,
        config: Dict[str, Any],
        job_id: str
    ) -> Dict[str, Any]:
        """
        执行异步比对
        """
        start_time = datetime.now()

        # 启动后台任务
        # 这里应该实现实际的异步任务调度

        return {
            "status": "started",
            "job_id": job_id,
            "message": "Comparison started in background",
            "start_time": start_time.isoformat(),
            "estimated_completion": (start_time + timedelta(minutes=30)).isoformat()
        }

    def _generate_mock_comparison_result(
        self,
        config: Dict[str, Any],
        job_id: str,
        start_time: datetime
    ) -> Dict[str, Any]:
        """
        生成模拟比对结果
        """
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        # 模拟比对统计
        total_rows_source = 100000
        total_rows_target = 99950

        # 模拟差异统计
        differences = {
            "missing_in_target": 45,
            "missing_in_source": 5,
            "value_differences": 23,
            "total_differences": 73
        }

        # 模拟示例差异数据
        sample_differences = [
            {
                "type": "missing_in_target",
                "key": {"id": 1001},
                "source_row": {"id": 1001, "name": "Alice", "email": "alice@example.com"},
                "target_row": None
            },
            {
                "type": "value_difference",
                "key": {"id": 1002},
                "source_row": {"id": 1002, "name": "Bob", "email": "bob@example.com"},
                "target_row": {"id": 1002, "name": "Bob", "email": "bob@company.com"},
                "differing_columns": ["email"]
            },
            {
                "type": "missing_in_source",
                "key": {"id": 1003},
                "source_row": None,
                "target_row": {"id": 1003, "name": "Charlie", "email": "charlie@example.com"}
            }
        ]

        return {
            "status": "completed",
            "job_id": job_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "execution_time_seconds": execution_time,
            "config": config,
            "statistics": {
                "source_table": config.get("source_table"),
                "target_table": config.get("target_table"),
                "total_rows_source": total_rows_source,
                "total_rows_target": total_rows_target,
                "rows_compared": min(total_rows_source, total_rows_target),
                "differences": differences,
                "match_rate": 1 - (differences["total_differences"] / min(total_rows_source, total_rows_target))
            },
            "sample_differences": sample_differences[:self.get_node_parameter("max_sample_size", 0) or 100],
            "summary": {
                "has_differences": differences["total_differences"] > 0,
                "match_percentage": round((1 - (differences["total_differences"] / min(total_rows_source, total_rows_target))) * 100, 2),
                "data_quality_score": "Good" if differences["total_differences"] < 100 else "Fair" if differences["total_differences"] < 1000 else "Poor"
            }
        }

    def _build_outputs(self, result: Dict[str, Any], item_index: int) -> tuple:
        """
        构建三个输出的数据
        """
        timestamp = datetime.now().isoformat()
        report_format = self.get_node_parameter("report_format", item_index)
        include_sample_data = self.get_node_parameter("include_sample_data", item_index)

        # 主要输出：完整结果
        main_output = {
            "json": {
                "comparison_result": result,
                "timestamp": timestamp,
                "report_format": report_format
            }
        }

        # 差异详情输出：仅包含差异数据
        differences_data = []
        if result.get("status") == "completed" and include_sample_data:
            differences_data = result.get("sample_differences", [])

        diff_output = {
            "json": {
                "job_id": result.get("job_id"),
                "differences": differences_data,
                "difference_count": len(differences_data),
                "timestamp": timestamp
            }
        }

        # 汇总输出：统计信息
        summary_data = {}
        if result.get("status") == "completed":
            summary_data = {
                "job_id": result.get("job_id"),
                "execution_time_seconds": result.get("execution_time_seconds"),
                "statistics": result.get("statistics", {}),
                "summary": result.get("summary", {}),
                "timestamp": timestamp
            }
        else:
            summary_data = {
                "job_id": result.get("job_id"),
                "status": result.get("status"),
                "message": result.get("message"),
                "timestamp": timestamp
            }

        summary_output = {
            "json": summary_data
        }

        return main_output, diff_output, summary_output
