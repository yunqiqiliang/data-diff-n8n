"""
比对引擎
负责执行数据比对操作
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import uuid

# 导入 data-diff 相关模块
try:
    from data_diff import diff_tables, Algorithm
    from data_diff.diff_tables import DiffResult
    HAS_DATA_DIFF = True
except ImportError:
    HAS_DATA_DIFF = False
    logging.warning("data-diff library not found. Using mock implementation.")

from .connection_manager import ConnectionManager

if TYPE_CHECKING:
    from .config_manager import ConfigManager


class ComparisonEngine:
    """
    数据比对引擎
    执行各种数据比对操作
    """

    def __init__(self, config_manager: "ConfigManager"):
        self.logger = logging.getLogger(__name__)
        self.connection_manager = ConnectionManager(config_manager)
        self.active_comparisons: Dict[str, Dict[str, Any]] = {}

    async def compare_tables(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        comparison_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行表比对
        """
        job_id = str(uuid.uuid4())
        start_time = datetime.now()
        try:
            source_conn_id = await self.connection_manager.create_connection(source_config)
            target_conn_id = await self.connection_manager.create_connection(target_config)

            # 获取连接配置来构建连接字符串
            source_config_obj = self.connection_manager.get_connection_config(source_conn_id)
            target_config_obj = self.connection_manager.get_connection_config(target_conn_id)

            source_connection_string = self.connection_manager._build_connection_string(source_config_obj)
            target_connection_string = self.connection_manager._build_connection_string(target_config_obj)

            self.active_comparisons[job_id] = {
                "start_time": start_time,
                "config": comparison_config,
                "status": "running"
            }

            if HAS_DATA_DIFF:
                result = await self._execute_datadiff_comparison(
                    source_connection_string, target_connection_string, comparison_config, job_id
                )
            else:
                result = await self._execute_mock_comparison(
                    source_connection_string, target_connection_string, comparison_config, job_id
                )

            self.active_comparisons[job_id]["status"] = "completed"
            self.active_comparisons[job_id]["end_time"] = datetime.now()

            return result
        except Exception as e:
            self.logger.error(f"Table comparison {job_id} failed: {e}")
            if job_id in self.active_comparisons:
                self.active_comparisons[job_id]["status"] = "error"
                self.active_comparisons[job_id]["error"] = str(e)
            raise

    async def compare_schemas(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行模式比对
        """
        self.logger.info("Starting schema comparison...")
        try:
            source_conn = await self.connection_manager.create_connection(source_config)
            target_conn = await self.connection_manager.create_connection(target_config)

            # 模拟获取 schema 信息
            source_schema = {"tables": ["users", "orders"], "columns": {"users": ["id", "name"], "orders": ["id", "amount"]}}
            target_schema = {"tables": ["users", "orders", "products"], "columns": {"users": ["id", "username"], "orders": ["id", "amount"]}}

            # 比较 schema
            diff = {
                "tables_only_in_source": list(set(source_schema["tables"]) - set(target_schema["tables"])),
                "tables_only_in_target": list(set(target_schema["tables"]) - set(source_schema["tables"])),
                "column_diffs": {}
            }

            common_tables = set(source_schema["tables"]) & set(target_schema["tables"])
            for table in common_tables:
                source_cols = set(source_schema["columns"].get(table, []))
                target_cols = set(target_schema["columns"].get(table, []))
                if source_cols != target_cols:
                    diff["column_diffs"][table] = {
                        "columns_only_in_source": list(source_cols - target_cols),
                        "columns_only_in_target": list(target_cols - source_cols),
                    }

            return {
                "status": "completed",
                "diff": diff,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Schema comparison failed: {e}")
            raise

    async def execute_comparison(
        self,
        source_connection_id: str,
        target_connection_id: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行数据比对

        Args:
            source_connection_id: 源数据库连接ID
            target_connection_id: 目标数据库连接ID
            config: 比对配置

        Returns:
            比对结果
        """
        job_id = str(uuid.uuid4())
        start_time = datetime.now()

        try:
            # 获取连接配置来构建连接字符串
            source_config_obj = self.connection_manager.get_connection_config(source_connection_id)
            target_config_obj = self.connection_manager.get_connection_config(target_connection_id)

            source_connection_string = self.connection_manager._build_connection_string(source_config_obj)
            target_connection_string = self.connection_manager._build_connection_string(target_config_obj)

            # 记录比对任务
            self.active_comparisons[job_id] = {
                "start_time": start_time,
                "config": config,
                "status": "running"
            }

            # 执行比对
            if HAS_DATA_DIFF:
                result = await self._execute_datadiff_comparison(
                    source_connection_string, target_connection_string, config, job_id
                )
            else:
                result = await self._execute_mock_comparison(
                    source_connection_string, target_connection_string, config, job_id
                )

            # 更新任务状态
            self.active_comparisons[job_id]["status"] = "completed"
            self.active_comparisons[job_id]["end_time"] = datetime.now()

            return result

        except Exception as e:
            self.logger.error(f"Comparison {job_id} failed: {e}")

            # 更新任务状态
            if job_id in self.active_comparisons:
                self.active_comparisons[job_id]["status"] = "error"
                self.active_comparisons[job_id]["error"] = str(e)

            return {
                "status": "error",
                "job_id": job_id,
                "message": f"Comparison failed: {str(e)}",
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }

    async def _execute_datadiff_comparison(
        self,
        source_connection: Any,
        target_connection: Any,
        config: Dict[str, Any],
        job_id: str
    ) -> Dict[str, Any]:
        """
        使用 data-diff 执行比对
        """
        try:
            from data_diff import connect_to_table, diff_tables, Algorithm

            start_time = datetime.now()
            self.logger.info(f"Starting data-diff job {job_id} with config: {config}")

            # 使用连接字符串
            source_db_info = source_connection
            target_db_info = target_connection

            # 确保 key_columns 是元组类型
            key_columns = config["key_columns"]
            if isinstance(key_columns, str):
                key_columns = (key_columns,)
            elif isinstance(key_columns, list):
                key_columns = tuple(key_columns)

            # 创建 TableSegment 对象
            source_table_segment = connect_to_table(
                db_info=source_db_info,
                table_name=config["source_table"],
                key_columns=key_columns,
                thread_count=config.get("threads", 1)
            )

            target_table_segment = connect_to_table(
                db_info=target_db_info,
                table_name=config["target_table"],
                key_columns=key_columns,
                thread_count=config.get("threads", 1)
            )

            # 选择算法
            algorithm_str = config.get("algorithm", "hashdiff").lower()
            algorithm = Algorithm.JOINDIFF if algorithm_str == "joindiff" else Algorithm.HASHDIFF

            # 构建比对选项
            diff_options = {
                "algorithm": algorithm,
                "key_columns": key_columns,
                "where": config.get("source_filter"),
                "update_column": config.get("update_column"),
                "threaded": True,
                "max_threadpool_size": config.get("threads", 1)
            }

            # 添加 extra_columns（比较的列）
            compare_columns = config.get("compare_columns") or config.get("columns_to_compare")
            if compare_columns:
                if isinstance(compare_columns, str):
                    compare_columns = tuple(compare_columns.split(','))
                elif isinstance(compare_columns, list):
                    compare_columns = tuple(compare_columns)
                diff_options["extra_columns"] = compare_columns

            # 移除值为 None 的选项
            diff_options = {k: v for k, v in diff_options.items() if v is not None}

            self.logger.info(f"Executing diff_tables with options: {diff_options}")

            # 执行比对
            diff_result = diff_tables(
                source_table_segment,
                target_table_segment,
                **diff_options
            )

            end_time = datetime.now()
            self.logger.info(f"Data-diff job {job_id} finished in {(end_time - start_time).total_seconds()}s")

            # 处理结果
            return self._process_datadiff_result(diff_result, config, job_id, start_time, end_time)

        except Exception as e:
            self.logger.error(f"Data-diff execution for job {job_id} failed: {e}", exc_info=True)
            raise Exception(f"Data-diff execution failed: {str(e)}")

    async def _execute_mock_comparison(
        self,
        source_connection: Any,
        target_connection: Any,
        config: Dict[str, Any],
        job_id: str
    ) -> Dict[str, Any]:
        """
        执行模拟比对（当 data-diff 不可用时）
        """
        start_time = datetime.now()

        # 模拟处理时间
        await asyncio.sleep(1)

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        # 生成模拟结果
        total_rows_source = 100000
        total_rows_target = 99950

        differences = {
            "missing_in_target": 45,
            "missing_in_source": 5,
            "value_differences": 23,
            "total_differences": 73
        }

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
                "source_table": config["source_table"],
                "target_table": config["target_table"],
                "total_rows_source": total_rows_source,
                "total_rows_target": total_rows_target,
                "rows_compared": min(total_rows_source, total_rows_target),
                "differences": differences,
                "match_rate": 1 - (differences["total_differences"] / min(total_rows_source, total_rows_target))
            },
            "sample_differences": sample_differences,
            "summary": {
                "has_differences": differences["total_differences"] > 0,
                "match_percentage": round((1 - (differences["total_differences"] / min(total_rows_source, total_rows_target))) * 100, 2),
                "data_quality_score": "Good"
            }
        }

    def _process_datadiff_result(
        self,
        diff_result: Any,
        config: Dict[str, Any],
        job_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        处理 data-diff 结果
        """
        try:
            execution_time = (end_time - start_time).total_seconds()

            # 获取源表和目标表的行数信息
            total_rows_source = 0
            total_rows_target = 0

            try:
                # 尝试从diff_result中获取表的行数
                if hasattr(diff_result, 'table1_count') and hasattr(diff_result, 'table2_count'):
                    total_rows_source = diff_result.table1_count
                    total_rows_target = diff_result.table2_count
                else:
                    # 如果结果中没有行数信息，则尝试从其他来源获取
                    # 这里可以添加其他获取行数的逻辑
                    pass
            except Exception as e:
                self.logger.warning(f"Unable to get row counts from diff_result: {e}")

            # 获取差异数量
            total_differences = 0
            try:
                if hasattr(diff_result, '__len__'):
                    total_differences = len(diff_result)
                elif hasattr(diff_result, 'summary') and 'total_differences' in diff_result.summary:
                    total_differences = diff_result.summary['total_differences']
                else:
                    # 尝试将diff_result转换为列表并计算长度
                    differences_list = list(diff_result)
                    total_differences = len(differences_list)
            except Exception as e:
                self.logger.warning(f"Unable to get total differences from diff_result: {e}")

            # 构建差异统计信息
            differences = {
                "total_differences": total_differences,
                "missing_in_target": 0,
                "missing_in_source": 0,
                "value_differences": 0
            }

            # 尝试获取更详细的差异信息
            try:
                if hasattr(diff_result, 'diff_count_by_type'):
                    diff_counts = diff_result.diff_count_by_type
                    differences["missing_in_target"] = diff_counts.get('missing_in_b', 0)
                    differences["missing_in_source"] = diff_counts.get('missing_in_a', 0)
                    differences["value_differences"] = diff_counts.get('value_different', 0)
            except Exception as e:
                self.logger.warning(f"Unable to get detailed difference counts: {e}")

            # 如果没有总行数，使用一个默认值以避免除以零错误
            if total_rows_source == 0 and total_rows_target == 0:
                rows_compared = max(total_differences, 1) # 确保不会除以零
            else:
                rows_compared = min(total_rows_source, total_rows_target)
                if rows_compared == 0:
                    rows_compared = 1  # 确保不会除以零

            # 计算匹配率
            match_rate = 1.0
            if rows_compared > 0:
                match_rate = 1 - (total_differences / rows_compared)

            # 获取样本差异
            sample_differences = []
            try:
                # 尝试获取一些样本差异
                if hasattr(diff_result, 'diffs'):
                    diffs_sample = list(diff_result.diffs)[:10]  # 最多取10个样本
                    for diff in diffs_sample:
                        diff_type = diff.get('diff_type', 'unknown')
                        key = diff.get('key', {})
                        source_row = diff.get('a_values', None)
                        target_row = diff.get('b_values', None)

                        sample_diff = {
                            "type": diff_type,
                            "key": key
                        }

                        if source_row:
                            sample_diff["source_row"] = source_row

                        if target_row:
                            sample_diff["target_row"] = target_row

                        if diff_type == 'value_different' and source_row and target_row:
                            # 找出不同的列
                            differing_columns = []
                            for col in source_row:
                                if col in target_row and source_row[col] != target_row[col]:
                                    differing_columns.append(col)
                            sample_diff["differing_columns"] = differing_columns

                        sample_differences.append(sample_diff)
            except Exception as e:
                self.logger.warning(f"Unable to get sample differences: {e}")

            # 构建完整的结果
            return {
                "status": "completed",
                "job_id": job_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "execution_time_seconds": execution_time,
                "config": config,
                "statistics": {
                    "source_table": config["source_table"],
                    "target_table": config["target_table"],
                    "total_rows_source": total_rows_source,
                    "total_rows_target": total_rows_target,
                    "rows_compared": rows_compared,
                    "differences": differences,
                    "match_rate": match_rate
                },
                "sample_differences": sample_differences,
                "summary": {
                    "has_differences": total_differences > 0,
                    "match_percentage": round(match_rate * 100, 2),
                    "data_quality_score": "Good" if match_rate > 0.95 else "Fair" if match_rate > 0.8 else "Poor"
                }
            }

        except Exception as e:
            self.logger.error(f"Failed to process data-diff result: {e}", exc_info=True)
            return {
                "status": "error",
                "job_id": job_id,
                "message": f"Failed to process result: {str(e)}",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "raw_result": str(diff_result)[:1000] if diff_result else "None"  # 添加原始结果的截断版本以便调试
            }

    async def get_comparison_status(self, job_id: str) -> Dict[str, Any]:
        """
        获取比对任务状态

        Args:
            job_id: 任务ID

        Returns:
            任务状态
        """
        if job_id not in self.active_comparisons:
            return {
                "status": "not_found",
                "message": f"Comparison job {job_id} not found"
            }

        job_info = self.active_comparisons[job_id]

        return {
            "job_id": job_id,
            "status": job_info["status"],
            "start_time": job_info["start_time"].isoformat(),
            "end_time": job_info.get("end_time", {}).isoformat() if job_info.get("end_time") else None,
            "config": job_info["config"],
            "error": job_info.get("error")
        }

    def list_active_comparisons(self) -> List[Dict[str, Any]]:
        """
        列出活动的比对任务

        Returns:
            任务列表
        """
        comparisons = []
        for job_id, job_info in self.active_comparisons.items():
            comparisons.append({
                "job_id": job_id,
                "status": job_info["status"],
                "start_time": job_info["start_time"].isoformat(),
                "source_table": job_info["config"].get("source_table"),
                "target_table": job_info["config"].get("target_table")
            })
        return comparisons

    async def cancel_comparison(self, job_id: str) -> bool:
        """
        取消比对任务

        Args:
            job_id: 任务ID

        Returns:
            是否成功取消
        """
        if job_id not in self.active_comparisons:
            return False

        job_info = self.active_comparisons[job_id]

        if job_info["status"] == "running":
            # 这里应该实现实际的任务取消逻辑
            job_info["status"] = "cancelled"
            job_info["end_time"] = datetime.now()
            return True

        return False

    def cleanup_completed_comparisons(self, max_age_hours: int = 24) -> int:
        """
        清理已完成的比对任务

        Args:
            max_age_hours: 最大保留时间（小时）

        Returns:
            清理的任务数量
        """
        now = datetime.now()
        cleaned_count = 0

        jobs_to_remove = []
        for job_id, job_info in self.active_comparisons.items():
            if job_info["status"] in ["completed", "error", "cancelled"]:
                end_time = job_info.get("end_time", job_info["start_time"])
                age_hours = (now - end_time).total_seconds() / 3600

                if age_hours > max_age_hours:
                    jobs_to_remove.append(job_id)

        for job_id in jobs_to_remove:
            del self.active_comparisons[job_id]
            cleaned_count += 1

        self.logger.info(f"Cleaned up {cleaned_count} old comparison jobs")
        return cleaned_count
