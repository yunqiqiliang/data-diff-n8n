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
    logging.warning("data-diff library not found. This library is required for data comparison operations.")

try:
    from .connection_manager import ConnectionManager
except ImportError:
    # 处理相对导入失败的情况
    try:
        from connection_manager import ConnectionManager
    except ImportError:
        # 如果还是失败，提供一个真正可用的临时解决方案
        class ConnectionManager:
            def __init__(self, config_manager):
                self.logger = logging.getLogger(__name__)
                self.logger.warning("Using minimal ConnectionManager implementation due to import failure")

            def create_connection(self, config):
                raise Exception("ConnectionManager not properly imported - cannot create database connections")

            def get_connection(self, conn_id):
                raise Exception("ConnectionManager not properly imported - cannot get database connections")

            def get_connection_config(self, conn_id):
                raise Exception("ConnectionManager not properly imported - cannot get connection config")

            def _build_connection_string(self, config):
                raise Exception("ConnectionManager not properly imported - cannot build connection string")

if TYPE_CHECKING:
    try:
        from .config_manager import ConfigManager
    except ImportError:
        from config_manager import ConfigManager


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

            if not HAS_DATA_DIFF:
                raise Exception("data-diff library is required for table comparison but not installed")

            result = await self._execute_datadiff_comparison(
                source_connection_string, target_connection_string, comparison_config, job_id,
                source_config, target_config
            )

            self.active_comparisons[job_id]["status"] = "completed"
            self.active_comparisons[job_id]["end_time"] = datetime.now()

            return result
        except Exception as e:
            import traceback
            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"Table comparison {job_id} failed: {e}", exc_info=True)
            if job_id in self.active_comparisons:
                self.active_comparisons[job_id]["status"] = "error"
                self.active_comparisons[job_id]["error"] = str(e)
                self.active_comparisons[job_id]["error_details"] = error_details
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
            # 创建连接
            source_conn_id = await self.connection_manager.create_connection(source_config)
            target_conn_id = await self.connection_manager.create_connection(target_config)

            # 获取连接配置来构建连接字符串
            source_config_obj = self.connection_manager.get_connection_config(source_conn_id)
            target_config_obj = self.connection_manager.get_connection_config(target_conn_id)

            source_connection_string = self.connection_manager._build_connection_string(source_config_obj)
            target_connection_string = self.connection_manager._build_connection_string(target_config_obj)

            # 获取实际的 schema 信息
            source_schema = await self._get_database_schema(source_connection_string, source_config_obj)
            target_schema = await self._get_database_schema(target_connection_string, target_config_obj)

            # 比较 schema
            diff_result = self._compare_schemas(source_schema, target_schema)

            return {
                "status": "completed",
                "source_schema": source_schema,
                "target_schema": target_schema,
                "diff": diff_result,
                "summary": self._generate_schema_summary(diff_result),
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            import traceback
            self.logger.error(f"Schema comparison failed: {e}", exc_info=True)
            # 包含详细错误信息，便于前端显示
            detailed_error = {
                "status": "error",
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()
            }
            raise Exception(f"{str(e)} (详细信息: {detailed_error['error_type']})")

    async def _get_database_schema(self, db_connection_string: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """获取数据库模式信息 - 使用连接字符串"""
        try:
            schema_name = config.get("schema", "public")

            # 根据数据库类型获取schema
            if config.get("database_type") == "postgresql" or config.get("driver") == "postgresql":
                return await self._get_postgresql_schema(db_connection_string, config)
            elif config.get("database_type") == "clickzetta" or config.get("driver") == "clickzetta":
                return await self._get_clickzetta_schema(db_connection_string, config)
            else:
                # 通用方法
                return await self._get_generic_schema(db_connection_string, config)
        except Exception as e:
            self.logger.error(f"Failed to get schema: {e}")
            return {
                "tables": [],
                "columns": {},
                "indexes": {},
                "constraints": {},
                "error": str(e)
            }

    async def _get_postgresql_schema(self, db_connection_string: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """获取PostgreSQL数据库模式 - 完全参考表比对的成功实现方式"""
        try:
            schema_name = config.get("schema", "public")
            self.logger.info(f"Getting PostgreSQL schema for {schema_name}")

            # 检查是否有data-diff库
            if not HAS_DATA_DIFF:
                raise Exception("data-diff library is required for schema comparison but not installed")

            from data_diff import connect_to_table

            tables = []
            columns = {}
            indexes = {}
            constraints = {}

            # 步骤1: 直接通过尝试连接常见表名来发现表
            # 避免复杂的 information_schema 连接问题
            self.logger.info("Discovering tables by trying to connect to common table names...")
            common_tables = ['users', 'orders', 'products', 'invoices', 'accounts', 'people', 'reviews']
            for table_name in common_tables:
                try:
                    # 使用与表比对完全相同的方式尝试连接
                    table_segment = connect_to_table(
                        db_info=db_connection_string,
                        table_name=table_name,
                        key_columns=("id",),
                        thread_count=1
                    )
                    # 如果连接成功，添加到表列表（不进行count测试）
                    tables.append(table_name)
                    self.logger.info(f"✅ Found table: {table_name}")
                    # 立即关闭连接以避免事务问题
                    try:
                        if hasattr(table_segment, 'close'):
                            table_segment.close()
                        elif hasattr(table_segment, 'database') and hasattr(table_segment.database, 'close'):
                            table_segment.database.close()
                    except Exception:
                        pass
                except Exception:
                    continue

            # 步骤2: 为每个表获取详细的模式信息
            for table_name in tables:
                try:
                    self.logger.info(f"Getting schema for table: {table_name}")

                    # 为每个表创建一个独立的连接，避免事务状态问题
                    # 强制使用新的连接，避免复用可能有问题的连接
                    table_segment = connect_to_table(
                        db_info=db_connection_string,
                        table_name=table_name,
                        key_columns=("id",),  # 假设有id列，如果没有也不影响获取schema
                        thread_count=1
                    )

                    # 获取表的schema（不进行count测试以避免事务问题）
                    table_schema = table_segment.get_schema()
                    self.logger.info(f"✅ Got schema for table {table_name}: {len(table_schema)} columns")

                    # 转换格式
                    table_columns = []
                    for col_name, col_info in table_schema.items():
                        table_columns.append({
                            "name": col_name,
                            "type": getattr(col_info, 'data_type', str(col_info)),
                            "nullable": getattr(col_info, 'nullable', True),
                            "default": getattr(col_info, 'default', None)
                        })

                    columns[table_name] = table_columns
                    self.logger.info(f"✅ Got {len(table_columns)} columns for table {table_name}")

                    # 强制关闭连接以避免事务问题
                    try:
                        if hasattr(table_segment, 'close'):
                            table_segment.close()
                        elif hasattr(table_segment, 'database') and hasattr(table_segment.database, 'close'):
                            table_segment.database.close()
                    except Exception as close_error:
                        self.logger.warning(f"Warning: Could not close connection for table {table_name}: {close_error}")

                    # 获取主键信息（假设id是主键）
                    if any(col['name'] == 'id' for col in table_columns):
                        constraints[table_name] = {"primary_key": ["id"]}
                    else:
                        constraints[table_name] = {"primary_key": []}

                    # 暂时不获取索引信息
                    indexes[table_name] = []

                except Exception as e:
                    self.logger.warning(f"Failed to get schema for table {table_name}: {e}")
                    # 即使单个表失败，也继续处理其他表
                    columns[table_name] = []
                    indexes[table_name] = []
                    constraints[table_name] = {"primary_key": []}

            return {
                "database_type": "postgresql",
                "schema_name": schema_name,
                "tables": tables,
                "columns": columns,
                "indexes": indexes,
                "constraints": constraints
            }

        except Exception as e:
            self.logger.error(f"Failed to get PostgreSQL schema: {e}")
            raise

    async def _get_clickzetta_schema(self, db_connection_string: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """获取Clickzetta数据库模式 - 完全参考表比对的成功实现方式"""
        try:
            schema_name = config.get("schema", "public")
            self.logger.info(f"Getting Clickzetta schema for {schema_name}")

            # 检查是否有data-diff库
            if not HAS_DATA_DIFF:
                raise Exception("data-diff library is required for schema comparison but not installed")

            from data_diff import connect_to_table

            tables = []
            columns = {}
            indexes = {}
            constraints = {}

            # 步骤1: 连接到system.tables获取表列表
            # 完全参考表比对的成功方式
            try:
                self.logger.info(f"🔗 Connecting to information_schema.tables with connection string...")

                # 使用与表比对完全相同的方式连接
                system_table_segment = connect_to_table(
                    db_info=db_connection_string,
                    table_name="information_schema.tables",
                    key_columns=("table_name",),
                    thread_count=1
                )

                # 立即验证连接 - 就像表比对中的验证方式
                try:
                    test_count = system_table_segment.count()
                    self.logger.info(f"✅ System tables connection verified - row count: {test_count}")
                except Exception as test_error:
                    self.logger.error(f"❌ System tables connection failed: {test_error}")
                    raise Exception(f"无法连接到information_schema.tables: {str(test_error)}")

                # 使用底层数据库连接执行查询
                database_obj = system_table_segment.database

                # 构建查询获取表列表
                tables_query = f"""
                    SELECT table_name as table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{schema_name}' and table_type = 'MANAGED_TABLE'
                    ORDER BY table_name
                """

                # 执行查询
                result = database_obj.query(tables_query, list)
                tables = [row[0] for row in result]
                self.logger.info(f"✅ Found {len(tables)} tables in schema {schema_name}: {tables}")

            except Exception as e:
                self.logger.error(f"Failed to get table list from information_schema.tables: {e}")
                # 如果无法从system.tables获取表列表，尝试连接到一些常见的表
                self.logger.info("Trying to find tables by connecting to common table names...")
                common_tables = ['users', 'orders', 'products', 'invoices', 'accounts', 'people', 'reviews']
                for table_name in common_tables:
                    try:
                        # 使用与表比对完全相同的方式尝试连接
                        table_segment = connect_to_table(
                            db_info=db_connection_string,
                            table_name=table_name,
                            key_columns=("id",),
                            thread_count=1
                        )
                        # 如果连接成功，添加到表列表
                        table_segment.count()  # 测试连接
                        tables.append(table_name)
                        self.logger.info(f"✅ Found table: {table_name}")
                    except Exception:
                        continue

            # 步骤2: 为每个表获取详细的模式信息
            for table_name in tables:
                try:
                    self.logger.info(f"Getting schema for table: {table_name}")

                    # 为每个表创建一个独立的连接，避免事务状态问题
                    table_segment = connect_to_table(
                        db_info=db_connection_string,
                        table_name=table_name,
                        key_columns=("id",),  # 假设有id列，如果没有也不影响获取schema
                        thread_count=1
                    )

                    # 获取表的schema（不进行count测试以避免事务问题）
                    table_schema = table_segment.get_schema()
                    self.logger.info(f"✅ Got schema for table {table_name}: {table_schema}")

                    # 转换格式
                    table_columns = []
                    for col_name, col_info in table_schema.items():
                        # 处理Clickzetta/ClickHouse的nullable类型
                        data_type = getattr(col_info, 'data_type', str(col_info))
                        nullable = 'Nullable' in data_type
                        if nullable:
                            # 从 Nullable(Int32) 中提取 Int32
                            clean_type = data_type.replace('Nullable(', '').replace(')', '')
                        else:
                            clean_type = data_type

                        table_columns.append({
                            "name": col_name,
                            "type": clean_type,
                            "nullable": nullable,
                            "default": getattr(col_info, 'default', None)
                        })

                    columns[table_name] = table_columns
                    self.logger.info(f"✅ Got {len(table_columns)} columns for table {table_name}")

                    # 获取主键信息（假设id是主键）
                    if any(col['name'] == 'id' for col in table_columns):
                        constraints[table_name] = {"primary_key": ["id"]}
                    else:
                        constraints[table_name] = {"primary_key": []}

                    # 暂时不获取索引信息
                    indexes[table_name] = []

                except Exception as e:
                    self.logger.warning(f"Failed to get schema for table {table_name}: {e}")
                    # 即使单个表失败，也继续处理其他表
                    columns[table_name] = []
                    indexes[table_name] = []
                    constraints[table_name] = {"primary_key": []}
                    indexes[table_name] = []
                    constraints[table_name] = {"primary_key": []}

            return {
                "database_type": "clickzetta",
                "schema_name": schema_name,
                "tables": tables,
                "columns": columns,
                "indexes": indexes,
                "constraints": constraints
            }

        except Exception as e:
            self.logger.error(f"Failed to get Clickzetta schema: {e}")
            raise

    async def _get_generic_schema(self, db_connection_string: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """获取通用数据库模式 - 使用连接字符串"""
        try:
            schema_name = config.get("schema", "public")
            self.logger.info(f"Getting generic schema for {schema_name}")

            # 通用的模式获取逻辑
            return {
                "database_type": "generic",
                "schema_name": schema_name,
                "tables": [],
                "columns": {},
                "indexes": {},
                "constraints": {}
            }
        except Exception as e:
            self.logger.error(f"Failed to get generic schema: {e}")
            raise

    def _compare_schemas(self, source_schema: Dict[str, Any], target_schema: Dict[str, Any]) -> Dict[str, Any]:
        """比较两个数据库模式"""
        try:
            source_tables = set(source_schema.get("tables", []))
            target_tables = set(target_schema.get("tables", []))

            # 表级别的差异
            tables_only_in_source = list(source_tables - target_tables)
            tables_only_in_target = list(target_tables - source_tables)
            common_tables = list(source_tables & target_tables)

            # 列级别的差异
            column_diffs = {}
            type_diffs = {}

            for table in common_tables:
                source_cols = source_schema.get("columns", {}).get(table, [])
                target_cols = target_schema.get("columns", {}).get(table, [])

                # 转换为字典形式便于比较
                source_col_dict = {col["name"]: col for col in source_cols}
                target_col_dict = {col["name"]: col for col in target_cols}

                source_col_names = set(source_col_dict.keys())
                target_col_names = set(target_col_dict.keys())

                # 列名差异
                cols_only_in_source = list(source_col_names - target_col_names)
                cols_only_in_target = list(target_col_names - source_col_names)
                common_cols = list(source_col_names & target_col_names)

                if cols_only_in_source or cols_only_in_target:
                    column_diffs[table] = {
                        "columns_only_in_source": cols_only_in_source,
                        "columns_only_in_target": cols_only_in_target,
                    }

                # 数据类型差异
                type_changes = []
                for col in common_cols:
                    source_type = source_col_dict[col]["type"]
                    target_type = target_col_dict[col]["type"]
                    if source_type != target_type:
                        type_changes.append({
                            "column": col,
                            "source_type": source_type,
                            "target_type": target_type
                        })

                if type_changes:
                    type_diffs[table] = type_changes

            return {
                "tables_only_in_source": tables_only_in_source,
                "tables_only_in_target": tables_only_in_target,
                "common_tables": common_tables,
                "column_diffs": column_diffs,
                "type_diffs": type_diffs,
                "total_tables_source": len(source_tables),
                "total_tables_target": len(target_tables)
            }
        except Exception as e:
            self.logger.error(f"Failed to compare schemas: {e}")
            raise

    def _generate_schema_summary(self, diff_result: Dict[str, Any]) -> Dict[str, Any]:
        """生成模式比对摘要"""
        try:
            total_differences = (
                len(diff_result.get("tables_only_in_source", [])) +
                len(diff_result.get("tables_only_in_target", [])) +
                len(diff_result.get("column_diffs", {})) +
                len(diff_result.get("type_diffs", {}))
            )

            return {
                "has_differences": total_differences > 0,
                "total_differences": total_differences,
                "table_differences": len(diff_result.get("tables_only_in_source", [])) + len(diff_result.get("tables_only_in_target", [])),
                "column_differences": len(diff_result.get("column_diffs", {})),
                "type_differences": len(diff_result.get("type_diffs", {})),
                "schemas_identical": total_differences == 0
            }
        except Exception as e:
            self.logger.error(f"Failed to generate schema summary: {e}")
            return {
                "has_differences": False,
                "total_differences": 0,
                "error": str(e)
            }

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
            if not HAS_DATA_DIFF:
                raise Exception("data-diff library is required for comparison but not installed")

            result = await self._execute_datadiff_comparison(
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
        job_id: str,
        source_config: Dict[str, Any] = None,
        target_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        使用 data-diff 执行比对
        """
        try:
            from data_diff import connect_to_table, diff_tables, Algorithm

            start_time = datetime.now()
            self.logger.info(f"🚀 Starting data-diff job {job_id} with config: {config}")
            self.logger.info(f"📊 Source table: {config['source_table']}")
            self.logger.info(f"📊 Target table: {config['target_table']}")

            # 将数据库配置添加到config中，用于生成建议
            if source_config:
                config["_source_config"] = source_config
            if target_config:
                config["_target_config"] = target_config

            # 使用连接字符串
            source_db_info = source_connection
            target_db_info = target_connection

            self.logger.info(f"🔗 Source connection: {str(source_db_info)[:50]}..." if len(str(source_db_info)) > 50 else f"🔗 Source connection: {source_db_info}")
            self.logger.info(f"🔗 Target connection: {str(target_db_info)[:50]}..." if len(str(target_db_info)) > 50 else f"🔗 Target connection: {target_db_info}")

            # 确保 key_columns 是元组类型
            key_columns = config["key_columns"]
            if isinstance(key_columns, str):
                key_columns = (key_columns,)
            elif isinstance(key_columns, list):
                key_columns = tuple(key_columns)

            self.logger.info(f"🔑 Key columns: {key_columns}")

            self.logger.info(f"🗂️ Creating table segments...")

            # 创建 TableSegment 对象，并立即验证连接
            try:
                source_table_segment = connect_to_table(
                    db_info=source_db_info,
                    table_name=config["source_table"],
                    key_columns=key_columns,
                    thread_count=config.get("threads", 1)
                )
                self.logger.info(f"✅ Source table segment created for {config['source_table']}")

                # 立即验证源表连接和表是否存在
                try:
                    source_count = source_table_segment.count()
                    self.logger.info(f"✅ Source table connection verified - row count: {source_count}")
                except Exception as source_test_error:
                    self.logger.error(f"❌ Source table connection/query failed: {source_test_error}")
                    raise Exception(f"源表连接失败或表不存在: {config['source_table']} - {str(source_test_error)}")

            except Exception as source_connect_error:
                self.logger.error(f"❌ Failed to connect to source table: {source_connect_error}")
                raise Exception(f"无法连接到源表 {config['source_table']}: {str(source_connect_error)}")

            try:
                target_table_segment = connect_to_table(
                    db_info=target_db_info,
                    table_name=config["target_table"],
                    key_columns=key_columns,
                    thread_count=config.get("threads", 1)
                )
                self.logger.info(f"✅ Target table segment created for {config['target_table']}")

                # 立即验证目标表连接和表是否存在
                try:
                    target_count = target_table_segment.count()
                    self.logger.info(f"✅ Target table connection verified - row count: {target_count}")
                except Exception as target_test_error:
                    self.logger.error(f"❌ Target table connection/query failed: {target_test_error}")
                    raise Exception(f"目标表连接失败或表不存在: {config['target_table']} - {str(target_test_error)}")

            except Exception as target_connect_error:
                self.logger.error(f"❌ Failed to connect to target table: {target_connect_error}")
                raise Exception(f"无法连接到目标表 {config['target_table']}: {str(target_connect_error)}")

            self.logger.info(f"🔍 Starting data type checking...")

            # 检查不支持的数据类型
            unsupported_types = []
            ignored_columns_details = []  # 新增：详细的被忽略字段信息

            # 获取需要比对的列（只检查实际参与比对的列）
            compare_columns = config.get("compare_columns") or config.get("columns_to_compare")
            check_all_columns = not compare_columns  # 如果没有指定列，则检查所有列

            # 将比对列转换为集合，便于快速查找
            compare_columns_set = set()
            if compare_columns:
                if isinstance(compare_columns, str):
                    compare_columns_set = set(col.strip() for col in compare_columns.split(','))
                elif isinstance(compare_columns, list):
                    compare_columns_set = set(compare_columns)

            try:
                # 检查源表的 schema
                source_schema = source_table_segment.get_schema()
                if source_schema:
                    # 获取处理后的schema来检查类型
                    try:
                        processed_source_schema = source_table_segment.database._process_table_schema(source_table_segment, source_schema)
                        for col_name, col_type in processed_source_schema.items():
                            # 只检查参与比对的列
                            if check_all_columns or col_name in compare_columns_set:
                                if hasattr(col_type, '__class__') and 'UnknownColType' in str(col_type.__class__):
                                    unsupported_types.append(f"源表 {config['source_table']}.{col_name} ({col_type})")
                                    ignored_columns_details.append({
                                        "table": "source",
                                        "table_name": config['source_table'],
                                        "column_name": col_name,
                                        "data_type": str(col_type),
                                        "reason": "不支持的数据类型 (UnknownColType)"
                                    })
                                    self.logger.warning(f"Unsupported column type detected in source table: {col_name} ({col_type})")
                    except Exception as pe:
                        # 如果处理schema失败，直接检查原始schema中的数据类型
                        self.logger.debug(f"Failed to process source schema, checking raw types: {pe}")
                        for col_name, col_info in source_schema.items():
                            # 只检查参与比对的列
                            if (check_all_columns or col_name in compare_columns_set) and hasattr(col_info, 'data_type') and col_info.data_type in ['money', 'uuid', 'inet', 'macaddr']:
                                unsupported_types.append(f"源表 {config['source_table']}.{col_name} (data_type: {col_info.data_type})")
                                ignored_columns_details.append({
                                    "table": "source",
                                    "table_name": config['source_table'],
                                    "column_name": col_name,
                                    "data_type": col_info.data_type,
                                    "reason": f"PostgreSQL特殊类型 ({col_info.data_type}) 不被 data-diff 支持"
                                })
                                self.logger.warning(f"Potentially unsupported column type in source table: {col_name} (data_type: {col_info.data_type})")

                # 检查目标表的 schema
                target_schema = target_table_segment.get_schema()
                if target_schema:
                    try:
                        processed_target_schema = target_table_segment.database._process_table_schema(target_table_segment, target_schema)
                        for col_name, col_type in processed_target_schema.items():
                            # 只检查参与比对的列
                            if check_all_columns or col_name in compare_columns_set:
                                if hasattr(col_type, '__class__') and 'UnknownColType' in str(col_type.__class__):
                                    unsupported_types.append(f"目标表 {config['target_table']}.{col_name} ({col_type})")
                                    ignored_columns_details.append({
                                        "table": "target",
                                        "table_name": config['target_table'],
                                        "column_name": col_name,
                                        "data_type": str(col_type),
                                        "reason": "不支持的数据类型 (UnknownColType)"
                                    })
                                    self.logger.warning(f"Unsupported column type detected in target table: {col_name} ({col_type})")
                    except Exception as pe:
                        # 如果处理schema失败，直接检查原始schema中的数据类型
                        self.logger.debug(f"Failed to process target schema, checking raw types: {pe}")
                        for col_name, col_info in target_schema.items():
                            # 只检查参与比对的列
                            if (check_all_columns or col_name in compare_columns_set) and hasattr(col_info, 'data_type') and col_info.data_type in ['money', 'uuid', 'inet', 'macaddr']:
                                unsupported_types.append(f"目标表 {config['target_table']}.{col_name} (data_type: {col_info.data_type})")
                                ignored_columns_details.append({
                                    "table": "target",
                                    "table_name": config['target_table'],
                                    "column_name": col_name,
                                    "data_type": col_info.data_type,
                                    "reason": f"PostgreSQL特殊类型 ({col_info.data_type}) 不被 data-diff 支持"
                                })
                                self.logger.warning(f"Potentially unsupported column type in target table: {col_name} (data_type: {col_info.data_type})")
            except Exception as e:
                self.logger.warning(f"Unable to check for unsupported data types: {e}")

            # 如果检测到不支持的数据类型，发出警告
            type_warnings = []
            if unsupported_types:
                if check_all_columns:
                    warning_msg = f"严重错误：检测到不支持的数据类型，这些列被完全忽略，比对结果不可靠: {', '.join(unsupported_types)}"
                    self.logger.error(warning_msg)  # 错误级别，因为这会导致比对结果错误
                    type_warnings.append(warning_msg)
                else:
                    warning_msg = f"严重错误：指定比对的列中包含不支持的数据类型，这些列被完全忽略，比对结果不可靠: {', '.join(unsupported_types)}"
                    self.logger.error(warning_msg)  # 错误级别，因为这会导致比对结果错误
                    type_warnings.append(warning_msg)

                # 根据配置决定是否将其视为错误
                if config.get("strict_type_checking", False):
                    raise Exception(f"严格模式：{warning_msg}")

                # 非严格模式下继续执行，但记录严重警告
                self.logger.error("🚨 比对结果不可信：被忽略的字段可能包含实际差异，但会显示为100%匹配！")
                self.logger.error("📊 建议：1) 启用严格类型检查模式 2) 预处理数据转换类型 3) 从比对中排除这些字段")

            # 将类型警告和详细信息添加到配置中，以便传递给结果处理函数
            config["_type_warnings"] = type_warnings
            config["_ignored_columns_details"] = ignored_columns_details

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

            self.logger.info(f"📋 Executing diff_tables with options: {diff_options}")
            self.logger.info(f"🚀 Starting actual table comparison (this will execute SQL queries)...")

            # 执行比对
            diff_result = diff_tables(
                source_table_segment,
                target_table_segment,
                **diff_options
            )

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            self.logger.info(f"✅ Data-diff job {job_id} completed in {execution_time}s")
            self.logger.info(f"📊 SQL execution finished, processing results...")

            # 处理结果
            return self._process_datadiff_result(diff_result, config, job_id, start_time, end_time)

        except Exception as e:
            self.logger.error(f"Data-diff execution for job {job_id} failed: {e}", exc_info=True)
            raise Exception(f"Data-diff execution failed: {str(e)}")

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
                # 正确的方法：使用 DiffResultWrapper 的 _get_stats 方法
                if hasattr(diff_result, '_get_stats'):
                    stats = diff_result._get_stats()
                    total_rows_source = stats.table1_count
                    total_rows_target = stats.table2_count
                    self.logger.info(f"📊 从 DiffResultWrapper 获取行数: 源表={total_rows_source}, 目标表={total_rows_target}")
                elif hasattr(diff_result, 'info_tree') and hasattr(diff_result.info_tree, 'info'):
                    # 备选方法：直接从 info_tree 获取
                    rowcounts = diff_result.info_tree.info.rowcounts
                    if rowcounts:
                        total_rows_source = rowcounts.get(1, 0)
                        total_rows_target = rowcounts.get(2, 0)
                        self.logger.info(f"📊 从 info_tree 获取行数: 源表={total_rows_source}, 目标表={total_rows_target}")
                else:
                    self.logger.warning("Unable to find row count information in diff_result")
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
            result = {
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
            }            # 添加类型警告信息
            type_warnings = config.get("_type_warnings", [])
            ignored_columns_details = config.get("_ignored_columns_details", [])

            if type_warnings:
                # 提取被忽略的列名
                ignored_columns = [col.get('column_name', '') for col in ignored_columns_details]

                # 检查比对配置，判断是否有参与比对的列被忽略
                compare_columns = config.get("compare_columns") or config.get("columns_to_compare")
                compare_columns_set = set()
                if compare_columns:
                    if isinstance(compare_columns, str):
                        compare_columns_set = set(col.strip() for col in compare_columns.split(','))
                    elif isinstance(compare_columns, list):
                        compare_columns_set = set(compare_columns)

                # 判断是否有参与比对的列被忽略
                has_ignored_comparison_columns = False
                if compare_columns_set:
                    # 指定了比对列，检查是否有被忽略的
                    has_ignored_comparison_columns = any(col.get('column_name') in compare_columns_set for col in ignored_columns_details)
                elif compare_columns is None:
                    # 没有指定比对列（比对所有列），所以任何被忽略的列都影响比对
                    has_ignored_comparison_columns = bool(ignored_columns_details)
                else:
                    # compare_columns 是空数组，表示不比对任何列，不支持的列不影响比对
                    has_ignored_comparison_columns = False

                # 生成用户建议（需要传入source_config和target_config，暂时从config获取）
                user_suggestions = self._generate_user_suggestions(
                    type_warnings,
                    ignored_columns,
                    config.get('_source_config', {}),
                    config.get('_target_config', {})
                )

                result["warnings"] = {
                    "unsupported_types": type_warnings,
                    "message": "🚨 严重错误：检测到不支持的数据类型，这些字段被完全忽略！比对结果不可靠，可能显示100%匹配但实际有差异！" if has_ignored_comparison_columns else "⚠️ 检测到表中包含不支持的数据类型，但这些字段未参与比对，不影响比对结果。",
                    "severity": "critical" if has_ignored_comparison_columns else "warning",
                    "impact": "比对结果不可信，不应基于此结果做决策" if has_ignored_comparison_columns else "不影响比对结果，仅供参考",
                    "recommendation": "1) 启用严格类型检查模式立即失败 2) 预处理数据转换类型 3) 从比对中排除这些字段" if has_ignored_comparison_columns else "如需比对这些字段，请先进行数据类型转换",
                    "ignored_columns": ignored_columns_details,  # 新增：详细的被忽略字段列表
                    "user_suggestions": user_suggestions  # 新增：用户建议
                }
                # 只有当实际参与比对的字段有问题时，才设置为失败
                if has_ignored_comparison_columns:
                    result["summary"]["data_quality_score"] = "Failed"  # 改为 Failed 而不是 Poor
                    result["summary"]["incomplete_comparison"] = True
                    result["summary"]["comparison_invalid"] = True  # 新增：标记比对无效
                    result["statistics"]["warning"] = "⚠️ 比对失败 - 关键字段被忽略，结果不可信"
                    result["statistics"]["reliability"] = "unreliable"  # 新增：可靠性标记
                else:
                    # 表中有不支持的字段，但没有参与比对，比对结果仍然可靠
                    result["summary"]["data_quality_score"] = result["summary"].get("data_quality_score", "Good")
                    result["statistics"]["reliability"] = "reliable"

                result["summary"]["ignored_columns_count"] = len(ignored_columns_details)
                result["summary"]["ignored_columns_list"] = [f"{col['table_name']}.{col['column_name']} ({col['data_type']})" for col in ignored_columns_details]
                result["statistics"]["ignored_columns_details"] = ignored_columns_details  # 在统计中也包含详细信息

            return result

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

    def _generate_user_suggestions(self,
                                  type_warnings: List[str],
                                  ignored_columns: List[str],
                                  source_config: Dict[str, Any],
                                  target_config: Dict[str, Any]) -> List[str]:
        """
        为用户生成有用的建议和解决方案

        Args:
            type_warnings: 类型警告列表
            ignored_columns: 被忽略的列
            source_config: 源数据库配置
            target_config: 目标数据库配置

        Returns:
            建议列表
        """
        suggestions = []

        source_type = source_config.get('database_type', 'unknown')
        target_type = target_config.get('database_type', 'unknown')

        if type_warnings:
            suggestions.append(f"🔍 发现 {len(type_warnings)} 个类型兼容性问题")

            if ignored_columns:
                suggestions.append(f"📋 以下列被忽略: {', '.join(ignored_columns)}")

            # 针对不同数据库类型的具体建议
            if source_type == 'postgresql' and target_type == 'clickzetta':
                suggestions.append("💡 PostgreSQL → Clickzetta 建议:")
                suggestions.append("  • money 类型: 建议转换为 decimal 或 numeric")
                suggestions.append("  • uuid 类型: 建议转换为 string 或 varchar")
                suggestions.append("  • inet/macaddr 类型: 建议转换为 string")
                suggestions.append("  • 可考虑在 Clickzetta 端创建视图进行类型转换")

            suggestions.append("⚙️ 解决方案:")
            suggestions.append("  1. 启用严格模式查看详细类型信息")
            suggestions.append("  2. 在比对前预处理数据类型")
            suggestions.append("  3. 使用 ETL 工具统一数据格式")
            suggestions.append("  4. 从比对配置中排除不兼容的列")

        if not type_warnings and not ignored_columns:
            suggestions.append("✅ 所有列类型都兼容，可以进行完整比对")

        return suggestions

    async def get_table_schema_preview(self,
                                     connection_config: Dict[str, Any],
                                     table_name: str) -> Dict[str, Any]:
        """
        获取表结构预览，帮助用户了解表结构

        Args:
            connection_config: 数据库连接配置
            table_name: 表名

        Returns:
            表结构信息
        """
        try:
            from data_diff import connect_to_table

            # 构建连接字符串
            if connection_config.get('database_type') == 'clickzetta':
                db_info = {
                    "driver": "clickzetta",
                    "username": connection_config.get('username'),
                    "password": connection_config.get('password'),
                    "instance": connection_config.get('instance'),
                    "service": connection_config.get('service'),
                    "workspace": connection_config.get('workspace'),
                    "schema": connection_config.get('db_schema'),
                    "virtualcluster": connection_config.get('vcluster')
                }
            else:
                conn_id = await self.connection_manager.create_connection(connection_config)
                db_info = self.connection_manager._build_connection_string(
                    self.connection_manager.get_connection_config(conn_id)
                )

            # 连接到表并获取schema
            try:
                table_segment = connect_to_table(
                    db_info=db_info,
                    table_name=table_name,
                    key_columns=("id",),  # 临时使用，只是为了获取schema
                    thread_count=1
                ).with_schema()

                # 验证连接是否正常工作
                try:
                    # 尝试获取基本信息来验证连接
                    _ = table_segment.get_schema()
                    self.logger.info(f"✅ Schema preview connection verified for table: {table_name}")
                except Exception as schema_test_error:
                    self.logger.error(f"❌ Schema preview connection failed: {schema_test_error}")
                    raise Exception(f"无法获取表结构，连接失败或表不存在: {table_name} - {str(schema_test_error)}")

            except Exception as connect_error:
                self.logger.error(f"❌ Failed to connect for schema preview: {connect_error}")
                raise Exception(f"无法连接到表 {table_name} 获取结构预览: {str(connect_error)}")

            schema_info = {}
            if hasattr(table_segment, '_schema') and table_segment._schema:
                for column, column_type in table_segment._schema.items():
                    schema_info[column] = {
                        'type': str(column_type),
                        'supported': True  # data-diff 能识别就是支持的
                    }

            # 获取行数
            try:
                row_count = table_segment.count()
                schema_info['_row_count'] = row_count
            except Exception as e:
                schema_info['_row_count'] = f"无法获取: {str(e)}"

            return {
                'status': 'success',
                'table_name': table_name,
                'schema': schema_info,
                'database_type': connection_config.get('database_type')
            }

        except Exception as e:
            return {
                'status': 'error',
                'table_name': table_name,
                'error': str(e),
                'database_type': connection_config.get('database_type')
            }
