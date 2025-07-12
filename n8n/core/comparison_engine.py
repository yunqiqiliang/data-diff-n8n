"""
比对引擎
负责执行数据比对操作
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple, TYPE_CHECKING
from datetime import datetime
import uuid

# 导入采样引擎
try:
    from .sampling_engine import SamplingEngine, SamplingConfig
    from .result_materializer import ResultMaterializer
except ImportError:
    try:
        from n8n.core.sampling_engine import SamplingEngine, SamplingConfig
        from n8n.core.result_materializer import ResultMaterializer
    except ImportError:
        # 如果都失败了，定义一个临时的
        class SamplingConfig:
            def __init__(self, **kwargs):
                self.enabled = kwargs.get('enabled', True)
                self.confidence_level = kwargs.get('confidence_level', 0.95)
                self.margin_of_error = kwargs.get('margin_of_error', 0.01)
                self.min_sample_size = kwargs.get('min_sample_size', 1000)
                self.max_sample_size = kwargs.get('max_sample_size', 1000000)
                self.auto_sample_threshold = kwargs.get('auto_sample_threshold', 100000)
        
        class SamplingEngine:
            def __init__(self):
                pass
            
            def should_use_sampling(self, row_count, config):
                return False, None
        
        ResultMaterializer = None

# 导入 data-diff 相关模块
try:
    from data_diff import diff_tables, Algorithm
    from data_diff.diff_tables import DiffResult
    HAS_DATA_DIFF = True
except ImportError:
    HAS_DATA_DIFF = False
    logging.warning("data-diff library not found. This library is required for data comparison operations.")

# 导入列统计模块
try:
    from data_diff.column_statistics import ColumnStatisticsCollector
except ImportError:
    ColumnStatisticsCollector = None
    logging.info("Column statistics collector not available")

# 导入差异分类器
try:
    from data_diff.difference_classifier import DifferenceClassifier, ClassifiedDifference
except ImportError:
    DifferenceClassifier = None
    ClassifiedDifference = None
    logging.info("Difference classifier not available")

# 导入时间线分析模块
try:
    from data_diff.timeline_analyzer import TimelineAnalyzer
except ImportError:
    TimelineAnalyzer = None
    logging.info("Timeline analyzer not available")

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
        self.sampling_engine = SamplingEngine()
        
        # 初始化结果物化器（如果可用）
        self.result_materializer = None
        if ResultMaterializer:
            try:
                # 使用 Docker Compose 中定义的 PostgreSQL 配置
                db_config = {
                    'host': 'postgres',  # Docker 网络中的服务名
                    'port': 5432,
                    'database': 'datadiff',
                    'user': 'postgres',
                    'password': 'password'
                }
                self.result_materializer = ResultMaterializer(db_config)
                # 确保 schema 和表存在
                self.result_materializer.ensure_schema_exists()
                self.logger.info("Result materializer initialized successfully")
            except Exception as e:
                self.logger.warning(f"Failed to initialize result materializer: {e}")
                self.result_materializer = None

    async def compare_tables(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        comparison_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行表比对
        """
        job_id = comparison_config.get('comparison_id', str(uuid.uuid4()))
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
                source_config, target_config, start_time=start_time
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
        job_id = config.get('comparison_id', str(uuid.uuid4()))
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
                source_connection_string, target_connection_string, config, job_id,
                start_time=start_time
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
        target_config: Dict[str, Any] = None,
        start_time: datetime = None
    ) -> Dict[str, Any]:
        """
        使用 data-diff 执行比对
        """
        try:
            from data_diff import connect_to_table, diff_tables, Algorithm

            if start_time is None:
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
                    config["_source_count"] = source_count  # 保存行数供后续使用
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
                    config["_target_count"] = target_count  # 保存行数供后续使用
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
                
                # 保存列名到配置中，便于后续处理差异时使用
                if source_schema:
                    config['_column_names'] = list(source_schema.keys())
                    self.logger.info(f"📦 Column names: {config['_column_names']}")
                
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

            # 选择算法 - 添加自动选择逻辑
            algorithm_str = config.get("algorithm", "auto").lower()
            
            if algorithm_str == "auto":
                # 自动选择算法：同数据库用 JOINDIFF，跨数据库用 HASHDIFF
                source_type = source_config.get("type", "").lower() if source_config else ""
                target_type = target_config.get("type", "").lower() if target_config else ""
                
                if source_type and target_type and source_type == target_type:
                    algorithm = Algorithm.JOINDIFF
                    self.logger.info(f"🤖 Auto-selected JOINDIFF algorithm (same database type: {source_type})")
                else:
                    algorithm = Algorithm.HASHDIFF
                    self.logger.info(f"🤖 Auto-selected HASHDIFF algorithm (cross-database: {source_type} -> {target_type})")
            elif algorithm_str == "joindiff":
                algorithm = Algorithm.JOINDIFF
            elif algorithm_str == "hashdiff":
                algorithm = Algorithm.HASHDIFF
            else:
                # 默认使用 HASHDIFF
                algorithm = Algorithm.HASHDIFF
                self.logger.warning(f"Unknown algorithm '{algorithm_str}', using HASHDIFF as default")

            # 构建比对选项
            diff_options = {
                "algorithm": algorithm,
                "key_columns": key_columns,
                "where": config.get("source_filter"),
                "update_column": config.get("update_column"),
                "threaded": True,
                "max_threadpool_size": config.get("threads", 1)
            }
            
            # 添加分段比对参数（仅对 HASHDIFF 算法有效）
            if algorithm == Algorithm.HASHDIFF:
                # bisection_factor: 每次迭代的段数，默认 32
                if config.get("bisection_factor"):
                    diff_options["bisection_factor"] = config.get("bisection_factor")
                    self.logger.info(f"📊 Using bisection_factor: {diff_options['bisection_factor']}")
                
                # bisection_threshold: 最小分段阈值，默认 16384
                if config.get("bisection_threshold"):
                    diff_options["bisection_threshold"] = config.get("bisection_threshold")
                    self.logger.info(f"📊 Using bisection_threshold: {diff_options['bisection_threshold']}")

            # 添加 extra_columns（比较的列）
            compare_columns = config.get("compare_columns") or config.get("columns_to_compare")
            if compare_columns:
                if isinstance(compare_columns, str):
                    compare_columns = tuple(compare_columns.split(','))
                elif isinstance(compare_columns, list):
                    compare_columns = tuple(compare_columns)
                diff_options["extra_columns"] = compare_columns
            
            # 添加数据类型处理参数
            if config.get("float_tolerance") is not None:
                diff_options["float_tolerance"] = config.get("float_tolerance")
                self.logger.info(f"🔢 Float tolerance enabled: {diff_options['float_tolerance']}")
            
            if config.get("timestamp_precision"):
                diff_options["timestamp_precision"] = config.get("timestamp_precision")
                self.logger.info(f"⏰ Timestamp precision enabled: {diff_options['timestamp_precision']}")
            
            if config.get("json_comparison_mode"):
                diff_options["json_comparison_mode"] = config.get("json_comparison_mode")
                self.logger.info(f"📄 JSON comparison mode enabled: {diff_options['json_comparison_mode']}")
            
            if config.get("column_remapping"):
                diff_options["column_remapping"] = config.get("column_remapping")
                diff_options["case_sensitive_remapping"] = config.get("case_sensitive_remapping", True)
                self.logger.info(f"🔄 Column remapping enabled: {diff_options['column_remapping']}")

            # 移除值为 None 的选项
            diff_options = {k: v for k, v in diff_options.items() if v is not None}

            # 应用采样逻辑
            sampling_applied = False
            sample_size = None
            
            # 现在 data-diff 支持采样了！
            if config.get("enable_sampling", False):
                # 构建采样配置
                sampling_config = SamplingConfig(
                    enabled=True,
                    confidence_level=config.get("sampling_confidence", 0.95),
                    margin_of_error=config.get("sampling_margin_of_error", 0.01),
                    min_sample_size=config.get("min_sample_size", 1000),
                    max_sample_size=config.get("max_sample_size", 1000000),
                    auto_sample_threshold=config.get("auto_sample_threshold", 100000)
                )
                
                # 检查是否应该使用采样（基于行数）
                source_count = config.get("_source_count", 0)
                target_count = config.get("_target_count", 0)
                max_row_count = max(source_count, target_count)
                
                # 处理百分比采样
                if config.get("sampling_percent"):
                    sampling_applied = True
                    sampling_percent = config["sampling_percent"]
                    config["_sampling_applied"] = True
                    config["_sampling_percent"] = sampling_percent
                    
                    # 添加采样参数到 diff_options
                    diff_options["sampling_percent"] = sampling_percent
                    diff_options["sampling_method"] = config.get("sampling_method", "DETERMINISTIC")
                    
                    self.logger.info(f"📊 Percentage sampling enabled: {sampling_percent}% (method: {config.get('sampling_method', 'DETERMINISTIC')})")
                    
                # 处理固定大小采样
                elif config.get("sample_size") and config["sample_size"] > 0:
                    # 用户手动指定了采样大小
                    sampling_applied = True
                    sample_size = config["sample_size"]
                    config["_sampling_applied"] = True
                    config["_actual_sample_size"] = sample_size
                    
                    # 添加采样参数到 diff_options
                    diff_options["sample_size"] = sample_size
                    diff_options["sampling_method"] = config.get("sampling_method", "DETERMINISTIC")
                    
                    self.logger.info(f"📊 Fixed size sampling enabled: {sample_size} rows (method: {config.get('sampling_method', 'DETERMINISTIC')})")
                    
                # 自动采样逻辑
                else:
                    should_sample, calculated_sample_size = self.sampling_engine.should_use_sampling(
                        max_row_count,
                        sampling_config
                    )
                    
                    if should_sample and calculated_sample_size:
                        # 自动计算的采样大小
                        sample_size = calculated_sample_size
                        
                        # 更新比对选项 - 现在 data-diff 支持采样了！
                        sampling_applied = True
                        config["_sampling_applied"] = True
                        config["_actual_sample_size"] = sample_size
                        config["_sampling_config"] = {
                            "confidence_level": sampling_config.confidence_level,
                            "margin_of_error": sampling_config.margin_of_error
                        }
                        
                        # 添加采样参数到 diff_options
                        diff_options["sample_size"] = sample_size
                        diff_options["sampling_method"] = config.get("sampling_method", "DETERMINISTIC")
                        
                        self.logger.info(f"📊 Auto sampling enabled: {sample_size} rows (confidence: {sampling_config.confidence_level*100}%, margin: {sampling_config.margin_of_error*100}%)")

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

            # 收集列级统计信息（如果启用）
            column_statistics = None
            if config.get("enable_column_statistics", False) and ColumnStatisticsCollector:
                self.logger.info(f"📊 Collecting column-level statistics...")
                try:
                    column_statistics = await self._collect_column_statistics(
                        source_table_segment,
                        target_table_segment,
                        config
                    )
                except Exception as stats_error:
                    self.logger.warning(f"Failed to collect column statistics: {stats_error}")
                    # 继续处理，统计失败不应影响主要比对结果

            # 执行时间线分析（如果配置了时间列）
            timeline_analysis = None
            if config.get("timeline_column") and TimelineAnalyzer:
                self.logger.info(f"📈 Performing timeline analysis on column: {config['timeline_column']}")
                self.logger.info(f"🔍 TimelineAnalyzer available: {TimelineAnalyzer is not None}")
                try:
                    timeline_analysis = await self._analyze_timeline(
                        source_table_segment,
                        target_table_segment,
                        diff_result,
                        config
                    )
                    self.logger.info(f"🎯 Timeline analysis result: {timeline_analysis is not None}")
                    if timeline_analysis:
                        self.logger.info(f"📊 Timeline analysis keys: {list(timeline_analysis.keys()) if isinstance(timeline_analysis, dict) else 'not a dict'}")
                except Exception as timeline_error:
                    self.logger.warning(f"⚠️ Failed to perform timeline analysis: {timeline_error}")
                    import traceback
                    self.logger.warning(f"Timeline traceback: {traceback.format_exc()}")
                    # 继续处理，时间线分析失败不应影响主要比对结果
            else:
                self.logger.info(f"🚫 Timeline analysis skipped. Column: {config.get('timeline_column')}, Analyzer: {TimelineAnalyzer is not None}")

            # 处理结果
            return self._process_datadiff_result(
                diff_result, config, job_id, start_time, end_time, 
                column_statistics=column_statistics,
                timeline_analysis=timeline_analysis
            )

        except Exception as e:
            self.logger.error(f"Data-diff execution for job {job_id} failed: {e}", exc_info=True)
            raise Exception(f"Data-diff execution failed: {str(e)}")

    def _process_datadiff_result(
        self,
        diff_result: Any,
        config: Dict[str, Any],
        job_id: str,
        start_time: datetime,
        end_time: datetime,
        column_statistics: Optional[Dict[str, Any]] = None,
        timeline_analysis: Optional[Dict[str, Any]] = None
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

            # 如果从 diff_result 获取的行数不正确，使用配置中保存的行数
            if (total_rows_source == 0 or total_rows_target == 0) and config.get('_source_count'):
                total_rows_source = config.get('_source_count', 0)
                total_rows_target = config.get('_target_count', 0)
                self.logger.info(f"📊 使用配置中的行数: 源表={total_rows_source}, 目标表={total_rows_target}")
            
            # 计算比对的行数
            if total_rows_source == 0 and total_rows_target == 0:
                rows_compared = max(total_differences, 1) # 确保不会除以零
            else:
                rows_compared = max(total_rows_source, total_rows_target)  # 使用 max 而不是 min
                if rows_compared == 0:
                    rows_compared = 1  # 确保不会除以零

            # 计算匹配率
            match_rate = 1.0
            if rows_compared > 0:
                match_rate = 1 - (total_differences / rows_compared)

            # 获取样本差异并进行分类
            sample_differences = []
            classified_differences = []
            classification_summary = {}
            
            try:
                # 尝试获取一些样本差异
                self.logger.info(f"🔍 Attempting to extract sample differences from diff_result")
                self.logger.info(f"diff_result type: {type(diff_result)}")
                self.logger.info(f"diff_result attributes: {dir(diff_result)}")
                
                if hasattr(diff_result, 'diffs'):
                    self.logger.info(f"✅ diff_result has 'diffs' attribute")
                    diffs_sample = list(diff_result.diffs)[:100]  # 获取更多样本用于分类
                    self.logger.info(f"📊 Got {len(diffs_sample)} sample differences")
                else:
                    # 如果diff_result是一个生成器或迭代器
                    self.logger.info(f"🔄 diff_result doesn't have 'diffs' attribute, trying to iterate")
                    diffs_sample = []
                    for i, diff in enumerate(diff_result):
                        if i >= 100:
                            break
                        diffs_sample.append(diff)
                    self.logger.info(f"📊 Got {len(diffs_sample)} differences by iterating")
                    
                    # 初始化差异分类器
                    if DifferenceClassifier:
                        classifier_config = {
                            'case_sensitive': config.get('case_sensitive', True),
                            'numeric_tolerance': config.get('tolerance', 0.0),
                            'treat_null_as_critical': config.get('treat_null_as_critical', False)
                        }
                        classifier = DifferenceClassifier(classifier_config)
                        
                        # 准备差异数据用于分类
                        diffs_for_classification = []
                    
                    for i, diff in enumerate(diffs_sample):
                        # 记录差异的原始格式
                        if i == 0:
                            self.logger.info(f"🔍 First diff sample type: {type(diff)}")
                            self.logger.info(f"🔍 First diff sample: {diff}")
                        
                        # 处理不同的差异格式
                        if isinstance(diff, dict):
                            diff_type = diff.get('diff_type', 'unknown')
                            key = diff.get('key', {})
                            source_row = diff.get('a_values', None)
                            target_row = diff.get('b_values', None)
                        elif isinstance(diff, tuple) and len(diff) >= 2:
                            # data-diff 返回的格式: ('+'/'-'/('!', ...)), (row_data))
                            diff_sign = diff[0]
                            row_data = diff[1]
                            
                            # 将元组转换为字典
                            column_names = config.get('_column_names', [])
                            if not column_names:
                                # 如果没有列名信息，尝试使用默认值
                                self.logger.warning("⚠️ No column names in config, using default")
                                column_names = ['product_id', 'product_name', 'price', 'quantity', 'created_at']
                            if config.get('key_columns'):
                                key_col = config['key_columns'][0]
                                key_index = column_names.index(key_col) if key_col in column_names else 0
                                key = {key_col: row_data[key_index]}
                            else:
                                key = {'product_id': row_data[0]}
                            
                            if diff_sign == '-':
                                diff_type = 'missing_in_target'
                                source_row = dict(zip(column_names, row_data))
                                target_row = None
                            elif diff_sign == '+':
                                diff_type = 'missing_in_source'
                                source_row = None
                                target_row = dict(zip(column_names, row_data))
                            elif isinstance(diff_sign, tuple) and diff_sign[0] == '!':
                                # 值差异: ('!', column_index)
                                diff_type = 'value_different'
                                # TODO: 处理值差异的情况
                                continue
                            else:
                                self.logger.warning(f"⚠️ Unknown diff sign: {diff_sign}")
                                continue
                        else:
                            # 其他未知格式
                            self.logger.warning(f"⚠️ Unknown diff format: {type(diff)}, content: {diff}")
                            continue

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
                            columns_diff = {}
                            for col in source_row:
                                if col in target_row and source_row[col] != target_row[col]:
                                    differing_columns.append(col)
                                    columns_diff[col] = {
                                        'source': source_row[col],
                                        'target': target_row[col]
                                    }
                            sample_diff["differing_columns"] = differing_columns
                            
                            # 准备分类数据
                            if DifferenceClassifier and columns_diff:
                                diff_for_classification = key.copy()
                                diff_for_classification['columns'] = columns_diff
                                diffs_for_classification.append(diff_for_classification)

                        # 只保留前10个作为样本
                        if i < 10:
                            sample_differences.append(sample_diff)
                    
                    # 执行差异分类
                    if DifferenceClassifier and diffs_for_classification:
                        # 获取列类型信息（如果可用）
                        column_types = {}
                        try:
                            if hasattr(source_table_segment, 'get_schema'):
                                schema = source_table_segment.get_schema()
                                for col_name, col_info in schema.items():
                                    column_types[col_name] = str(col_info.data_type) if hasattr(col_info, 'data_type') else str(col_info)
                        except:
                            pass
                        
                        # 分类差异
                        classified = classifier.classify_differences(diffs_for_classification, column_types)
                        classified_differences = [c.to_dict() for c in classified[:20]]  # 保留前20个分类结果
                        
                        # 生成分类摘要
                        classification_summary = classifier.generate_summary(classified)
                        
            except Exception as e:
                self.logger.warning(f"Unable to get or classify sample differences: {e}")

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
                    "data_quality_score": "Good" if match_rate > 0.95 else "Fair" if match_rate > 0.8 else "Poor",
                    "total_rows": rows_compared,
                    "rows_matched": rows_compared - total_differences if rows_compared >= total_differences else 0,
                    "rows_different": total_differences,
                    "match_rate": round(match_rate * 100, 2),
                    "execution_time": execution_time
                }
            }
            
            # 添加差异分类结果
            if classified_differences or classification_summary:
                result["difference_classification"] = {
                    "classified_samples": classified_differences,
                    "summary": classification_summary
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

            # 添加列级统计信息（如果有）
            if column_statistics:
                result["column_statistics"] = column_statistics
                # 在摘要中添加统计概览
                if "comparison" in column_statistics and "summary" in column_statistics["comparison"]:
                    stats_summary = column_statistics["comparison"]["summary"]
                    result["summary"]["column_statistics_summary"] = {
                        "total_columns": stats_summary.get("total_columns", 0),
                        "columns_with_differences": stats_summary.get("columns_with_differences", 0),
                        "has_warnings": len(column_statistics["comparison"].get("warnings", [])) > 0
                    }

            # 添加时间线分析信息（如果有）
            if timeline_analysis:
                result["timeline_analysis"] = timeline_analysis
                # 在摘要中添加时间线概览
                if "summary" in timeline_analysis:
                    timeline_summary = timeline_analysis["summary"]
                    result["summary"]["timeline_summary"] = {
                        "time_column": timeline_summary.get("time_column"),
                        "total_time_periods": timeline_summary.get("total_time_periods", 0),
                        "average_match_rate": timeline_summary.get("average_match_rate", 100),
                        "has_patterns": len(timeline_analysis.get("patterns", [])) > 0
                    }
            
            # 尝试物化结果到数据库（如果启用）
            if self.result_materializer and config.get('materialize_results', True):  # 默认启用
                try:
                    # 准备物化数据
                    materialization_data = {
                        'config': config,
                        'summary': result.get('summary', {}),
                        'differences': result.get('sample_differences', []),
                        'column_stats': column_statistics,
                        'timeline_analysis': timeline_analysis,
                        'performance_metrics': {
                            'execution_time': execution_time,
                            'rows_per_second': total_rows_source / execution_time if execution_time > 0 else 0,
                            'memory_usage_mb': 0  # TODO: 实现内存使用监控
                        },
                        'start_time': start_time,
                        'end_time': end_time
                    }
                    
                    # 执行物化
                    success = self.result_materializer.materialize_results(job_id, materialization_data)
                    if success:
                        result['summary']['result_materialized'] = True
                        self.logger.info(f"Results successfully materialized for comparison {job_id}")
                    else:
                        self.logger.warning(f"Failed to materialize results for comparison {job_id}")
                except Exception as e:
                    self.logger.error(f"Error materializing results: {e}")

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

    async def _collect_column_statistics(
        self,
        source_table_segment: Any,
        target_table_segment: Any,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        收集列级统计信息
        
        Args:
            source_table_segment: 源表段
            target_table_segment: 目标表段
            config: 比对配置
            
        Returns:
            包含列统计的字典
        """
        try:
            # 创建统计收集器
            source_collector = ColumnStatisticsCollector(source_table_segment.database.dialect)
            target_collector = ColumnStatisticsCollector(target_table_segment.database.dialect)
            
            # 获取需要统计的列
            source_schema = source_table_segment.get_schema()
            target_schema = target_table_segment.get_schema()
            
            # 转换为列类型元组列表
            source_columns = []
            target_columns = []
            
            # 获取处理后的schema
            try:
                # 获取表路径
                source_path = source_table_segment.table_path
                target_path = target_table_segment.table_path
                
                # 获取比对列
                compare_columns = config.get('columns_to_compare') or config.get('compare_columns')
                if compare_columns and isinstance(compare_columns, list):
                    filter_columns = compare_columns
                else:
                    filter_columns = list(source_schema.keys())
                
                processed_source_schema = source_table_segment.database._process_table_schema(
                    source_path, source_schema, filter_columns
                )
                for col_name, col_type in processed_source_schema.items():
                    source_columns.append((col_name, col_type))
                    
                processed_target_schema = target_table_segment.database._process_table_schema(
                    target_path, target_schema, filter_columns
                )
                for col_name, col_type in processed_target_schema.items():
                    target_columns.append((col_name, col_type))
            except Exception as e:
                self.logger.warning(f"Failed to process schemas for statistics: {e}")
                return {"error": str(e)}
            
            # 生成统计查询SQL
            source_stats_sql = source_collector.generate_statistics_sql(
                config["source_table"],
                source_columns,
                sample_size=config.get("_actual_sample_size"),
                where_clause=config.get("source_filter")
            )
            
            target_stats_sql = target_collector.generate_statistics_sql(
                config["target_table"],
                target_columns,
                sample_size=config.get("_actual_sample_size"),
                where_clause=config.get("target_filter")
            )
            
            self.logger.debug(f"Source statistics SQL: {source_stats_sql}")
            self.logger.debug(f"Target statistics SQL: {target_stats_sql}")
            
            # 执行统计查询
            source_stats_result = None
            target_stats_result = None
            
            # 使用数据库连接执行查询
            with source_table_segment.database.create_connection() as source_conn:
                with source_conn.cursor() as cursor:
                    cursor.execute(source_stats_sql)
                    source_stats_result = dict(zip(
                        [desc[0] for desc in cursor.description],
                        cursor.fetchone()
                    ))
                    
            with target_table_segment.database.create_connection() as target_conn:
                with target_conn.cursor() as cursor:
                    cursor.execute(target_stats_sql)
                    target_stats_result = dict(zip(
                        [desc[0] for desc in cursor.description],
                        cursor.fetchone()
                    ))
            
            # 解析结果
            source_stats = source_collector.parse_statistics_result(
                source_stats_result, source_columns
            )
            target_stats = target_collector.parse_statistics_result(
                target_stats_result, target_columns
            )
            
            # 比较统计信息
            comparison = source_collector.compare_column_statistics(
                source_stats, target_stats
            )
            
            # 生成报告
            report = source_collector.generate_statistics_report(
                source_stats, target_stats, comparison
            )
            
            return {
                "source_statistics": {col: stats.to_dict() for col, stats in source_stats.items()},
                "target_statistics": {col: stats.to_dict() for col, stats in target_stats.items()},
                "comparison": comparison,
                "report": report
            }
            
        except Exception as e:
            self.logger.error(f"Failed to collect column statistics: {e}", exc_info=True)
            return {"error": str(e)}

    async def _analyze_timeline(
        self,
        source_table_segment: Any,
        target_table_segment: Any,
        diff_result: Any,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行时间线分析
        
        Args:
            source_table_segment: 源表段
            target_table_segment: 目标表段
            diff_result: 比对结果
            config: 比对配置
            
        Returns:
            时间线分析结果
        """
        try:
            time_column = config["timeline_column"]
            self.logger.info(f"📈 Starting timeline analysis for column: {time_column}")
            
            # 创建时间线分析器
            analyzer = TimelineAnalyzer(time_column, source_table_segment.database.dialect)
            self.logger.info(f"✅ TimelineAnalyzer created")
            
            # 获取时间范围
            start_time = None
            end_time = None
            
            if config.get("timeline_start_time"):
                start_time = datetime.fromisoformat(config["timeline_start_time"])
            if config.get("timeline_end_time"):
                end_time = datetime.fromisoformat(config["timeline_end_time"])
            
            # 如果没有指定时间范围，从数据中自动检测
            if not start_time or not end_time:
                # 获取源表时间范围
                time_range_sql = f"""
                SELECT 
                    MIN({analyzer.dialect.quote(time_column)}) as min_time,
                    MAX({analyzer.dialect.quote(time_column)}) as max_time
                FROM {config["source_table"]}
                """
                
                if config.get("source_filter"):
                    time_range_sql += f" WHERE {config['source_filter']}"
                
                with source_table_segment.database.create_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(time_range_sql)
                        result = cursor.fetchone()
                        if result:
                            if not start_time and result[0]:
                                start_time = result[0]
                            if not end_time and result[1]:
                                end_time = result[1]
            
            if not start_time or not end_time:
                return {"error": "Could not determine time range for analysis"}
            
            # 确定时间窗口大小
            window_size = analyzer.determine_window_size(
                start_time, end_time,
                target_buckets=config.get("timeline_buckets", 20)
            )
            
            # 创建时间桶
            time_buckets = analyzer.create_time_buckets(start_time, end_time, window_size)
            
            # 收集差异样本（限制数量以提高性能）
            differences = []
            try:
                diff_count = 0
                max_diffs = config.get("timeline_max_differences", 10000)
                
                for diff in diff_result:
                    if diff_count >= max_diffs:
                        break
                    
                    # 转换差异格式（处理元组格式）
                    if isinstance(diff, tuple) and len(diff) >= 2:
                        diff_sign = diff[0]
                        row_data = diff[1]
                        
                        # 获取列名
                        column_names = config.get('_column_names', [])
                        if not column_names:
                            self.logger.warning("⚠️ No column names for timeline analysis")
                            continue
                        
                        # 将元组转换为字典
                        row_dict = dict(zip(column_names, row_data))
                        
                        if diff_sign == '-':
                            diff_dict = {
                                "type": "missing_in_target",
                                "key": {config['key_columns'][0]: row_data[0]},
                                "source_row": row_dict,
                                "target_row": None
                            }
                        elif diff_sign == '+':
                            diff_dict = {
                                "type": "missing_in_source",
                                "key": {config['key_columns'][0]: row_data[0]},
                                "source_row": None,
                                "target_row": row_dict
                            }
                        else:
                            continue
                    elif isinstance(diff, dict):
                        # 如果已经是字典格式
                        diff_dict = {
                            "type": diff.get("diff_type", "unknown"),
                            "key": diff.get("key", {}),
                            "source_row": diff.get("a_values"),
                            "target_row": diff.get("b_values")
                        }
                    else:
                        self.logger.warning(f"⚠️ Unknown diff format in timeline: {type(diff)}")
                        continue
                    
                    differences.append(diff_dict)
                    diff_count += 1
                    
            except Exception as e:
                self.logger.warning(f"Error collecting differences for timeline: {e}")
            
            # 分析差异
            timeline_data = analyzer.analyze_differences(
                differences, time_buckets, time_column
            )
            
            # 生成时间线报告
            timeline_report = analyzer.generate_timeline_report(
                timeline_data,
                config["source_table"],
                config["target_table"]
            )
            
            self.logger.info(f"✅ Timeline analysis completed successfully")
            self.logger.info(f"📈 Timeline report summary: {timeline_report.get('summary', {})}")
            
            return timeline_report
            
        except Exception as e:
            self.logger.error(f"❌ Failed to analyze timeline: {e}", exc_info=True)
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return {"error": str(e)}

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
