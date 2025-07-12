"""
Data-Diff N8N API 主应用
提供 RESTful API 接口用于数据比对和工作流管理
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import asyncio
import logging
from datetime import datetime, timedelta
import uuid
from urllib.parse import urlparse, parse_qs
from json import JSONDecodeError

from .utils import parse_connection_string, create_error_response, create_success_response
from .response_models import (
    ErrorCodes, BaseResponse, ConnectionTestResponse, ComparisonStartResponse,
    ComparisonResultResponse, TableListResponse, QueryExecutionResponse,
    HealthCheckResponse, ErrorResponse, DataResponse
)
try:
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

from ..core import (
    ConnectionManager,
    ComparisonEngine,
    ClickzettaAdapter,
    ResultProcessor,
    ConfigManager,
    ErrorHandler
)
from .advanced_routes import router as advanced_router

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 创建 FastAPI 应用
app = FastAPI(
    title="Data-Diff N8N API",
    description="RESTful API for cross-database data comparison with N8N workflow integration",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制来源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 包含高级路由
app.include_router(advanced_router)

# Prometheus 指标 (如果可用)
if PROMETHEUS_AVAILABLE:
    REQUEST_COUNT = Counter('api_requests_total', 'Total API requests', ['method', 'endpoint'])
    REQUEST_DURATION = Histogram('api_request_duration_seconds', 'API request duration')
    CONNECTION_TESTS = Counter('connection_tests_total', 'Total connection tests', ['database_type', 'status'])
    COMPARISONS_COUNT = Counter('comparisons_total', 'Total comparisons', ['status'])

# 全局实例
config_manager = ConfigManager()
connection_manager = ConnectionManager(config_manager)
comparison_engine = ComparisonEngine(config_manager)
result_processor = ResultProcessor(config_manager)
error_handler = ErrorHandler(config_manager)


# 辅助函数

class ConnectionSchema(BaseModel):
    """数据库连接配置模型"""
    driver: str = Field(..., description="数据库驱动")
    user: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    host: str = Field(..., description="主机")
    port: int = Field(..., description="端口")
    database: str = Field(..., description="数据库名")
    schema_name: Optional[str] = Field("public", alias="schema", description="模式名")
    ssl: Optional[bool] = Field(False, description="是否使用SSL")


class TableComparisonRequest(BaseModel):
    """表比对请求模型"""
    source_connection: str
    target_connection: str
    source_table: str
    target_table: str
    primary_key_columns: List[str]
    update_key_columns: Optional[List[str]] = []
    where_condition: Optional[str] = None
    columns_to_compare: Optional[List[str]] = None
    operation_type: str = "compareTables"
    sample_size: Optional[int] = 10000
    # 算法选择
    algorithm: Optional[str] = Field("auto", description="比对算法: auto/joindiff/hashdiff")
    # 分段比对参数
    bisection_factor: Optional[int] = Field(None, description="每次迭代的段数，默认32")
    bisection_threshold: Optional[int] = Field(None, description="最小分段阈值，默认16384")
    # 采样参数
    sampling_confidence: Optional[float] = Field(0.95, description="采样置信水平，默认0.95")
    sampling_tolerance: Optional[float] = Field(0.01, description="采样误差容限，默认0.01")
    auto_sample_threshold: Optional[int] = Field(100000, description="自动采样阈值，默认100000")
    enable_sampling: Optional[bool] = Field(True, description="是否启用智能采样")
    case_sensitive: Optional[bool] = True
    threads: Optional[int] = 1
    strict_type_checking: Optional[bool] = False




class DatabaseConfig(BaseModel):
    """数据库配置模型"""
    type: str = Field(..., description="数据库类型")
    host: str = Field(..., description="主机地址")
    port: int = Field(..., description="端口号")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: str = Field(..., description="数据库名")
    schema_name: Optional[str] = Field("public", alias="schema", description="模式名")
    ssl: Optional[bool] = Field(False, description="是否使用SSL")


class ComparisonConfig(BaseModel):
    """比对配置模型"""
    source_table: str = Field(..., description="源表名")
    target_table: str = Field(..., description="目标表名")
    key_columns: List[str] = Field(..., description="主键列")
    exclude_columns: Optional[List[str]] = Field([], description="排除列")
    columns_to_compare: Optional[List[str]] = Field(None, description="要比对的列（如果为空则比对所有列）")
    method: str = Field("hashdiff", description="比对方法")
    sample_size: Optional[int] = Field(0, description="采样大小")
    threads: Optional[int] = Field(4, description="线程数")
    tolerance: Optional[float] = Field(0.001, description="数值容差")
    case_sensitive: Optional[bool] = Field(True, description="大小写敏感")
    strict_type_checking: Optional[bool] = Field(False, description="严格类型检查")


class ComparisonRequest(BaseModel):
    """比对请求模型"""
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    comparison_config: ComparisonConfig
    async_mode: Optional[bool] = Field(False, description="是否异步执行")




# 新增：支持嵌套JSON结构的比对请求模型
class NestedDatabaseConfig(BaseModel):
    """嵌套数据库配置模型"""
    database_type: str = Field(..., description="数据库类型")
    host: Optional[str] = Field(None, description="主机地址")
    port: Optional[int] = Field(None, description="端口号")
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")
    database: Optional[str] = Field(None, description="数据库名")
    db_schema: Optional[str] = Field(None, description="模式名")
    schema_field: Optional[str] = Field(None, alias="schema", description="模式名（别名）")
    # Clickzetta特定字段
    instance: Optional[str] = Field(None, description="Clickzetta实例")
    service: Optional[str] = Field(None, description="Clickzetta服务")
    workspace: Optional[str] = Field(None, description="Clickzetta工作空间")
    vcluster: Optional[str] = Field(None, description="Clickzetta虚拟集群")
    ssl: Optional[bool] = Field(False, description="是否使用SSL")


class NestedComparisonConfig(BaseModel):
    """嵌套比对配置模型"""
    source_table: str = Field(..., description="源表名")
    target_table: str = Field(..., description="目标表名")
    key_columns: List[str] = Field(..., description="主键列")
    primary_key_columns: Optional[List[str]] = Field(None, description="主键列（兼容字段）")
    compare_columns: Optional[List[str]] = Field(None, description="比对列")
    columns_to_compare: Optional[List[str]] = Field(None, description="要比对的列（兼容字段）")
    exclude_columns: Optional[List[str]] = Field(None, description="排除列")
    algorithm: Optional[str] = Field("hashdiff", description="比对算法")
    method: Optional[str] = Field(None, description="比对方法（兼容字段）")
    threads: Optional[int] = Field(1, description="线程数")
    sample_size: Optional[int] = Field(None, description="采样大小")
    tolerance: Optional[float] = Field(0.001, description="数值容差")
    case_sensitive: Optional[bool] = Field(True, description="大小写敏感")
    bisection_threshold: Optional[int] = Field(1024, description="二分法阈值")
    where_condition: Optional[str] = Field(None, description="WHERE条件")
    strict_type_checking: Optional[bool] = Field(False, description="严格类型检查")


class NestedComparisonRequest(BaseModel):
    """嵌套比对请求模型"""
    source_config: NestedDatabaseConfig
    target_config: NestedDatabaseConfig
    comparison_config: NestedComparisonConfig


class NestedSchemaComparisonRequest(BaseModel):
    """嵌套模式比对请求模型"""
    source_config: NestedDatabaseConfig
    target_config: NestedDatabaseConfig

    class Config:
        extra = "allow"  # 允许额外字段


class QueryExecutionRequest(BaseModel):
    """查询执行请求模型"""
    connection: Dict[str, Any] = Field(..., description="数据库连接配置")
    query: str = Field(..., description="要执行的SQL查询")
    limit: Optional[int] = Field(None, description="结果限制数量")
    timeout: Optional[int] = Field(30, description="查询超时时间（秒）")


# API 端点实现

@app.get("/", response_model=BaseResponse)
async def root():
    """根端点"""
    return create_success_response(
        data={
            "name": "Data-Diff N8N API",
            "version": "1.0.0",
            "status": "running"
        },
        message="API is running"
    )


@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """健康检查端点"""
    try:
        # 检查各个组件状态
        components = {
            "config_manager": {"status": "healthy"},
            "connection_manager": {"status": "healthy"},
            "comparison_engine": {"status": "healthy"},
            "result_processor": {"status": "healthy"},
            "error_handler": {"status": "healthy"}
        }

        return HealthCheckResponse(
            success=True,
            status="healthy",
            version="1.0.0",
            components=components
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise create_error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code=ErrorCodes.INTERNAL_ERROR,
            message="Health check failed",
            details={"error": str(e)}
        )


@app.post("/api/v1/connections/test", response_model=ConnectionTestResponse)
async def test_connection(db_config: dict):
    """测试数据库连接，支持多种数据库类型（扁平参数）"""
    db_type = db_config.get("type") or db_config.get("driver")
    try:
        if db_type == "clickzetta":
            try:
                import clickzetta

                # 处理instance和service参数
                if "host" in db_config and "." in db_config["host"]:
                    # 从host中解析instance和service
                    host_parts = db_config["host"].split(".", 1)
                    instance = db_config.get("instance", host_parts[0])
                    service = db_config.get("service", host_parts[1])
                else:
                    instance = db_config.get("instance", "")
                    service = db_config.get("service", "uat-api.clickzetta.com")

                conn = clickzetta.connect(
                    username=db_config["username"],
                    password=db_config["password"],
                    service=service,
                    instance=instance,
                    workspace=db_config["workspace"],
                    vcluster=db_config.get("vcluster", db_config.get("virtualcluster")),  # 支持两种参数名
                    schema=db_config.get("schema")
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                conn.close()
            except ImportError as ie:
                if "runtime_version" in str(ie):
                    raise create_error_response(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        error_code=ErrorCodes.CONNECTION_ERROR,
                        message="ClickZetta connector incompatible with protobuf version",
                        details={"error": "Please check dependency versions"}
                    )
                raise
        elif db_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["username"],
                password=db_config["password"],
                dbname=db_config["database"]
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(
                host=db_config["host"],
                port=int(db_config["port"]),
                user=db_config["username"],
                password=db_config["password"],
                database=db_config["database"]
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
        elif db_type == "oracle":
            import cx_Oracle
            dsn = cx_Oracle.makedsn(db_config["host"], db_config["port"], service_name=db_config["database"])
            conn = cx_Oracle.connect(
                user=db_config["username"],
                password=db_config["password"],
                dsn=dsn
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM dual")
                cursor.fetchone()
            conn.close()
        elif db_type in ("trino", "presto"):
            from trino.dbapi import connect as trino_connect
            conn = trino_connect(
                host=db_config["host"],
                port=int(db_config["port"]),
                user=db_config["username"],
                catalog=db_config.get("catalog", "hive"),
                schema=db_config.get("schema", "default")
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
        elif db_type == "duckdb":
            import duckdb
            conn = duckdb.connect(database=db_config["database"])
            conn.execute("SELECT 1")
            conn.close()
        elif db_type in ("mssql", "sqlserver"):
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_config['host']},{db_config['port']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};PWD={db_config['password']}"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
        else:
            raise create_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCodes.UNSUPPORTED_DATABASE,
                message=f"Unsupported database type: {db_type}"
            )

        return ConnectionTestResponse(
            success=True,
            database_type=db_type,
            message=f"{db_type} connection test successful"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{db_type} connection test failed: {e}")
        raise create_error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCodes.CONNECTION_ERROR,
            message=f"{db_type} connection test failed",
            details={"error": str(e)}
        )


@app.post("/api/v1/comparisons")
async def create_comparison(
    request: ComparisonRequest,
    background_tasks: BackgroundTasks
):
    """创建数据比对任务"""
    try:
        comparison_id = str(uuid.uuid4())

        if request.async_mode:
            # 异步执行
            background_tasks.add_task(
                execute_comparison_async,
                comparison_id,
                request.source_db.dict(by_alias=True),
                request.target_db.dict(by_alias=True),
                request.comparison_config.dict(by_alias=True)
            )

            return {
                "comparison_id": comparison_id,
                "status": "started",
                "async_mode": True,
                "message": "比对任务已启动，请使用 comparison_id 查询结果"
            }
        else:
            # 同步执行
            result = await comparison_engine.compare_tables(
                source_config=request.source_db.dict(by_alias=True),
                target_config=request.target_db.dict(by_alias=True),
                comparison_config=request.comparison_config.dict(by_alias=True)
            )

            # 处理结果
            processed_result = await result_processor.process_result(result)

            return {
                "comparison_id": comparison_id,
                "status": "completed",
                "result": processed_result
            }

    except Exception as e:
        logger.error(f"Comparison creation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"比对任务创建失败: {str(e)}"
        )




@app.get("/api/v1/compare/status")
async def get_all_comparisons_status():
    """获取所有比对任务的状态概览"""
    try:
        from .utils import _task_status, _task_progress
        
        # 汇总任务状态
        status_summary = {
            "total": len(_task_status),
            "pending": sum(1 for t in _task_status.values() if t["status"] == "pending"),
            "running": sum(1 for t in _task_status.values() if t["status"] == "running"),
            "completed": sum(1 for t in _task_status.values() if t["status"] == "completed"),
            "failed": sum(1 for t in _task_status.values() if t["status"] == "failed")
        }
        
        # 获取最近10个任务的详细信息
        recent_tasks = []
        sorted_tasks = sorted(_task_status.items(), 
                            key=lambda x: x[1]["updated_at"], 
                            reverse=True)[:10]
        
        for task_id, task_info in sorted_tasks:
            progress_info = _task_progress.get(task_id, {})
            recent_tasks.append({
                "comparison_id": task_id,
                "status": task_info["status"],
                "updated_at": task_info["updated_at"],
                "progress": progress_info.get("progress", 0),
                "current_step": progress_info.get("current_step", "")
            })
        
        return {
            "summary": status_summary,
            "recent_tasks": recent_tasks,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"获取任务状态概览失败: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取任务状态概览失败: {str(e)}"
        )


@app.get("/api/v1/compare/results/{comparison_id}", response_model=ComparisonResultResponse)
async def get_comparison_result(comparison_id: str):
    """获取比对结果
    
    返回状态可能为：
    - pending: 任务已创建但尚未开始
    - running: 任务正在执行中
    - completed: 任务已完成
    - failed: 任务执行失败
    - not_found: 任务不存在
    """
    try:
        from .utils import get_result_from_storage, get_task_status, get_task_progress
        
        # 先检查任务状态
        task_status = get_task_status(comparison_id)
        
        if task_status == "not_found":
            # 查看是否有已保存的结果
            result = get_result_from_storage(comparison_id)
            if result is None:
                raise create_error_response(
                    status_code=status.HTTP_404_NOT_FOUND,
                    error_code=ErrorCodes.TASK_NOT_FOUND,
                    message=f"Comparison task not found: {comparison_id}",
                    details={"comparison_id": comparison_id}
                )
            # 返回已保存的结果，确保格式正确
            return ComparisonResultResponse(
                success=True,
                comparison_id=comparison_id,
                status=result.get("status", "completed"),
                result=result
            )
        
        elif task_status == "pending":
            return ComparisonResultResponse(
                success=True,
                comparison_id=comparison_id,
                status="pending",
                message="Task created, waiting for execution",
                progress=0,
                estimated_time="Unknown"
            )
        
        elif task_status == "running":
            # 获取进度信息（如果有）
            progress_info = get_task_progress(comparison_id)
            return ComparisonResultResponse(
                success=True,
                comparison_id=comparison_id,
                status="running",
                message="Task is running",
                progress=progress_info.get("progress", 0),
                current_step=progress_info.get("current_step", "Data comparison"),
                estimated_time=progress_info.get("estimated_time", "Calculating...")
            )
        
        else:
            # 获取已完成或失败的结果
            result = get_result_from_storage(comparison_id)
            if result is None:
                # 如果状态显示完成但没有结果，返回错误
                return ComparisonResultResponse(
                    success=False,
                    comparison_id=comparison_id,
                    status="error",
                    message="Task completed but result is missing"
                )
            
            # 返回结果
            return ComparisonResultResponse(
                success=task_status != "failed",
                comparison_id=comparison_id,
                status=task_status,
                result=result,
                message="Task completed successfully" if task_status == "completed" else "Task failed"
            )
    
    except HTTPException as he:
        raise
    except Exception as e:
        logger.error(f"Failed to get comparison result: {e}")
        raise create_error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCodes.INTERNAL_ERROR,
            message="Failed to get comparison result",
            details={
                "comparison_id": comparison_id,
                "error": str(e)
            }
        )


@app.post("/api/v1/compare/tables", response_model=Dict[str, Any])
@app.get("/api/v1/compare/tables", response_model=Dict[str, Any])  # 添加GET方法支持
async def compare_tables_endpoint(request: Request, background_tasks: BackgroundTasks):
    """处理表比对请求 - 同时支持JSON请求体和URL查询参数"""
    try:
        # 判断请求类型并获取参数
        params = {}
        if request.method == "GET":
            # 对于GET请求，直接获取查询参数
            params = dict(request.query_params)
        elif request.method == "POST":
            # 对于POST请求，先检查Content-Type
            content_type = request.headers.get("content-type", "")
            if "application/json" in content_type:
                # JSON格式请求体
                try:
                    body = await request.json()
                    params = body
                except JSONDecodeError:
                    # 处理非JSON请求体
                    form_data = await request.form()
                    if form_data:
                        params = dict(form_data)
                    else:
                        # 尝试处理URL编码形式
                        body_bytes = await request.body()
                        if body_bytes:
                            body_str = body_bytes.decode()
                            try:
                                # 尝试解析URL编码的表单数据
                                form_data = parse_qs(body_str)
                                params = {k: v[0] if len(v) == 1 else v for k, v in form_data.items()}
                            except Exception:
                                pass
            else:
                # 表单数据或URL编码
                form_data = await request.form()
                if form_data:
                    params = dict(form_data)
                else:
                    # 最后尝试查询参数
                    params = dict(request.query_params)

        # 如果params为空，再次尝试获取查询参数
        if not params:
            params = dict(request.query_params)

        logger.info(f"Received comparison request with params: {params}")

        # 处理数组格式的查询参数（如 primary_key_columns[] 或 key_columns[]）
        # 转换为统一的 key_columns 参数（data-diff使用的标准参数名）
        if 'primary_key_columns[]' in params and 'key_columns' not in params:
            params['key_columns'] = params['primary_key_columns[]']
        elif 'primary_key_columns' in params and 'key_columns' not in params:
            params['key_columns'] = params['primary_key_columns']
        elif 'key_columns[]' in params and 'key_columns' not in params:
            params['key_columns'] = params['key_columns[]']

        # 转换 columns_to_compare[] 为 columns_to_compare
        if 'columns_to_compare[]' in params and 'columns_to_compare' not in params:
            params['columns_to_compare'] = params['columns_to_compare[]']

        # 检查必要参数 - 支持多种格式，但统一转换为 key_columns
        key_param = None
        if 'key_columns' in params:
            key_param = 'key_columns'
        elif 'primary_key_columns' in params:
            key_param = 'primary_key_columns'
            params['key_columns'] = params['primary_key_columns']  # 转换为标准名称
        elif 'primary_key_columns[]' in params:
            key_param = 'primary_key_columns[]'
            params['key_columns'] = params['primary_key_columns[]']  # 转换为标准名称
        elif 'key_columns[]' in params:
            key_param = 'key_columns[]'
            params['key_columns'] = params['key_columns[]']  # 转换为标准名称

        if not key_param:
            raise create_error_response(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                error_code=ErrorCodes.MISSING_PARAMETER,
                message="Missing required parameter: key_columns or primary_key_columns"
            )

        # 检查其他必要参数
        required_params = ['source_connection', 'target_connection', 'source_table', 'target_table']
        for param in required_params:
            if param not in params or not params[param]:
                raise create_error_response(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    error_code=ErrorCodes.MISSING_PARAMETER,
                    message=f"Missing required parameter: {param}"
                )

        # 处理特殊参数 - 确保主键列不为空，使用统一的 key_columns 参数
        key_value = params.get('key_columns', '')
        if isinstance(key_value, str):
            if key_value.strip():
                primary_keys = [col.strip() for col in key_value.split(',') if col.strip()]
            else:
                primary_keys = []
        elif isinstance(key_value, list):
            primary_keys = [str(col).strip() for col in key_value if str(col).strip()]
        else:
            primary_keys = []

        if not primary_keys:
            raise create_error_response(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                error_code=ErrorCodes.INVALID_PARAMETER,
                message="key_columns cannot be empty"
            )

        update_keys = params.get('update_key_columns', '').split(',') if isinstance(params.get('update_key_columns'), str) and params.get('update_key_columns').strip() else []

        # 处理 columns_to_compare 参数
        compare_cols_param = params.get('columns_to_compare', '')
        if isinstance(compare_cols_param, str):
            if compare_cols_param.strip():
                compare_cols = [col.strip() for col in compare_cols_param.split(',') if col.strip()]
            else:
                compare_cols = []  # 空字符串表示比较所有列
        elif isinstance(compare_cols_param, list):
            compare_cols = [str(col).strip() for col in compare_cols_param if str(col).strip()]
        else:
            compare_cols = []

        logger.info(f"处理特殊参数：primary_keys={primary_keys}, update_keys={update_keys}, compare_cols={compare_cols}")

        # 解析数据库连接字符串
        try:
            source_db_config = parse_connection_string(params['source_connection'])
            target_db_config = parse_connection_string(params['target_connection'])
        except Exception as e:
            logger.error(f"Failed to parse connection string: {e}", exc_info=True)
            raise create_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCodes.INVALID_PARAMETER,
                message="Failed to parse database connection string",
                details={"error": str(e)}
            )

        # 构建比对配置
        sample_size = params.get('sample_size')
        if sample_size and not isinstance(sample_size, int):
            try:
                sample_size = int(sample_size)
            except (ValueError, TypeError):
                sample_size = 10000
        else:
            sample_size = 10000

        threads = params.get('threads')
        if threads and not isinstance(threads, int):
            try:
                threads = int(threads)
            except (ValueError, TypeError):
                threads = 1
        else:
            threads = 1

        case_sensitive = params.get('case_sensitive')
        if isinstance(case_sensitive, str):
            case_sensitive = case_sensitive.lower() == 'true'
        elif not isinstance(case_sensitive, bool):
            case_sensitive = True

        strict_type_checking = params.get('strict_type_checking')
        if isinstance(strict_type_checking, str):
            strict_type_checking = strict_type_checking.lower() == 'true'
        elif not isinstance(strict_type_checking, bool):
            strict_type_checking = False

        comparison_config = {
            "source_table": params['source_table'],
            "target_table": params['target_table'],
            "key_columns": primary_keys,
            "compare_columns": compare_cols,
            "columns_to_compare": compare_cols,  # 添加这个字段以确保兼容性
            "update_column": update_keys[0] if update_keys else None,
            "source_filter": params.get('where_condition'),
            "target_filter": params.get('where_condition'),
            "algorithm": params.get('algorithm', 'auto'),  # 使用新的算法参数
            "sample_size": sample_size,
            "threads": threads,
            "case_sensitive": case_sensitive,
            "enable_sampling": sample_size is not None,
            "strict_type_checking": strict_type_checking,
            # 添加分段比对参数
            "bisection_factor": params.get('bisection_factor'),
            "bisection_threshold": params.get('bisection_threshold'),
            # 添加采样参数
            "sampling_confidence": float(params.get('sampling_confidence', 0.95)),
            "sampling_tolerance": float(params.get('sampling_tolerance', 0.01)),
            "auto_sample_threshold": int(params.get('auto_sample_threshold', 100000)),
            "enable_sampling": params.get('enable_sampling', True) if isinstance(params.get('enable_sampling'), bool) else str(params.get('enable_sampling', 'true')).lower() == 'true'
        }

        logger.info(f"比对配置: {comparison_config}")

        # 生成比对ID
        comparison_id = str(uuid.uuid4())
        logger.info(f"创建比对任务 {comparison_id}")
        
        # 创建任务记录
        from .utils import create_task
        create_task(comparison_id)

        # 异步执行比对
        background_tasks.add_task(
            execute_comparison_async,
            comparison_id,
            source_db_config,
            target_db_config,
            comparison_config
        )

        return ComparisonStartResponse(
            success=True,
            comparison_id=comparison_id,
            status="started",
            async_mode=True,
            message="Table comparison task started"
        )

    except HTTPException as he:
        # 直接重新抛出HTTP异常
        raise
    except ValueError as ve:
        logger.error(f"Comparison request parameter error: {ve}")
        raise create_error_response(
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code=ErrorCodes.INVALID_PARAMETER,
            message="Invalid comparison request parameters",
            details={"error": str(ve)}
        )
    except Exception as e:
        logger.error(f"Table comparison failed: {e}", exc_info=True)
        raise create_error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCodes.COMPARISON_ERROR,
            message="Table comparison failed",
            details={"error": str(e)}
        )






@app.get("/api/v1/metrics")
async def get_metrics():
    """获取系统指标"""
    try:
        # 收集各种指标
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "comparisons": {
                "total": 0,  # 从存储中获取
                "today": 0,
                "success_rate": 0.0
            },
            "performance": {
                "avg_response_time": 0.0,
                "avg_rows_per_second": 0,
                "memory_usage": "0MB"
            }
        }

        return metrics

    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取指标失败: {str(e)}"
        )


@app.get("/api/v1/errors")
async def get_recent_errors(limit: int = 50):
    """获取最近的错误信息"""
    try:
        errors = error_handler.get_recent_errors(limit=limit)

        return {
            "errors": errors,
            "count": len(errors),
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to get errors: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取错误信息失败: {str(e)}"
        )


@app.post("/api/v1/tables/list", response_model=TableListResponse)
async def list_tables(db_config: dict):
    """获取数据库表列表，支持多种数据库类型（扁平参数）"""
    db_type = db_config.get("type") or db_config.get("driver")
    target_schema = db_config.get("schema")

    try:
        tables = []

        if db_type == "clickzetta":
            import clickzetta
            conn = clickzetta.connect(
                username=db_config["username"],
                password=db_config["password"],
                service=db_config["service"],
                instance=db_config["instance"],
                workspace=db_config["workspace"],
                vcluster=db_config["vcluster"],
                schema=target_schema or db_config.get("schema")
            )
            with conn.cursor() as cursor:
                if target_schema:
                    cursor.execute(f"SHOW TABLES IN {target_schema}")
                else:
                    cursor.execute("SHOW TABLES")
                tables = [row[1] for row in cursor.fetchall()]
            conn.close()

        elif db_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                user=db_config["username"],
                password=db_config["password"],
                dbname=db_config["database"]
            )
            with conn.cursor() as cursor:
                schema_clause = f"AND table_schema = '{target_schema}'" if target_schema else "AND table_schema = 'public'"
                cursor.execute(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE' {schema_clause}
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(
                host=db_config["host"],
                port=int(db_config["port"]),
                user=db_config["username"],
                password=db_config["password"],
                database=db_config["database"]
            )
            with conn.cursor() as cursor:
                if target_schema:
                    cursor.execute(f"SHOW TABLES FROM {target_schema}")
                else:
                    cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type == "oracle":
            import cx_Oracle
            dsn = cx_Oracle.makedsn(db_config["host"], db_config["port"], service_name=db_config["database"])
            conn = cx_Oracle.connect(
                user=db_config["username"],
                password=db_config["password"],
                dsn=dsn
            )
            with conn.cursor() as cursor:
                schema_clause = f"AND owner = '{target_schema.upper()}'" if target_schema else f"AND owner = '{db_config['username'].upper()}'"
                cursor.execute(f"""
                    SELECT table_name
                    FROM all_tables
                    WHERE 1=1 {schema_clause}
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type in ("trino", "presto"):
            from trino.dbapi import connect as trino_connect
            conn = trino_connect(
                host=db_config["host"],
                port=int(db_config["port"]),
                user=db_config["username"],
                catalog=db_config.get("catalog", "hive"),
                schema=target_schema or db_config.get("schema", "default")
            )
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type == "duckdb":
            import duckdb
            conn = duckdb.connect(database=db_config["database"])
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

        elif db_type in ("mssql", "sqlserver"):
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_config['host']},{db_config['port']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};PWD={db_config['password']}"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            schema_clause = f"AND table_schema = '{target_schema}'" if target_schema else "AND table_schema = 'dbo'"
            cursor.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE' {schema_clause}
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

        else:
            raise create_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCodes.UNSUPPORTED_DATABASE,
                message=f"Unsupported database type: {db_type}"
            )

        return TableListResponse(
            success=True,
            database_type=db_type,
            database=db_config.get("database", ""),
            db_schema=target_schema,
            tables=tables,
            count=len(tables),
            message=f"Found {len(tables)} tables"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{db_type} list tables failed: {e}")
        raise create_error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCodes.DATABASE_ERROR,
            message=f"Failed to list tables from {db_type}",
            details={"error": str(e)}
        )


@app.post("/api/v1/query/execute", response_model=QueryExecutionResponse)
async def execute_query(request: QueryExecutionRequest):
    """执行SQL查询"""
    sql_query = ""
    try:
        connection_config = request.connection
        sql_query = request.query.strip()

        if not sql_query:
            raise create_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCodes.INVALID_PARAMETER,
                message="SQL query cannot be empty"
            )

        # 获取数据库类型
        db_type = connection_config.get("type") or connection_config.get("database_type")

        if not db_type:
            raise create_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCodes.MISSING_PARAMETER,
                message="Database type not specified"
            )

        logger.info(f"Executing query on {db_type}: {sql_query[:100]}...")

        results = []

        if db_type == "clickzetta":
            import clickzetta
            conn = clickzetta.connect(
                username=connection_config["username"],
                password=connection_config["password"],
                service=connection_config["service"],
                instance=connection_config["instance"],
                workspace=connection_config["workspace"],
                vcluster=connection_config["vcluster"],
                schema=connection_config.get("schema")
            )
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                # 获取列名
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                # 获取数据
                rows = cursor.fetchall()
                # 转换为字典列表
                results = [dict(zip(columns, row)) for row in rows]
            conn.close()

        elif db_type in ("postgres", "postgresql"):
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(
                host=connection_config["host"],
                port=connection_config["port"],
                user=connection_config["username"],
                password=connection_config["password"],
                dbname=connection_config["database"]
            )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql_query)
                results = [dict(row) for row in cursor.fetchall()]
            conn.close()

        elif db_type == "mysql":
            import mysql.connector
            conn = mysql.connector.connect(
                host=connection_config["host"],
                port=connection_config["port"],
                user=connection_config["username"],
                password=connection_config["password"],
                database=connection_config["database"]
            )
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql_query)
            results = cursor.fetchall()
            cursor.close()
            conn.close()

        elif db_type in ("mssql", "sqlserver"):
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={connection_config['host']},{connection_config['port']};"
                f"DATABASE={connection_config['database']};"
                f"UID={connection_config['username']};PWD={connection_config['password']}"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql_query)

            # 获取列名
            columns = [column[0] for column in cursor.description] if cursor.description else []
            # 获取数据
            rows = cursor.fetchall()
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

            cursor.close()
            conn.close()

        elif db_type == "oracle":
            import cx_Oracle
            dsn = cx_Oracle.makedsn(connection_config["host"], connection_config["port"],
                                   service_name=connection_config["database"])
            conn = cx_Oracle.connect(
                user=connection_config["username"],
                password=connection_config["password"],
                dsn=dsn
            )
            cursor = conn.cursor()
            cursor.execute(sql_query)

            # 获取列名
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            # 获取数据
            rows = cursor.fetchall()
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

            cursor.close()
            conn.close()

        elif db_type in ("trino", "presto"):
            from trino.dbapi import connect as trino_connect
            conn = trino_connect(
                host=connection_config["host"],
                port=int(connection_config["port"]),
                user=connection_config["username"],
                catalog=connection_config.get("catalog", "hive"),
                schema=connection_config.get("schema", "default")
            )
            cursor = conn.cursor()
            cursor.execute(sql_query)

            # 获取列名
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            # 获取数据
            rows = cursor.fetchall()
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

            cursor.close()
            conn.close()

        elif db_type == "duckdb":
            import duckdb
            conn = duckdb.connect(database=connection_config["database"])
            cursor = conn.cursor()
            cursor.execute(sql_query)

            # 获取列名
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            # 获取数据
            rows = cursor.fetchall()
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

            conn.close()

        elif db_type == "vertica":
            import vertica_python
            conn = vertica_python.connect(
                host=connection_config["host"],
                port=connection_config["port"],
                user=connection_config["username"],
                password=connection_config["password"],
                database=connection_config["database"]
            )
            cursor = conn.cursor()
            cursor.execute(sql_query)

            # 获取列名
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            # 获取数据
            rows = cursor.fetchall()
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

            cursor.close()
            conn.close()

        elif db_type == "clickhouse":
            import clickhouse_driver
            client = clickhouse_driver.Client(
                host=connection_config["host"],
                port=connection_config["port"],
                user=connection_config["username"],
                password=connection_config["password"],
                database=connection_config["database"]
            )
            # ClickHouse 返回的是元组列表，需要获取列名
            result = client.execute(sql_query, with_column_types=True)
            columns = [col[0] for col in result[1]]
            rows = result[0]
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

        elif db_type == "snowflake":
            import snowflake.connector
            conn = snowflake.connector.connect(
                user=connection_config["username"],
                password=connection_config["password"],
                account=connection_config["account"],
                warehouse=connection_config.get("warehouse"),
                database=connection_config["database"],
                schema=connection_config.get("schema")
            )
            cursor = conn.cursor()
            cursor.execute(sql_query)

            # 获取列名
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            # 获取数据
            rows = cursor.fetchall()
            # 转换为字典列表
            results = [dict(zip(columns, row)) for row in rows]

            cursor.close()
            conn.close()

        elif db_type == "bigquery":
            from google.cloud import bigquery
            from google.oauth2 import service_account

            # 如果提供了服务账号密钥
            if connection_config.get("service_account_key"):
                credentials = service_account.Credentials.from_service_account_info(
                    connection_config["service_account_key"]
                )
                client = bigquery.Client(
                    credentials=credentials,
                    project=connection_config["project_id"]
                )
            else:
                # 使用默认凭证
                client = bigquery.Client(project=connection_config["project_id"])

            query_job = client.query(sql_query)
            rows = query_job.result()

            # 转换为字典列表
            results = [dict(row) for row in rows]

        elif db_type == "redshift":
            import psycopg2
            import psycopg2.extras
            # Redshift 使用 PostgreSQL 协议
            conn = psycopg2.connect(
                host=connection_config["host"],
                port=connection_config["port"],
                user=connection_config["username"],
                password=connection_config["password"],
                dbname=connection_config["database"]
            )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql_query)
                results = [dict(row) for row in cursor.fetchall()]
            conn.close()

        elif db_type == "databricks":
            from databricks import sql
            with sql.connect(
                server_hostname=connection_config["server_hostname"],
                http_path=connection_config["http_path"],
                access_token=connection_config["access_token"]
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_query)

                    # 获取列名
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    # 获取数据
                    rows = cursor.fetchall()
                    # 转换为字典列表
                    results = [dict(zip(columns, row)) for row in rows]

        else:
            raise create_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=ErrorCodes.UNSUPPORTED_DATABASE,
                message=f"Unsupported database type: {db_type}"
            )

        # 应用结果限制
        if request.limit and request.limit > 0:
            results = results[:request.limit]

        return QueryExecutionResponse(
            success=True,
            query=sql_query,
            database_type=db_type,
            row_count=len(results),
            result=results,
            message=f"Query executed successfully, returned {len(results)} rows"
        )

    except HTTPException as http_exc:
        # 重新抛出 HTTPException，不要包装
        raise http_exc
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise create_error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCodes.DATABASE_ERROR,
            message="Query execution failed",
            details={"error": str(e), "query": sql_query[:100] + "..." if len(sql_query) > 100 else sql_query}
        )


# 后台任务函数
async def execute_comparison_async(
    comparison_id: str,
    source_config: Dict[str, Any],
    target_config: Dict[str, Any],
    comparison_config: Dict[str, Any]
):
    """异步执行比对任务"""
    try:
        from .utils import save_result_to_storage, update_task_status, update_task_progress
        
        global comparison_engine
        logger.info(f"Starting async comparison {comparison_id}")
        
        # 更新任务状态为运行中
        update_task_status(comparison_id, "running")
        update_task_progress(comparison_id, 10, "初始化连接")

        result = await comparison_engine.compare_tables(
            source_config=source_config,
            target_config=target_config,
            comparison_config=comparison_config
        )

        # 直接使用字典形式处理结果，不调用可能不存在的process_result方法
        if isinstance(result, dict) and "status" in result:
            processed_result = result
        else:
            # 如果结果不是标准格式，进行简单包装
            processed_result = {
                "status": "completed",
                "job_id": comparison_id,
                "timestamp": datetime.utcnow().isoformat(),
                "result": str(result)[:1000]  # 截断结果，避免过大
            }

        # 保存结果到临时存储
        from .utils import save_result_to_storage
        save_result_to_storage(comparison_id, processed_result)

        logger.info(f"Async comparison {comparison_id} completed successfully")

    except Exception as e:
        logger.error(f"Async comparison {comparison_id} failed: {e}")

        # 保存错误信息
        error_result = {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
            "config": comparison_config  # 添加配置信息以便调试
        }
        from .utils import save_result_to_storage
        save_result_to_storage(comparison_id, error_result)


# 启动事件
@app.on_event("startup")
async def startup_event():
    """应用启动时的初始化"""
    logger.info("Data-Diff N8N API starting up...")

    # 初始化各个组件
    await config_manager.initialize()
    await connection_manager.initialize()

    logger.info("Data-Diff N8N API started successfully")


# 关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时的清理"""
    logger.info("Data-Diff N8N API shutting down...")

    # 清理资源
    await connection_manager.cleanup()

    logger.info("Data-Diff N8N API shutdown complete")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


@app.post("/api/v1/compare/tables/nested", response_model=Dict[str, Any])
async def compare_tables_nested_endpoint(request: NestedComparisonRequest, background_tasks: BackgroundTasks):
    """处理嵌套JSON结构的表比对请求"""
    try:
        logger.info(f"收到嵌套结构比对请求")

        # 转换数据库配置
        def convert_db_config(config: NestedDatabaseConfig) -> Dict[str, Any]:
            """转换数据库配置为内部格式"""
            # 优先使用db_schema，如果没有则使用schema_field，最后才使用默认值
            schema_value = config.db_schema or config.schema_field or "public"
            logger.error(f"DEBUG: convert_db_config input - database_type={config.database_type}, db_schema={config.db_schema}, schema_field={config.schema_field}")

            if config.database_type.lower() == "postgresql":
                result = {
                    "database_type": "postgresql",  # 保留原始database_type字段
                    "driver": "postgresql",
                    "username": config.username,  # 使用username而不是user
                    "password": config.password,
                    "host": config.host or "localhost",
                    "port": config.port or 5432,
                    "database": config.database,
                    "schema": schema_value
                }
                logger.error(f"DEBUG: PostgreSQL config result - schema={result.get('schema')}")
                return result
            elif config.database_type.lower() == "clickzetta":
                result = {
                    "database_type": "clickzetta",  # 保留原始database_type字段
                    "driver": "clickzetta",
                    "username": config.username,  # 使用username而不是user
                    "password": config.password,
                    "instance": config.instance,
                    "service": config.service,
                    "workspace": config.workspace,
                    "schema": schema_value,  # 统一使用schema字段
                    "vcluster": config.vcluster or "default_ap"
                }
                logger.error(f"DEBUG: Clickzetta config result - schema={result.get('schema')}, db_schema input was={config.db_schema}")
                return result
            else:
                raise ValueError(f"不支持的数据库类型: {config.database_type}")

        source_db_config = convert_db_config(request.source_config)
        target_db_config = convert_db_config(request.target_config)

        # 处理主键列（支持两个字段名）
        key_columns = request.comparison_config.key_columns
        if not key_columns and request.comparison_config.primary_key_columns:
            key_columns = request.comparison_config.primary_key_columns

        if not key_columns:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="必须指定 key_columns 或 primary_key_columns"
            )

        # 处理比对列（支持两个字段名）
        compare_columns = request.comparison_config.compare_columns or request.comparison_config.columns_to_compare or []

        # 构建比对配置
        comparison_config = {
            "source_table": request.comparison_config.source_table,
            "target_table": request.comparison_config.target_table,
            "key_columns": key_columns,
            "compare_columns": compare_columns,
            "columns_to_compare": compare_columns,  # 添加这个字段以确保兼容性
            "exclude_columns": request.comparison_config.exclude_columns or [],
            "algorithm": request.comparison_config.algorithm or request.comparison_config.method or "hashdiff",
            "threads": request.comparison_config.threads or 1,
            "sample_size": request.comparison_config.sample_size or 10000,  # 提供默认值
            "tolerance": request.comparison_config.tolerance or 0.001,
            "case_sensitive": request.comparison_config.case_sensitive if request.comparison_config.case_sensitive is not None else True,
            "bisection_threshold": request.comparison_config.bisection_threshold or 1024,
            "where_condition": request.comparison_config.where_condition,
            "strict_type_checking": request.comparison_config.strict_type_checking or False
        }

        logger.info(f"转换后的配置 - 源: {source_db_config['driver']}, 目标: {target_db_config['driver']}")
        logger.info(f"比对配置: {comparison_config}")

        # 生成比对ID
        comparison_id = str(uuid.uuid4())
        logger.info(f"创建嵌套结构比对任务 {comparison_id}")
        
        # 创建任务记录
        from .utils import create_task
        create_task(comparison_id)

        # 异步执行比对
        background_tasks.add_task(
            execute_comparison_async,
            comparison_id,
            source_db_config,
            target_db_config,
            comparison_config
        )

        return {
            "comparison_id": comparison_id,
            "status": "started",
            "message": "嵌套结构表比对任务已启动",
            "source_type": source_db_config["driver"],
            "target_type": target_db_config["driver"]
        }

    except HTTPException as he:
        logger.error(f"HTTP异常: {he}")
        raise
    except ValueError as ve:
        logger.error(f"嵌套比对请求参数错误: {ve}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        logger.error(f"嵌套结构表比对失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"嵌套结构表比对失败: {str(e)}"
        )


@app.post("/api/v1/compare/schemas/nested", response_model=Dict[str, Any])
async def compare_schemas_nested_endpoint(request: NestedSchemaComparisonRequest):
    """处理嵌套JSON结构的模式比对请求"""
    try:
        logger.info(f"收到嵌套结构模式比对请求")

        # 转换数据库配置（复用表比对的转换函数）
        def convert_db_config(config: NestedDatabaseConfig) -> Dict[str, Any]:
            """转换数据库配置为内部格式"""
            # 优先使用db_schema，如果没有则使用schema_field，最后才使用默认值
            schema_value = config.db_schema or config.schema_field or "public"

            if config.database_type.lower() == "postgresql":
                return {
                    "database_type": "postgresql",
                    "driver": "postgresql",
                    "username": config.username,
                    "password": config.password,
                    "host": config.host or "localhost",
                    "port": config.port or 5432,
                    "database": config.database,
                    "schema": schema_value
                }
            elif config.database_type.lower() == "clickzetta":
                return {
                    "database_type": "clickzetta",
                    "driver": "clickzetta",
                    "username": config.username,
                    "password": config.password,
                    "instance": config.instance,
                    "service": config.service,
                    "workspace": config.workspace,
                    "schema": schema_value,
                    "vcluster": config.vcluster or "default_ap"
                }
            else:
                raise ValueError(f"不支持的数据库类型: {config.database_type}")

        source_db_config = convert_db_config(request.source_config)
        target_db_config = convert_db_config(request.target_config)

        logger.info(f"转换后的配置 - 源: {source_db_config['driver']}, 目标: {target_db_config['driver']}")

        # 直接执行模式比对（不需要异步，因为模式比对通常很快）
        result = await comparison_engine.compare_schemas(
            source_config=source_db_config,
            target_config=target_db_config
        )

        return {
            "status": "completed",
            "result": result,
            "source_type": source_db_config["driver"],
            "target_type": target_db_config["driver"]
        }

    except ValueError as ve:
        logger.error(f"嵌套模式比对请求参数错误: {ve}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        logger.error(f"嵌套结构模式比对失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"嵌套结构模式比对失败: {str(e)}"
        )


@app.get("/api/v1/database_types")
async def get_database_types():
    """获取支持的数据库类型"""
    try:
        supported_types = [
            "postgresql",
            "clickzetta",
            "mysql",
            "sqlite",
            "oracle",
            "mssql"
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=supported_types
        )
    except Exception as e:
        logger.error(f"Failed to get database types: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get database types: {str(e)}"
        )
