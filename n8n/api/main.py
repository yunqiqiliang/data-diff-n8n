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

from .utils import parse_connection_string
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
    ErrorHandler,
    WorkflowScheduler
)
from ..workflows import TemplateManager, WorkflowBuilder
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
workflow_builder = WorkflowBuilder(config_manager)
scheduler = WorkflowScheduler(config_manager)
template_manager = TemplateManager()
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
    bisection_threshold: Optional[int] = 1024
    case_sensitive: Optional[bool] = True
    threads: Optional[int] = 1


class SchemaComparisonRequest(BaseModel):
    """模式比对请求模型"""
    source_connection: str
    target_connection: str
    operation_type: str = "compareSchemas"


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
    method: str = Field("hashdiff", description="比对方法")
    sample_size: Optional[int] = Field(0, description="采样大小")
    threads: Optional[int] = Field(4, description="线程数")
    tolerance: Optional[float] = Field(0.001, description="数值容差")
    case_sensitive: Optional[bool] = Field(True, description="大小写敏感")


class ComparisonRequest(BaseModel):
    """比对请求模型"""
    source_db: DatabaseConfig
    target_db: DatabaseConfig
    comparison_config: ComparisonConfig
    async_mode: Optional[bool] = Field(False, description="是否异步执行")


class WorkflowConfig(BaseModel):
    """工作流配置模型"""
    name: str = Field(..., description="工作流名称")
    description: Optional[str] = Field("", description="工作流描述")
    template: Optional[str] = Field("simple_comparison", description="模板名称")
    schedule: Optional[str] = Field(None, description="调度表达式")
    enabled: Optional[bool] = Field(True, description="是否启用")


class ScheduleConfig(BaseModel):
    """调度配置模型"""
    type: str = Field("cron", description="调度类型")
    expression: str = Field(..., description="调度表达式")
    timezone: Optional[str] = Field("UTC", description="时区")
    enabled: Optional[bool] = Field(True, description="是否启用")


# API 端点实现

@app.get("/")
async def root():
    """根端点"""
    return {
        "message": "Data-Diff N8N API",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health_check():
    """健康检查端点"""
    try:
        # 检查各个组件状态
        health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "components": {
                "config_manager": "ok",
                "connection_manager": "ok",
                "comparison_engine": "ok",
                "result_processor": "ok",
                "workflow_builder": "ok",
                "scheduler": "ok"
            }
        }

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=health_status
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@app.post("/api/v1/connections/test")
async def test_connection(db_config: dict):
    """测试数据库连接，支持多种数据库类型（扁平参数）"""
    db_type = db_config.get("type") or db_config.get("driver")
    try:
        if db_type == "clickzetta":
            try:
                import clickzetta
                conn = clickzetta.connect(
                    username=db_config["username"],
                    password=db_config["password"],
                    service=db_config["service"],
                    instance=db_config["instance"],
                    workspace=db_config["workspace"],
                    vcluster=db_config["vcluster"],
                    schema=db_config.get("schema")
                )
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                conn.close()
            except ImportError as ie:
                if "runtime_version" in str(ie):
                    raise HTTPException(
                        status_code=500,
                        detail="ClickZetta连接器与protobuf版本不兼容。请检查依赖版本。"
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
            raise HTTPException(status_code=400, detail=f"不支持的数据库类型: {db_type}")

        return {"success": True, "message": f"{db_type} 连接测试成功"}
    except Exception as e:
        logger.error(f"{db_type} connection test failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"{db_type} 连接测试失败: {str(e)}"
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


@app.get("/api/v1/comparisons/{comparison_id}")
async def get_comparison_result(comparison_id: str):
    """获取比对结果"""
    try:
        # 从缓存或存储中获取结果
        from .utils import get_result_from_storage
        result = get_result_from_storage(comparison_id)

        if result:
            return {
                "comparison_id": comparison_id,
                "status": "completed",
                "result": result
            }
        else:
            # 检查是否在活跃任务中
            if comparison_engine and hasattr(comparison_engine, 'active_comparisons'):
                active_job = comparison_engine.active_comparisons.get(comparison_id)
                if active_job:
                    return {
                        "comparison_id": comparison_id,
                        "status": active_job.get("status", "running"),
                        "message": "比对任务正在进行中"
                    }

            return {
                "comparison_id": comparison_id,
                "status": "not_found",
                "message": "比对结果未找到或仍在处理中"
            }

    except Exception as e:
        logger.error(f"Failed to get comparison result: {e}", exc_info=True)
        return {
            "comparison_id": comparison_id,
            "status": "error",
            "message": f"获取比对结果失败: {str(e)}"
        }
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取比对结果失败: {str(e)}"
        )


@app.get("/api/v1/compare/results/{comparison_id}")
async def get_comparison_result_alias(comparison_id: str):
    """获取比对结果 - 别名端点，与 /api/v1/comparisons/{comparison_id} 功能相同"""
    return await get_comparison_result(comparison_id)


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
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="请求参数无效：缺少必要参数 key_columns, primary_key_columns 或对应的数组格式"
            )

        # 检查其他必要参数
        required_params = ['source_connection', 'target_connection', 'source_table', 'target_table']
        for param in required_params:
            if param not in params or not params[param]:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"请求参数无效：缺少必要参数 {param}"
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
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="请求参数无效：key_columns 不能为空"
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
            logger.error(f"解析连接字符串失败: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"解析数据库连接字符串失败: {str(e)}"
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

        comparison_config = {
            "source_table": params['source_table'],
            "target_table": params['target_table'],
            "key_columns": primary_keys,
            "compare_columns": compare_cols,
            "update_column": update_keys[0] if update_keys else None,
            "source_filter": params.get('where_condition'),
            "target_filter": params.get('where_condition'),
            "algorithm": params.get('operation_type', 'compareTables').lower(),
            "sample_size": sample_size,
            "threads": threads,
            "case_sensitive": case_sensitive,
            "enable_sampling": sample_size is not None
        }

        logger.info(f"比对配置: {comparison_config}")

        # 生成比对ID
        comparison_id = str(uuid.uuid4())
        logger.info(f"创建比对任务 {comparison_id}")

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
            "message": "表比对任务已启动"
        }

    except HTTPException as he:
        # 直接重新抛出HTTP异常
        logger.error(f"HTTP异常: {he}", exc_info=True)
        raise
    except ValueError as ve:
        logger.error(f"比对请求参数错误: {ve}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        logger.error(f"表比对失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"表比对失败: {str(e)}"
        )


@app.post("/api/v1/compare/schemas")
async def compare_schemas_endpoint(request: SchemaComparisonRequest):
    """处理模式比对请求"""
    try:
        source_db_config = parse_connection_string(request.source_connection)
        target_db_config = parse_connection_string(request.target_connection)

        # 假设 comparison_engine 有一个 compare_schemas 方法
        # 如果没有，需要先在 comparison_engine.py 中实现
        result = await comparison_engine.compare_schemas(
            source_config=source_db_config,
            target_config=target_db_config
        )

        return {
            "status": "completed",
            "result": result
        }
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except Exception as e:
        logger.error(f"Schema comparison failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"模式比对失败: {str(e)}"
        )


@app.get("/api/v1/templates")
async def list_workflow_templates():
    """获取工作流模板列表"""
    try:
        templates = template_manager.list_templates()
        template_details = []

        for template_name in templates:
            template = template_manager.get_template(template_name)
            template_details.append({
                "name": template_name,
                "description": template.get("description", ""),
                "parameters": template.get("parameters", [])
            })

        return {
            "templates": template_details,
            "count": len(template_details)
        }

    except Exception as e:
        logger.error(f"Failed to list templates: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取模板列表失败: {str(e)}"
        )


@app.post("/api/v1/workflows")
async def create_workflow(
    workflow_config: WorkflowConfig,
    source_db: DatabaseConfig,
    target_db: DatabaseConfig,
    comparison_config: ComparisonConfig
):
    """创建 N8N 工作流"""
    try:
        workflow_def = await workflow_builder.build_comparison_workflow(
            name=workflow_config.name,
            source_db=source_db.dict(by_alias=True),
            target_db=target_db.dict(by_alias=True),
            comparison_config=comparison_config.dict(by_alias=True),
            schedule=workflow_config.schedule,
            template=workflow_config.template
        )

        workflow_id = str(uuid.uuid4())

        # 保存工作流定义
        await workflow_builder.save_workflow(workflow_id, workflow_def)

        return {
            "workflow_id": workflow_id,
            "name": workflow_config.name,
            "status": "created",
            "workflow_definition": workflow_def
        }

    except Exception as e:
        logger.error(f"Workflow creation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"工作流创建失败: {str(e)}"
        )


@app.post("/api/v1/workflows/{workflow_id}/schedule")
async def schedule_workflow(
    workflow_id: str,
    schedule_config: ScheduleConfig
):
    """调度工作流"""
    try:
        job_info = await scheduler.schedule_workflow(
            workflow_id=workflow_id,
            schedule_config=schedule_config.dict(by_alias=True)
        )

        return {
            "workflow_id": workflow_id,
            "job_id": job_info.get("job_id"),
            "status": "scheduled",
            "next_run": job_info.get("next_run"),
            "schedule": schedule_config.expression
        }

    except Exception as e:
        logger.error(f"Workflow scheduling failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"工作流调度失败: {str(e)}"
        )


@app.get("/api/v1/workflows/{workflow_id}/status")
async def get_workflow_status(workflow_id: str):
    """获取工作流状态"""
    try:
        status_info = await scheduler.get_workflow_status(workflow_id)

        return {
            "workflow_id": workflow_id,
            "status": status_info.get("status", "unknown"),
            "last_run": status_info.get("last_run"),
            "next_run": status_info.get("next_run"),
            "run_count": status_info.get("run_count", 0)
        }

    except Exception as e:
        logger.error(f"Failed to get workflow status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取工作流状态失败: {str(e)}"
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
            "workflows": {
                "total": 0,
                "active": 0,
                "scheduled": 0
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


@app.post("/api/v1/tables/list")
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
            raise HTTPException(status_code=400, detail=f"不支持的数据库类型: {db_type}")

        return {
            "success": True,
            "tables": tables,
            "schema": target_schema,
            "database_type": db_type,
            "count": len(tables)
        }

    except Exception as e:
        logger.error(f"{db_type} list tables failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"{db_type} 获取表列表失败: {str(e)}"
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
        global comparison_engine
        logger.info(f"Starting async comparison {comparison_id}")

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
            "timestamp": datetime.utcnow().isoformat()
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
    await scheduler.initialize()

    logger.info("Data-Diff N8N API started successfully")


# 关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时的清理"""
    logger.info("Data-Diff N8N API shutting down...")

    # 清理资源
    await connection_manager.cleanup()
    await scheduler.shutdown()

    logger.info("Data-Diff N8N API shutdown complete")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
@app.post("/api/v1/compare/tables/query", response_model=Dict[str, Any])
@app.get("/api/v1/compare/tables/query", response_model=Dict[str, Any])
async def compare_tables_query_endpoint(request: Request, background_tasks: BackgroundTasks):
    """处理表比对查询请求 - 专为 n8n 适配的路由"""
    # 直接调用主要端点的处理逻辑
    return await compare_tables_endpoint(request, background_tasks)
