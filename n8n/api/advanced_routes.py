"""
高级 API 端点 - 扩展功能
"""

from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import asyncio
import logging
from datetime import datetime
import json

# 从 utils.py 导入 parse_connection_string 函数
from .utils import parse_connection_string

from ..core import (
    ConnectionManager,
    ComparisonEngine,
    ClickzettaAdapter,
    ResultProcessor,
    ConfigManager,
    ErrorHandler,
    WorkflowScheduler
)

# 创建配置管理器实例
config_manager = ConfigManager()

router = APIRouter(prefix="/api/v1/advanced", tags=["advanced"])
logger = logging.getLogger(__name__)


class ResultFormatRequest(BaseModel):
    """结果格式化请求"""
    data: Dict[str, Any] = Field(..., description="原始比对数据")
    format: str = Field("json", description="输出格式")
    include_details: bool = Field(True, description="是否包含详细信息")
    options: Optional[Dict[str, Any]] = Field({}, description="额外选项")


class NotificationConfig(BaseModel):
    """通知配置"""
    type: str = Field(..., description="通知类型")
    recipients: List[str] = Field(..., description="接收者列表")
    template: Optional[str] = Field(None, description="消息模板")
    conditions: List[str] = Field(["error"], description="触发条件")


class BatchComparisonRequest(BaseModel):
    """批量比对请求"""
    comparisons: List[Dict[str, Any]] = Field(..., description="比对配置列表")
    parallel_limit: int = Field(5, description="并行数量限制")
    timeout: int = Field(3600, description="超时时间(秒)")


@router.post("/results/format")
async def format_results(request: ResultFormatRequest):
    """格式化比对结果"""
    try:
        # 获取结果处理器实例（需要依赖注入）
        result_processor = ResultProcessor()

        formatted_result = await result_processor.format_result(
            data=request.data,
            format_type=request.format,
            include_details=request.include_details,
            options=request.options
        )

        return {
            "formatted_result": formatted_result,
            "format": request.format,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Result formatting failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"结果格式化失败: {str(e)}"
        )


@router.post("/results/summary")
async def extract_summary(data: Dict[str, Any], fields: List[str]):
    """提取结果摘要"""
    try:
        result_processor = ResultProcessor()

        summary = await result_processor.extract_summary(
            data=data,
            fields=fields
        )

        return {
            "summary": summary,
            "fields": fields,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Summary extraction failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"摘要提取失败: {str(e)}"
        )


@router.post("/results/filter")
async def filter_differences(data: Dict[str, Any], difference_types: List[str]):
    """过滤差异类型"""
    try:
        result_processor = ResultProcessor()

        filtered_result = await result_processor.filter_differences(
            data=data,
            difference_types=difference_types
        )

        return {
            "filtered_result": filtered_result,
            "difference_types": difference_types,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Difference filtering failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"差异过滤失败: {str(e)}"
        )


@router.post("/results/export")
async def export_results(
    data: Dict[str, Any],
    format: str,
    file_path: str,
    options: Optional[Dict[str, Any]] = None
):
    """导出结果到文件"""
    try:
        result_processor = ResultProcessor()

        export_result = await result_processor.export_to_file(
            data=data,
            format_type=format,
            file_path=file_path,
            options=options or {}
        )

        return {
            "export_result": export_result,
            "file_path": file_path,
            "format": format,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Result export failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"结果导出失败: {str(e)}"
        )


@router.post("/notifications/send")
async def send_notification(data: Dict[str, Any], config: NotificationConfig):
    """发送通知"""
    try:
        # 实现通知发送逻辑
        notification_result = await _send_notification_internal(data, config)

        return {
            "notification_result": notification_result,
            "recipients": config.recipients,
            "type": config.type,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Notification sending failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"通知发送失败: {str(e)}"
        )


@router.post("/scheduler/register")
async def register_schedule(schedule_data: Dict[str, Any]):
    """注册调度任务"""
    try:
        scheduler = WorkflowScheduler()

        scheduler_id = await scheduler.register_schedule(schedule_data)
        next_execution = await scheduler.get_next_execution(scheduler_id)

        return {
            "scheduler_id": scheduler_id,
            "status": "registered",
            "next_execution": next_execution,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Schedule registration failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"调度注册失败: {str(e)}"
        )


@router.post("/compare/tables")
async def compare_tables_advanced(
    source_connection: str,
    target_connection: str,
    source_table: str,
    target_table: str,
    primary_key_columns: List[str],
    update_key_columns: Optional[List[str]] = None,
    where_condition: Optional[str] = None,
    columns_to_compare: Optional[List[str]] = None,
    operation_type: str = "compareTables",
    **kwargs
):
    """高级表比对"""
    try:
        comparison_engine = ComparisonEngine(config_manager)

        comparison_config = {
            "source_connection": source_connection,
            "target_connection": target_connection,
            "source_table": source_table,
            "target_table": target_table,
            "key_columns": primary_key_columns,  # 使用 key_columns 而不是 primary_key_columns
            "update_key_columns": update_key_columns or [],
            "where_condition": where_condition,
            "columns_to_compare": columns_to_compare,
            "operation_type": operation_type,
            **kwargs
        }

        # 解析连接字符串
        source_db_config = parse_connection_string(source_connection)
        target_db_config = parse_connection_string(target_connection)

        # 执行比对
        result = await comparison_engine.compare_tables(
            source_config=source_db_config,
            target_config=target_db_config,
            comparison_config=comparison_config
        )

        return {
            "comparison_result": result,
            "operation_type": operation_type,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Advanced table comparison failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"高级表比对失败: {str(e)}"
        )


@router.post("/compare/schemas")
async def compare_schemas(
    source_connection: str,
    target_connection: str,
    operation_type: str = "compareSchemas"
):
    """比对数据库模式"""
    try:
        comparison_engine = ComparisonEngine(config_manager)

        comparison_config = {
            "source_connection": source_connection,
            "target_connection": target_connection,
            "operation_type": operation_type
        }

        result = await comparison_engine.compare_schemas(comparison_config)

        return {
            "schema_comparison_result": result,
            "operation_type": operation_type,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Schema comparison failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"模式比对失败: {str(e)}"
        )


@router.post("/comparisons/batch")
async def batch_comparison(request: BatchComparisonRequest):
    """批量数据比对"""
    try:
        comparison_engine = ComparisonEngine(config_manager)

        # 并行执行比对
        semaphore = asyncio.Semaphore(request.parallel_limit)

        async def run_single_comparison(comparison_config):
            async with semaphore:
                # 解析连接字符串
                source_db_config = parse_connection_string(comparison_config["source_connection"])
                target_db_config = parse_connection_string(comparison_config["target_connection"])

                # 执行比对
                return await comparison_engine.compare_tables(
                    source_config=source_db_config,
                    target_config=target_db_config,
                    comparison_config=comparison_config
                )

        # 创建任务列表
        tasks = [
            run_single_comparison(config)
            for config in request.comparisons
        ]

        # 等待所有任务完成（带超时）
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=request.timeout
        )

        # 处理结果
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "index": i,
                    "status": "failed",
                    "error": str(result)
                })
            else:
                processed_results.append({
                    "index": i,
                    "status": "success",
                    "result": result
                })

        return {
            "batch_results": processed_results,
            "total_comparisons": len(request.comparisons),
            "successful": sum(1 for r in processed_results if r["status"] == "success"),
            "failed": sum(1 for r in processed_results if r["status"] == "failed"),
            "timestamp": datetime.utcnow().isoformat()
        }

    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=f"批量比对超时，限制时间: {request.timeout} 秒"
        )
    except Exception as e:
        logger.error(f"Batch comparison failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"批量比对失败: {str(e)}"
        )


@router.get("/databases/supported")
async def get_supported_databases():
    """获取支持的数据库类型 - 从数据库注册表动态获取"""
    try:
        from ..core.database_registry import database_registry

        databases = []
        for db_type in database_registry.get_supported_databases():
            db_config = database_registry.get_database_config(db_type)

            # 构建数据库信息
            db_info = {
                "type": db_type,
                "name": db_type.title(),
                "description": f"{db_type.title()} database",
                "connection_help": db_config.connect_uri_help,
                "connection_params": db_config.connect_uri_params,
                "default_port": db_config.default_port,
                "default_schema": db_config.default_schema,
                "supports_unique_constraint": db_config.supports_unique_constraint,
                "supports_alphanums": db_config.supports_alphanums,
                "threading_model": db_config.threading_model
            }

            # 添加可选参数
            if db_config.connect_uri_kwparams:
                db_info["optional_params"] = db_config.connect_uri_kwparams

            if db_config.extra_params:
                db_info["extra_params"] = db_config.extra_params

            databases.append(db_info)

        return {
            "databases": databases,
            "total": len(databases)
        }

    except Exception as e:
        logger.error(f"Failed to get supported databases: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取支持的数据库失败: {str(e)}"
        )


# 内部辅助函数
async def _send_notification_internal(data: Dict[str, Any], config: NotificationConfig):
    """内部通知发送实现"""
    # 这里实现具体的通知发送逻辑
    # 支持 email, slack, webhook, teams 等

    notification_result = {
        "status": "sent",
        "type": config.type,
        "recipients_count": len(config.recipients),
        "message_id": f"msg_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    }

    # 模拟发送延迟
    await asyncio.sleep(0.1)

    return notification_result
