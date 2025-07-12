"""
API响应模型定义
统一的响应格式和错误处理
"""

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime


class BaseResponse(BaseModel):
    """基础响应模型"""
    success: bool = Field(..., description="请求是否成功")
    message: Optional[str] = Field(None, description="响应消息")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat(), description="响应时间戳")


class ErrorDetail(BaseModel):
    """错误详情模型"""
    code: str = Field(..., description="错误代码")
    message: str = Field(..., description="错误消息")
    details: Optional[Dict[str, Any]] = Field(None, description="额外的错误详情")


class ErrorResponse(BaseResponse):
    """错误响应模型"""
    success: bool = Field(default=False)
    error: ErrorDetail = Field(..., description="错误信息")


class DataResponse(BaseResponse):
    """数据响应模型"""
    success: bool = Field(default=True)
    data: Dict[str, Any] = Field(..., description="响应数据")


class ConnectionTestResponse(BaseResponse):
    """连接测试响应"""
    success: bool = Field(default=True)
    database_type: str = Field(..., description="数据库类型")


class ComparisonStartResponse(BaseResponse):
    """比对任务启动响应"""
    success: bool = Field(default=True)
    comparison_id: str = Field(..., description="比对任务ID")
    status: str = Field(..., description="任务状态")
    async_mode: bool = Field(default=True, description="是否异步模式")


class ComparisonResultResponse(BaseResponse):
    """比对结果响应"""
    comparison_id: str = Field(..., description="比对任务ID")
    status: str = Field(..., description="任务状态")
    progress: Optional[int] = Field(None, description="任务进度百分比")
    current_step: Optional[str] = Field(None, description="当前执行步骤")
    estimated_time: Optional[str] = Field(None, description="预计剩余时间")
    result: Optional[Dict[str, Any]] = Field(None, description="比对结果")


class TableListResponse(BaseResponse):
    """表列表响应"""
    success: bool = Field(default=True)
    database_type: str = Field(..., description="数据库类型")
    database: str = Field(..., description="数据库名称")
    db_schema: Optional[str] = Field(None, alias="schema", description="模式名称")
    tables: List[str] = Field(..., description="表名列表")
    count: int = Field(..., description="表数量")


class QueryExecutionResponse(BaseResponse):
    """查询执行响应"""
    success: bool = Field(default=True)
    query: str = Field(..., description="执行的查询")
    database_type: str = Field(..., description="数据库类型")
    row_count: int = Field(..., description="返回行数")
    result: List[Dict[str, Any]] = Field(..., description="查询结果")
    execution_time: Optional[float] = Field(None, description="执行时间（秒）")


class HealthCheckResponse(BaseResponse):
    """健康检查响应"""
    status: str = Field(..., description="服务状态")
    version: str = Field(..., description="API版本")
    components: Optional[Dict[str, Dict[str, Any]]] = Field(None, description="组件状态")


# 错误代码常量
class ErrorCodes:
    """统一的错误代码"""
    # 客户端错误 (4xx)
    INVALID_REQUEST = "INVALID_REQUEST"
    MISSING_PARAMETER = "MISSING_PARAMETER"
    INVALID_PARAMETER = "INVALID_PARAMETER"
    NOT_FOUND = "NOT_FOUND"
    UNAUTHORIZED = "UNAUTHORIZED"
    
    # 服务端错误 (5xx)
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    CONNECTION_ERROR = "CONNECTION_ERROR"
    COMPARISON_ERROR = "COMPARISON_ERROR"
    
    # 业务错误
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    TASK_FAILED = "TASK_FAILED"
    UNSUPPORTED_DATABASE = "UNSUPPORTED_DATABASE"