"""
历史记录相关的API路由
提供查询比对历史和详细结果的接口
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from .response_models import BaseResponse, DataResponse
from ..core.result_materializer import ResultMaterializer

logger = logging.getLogger(__name__)

# 创建路由器
router = APIRouter(prefix="/api/v1/history", tags=["history"])

# 初始化结果物化器
try:
    db_config = {
        'host': 'postgres',
        'port': 5432,
        'database': 'datadiff',
        'user': 'postgres',
        'password': 'password'
    }
    result_materializer = ResultMaterializer(db_config)
except Exception as e:
    logger.error(f"Failed to initialize result materializer: {e}")
    result_materializer = None


@router.get("/comparisons", response_model=DataResponse)
async def get_comparison_history(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results to return"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    source_table: Optional[str] = Query(None, description="Filter by source table name"),
    target_table: Optional[str] = Query(None, description="Filter by target table name"),
    start_date: Optional[datetime] = Query(None, description="Filter by start date (inclusive)"),
    end_date: Optional[datetime] = Query(None, description="Filter by end date (inclusive)"),
):
    """
    获取比对历史记录
    
    Args:
        limit: 返回结果数量限制
        offset: 跳过的结果数量
        source_table: 源表名过滤
        target_table: 目标表名过滤
        start_date: 开始日期过滤
        end_date: 结束日期过滤
        
    Returns:
        比对历史列表
    """
    if not result_materializer:
        raise HTTPException(
            status_code=503,
            detail="Result materialization service is not available"
        )
    
    try:
        # 构建过滤条件
        filters = {}
        if source_table:
            filters['source_table'] = source_table
        if target_table:
            filters['target_table'] = target_table
        if start_date:
            filters['start_date'] = start_date
        if end_date:
            filters['end_date'] = end_date
            
        # 获取历史记录
        history = result_materializer.get_comparison_history(
            limit=limit,
            offset=offset,
            filters=filters
        )
        
        # 转换日期时间为字符串（用于JSON序列化）
        for record in history:
            for key in ['start_time', 'end_time', 'created_at']:
                if key in record and record[key]:
                    record[key] = record[key].isoformat()
                    
        return DataResponse(
            success=True,
            data={
                "comparisons": history,
                "pagination": {
                    "limit": limit,
                    "offset": offset,
                    "total": len(history)  # TODO: 实现总数查询
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get comparison history: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve comparison history: {str(e)}"
        )


@router.get("/comparisons/{comparison_id}", response_model=DataResponse)
async def get_comparison_details(comparison_id: str):
    """
    获取比对详细信息
    
    Args:
        comparison_id: 比对ID
        
    Returns:
        比对详细信息，包括差异详情、列统计等
    """
    if not result_materializer:
        raise HTTPException(
            status_code=503,
            detail="Result materialization service is not available"
        )
    
    try:
        # 获取详细信息
        details = result_materializer.get_comparison_details(comparison_id)
        
        if not details:
            raise HTTPException(
                status_code=404,
                detail=f"Comparison {comparison_id} not found"
            )
        
        # 转换日期时间为字符串
        for section in ['summary', 'differences', 'column_stats', 'performance_metrics']:
            if section in details and details[section]:
                if isinstance(details[section], dict):
                    for key in ['start_time', 'end_time', 'created_at', 'recorded_at']:
                        if key in details[section] and details[section][key]:
                            details[section][key] = details[section][key].isoformat()
                elif isinstance(details[section], list):
                    for record in details[section]:
                        for key in ['created_at', 'recorded_at']:
                            if key in record and record[key]:
                                record[key] = record[key].isoformat()
        
        return DataResponse(
            success=True,
            data=details
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get comparison details: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve comparison details: {str(e)}"
        )


@router.get("/statistics/summary", response_model=DataResponse)
async def get_comparison_statistics(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in statistics"),
):
    """
    获取比对统计摘要
    
    Args:
        days: 统计天数
        
    Returns:
        统计信息，包括每日比对数量、平均执行时间等
    """
    if not result_materializer:
        raise HTTPException(
            status_code=503,
            detail="Result materialization service is not available"
        )
    
    try:
        # 使用视图查询统计信息
        # TODO: 实现统计查询
        return DataResponse(
            success=True,
            data={
                "period_days": days,
                "total_comparisons": 0,
                "avg_execution_time": 0,
                "avg_match_rate": 0,
                "total_rows_compared": 0,
                "message": "Statistics endpoint is under development"
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to get comparison statistics: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve comparison statistics: {str(e)}"
        )


@router.delete("/comparisons/{comparison_id}", response_model=BaseResponse)
async def delete_comparison_result(comparison_id: str):
    """
    删除比对结果（包括所有相关数据）
    
    Args:
        comparison_id: 比对ID
        
    Returns:
        删除成功响应
    """
    if not result_materializer:
        raise HTTPException(
            status_code=503,
            detail="Result materialization service is not available"
        )
    
    try:
        # TODO: 实现删除功能
        return BaseResponse(
            success=False,
            message="Delete functionality is not yet implemented"
        )
        
    except Exception as e:
        logger.error(f"Failed to delete comparison result: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete comparison result: {str(e)}"
        )