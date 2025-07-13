"""
Business metrics updater from statistics API
"""
import asyncio
import logging
import httpx
from .metrics import (
    comparison_differences, difference_rate, rows_compared,
    column_null_rate, data_quality_score, schema_differences,
    comparison_total
)

logger = logging.getLogger(__name__)

async def update_business_metrics_from_stats():
    """从统计API获取数据并更新业务指标"""
    try:
        logger.info("开始从统计API更新业务指标")
        
        # 直接调用数据库更新函数
        from .metrics import update_business_metrics_from_db
        await update_business_metrics_from_db()
        return
        
        # 下面是备用的HTTP调用方式
        async with httpx.AsyncClient() as client:
            response = await client.get("http://localhost:8000/api/v1/maintenance/statistics")
            
            if response.status_code != 200:
                logger.warning(f"获取统计数据失败: {response.status_code}")
                return
                
            data = response.json()
            
            if not data.get('success'):
                logger.warning("统计数据返回失败")
                return
                
            statistics = data.get('statistics', {})
            
            # 更新比对总数指标
            table_completed = statistics.get('table_comparisons_completed', 0)
            table_failed = statistics.get('table_comparisons_failed', 0)
            schema_completed = statistics.get('schema_comparisons_completed', 0)
            schema_failed = statistics.get('schema_comparisons_failed', 0)
            
            comparison_total.labels(comparison_type='table', status='completed').inc(table_completed)
            comparison_total.labels(comparison_type='table', status='failed').inc(table_failed)
            comparison_total.labels(comparison_type='schema', status='completed').inc(schema_completed)
            comparison_total.labels(comparison_type='schema', status='failed').inc(schema_failed)
            
            # 从最近的比对结果中获取详细指标
            # 这里可以扩展调用其他API获取更详细的数据
            
            logger.info(f"业务指标已更新: 表比对完成 {table_completed}, 失败 {table_failed}")
            
    except Exception as e:
        logger.error(f"更新业务指标失败: {e}")
        import traceback
        logger.error(traceback.format_exc())