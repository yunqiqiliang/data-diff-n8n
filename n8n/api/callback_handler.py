"""
回调处理模块
负责处理异步比对的回调通知
"""

import aiohttp
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


async def send_callback_notification(
    url: str,
    headers: Optional[Dict[str, str]],
    data: Dict[str, Any]
) -> bool:
    """
    发送回调通知
    
    Args:
        url: 回调URL
        headers: 请求头
        data: 回调数据
        
    Returns:
        是否发送成功
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=data,
                headers=headers or {},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                logger.info(f"Callback sent to {url}, status: {response.status}")
                return response.status < 300
    except aiohttp.ClientError as e:
        logger.error(f"HTTP error sending callback to {url}: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to send callback to {url}: {e}")
        return False


async def send_progress_callback(
    url: str,
    headers: Optional[Dict[str, str]],
    comparison_id: str,
    progress: int,
    current_step: str,
    estimated_time: Optional[str] = None
) -> bool:
    """
    发送进度回调通知
    
    Args:
        url: 回调URL
        headers: 请求头
        comparison_id: 比对ID
        progress: 进度百分比
        current_step: 当前步骤
        estimated_time: 预计剩余时间
        
    Returns:
        是否发送成功
    """
    data = {
        'comparison_id': comparison_id,
        'status': 'running',
        'progress': progress,
        'current_step': current_step
    }
    
    if estimated_time:
        data['estimated_time'] = estimated_time
        
    return await send_callback_notification(url, headers, data)