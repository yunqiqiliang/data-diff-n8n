"""
内存优化工具模块
提供内存使用监控和优化功能
"""

import os
import gc
import psutil
import logging
import functools
from typing import Dict, Any, Iterator, List, Optional, Callable
from datetime import datetime
import tracemalloc

logger = logging.getLogger(__name__)


class MemoryMonitor:
    """内存使用监控器"""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.start_memory = None
        self.peak_memory = 0
        self.monitoring = False
        
    def start_monitoring(self):
        """开始监控内存"""
        self.start_memory = self.get_memory_usage()
        self.peak_memory = self.start_memory
        self.monitoring = True
        tracemalloc.start()
        logger.info(f"Memory monitoring started. Initial usage: {self.format_bytes(self.start_memory)}")
        
    def stop_monitoring(self) -> Dict[str, Any]:
        """停止监控并返回统计信息"""
        if not self.monitoring:
            return {}
            
        current_memory = self.get_memory_usage()
        memory_stats = {
            'start_memory_mb': round(self.start_memory / 1024 / 1024, 2),
            'end_memory_mb': round(current_memory / 1024 / 1024, 2),
            'peak_memory_mb': round(self.peak_memory / 1024 / 1024, 2),
            'memory_increase_mb': round((current_memory - self.start_memory) / 1024 / 1024, 2)
        }
        
        # 获取 tracemalloc 统计
        if tracemalloc.is_tracing():
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')[:10]
            memory_stats['top_allocations'] = [
                {
                    'file': stat.traceback.format()[0],
                    'size_mb': round(stat.size / 1024 / 1024, 2)
                }
                for stat in top_stats
            ]
            tracemalloc.stop()
            
        self.monitoring = False
        logger.info(f"Memory monitoring stopped. Peak usage: {self.format_bytes(self.peak_memory)}")
        
        return memory_stats
        
    def get_memory_usage(self) -> int:
        """获取当前内存使用量（字节）"""
        return self.process.memory_info().rss
        
    def update_peak(self):
        """更新峰值内存使用量"""
        if self.monitoring:
            current = self.get_memory_usage()
            if current > self.peak_memory:
                self.peak_memory = current
                
    @staticmethod
    def format_bytes(bytes_value: int) -> str:
        """格式化字节数为人类可读格式"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} TB"
        
    def check_memory_limit(self, limit_mb: int = 1024) -> bool:
        """检查是否超过内存限制"""
        current_mb = self.get_memory_usage() / 1024 / 1024
        if current_mb > limit_mb:
            logger.warning(f"Memory usage ({current_mb:.2f} MB) exceeds limit ({limit_mb} MB)")
            return False
        return True


class StreamingDifferenceProcessor:
    """流式处理差异数据，避免一次性加载所有数据到内存"""
    
    def __init__(self, max_samples: int = 10, max_buffer_size: int = 1000):
        self.max_samples = max_samples
        self.max_buffer_size = max_buffer_size
        self.memory_monitor = MemoryMonitor()
        
    def process_differences(
        self, 
        diff_iterator: Iterator,
        process_func: Optional[Callable] = None,
        collect_samples: bool = True,
        collect_stats: bool = True
    ) -> Dict[str, Any]:
        """
        流式处理差异数据
        
        Args:
            diff_iterator: 差异数据迭代器
            process_func: 对每个差异的处理函数
            collect_samples: 是否收集样本
            collect_stats: 是否收集统计信息
            
        Returns:
            处理结果，包含统计信息和样本
        """
        self.memory_monitor.start_monitoring()
        
        # 初始化结果
        result = {
            'total_differences': 0,
            'missing_in_target': 0,
            'missing_in_source': 0,
            'value_differences': 0,
            'samples': [],
            'memory_stats': {}
        }
        
        # 处理缓冲区
        buffer = []
        processed_count = 0
        
        try:
            for diff in diff_iterator:
                # 更新内存峰值
                self.memory_monitor.update_peak()
                
                # 检查内存限制
                if processed_count % 100 == 0:  # 每100条检查一次
                    if not self.memory_monitor.check_memory_limit():
                        logger.warning("Memory limit exceeded, stopping difference processing")
                        break
                
                # 收集统计信息
                if collect_stats:
                    result['total_differences'] += 1
                    diff_type = self._get_diff_type(diff)
                    if diff_type == 'missing_in_b':
                        result['missing_in_target'] += 1
                    elif diff_type == 'missing_in_a':
                        result['missing_in_source'] += 1
                    else:
                        result['value_differences'] += 1
                
                # 收集样本
                if collect_samples and len(result['samples']) < self.max_samples:
                    result['samples'].append(self._sanitize_diff(diff))
                
                # 添加到缓冲区
                if process_func:
                    buffer.append(diff)
                    
                    # 批量处理缓冲区
                    if len(buffer) >= self.max_buffer_size:
                        process_func(buffer)
                        buffer = []  # 清空缓冲区
                        gc.collect()  # 强制垃圾回收
                
                processed_count += 1
                
                # 定期输出进度
                if processed_count % 10000 == 0:
                    logger.info(f"Processed {processed_count} differences, memory: {self.memory_monitor.format_bytes(self.memory_monitor.get_memory_usage())}")
            
            # 处理剩余的缓冲区数据
            if buffer and process_func:
                process_func(buffer)
                
        except Exception as e:
            logger.error(f"Error processing differences: {e}")
            result['error'] = str(e)
            
        finally:
            # 获取内存统计
            result['memory_stats'] = self.memory_monitor.stop_monitoring()
            result['processed_count'] = processed_count
            
        return result
    
    def _get_diff_type(self, diff) -> str:
        """获取差异类型"""
        if isinstance(diff, tuple) and len(diff) >= 2:
            diff_sign = diff[0]
            if diff_sign == ('+',):
                return 'missing_in_a'
            elif diff_sign == ('-',):
                return 'missing_in_b'
            else:
                return 'value_different'
        elif isinstance(diff, dict):
            return diff.get('diff_type', 'unknown')
        return 'unknown'
    
    def _sanitize_diff(self, diff) -> Dict[str, Any]:
        """清理差异数据，只保留必要信息避免内存占用"""
        if isinstance(diff, tuple) and len(diff) >= 2:
            diff_sign = diff[0]
            row_data = diff[1]
            
            # 只保留前几个字段的数据
            sanitized_row = {}
            if isinstance(row_data, dict):
                for i, (k, v) in enumerate(row_data.items()):
                    if i < 5:  # 只保留前5个字段
                        sanitized_row[k] = str(v)[:100]  # 限制字符串长度
                    else:
                        sanitized_row['...'] = '...'
                        break
            
            return {
                'type': self._get_diff_type(diff),
                'data': sanitized_row
            }
        
        elif isinstance(diff, dict):
            # 已经是字典格式，进行清理
            return {
                'type': diff.get('diff_type', 'unknown'),
                'key': diff.get('key', {}),
                'data': {k: str(v)[:100] for k, v in diff.get('a_values', {}).items()}
            }
        
        return {'type': 'unknown', 'data': str(diff)[:200]}


class ChunkedDataProcessor:
    """分块处理大数据集"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.memory_monitor = MemoryMonitor()
        
    def process_in_chunks(
        self,
        data_source: Any,
        process_func: Callable,
        query: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        分块处理数据
        
        Args:
            data_source: 数据源（如数据库连接）
            process_func: 处理函数
            query: 查询语句
            
        Returns:
            处理结果
        """
        self.memory_monitor.start_monitoring()
        
        result = {
            'chunks_processed': 0,
            'total_rows': 0,
            'memory_stats': {}
        }
        
        try:
            offset = 0
            while True:
                # 构建分页查询
                chunk_query = f"{query} LIMIT {self.chunk_size} OFFSET {offset}"
                
                # 获取数据块
                chunk_data = data_source.execute(chunk_query).fetchall()
                
                if not chunk_data:
                    break
                
                # 处理数据块
                process_result = process_func(chunk_data)
                
                # 更新统计
                result['chunks_processed'] += 1
                result['total_rows'] += len(chunk_data)
                
                # 清理内存
                del chunk_data
                gc.collect()
                
                # 检查内存
                if not self.memory_monitor.check_memory_limit():
                    logger.warning("Memory limit exceeded in chunk processing")
                    break
                
                offset += self.chunk_size
                
                # 输出进度
                if result['chunks_processed'] % 10 == 0:
                    logger.info(f"Processed {result['chunks_processed']} chunks, {result['total_rows']} rows")
                    
        except Exception as e:
            logger.error(f"Error in chunk processing: {e}")
            result['error'] = str(e)
            
        finally:
            result['memory_stats'] = self.memory_monitor.stop_monitoring()
            
        return result


def memory_efficient_compare(func):
    """装饰器：为比对函数添加内存优化"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # 启用垃圾回收优化
        gc.enable()
        gc.set_threshold(700, 10, 10)  # 更激进的垃圾回收
        
        # 创建内存监控器
        monitor = MemoryMonitor()
        monitor.start_monitoring()
        
        try:
            # 执行原函数
            result = func(*args, **kwargs)
            
            # 添加内存统计到结果
            if isinstance(result, dict):
                result['memory_stats'] = monitor.stop_monitoring()
                
            return result
            
        finally:
            # 强制垃圾回收
            gc.collect()
            
    return wrapper


# 全局内存监控实例
global_memory_monitor = MemoryMonitor()