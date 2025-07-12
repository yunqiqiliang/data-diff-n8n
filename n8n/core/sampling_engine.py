"""
采样引擎
实现统计学意义的数据采样，支持置信度和容差配置
"""

import logging
import math
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


@dataclass
class SamplingConfig:
    """采样配置"""
    enabled: bool = True
    confidence_level: float = 0.95  # 置信水平 (95%)
    margin_of_error: float = 0.01   # 误差容限 (1%)
    min_sample_size: int = 1000     # 最小采样数
    max_sample_size: int = 1000000  # 最大采样数
    auto_sample_threshold: int = 100000  # 自动采样阈值


class SamplingEngine:
    """
    统计学采样引擎
    根据置信度和容差计算最优采样大小
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def calculate_sample_size(
        self,
        population_size: int,
        confidence_level: float = 0.95,
        margin_of_error: float = 0.01,
        expected_proportion: float = 0.5
    ) -> int:
        """
        计算所需的采样大小
        
        使用公式：
        n = (Z²×p×(1-p)) / E²
        
        其中：
        - Z = Z分数（基于置信水平）
        - p = 预期比例（默认0.5表示最大变异性）
        - E = 误差容限
        
        对于有限总体，使用有限总体校正：
        n_adjusted = n / (1 + (n-1)/N)
        """
        # 获取Z分数
        z_score = self._get_z_score(confidence_level)
        
        # 计算初始样本大小（无限总体）
        n = (z_score**2 * expected_proportion * (1 - expected_proportion)) / (margin_of_error**2)
        n = math.ceil(n)
        
        # 应用有限总体校正
        if population_size > 0:
            n_adjusted = n / (1 + (n - 1) / population_size)
            n_adjusted = math.ceil(n_adjusted)
        else:
            n_adjusted = n
        
        self.logger.info(
            f"Calculated sample size: {n_adjusted} "
            f"(population: {population_size}, confidence: {confidence_level}, "
            f"margin: {margin_of_error})"
        )
        
        return n_adjusted
    
    def _get_z_score(self, confidence_level: float) -> float:
        """获取给定置信水平的Z分数"""
        # 常用置信水平的Z分数
        z_scores = {
            0.90: 1.645,
            0.95: 1.96,
            0.99: 2.576,
            0.995: 2.807,
            0.999: 3.291
        }
        
        if confidence_level in z_scores:
            return z_scores[confidence_level]
        
        # 对于其他置信水平，使用scipy计算
        alpha = 1 - confidence_level
        z_score = stats.norm.ppf(1 - alpha/2)
        return z_score
    
    def should_use_sampling(
        self,
        row_count: int,
        config: SamplingConfig
    ) -> Tuple[bool, Optional[int]]:
        """
        决定是否应该使用采样，以及采样大小
        
        返回：(是否采样, 采样大小)
        """
        if not config.enabled:
            return False, None
        
        if row_count <= config.auto_sample_threshold:
            self.logger.info(
                f"Row count ({row_count}) below threshold ({config.auto_sample_threshold}), "
                f"sampling disabled"
            )
            return False, None
        
        # 计算采样大小
        sample_size = self.calculate_sample_size(
            population_size=row_count,
            confidence_level=config.confidence_level,
            margin_of_error=config.margin_of_error
        )
        
        # 应用最小和最大限制
        sample_size = max(config.min_sample_size, sample_size)
        sample_size = min(config.max_sample_size, sample_size)
        
        # 如果采样大小接近总体大小，不采样
        if sample_size >= row_count * 0.8:
            self.logger.info(
                f"Sample size ({sample_size}) too close to population ({row_count}), "
                f"sampling disabled"
            )
            return False, None
        
        self.logger.info(
            f"Sampling enabled: {sample_size} rows from {row_count} "
            f"({sample_size/row_count*100:.1f}%)"
        )
        return True, sample_size
    
    def generate_sampling_sql(
        self,
        table_name: str,
        sample_size: int,
        total_rows: int,
        database_type: str,
        key_column: Optional[str] = None,
        sampling_method: str = "SYSTEM"  # ROW 或 SYSTEM
    ) -> str:
        """
        生成不同数据库的采样SQL
        
        Args:
            table_name: 表名
            sample_size: 采样大小
            total_rows: 总行数
            database_type: 数据库类型
            key_column: 主键列名（可选）
            sampling_method: 采样方法，ROW（行级）或 SYSTEM（文件级/块级）
        """
        sample_ratio = sample_size / total_rows
        
        if database_type.lower() in ['postgresql', 'postgres']:
            # PostgreSQL: 使用 TABLESAMPLE
            return f"""
                SELECT * FROM {table_name}
                TABLESAMPLE BERNOULLI ({sample_ratio * 100})
                REPEATABLE (42)
            """
        
        elif database_type.lower() == 'mysql':
            # MySQL: 使用 RAND() 和 LIMIT
            return f"""
                SELECT * FROM {table_name}
                WHERE RAND() <= {sample_ratio}
                LIMIT {sample_size}
            """
        
        elif database_type.lower() in ['clickhouse', 'clickzetta']:
            # ClickZetta: 使用 TABLESAMPLE，支持 ROW/SYSTEM 和 百分比/行数
            method = sampling_method if sampling_method in ['ROW', 'SYSTEM'] else ''
            
            if sample_ratio < 0.01:  # 小于1%时使用行数模式
                return f"""
                    SELECT * FROM {table_name}
                    TABLESAMPLE {method} ({sample_size} ROWS)
                """.strip()
            else:
                # 使用百分比模式（0-100），不需要百分号
                return f"""
                    SELECT * FROM {table_name}
                    TABLESAMPLE {method} ({sample_ratio * 100})
                """.strip()
        
        elif database_type.lower() in ['sqlserver', 'mssql']:
            # SQL Server: 使用 TABLESAMPLE
            return f"""
                SELECT * FROM {table_name}
                TABLESAMPLE ({sample_size} ROWS)
            """
        
        elif database_type.lower() == 'oracle':
            # Oracle: 使用 SAMPLE
            return f"""
                SELECT * FROM {table_name}
                SAMPLE({sample_ratio * 100})
            """
        
        elif database_type.lower() in ['trino', 'presto']:
            # Trino/Presto: 使用 TABLESAMPLE
            return f"""
                SELECT * FROM {table_name}
                TABLESAMPLE BERNOULLI ({sample_ratio * 100})
            """
        
        else:
            # 默认：使用通用的随机采样（可能效率较低）
            if key_column:
                return f"""
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (ORDER BY {key_column}) as rn
                        FROM {table_name}
                    ) t
                    WHERE MOD(rn, {int(1/sample_ratio)}) = 0
                    LIMIT {sample_size}
                """
            else:
                # 没有主键，使用简单的 LIMIT
                return f"""
                    SELECT * FROM {table_name}
                    LIMIT {sample_size}
                """
    
    def calculate_sampling_statistics(
        self,
        sample_differences: int,
        sample_size: int,
        confidence_level: float = 0.95
    ) -> Dict[str, Any]:
        """
        计算采样结果的统计信息
        
        返回估计的总体差异数和置信区间
        """
        # 样本差异比例
        sample_proportion = sample_differences / sample_size if sample_size > 0 else 0
        
        # 计算标准误差
        if sample_size > 1:
            standard_error = math.sqrt(
                (sample_proportion * (1 - sample_proportion)) / sample_size
            )
        else:
            standard_error = 0
        
        # 获取Z分数
        z_score = self._get_z_score(confidence_level)
        
        # 计算置信区间
        margin_of_error = z_score * standard_error
        confidence_interval = (
            max(0, sample_proportion - margin_of_error),
            min(1, sample_proportion + margin_of_error)
        )
        
        return {
            "sample_proportion": sample_proportion,
            "standard_error": standard_error,
            "margin_of_error": margin_of_error,
            "confidence_interval": confidence_interval,
            "confidence_level": confidence_level
        }
    
    def extrapolate_to_population(
        self,
        sample_differences: int,
        sample_size: int,
        population_size: int,
        confidence_level: float = 0.95
    ) -> Dict[str, Any]:
        """
        从样本推断到总体
        """
        stats = self.calculate_sampling_statistics(
            sample_differences,
            sample_size,
            confidence_level
        )
        
        # 估计总体差异数
        estimated_differences = int(stats["sample_proportion"] * population_size)
        
        # 估计置信区间
        lower_bound = int(stats["confidence_interval"][0] * population_size)
        upper_bound = int(stats["confidence_interval"][1] * population_size)
        
        return {
            "estimated_differences": estimated_differences,
            "confidence_interval": (lower_bound, upper_bound),
            "confidence_level": confidence_level,
            "sample_statistics": stats,
            "extrapolation_factor": population_size / sample_size if sample_size > 0 else 0
        }