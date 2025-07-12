"""
浮点数容差比较支持
为 data-diff 添加浮点数容差比较功能
"""

from typing import Optional, Dict, Any
from data_diff.abcs.database_types import ColType, Float, Decimal, FractionalType


class FloatToleranceManager:
    """管理浮点数容差比较逻辑"""
    
    def __init__(self, tolerance: Optional[float] = None):
        """
        初始化浮点数容差管理器
        
        Args:
            tolerance: 浮点数比较的容差值，None 表示精确比较
        """
        self.tolerance = tolerance or 0.0
        self.enabled = tolerance is not None and tolerance > 0
    
    def should_use_tolerance(self, coltype: ColType) -> bool:
        """
        判断某个列类型是否应该使用容差比较
        
        Args:
            coltype: 列类型
            
        Returns:
            是否应该使用容差比较
        """
        if not self.enabled:
            return False
            
        # 只对浮点数和小数类型使用容差
        return isinstance(coltype, (Float, Decimal, FractionalType))
    
    def generate_comparison_sql(self, col1: str, col2: str, coltype: ColType, dialect: Any) -> str:
        """
        生成容差比较的 SQL 表达式
        
        Args:
            col1: 第一个列的 SQL 表达式
            col2: 第二个列的 SQL 表达式
            coltype: 列类型
            dialect: 数据库方言
            
        Returns:
            SQL 表达式，返回布尔值表示是否不同
        """
        if not self.should_use_tolerance(coltype):
            # 不使用容差，返回标准的不等比较
            return dialect.is_distinct_from(col1, col2)
        
        # 生成容差比较 SQL
        # ABS(col1 - col2) > tolerance
        abs_diff = self._generate_abs_diff(col1, col2, dialect)
        
        # 处理 NULL 值情况
        # 如果任一值为 NULL，使用标准的 is_distinct_from
        null_check = f"({col1} IS NULL OR {col2} IS NULL)"
        
        # 完整的比较逻辑：
        # 1. 如果任一值为 NULL，使用标准比较
        # 2. 否则，检查绝对差值是否大于容差
        return f"""
        CASE 
            WHEN {null_check} THEN {dialect.is_distinct_from(col1, col2)}
            ELSE {abs_diff} > {self.tolerance}
        END
        """.strip()
    
    def _generate_abs_diff(self, col1: str, col2: str, dialect: Any) -> str:
        """
        生成计算绝对差值的 SQL 表达式
        
        Args:
            col1: 第一个列
            col2: 第二个列
            dialect: 数据库方言
            
        Returns:
            ABS(col1 - col2) 的 SQL 表达式
        """
        db_name = dialect.name.lower()
        
        # 大多数数据库都支持 ABS 函数
        diff = f"({col1} - {col2})"
        
        if db_name in ["clickzetta", "mysql", "postgresql", "clickhouse", "snowflake", 
                       "bigquery", "oracle", "mssql", "redshift", "presto", "trino", "vertica"]:
            return f"ABS{diff}"
        else:
            # 对于不确定的数据库，使用 CASE 语句实现 ABS
            return f"CASE WHEN {diff} < 0 THEN -{diff} ELSE {diff} END"
    
    def apply_to_diff_options(self, diff_options: Dict[str, Any]) -> Dict[str, Any]:
        """
        将容差配置应用到 diff_options
        
        Args:
            diff_options: 原始的比对选项
            
        Returns:
            更新后的比对选项
        """
        if self.enabled:
            diff_options["float_tolerance"] = self.tolerance
            diff_options["_float_tolerance_manager"] = self
        
        return diff_options
    
    def get_column_precision(self, coltype: ColType) -> int:
        """
        获取列的精度设置
        
        Args:
            coltype: 列类型
            
        Returns:
            精度值
        """
        if hasattr(coltype, 'precision'):
            return coltype.precision
        
        # 默认精度
        if isinstance(coltype, Float):
            return 6  # 浮点数默认 6 位小数
        elif isinstance(coltype, Decimal):
            return 2  # 小数默认 2 位小数
        else:
            return 0
    
    def adjust_normalization(self, value: str, coltype: ColType, tolerance: float) -> str:
        """
        根据容差调整标准化逻辑
        
        如果容差较大，可以减少比较的精度
        
        Args:
            value: 原始值的 SQL 表达式
            coltype: 列类型
            tolerance: 容差值
            
        Returns:
            调整后的 SQL 表达式
        """
        if not self.enabled:
            return value
        
        # 根据容差计算合适的精度
        # 例如，如果容差是 0.01，那么比较到小数点后 2 位就足够了
        import math
        if tolerance > 0:
            # 计算需要的小数位数
            required_precision = max(0, -int(math.floor(math.log10(tolerance))))
            current_precision = self.get_column_precision(coltype)
            
            # 如果当前精度过高，可以降低以提高性能
            if current_precision > required_precision + 1:
                # 保留比容差多一位的精度
                return f"ROUND({value}, {required_precision + 1})"
        
        return value