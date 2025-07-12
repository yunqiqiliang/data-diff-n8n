"""
时间戳精度处理支持
为 data-diff 添加时间戳精度比较功能，处理时区问题
"""

from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timezone
from data_diff.abcs.database_types import ColType, TemporalType, Timestamp, TimestampTZ, Date, Datetime, Time
import logging

logger = logging.getLogger(__name__)


class TimestampPrecisionManager:
    """管理时间戳精度比较逻辑，处理时区问题"""
    
    # 精度级别映射（秒数）
    PRECISION_LEVELS = {
        'microsecond': 0.000001,  # 微秒
        'millisecond': 0.001,     # 毫秒
        'second': 1,              # 秒
        'minute': 60,             # 分钟
        'hour': 3600,             # 小时
        'day': 86400,             # 天
    }
    
    def __init__(self, precision: Optional[str] = None, handle_timezone: bool = True):
        """
        初始化时间戳精度管理器
        
        Args:
            precision: 精度级别 ('microsecond', 'millisecond', 'second', 'minute', 'hour', 'day')
            handle_timezone: 是否处理时区问题
        """
        self.precision = precision or 'microsecond'
        self.handle_timezone = handle_timezone
        self.precision_seconds = self.PRECISION_LEVELS.get(precision, 0.000001)
        
        # 验证精度级别
        if precision and precision not in self.PRECISION_LEVELS:
            raise ValueError(f"Invalid precision: {precision}. Must be one of {list(self.PRECISION_LEVELS.keys())}")
    
    def should_handle_timestamp(self, coltype: ColType) -> bool:
        """
        判断某个列类型是否需要时间戳精度处理
        
        Args:
            coltype: 列类型
            
        Returns:
            是否需要处理
        """
        return isinstance(coltype, (Timestamp, TimestampTZ, Datetime, Date, Time, TemporalType))
    
    def check_timezone_compatibility(self, db1_name: str, db2_name: str, coltype1: ColType, coltype2: ColType) -> Tuple[bool, Optional[str]]:
        """
        检查两个数据库的时区兼容性
        
        Args:
            db1_name: 第一个数据库名称
            db2_name: 第二个数据库名称
            coltype1: 第一个列的类型
            coltype2: 第二个列的类型
            
        Returns:
            (是否兼容, 警告信息)
        """
        # 不支持时区的数据库列表
        no_timezone_support = ['clickzetta', 'bigquery', 'clickhouse', 'mssql']
        
        db1_lower = db1_name.lower()
        db2_lower = db2_name.lower()
        
        # 检查是否有数据库不支持时区
        db1_no_tz = db1_lower in no_timezone_support
        db2_no_tz = db2_lower in no_timezone_support
        
        # 检查列类型是否包含时区
        col1_has_tz = isinstance(coltype1, TimestampTZ)
        col2_has_tz = isinstance(coltype2, TimestampTZ)
        
        warnings = []
        
        # 情况1：一个数据库支持时区，另一个不支持
        if (db1_no_tz and not db2_no_tz) or (db2_no_tz and not db1_no_tz):
            no_tz_db = db1_name if db1_no_tz else db2_name
            warnings.append(f"Database '{no_tz_db}' does not support timezone. Ensure all timestamps are in UTC.")
        
        # 情况2：列类型不匹配（一个有时区，一个没有）
        if col1_has_tz != col2_has_tz:
            warnings.append("Timestamp types mismatch: one has timezone, another doesn't. Results may be inaccurate.")
        
        # 情况3：ClickZetta 特殊警告
        if 'clickzetta' in [db1_lower, db2_lower]:
            warnings.append("ClickZetta does not support timezones. All timestamps must be pre-converted to UTC.")
        
        warning_msg = " ".join(warnings) if warnings else None
        is_compatible = len(warnings) == 0
        
        return is_compatible, warning_msg
    
    def generate_precision_sql(self, col_expr: str, coltype: ColType, precision: str, dialect: Any) -> str:
        """
        生成时间戳精度处理的 SQL 表达式
        
        Args:
            col_expr: 列的 SQL 表达式
            coltype: 列类型
            precision: 精度级别
            dialect: 数据库方言
            
        Returns:
            处理后的 SQL 表达式
        """
        db_name = dialect.name.lower()
        
        # 根据精度级别生成不同的 SQL
        if precision == 'microsecond':
            # 保持原样，这是最高精度
            return col_expr
        
        elif precision == 'millisecond':
            # 截断到毫秒
            return self._truncate_to_millisecond(col_expr, db_name, dialect)
        
        elif precision == 'second':
            # 截断到秒
            return self._truncate_to_second(col_expr, db_name, dialect)
        
        elif precision == 'minute':
            # 截断到分钟
            return self._truncate_to_minute(col_expr, db_name, dialect)
        
        elif precision == 'hour':
            # 截断到小时
            return self._truncate_to_hour(col_expr, db_name, dialect)
        
        elif precision == 'day':
            # 截断到天
            return self._truncate_to_day(col_expr, db_name, dialect)
        
        return col_expr
    
    def _truncate_to_millisecond(self, col_expr: str, db_name: str, dialect: Any) -> str:
        """截断到毫秒精度"""
        if db_name in ['postgresql', 'postgres']:
            return f"date_trunc('milliseconds', {col_expr})"
        elif db_name == 'mysql':
            # MySQL 需要特殊处理
            return f"CAST(CONCAT(DATE_FORMAT({col_expr}, '%Y-%m-%d %H:%i:%s.'), LPAD(FLOOR(MICROSECOND({col_expr})/1000), 3, '0'), '000') AS DATETIME(6))"
        elif db_name == 'clickzetta':
            # ClickZetta 使用 date_format
            return f"date_format({col_expr}, 'yyyy-MM-dd HH:mm:ss.SSS000')"
        elif db_name == 'clickhouse':
            return f"toDateTime64(floor(toUnixTimestamp64Micro({col_expr}) / 1000) * 1000 / 1000000, 6)"
        elif db_name == 'snowflake':
            return f"DATE_TRUNC('MILLISECOND', {col_expr})"
        elif db_name == 'bigquery':
            return f"TIMESTAMP_TRUNC({col_expr}, MILLISECOND)"
        else:
            # 通用方法：格式化后截断
            return col_expr
    
    def _truncate_to_second(self, col_expr: str, db_name: str, dialect: Any) -> str:
        """截断到秒精度"""
        if db_name in ['postgresql', 'postgres']:
            return f"date_trunc('second', {col_expr})"
        elif db_name == 'mysql':
            return f"DATE_FORMAT({col_expr}, '%Y-%m-%d %H:%i:%s.000000')"
        elif db_name == 'clickzetta':
            return f"date_format({col_expr}, 'yyyy-MM-dd HH:mm:ss.000000')"
        elif db_name == 'clickhouse':
            return f"toDateTime({col_expr})"
        elif db_name == 'snowflake':
            return f"DATE_TRUNC('SECOND', {col_expr})"
        elif db_name == 'bigquery':
            return f"TIMESTAMP_TRUNC({col_expr}, SECOND)"
        elif db_name == 'oracle':
            return f"TRUNC({col_expr}, 'SS')"
        else:
            return col_expr
    
    def _truncate_to_minute(self, col_expr: str, db_name: str, dialect: Any) -> str:
        """截断到分钟精度"""
        if db_name in ['postgresql', 'postgres']:
            return f"date_trunc('minute', {col_expr})"
        elif db_name == 'mysql':
            return f"DATE_FORMAT({col_expr}, '%Y-%m-%d %H:%i:00.000000')"
        elif db_name == 'clickzetta':
            return f"date_format({col_expr}, 'yyyy-MM-dd HH:mm:00.000000')"
        elif db_name == 'clickhouse':
            return f"toStartOfMinute({col_expr})"
        elif db_name == 'snowflake':
            return f"DATE_TRUNC('MINUTE', {col_expr})"
        elif db_name == 'bigquery':
            return f"TIMESTAMP_TRUNC({col_expr}, MINUTE)"
        elif db_name == 'oracle':
            return f"TRUNC({col_expr}, 'MI')"
        else:
            return col_expr
    
    def _truncate_to_hour(self, col_expr: str, db_name: str, dialect: Any) -> str:
        """截断到小时精度"""
        if db_name in ['postgresql', 'postgres']:
            return f"date_trunc('hour', {col_expr})"
        elif db_name == 'mysql':
            return f"DATE_FORMAT({col_expr}, '%Y-%m-%d %H:00:00.000000')"
        elif db_name == 'clickzetta':
            return f"date_format({col_expr}, 'yyyy-MM-dd HH:00:00.000000')"
        elif db_name == 'clickhouse':
            return f"toStartOfHour({col_expr})"
        elif db_name == 'snowflake':
            return f"DATE_TRUNC('HOUR', {col_expr})"
        elif db_name == 'bigquery':
            return f"TIMESTAMP_TRUNC({col_expr}, HOUR)"
        elif db_name == 'oracle':
            return f"TRUNC({col_expr}, 'HH')"
        else:
            return col_expr
    
    def _truncate_to_day(self, col_expr: str, db_name: str, dialect: Any) -> str:
        """截断到天精度"""
        if db_name in ['postgresql', 'postgres']:
            return f"date_trunc('day', {col_expr})"
        elif db_name == 'mysql':
            return f"DATE({col_expr})"
        elif db_name == 'clickzetta':
            return f"date_format({col_expr}, 'yyyy-MM-dd 00:00:00.000000')"
        elif db_name == 'clickhouse':
            return f"toStartOfDay({col_expr})"
        elif db_name == 'snowflake':
            return f"DATE_TRUNC('DAY', {col_expr})"
        elif db_name == 'bigquery':
            return f"TIMESTAMP_TRUNC({col_expr}, DAY)"
        elif db_name == 'oracle':
            return f"TRUNC({col_expr}, 'DD')"
        else:
            return col_expr
    
    def normalize_for_precision(self, value: str, coltype: ColType, precision: str, dialect: Any) -> str:
        """
        根据精度级别标准化时间戳
        
        这个方法在 normalize_timestamp 之后调用，进一步处理精度
        
        Args:
            value: 已经标准化的时间戳表达式
            coltype: 列类型
            precision: 精度级别
            dialect: 数据库方言
            
        Returns:
            处理后的表达式
        """
        if not self.should_handle_timestamp(coltype):
            return value
        
        # 如果精度是微秒（最高精度），不需要额外处理
        if precision == 'microsecond':
            return value
        
        # 对于其他精度，需要在标准化的基础上进行截断
        # 这里的 value 已经是字符串格式了，需要特殊处理
        db_name = dialect.name.lower()
        
        # 根据精度级别修改字符串
        if precision in ['second', 'minute', 'hour', 'day']:
            # 这些精度级别需要将微秒部分置零
            if db_name in ['postgresql', 'postgres', 'mysql', 'clickzetta']:
                # 使用字符串替换
                return f"CONCAT(SUBSTRING({value}, 1, 19), '.000000')"
            elif db_name == 'clickhouse':
                return f"concat(substring({value}, 1, 19), '.000000')"
            elif db_name == 'bigquery':
                return f"CONCAT(SUBSTR({value}, 1, 19), '.000000')"
        
        elif precision == 'millisecond':
            # 保留毫秒，但将微秒部分置零
            if db_name in ['postgresql', 'postgres', 'mysql']:
                return f"CONCAT(SUBSTRING({value}, 1, 23), '000')"
            elif db_name == 'clickzetta':
                return f"CONCAT(SUBSTRING({value}, 1, 23), '000')"
        
        return value
    
    def get_recommended_precision(self, db1_name: str, db2_name: str) -> str:
        """
        根据数据库类型推荐合适的精度级别
        
        Args:
            db1_name: 第一个数据库名称
            db2_name: 第二个数据库名称
            
        Returns:
            推荐的精度级别
        """
        # 某些数据库组合的推荐精度
        db_names = {db1_name.lower(), db2_name.lower()}
        
        # 如果涉及 ClickZetta，推荐使用毫秒或秒级精度
        if 'clickzetta' in db_names:
            return 'millisecond'
        
        # 如果都是高精度数据库，使用微秒
        high_precision_dbs = {'postgresql', 'postgres', 'mysql', 'clickhouse'}
        if db_names.issubset(high_precision_dbs):
            return 'microsecond'
        
        # 默认使用毫秒精度
        return 'millisecond'