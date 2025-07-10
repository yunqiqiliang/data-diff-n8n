"""
错误处理和异常定义
定义了 N8N data-diff 集成中使用的所有自定义异常
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime


class DataDiffError(Exception):
    """
    基础异常类
    """

    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典格式
        """
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class ConnectionError(DataDiffError):
    """
    连接相关错误
    """
    pass


class ClickzettaConnectionError(ConnectionError):
    """
    Clickzetta 连接错误
    """
    pass


class PostgreSQLConnectionError(ConnectionError):
    """
    PostgreSQL 连接错误
    """
    pass


class MySQLConnectionError(ConnectionError):
    """
    MySQL 连接错误
    """
    pass


class OracleConnectionError(ConnectionError):
    """
    Oracle 连接错误
    """
    pass


class SnowflakeConnectionError(ConnectionError):
    """
    Snowflake 连接错误
    """
    pass


class ConfigurationError(DataDiffError):
    """
    配置错误
    """
    pass


class ValidationError(DataDiffError):
    """
    验证错误
    """
    pass


class ComparisonError(DataDiffError):
    """
    比对执行错误
    """
    pass


class QueryError(DataDiffError):
    """
    查询执行错误
    """
    pass


class SchemaError(DataDiffError):
    """
    模式相关错误
    """
    pass


class DataTypeError(DataDiffError):
    """
    数据类型错误
    """
    pass


class TemplateError(DataDiffError):
    """
    模板相关错误
    """
    pass


class WorkflowError(DataDiffError):
    """
    工作流执行错误
    """
    pass


class AuthenticationError(DataDiffError):
    """
    认证错误
    """
    pass


class AuthorizationError(DataDiffError):
    """
    授权错误
    """
    pass


class TimeoutError(DataDiffError):
    """
    超时错误
    """
    pass


class ResourceExhaustedError(DataDiffError):
    """
    资源耗尽错误
    """
    pass


class NotFoundError(DataDiffError):
    """
    资源未找到错误
    """
    pass


class ConflictError(DataDiffError):
    """
    冲突错误
    """
    pass


class RateLimitError(DataDiffError):
    """
    速率限制错误
    """
    pass


class ErrorHandler:
    """
    错误处理器
    提供统一的错误处理和日志记录功能
    """

    def __init__(self, config_manager=None):
        self.logger = logging.getLogger(__name__)
        self.config_manager = config_manager
        self.error_stats = {
            "total_errors": 0,
            "error_types": {},
            "last_reset": datetime.now()
        }

    def handle_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
        severity: str = "ERROR"
    ) -> Dict[str, Any]:
        """
        处理错误并生成统一的错误响应

        Args:
            error: 异常对象
            context: 错误上下文
            severity: 错误严重程度

        Returns:
            错误响应字典
        """
        # 更新错误统计
        self.error_stats["total_errors"] += 1
        error_type = type(error).__name__
        self.error_stats["error_types"][error_type] = self.error_stats["error_types"].get(error_type, 0) + 1

        # 构建错误信息
        if isinstance(error, DataDiffError):
            error_info = error.to_dict()
        else:
            error_info = {
                "error_type": error_type,
                "error_code": error_type,
                "message": str(error),
                "details": {},
                "timestamp": datetime.now().isoformat()
            }

        # 添加上下文信息
        if context:
            error_info["context"] = context

        # 记录日志
        log_message = f"{error_type}: {error_info['message']}"
        if context:
            log_message += f" Context: {context}"

        if severity == "CRITICAL":
            self.logger.critical(log_message, exc_info=True)
        elif severity == "ERROR":
            self.logger.error(log_message, exc_info=True)
        elif severity == "WARNING":
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)

        return error_info

    def handle_connection_error(
        self,
        error: Exception,
        database_type: str,
        connection_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        处理数据库连接错误

        Args:
            error: 异常对象
            database_type: 数据库类型
            connection_details: 连接详情

        Returns:
            错误响应字典
        """
        context = {
            "database_type": database_type,
            "host": connection_details.get("host", "unknown"),
            "port": connection_details.get("port", "unknown"),
            "database": connection_details.get("database", "unknown")
        }

        # 根据数据库类型创建特定的错误
        if database_type.lower() == "clickzetta":
            connection_error = ClickzettaConnectionError(
                f"Failed to connect to Clickzetta: {str(error)}",
                details=context
            )
        elif database_type.lower() == "postgresql":
            connection_error = PostgreSQLConnectionError(
                f"Failed to connect to PostgreSQL: {str(error)}",
                details=context
            )
        elif database_type.lower() == "mysql":
            connection_error = MySQLConnectionError(
                f"Failed to connect to MySQL: {str(error)}",
                details=context
            )
        elif database_type.lower() == "oracle":
            connection_error = OracleConnectionError(
                f"Failed to connect to Oracle: {str(error)}",
                details=context
            )
        elif database_type.lower() == "snowflake":
            connection_error = SnowflakeConnectionError(
                f"Failed to connect to Snowflake: {str(error)}",
                details=context
            )
        else:
            connection_error = ConnectionError(
                f"Failed to connect to {database_type}: {str(error)}",
                details=context
            )

        return self.handle_error(connection_error, context, "ERROR")

    def handle_comparison_error(
        self,
        error: Exception,
        comparison_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        处理数据比对错误

        Args:
            error: 异常对象
            comparison_config: 比对配置

        Returns:
            错误响应字典
        """
        context = {
            "source_table": comparison_config.get("source_table"),
            "target_table": comparison_config.get("target_table"),
            "algorithm": comparison_config.get("algorithm"),
            "key_columns": comparison_config.get("key_columns")
        }

        comparison_error = ComparisonError(
            f"Data comparison failed: {str(error)}",
            details=context
        )

        return self.handle_error(comparison_error, context, "ERROR")

    def handle_validation_error(
        self,
        field_name: str,
        field_value: Any,
        expected_type: str = None,
        allowed_values: List[Any] = None
    ) -> Dict[str, Any]:
        """
        处理验证错误

        Args:
            field_name: 字段名
            field_value: 字段值
            expected_type: 期望类型
            allowed_values: 允许的值列表

        Returns:
            错误响应字典
        """
        message_parts = [f"Validation failed for field '{field_name}'"]

        if expected_type:
            message_parts.append(f"expected type: {expected_type}")

        if allowed_values:
            message_parts.append(f"allowed values: {allowed_values}")

        message = ", ".join(message_parts)

        context = {
            "field_name": field_name,
            "field_value": field_value,
            "expected_type": expected_type,
            "allowed_values": allowed_values
        }

        validation_error = ValidationError(message, details=context)

        return self.handle_error(validation_error, context, "WARNING")

    def get_error_stats(self) -> Dict[str, Any]:
        """
        获取错误统计信息

        Returns:
            错误统计字典
        """
        return {
            **self.error_stats,
            "uptime_hours": (datetime.now() - self.error_stats["last_reset"]).total_seconds() / 3600
        }

    def reset_error_stats(self):
        """
        重置错误统计
        """
        self.error_stats = {
            "total_errors": 0,
            "error_types": {},
            "last_reset": datetime.now()
        }
        self.logger.info("Error statistics reset")

    def is_retryable_error(self, error: Exception) -> bool:
        """
        判断错误是否可重试

        Args:
            error: 异常对象

        Returns:
            是否可重试
        """
        # 可重试的错误类型
        retryable_errors = [
            TimeoutError,
            ConnectionError,
            ResourceExhaustedError,
        ]

        # 不可重试的错误类型
        non_retryable_errors = [
            AuthenticationError,
            AuthorizationError,
            ValidationError,
            ConfigurationError,
            NotFoundError
        ]

        for error_type in non_retryable_errors:
            if isinstance(error, error_type):
                return False

        for error_type in retryable_errors:
            if isinstance(error, error_type):
                return True

        # 对于其他类型的错误，根据错误消息判断
        error_message = str(error).lower()

        # 网络相关错误通常可重试
        if any(keyword in error_message for keyword in ['timeout', 'connection', 'network', 'refused']):
            return True

        # 认证和权限错误不可重试
        if any(keyword in error_message for keyword in ['authentication', 'authorization', 'permission', 'access denied']):
            return False

        # 默认不重试
        return False

    def create_error_response(
        self,
        error: Exception,
        request_id: str = None,
        additional_info: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        创建标准化的错误响应

        Args:
            error: 异常对象
            request_id: 请求ID
            additional_info: 额外信息

        Returns:
            标准化错误响应
        """
        error_info = self.handle_error(error)

        response = {
            "success": False,
            "error": error_info,
            "timestamp": datetime.now().isoformat()
        }

        if request_id:
            response["request_id"] = request_id

        if additional_info:
            response["additional_info"] = additional_info

        return response


# 全局错误处理器实例
error_handler = ErrorHandler()
