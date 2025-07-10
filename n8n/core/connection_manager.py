"""
连接管理器
负责管理各种数据库连接
"""

import logging
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
import json
import uuid

# 导入 data-diff 相关模块
try:
    from data_diff import connect
    from data_diff.databases import Database
    HAS_DATA_DIFF = True
except ImportError:
    HAS_DATA_DIFF = False
    logging.warning("data-diff library not found. Using mock implementation.")

# 导入数据库注册表
from .database_registry import database_registry


class ConnectionManager:
    """
    数据库连接管理器
    支持多种数据库类型的连接管理和池化
    """

    def __init__(self, config: Optional[dict] = None, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.config = config or {}
        self.connections: Dict[str, Any] = {}
        self.connection_configs: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def parse_connection_string(connection_string: str) -> dict:
        """
        解析数据库连接字符串，返回字典结构
        支持常见的 URI 格式，如 postgresql://user:pass@host:port/dbname
        """
        parsed = urlparse(connection_string)
        return {
            'scheme': parsed.scheme,
            'username': parsed.username,
            'password': parsed.password,
            'host': parsed.hostname,
            'port': parsed.port,
            'database': parsed.path.lstrip('/') if parsed.path else None,
            'query': parsed.query,
        }

    async def create_connection(self, connection_id_or_config: str | Dict[str, Any], config: Dict[str, Any] = None) -> str:
        """
        创建数据库连接

        Args:
            connection_id_or_config: 连接唯一标识或配置字典
            config: 连接配置，如果第一个参数是ID，则此参数必须提供

        Returns:
            连接ID
        """
        try:
            # 如果第一个参数是字典，则视为配置
            if isinstance(connection_id_or_config, dict):
                config = connection_id_or_config
                connection_id = str(uuid.uuid4())
            else:
                connection_id = connection_id_or_config
                if config is None:
                    raise ValueError("When connection_id_or_config is a string ID, config must be provided")

            # 验证配置
            self._validate_connection_config(config)

            # 构建连接字符串
            connection_string = self._build_connection_string(config)

            if HAS_DATA_DIFF:
                # 使用 data-diff 创建连接
                connection = connect(connection_string)
                self.connections[connection_id] = connection
            else:
                # 模拟连接
                self.connections[connection_id] = {
                    "type": "mock",
                    "config": config,
                    "connection_string": connection_string
                }

            # 保存连接配置
            self.connection_configs[connection_id] = config

            self.logger.info(f"Created connection {connection_id} for {config.get('database_type')}")
            return connection_id

        except Exception as e:
            self.logger.error(f"Failed to create connection {connection_id}: {e}")
            raise

    async def test_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        测试数据库连接

        Args:
            connection_id: 连接ID

        Returns:
            测试结果
        """
        if connection_id not in self.connections:
            raise ValueError(f"Connection {connection_id} not found")

        try:
            connection = self.connections[connection_id]
            config = self.connection_configs[connection_id]

            if HAS_DATA_DIFF and hasattr(connection, 'query'):
                # 执行简单测试查询
                result = connection.query("SELECT 1 as test", limit=1)
                return {
                    "status": "success",
                    "message": "Connection test successful",
                    "database_type": config.get("database_type"),
                    "response_time_ms": 100  # 实际应该测量
                }
            else:
                # 模拟测试
                return {
                    "status": "success",
                    "message": "Mock connection test successful",
                    "database_type": config.get("database_type"),
                    "response_time_ms": 50
                }

        except Exception as e:
            self.logger.error(f"Connection test failed for {connection_id}: {e}")
            return {
                "status": "error",
                "message": f"Connection test failed: {str(e)}"
            }

    async def get_schema_info(self, connection_id: str, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取数据库架构信息

        Args:
            connection_id: 连接ID
            schema_name: 架构名称（可选）

        Returns:
            架构信息
        """
        if connection_id not in self.connections:
            raise ValueError(f"Connection {connection_id} not found")

        try:
            connection = self.connections[connection_id]
            config = self.connection_configs[connection_id]

            if HAS_DATA_DIFF and hasattr(connection, 'query'):
                # 使用 data-diff 获取架构信息
                tables = await self._get_tables_info(connection, schema_name)
                return {
                    "status": "success",
                    "database_type": config.get("database_type"),
                    "schema": schema_name or config.get("database", "default"),
                    "tables": tables
                }
            else:
                # 返回模拟架构信息
                return self._get_mock_schema_info(config)

        except Exception as e:
            self.logger.error(f"Failed to get schema info for {connection_id}: {e}")
            return {
                "status": "error",
                "message": f"Failed to get schema info: {str(e)}"
            }

    def get_connection(self, connection_id: str) -> Any:
        """
        获取连接对象

        Args:
            connection_id: 连接ID

        Returns:
            连接对象
        """
        if connection_id not in self.connections:
            raise ValueError(f"Connection {connection_id} not found")

        return self.connections[connection_id]

    def get_connection_config(self, connection_id: str) -> Dict[str, Any]:
        """
        获取连接配置

        Args:
            connection_id: 连接ID

        Returns:
            连接配置
        """
        if connection_id not in self.connection_configs:
            raise ValueError(f"Connection config {connection_id} not found")

        return self.connection_configs[connection_id].copy()

    async def close_connection(self, connection_id: str) -> bool:
        """
        关闭连接

        Args:
            connection_id: 连接ID

        Returns:
            是否成功关闭
        """
        try:
            if connection_id in self.connections:
                connection = self.connections[connection_id]

                if HAS_DATA_DIFF and hasattr(connection, 'close'):
                    connection.close()

                del self.connections[connection_id]
                del self.connection_configs[connection_id]

                self.logger.info(f"Closed connection {connection_id}")
                return True
            else:
                self.logger.warning(f"Connection {connection_id} not found for closing")
                return False

        except Exception as e:
            self.logger.error(f"Failed to close connection {connection_id}: {e}")
            return False

    def list_connections(self) -> List[Dict[str, Any]]:
        """
        列出所有连接

        Returns:
            连接列表
        """
        connections = []
        for conn_id, config in self.connection_configs.items():
            connections.append({
                "connection_id": conn_id,
                "database_type": config.get("database_type"),
                "host": config.get("host"),
                "database": config.get("database"),
                "status": "active" if conn_id in self.connections else "inactive"
            })
        return connections

    def _validate_connection_config(self, config: Dict[str, Any]) -> None:
        """
        验证连接配置 - 使用数据库注册表进行验证
        """
        if "database_type" not in config:
            raise ValueError("Missing required field: database_type")

        db_type = config["database_type"]

        # 使用数据库注册表验证
        errors = database_registry.validate_config(db_type, config)
        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")

    def _build_connection_string(self, config: Dict[str, Any]) -> str:
        """
        构建连接字符串 - 使用数据库注册表构建
        """
        db_type = config["database_type"]
        return database_registry.build_connection_string(db_type, config)

    async def _get_tables_info(self, connection: Any, schema_name: Optional[str]) -> List[Dict[str, Any]]:
        """
        获取表信息
        """
        # 这里应该实现实际的表信息查询
        # 暂时返回模拟数据
        return [
            {
                "name": "users",
                "schema": schema_name or "public",
                "columns": [
                    {"name": "id", "type": "bigint", "nullable": False},
                    {"name": "name", "type": "varchar", "nullable": True},
                    {"name": "email", "type": "varchar", "nullable": True}
                ],
                "row_count": 10000
            }
        ]

    def _get_mock_schema_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取模拟架构信息
        """
        db_type = config.get("database_type", "unknown")

        return {
            "status": "success",
            "database_type": db_type,
            "schema": config.get("database", "default"),
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        {"name": "id", "type": "bigint", "nullable": False},
                        {"name": "name", "type": "varchar(100)", "nullable": True},
                        {"name": "email", "type": "varchar(255)", "nullable": True},
                        {"name": "created_at", "type": "timestamp", "nullable": False}
                    ],
                    "row_count": 50000
                },
                {
                    "name": "orders",
                    "columns": [
                        {"name": "id", "type": "bigint", "nullable": False},
                        {"name": "user_id", "type": "bigint", "nullable": False},
                        {"name": "amount", "type": "decimal(10,2)", "nullable": False},
                        {"name": "order_date", "type": "date", "nullable": False}
                    ],
                    "row_count": 150000
                }
            ]
        }

    async def initialize(self):
        """
        异步初始化方法（占位），兼容 FastAPI 生命周期钩子
        """
        pass

    async def cleanup(self):
        """
        清理所有连接和资源
        """
        try:
            # 关闭所有活跃连接
            connection_ids = list(self.connections.keys())
            for connection_id in connection_ids:
                await self.close_connection(connection_id)

            self.logger.info("ConnectionManager cleanup completed successfully")
        except Exception as e:
            self.logger.error(f"Error during ConnectionManager cleanup: {e}")
            raise
