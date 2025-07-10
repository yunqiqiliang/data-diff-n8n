"""
Clickzetta 连接器节点
用于创建和管理 Clickzetta 数据库连接
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime
import os
import aiohttp

from n8n_sdk import Node, NodePropertyTypes, INodeExecutionData


class ClickzettaConnector(Node):
    """
    Clickzetta 数据库连接器
    支持创建、测试和管理 Clickzetta 数据库连接
    使用 instance.service/workspace 格式
    """

    display_name = "Clickzetta Connector"
    description = "Connect to Clickzetta database and manage connections"
    group = "transform"
    version = 1

    # 节点输入输出配置
    inputs = ["main"]
    outputs = ["main"]

    # 节点属性配置
    properties = [
        {
            "displayName": "Operation",
            "name": "operation",
            "type": NodePropertyTypes.OPTIONS,
            "noDataExpression": True,
            "default": "create_connection",
            "options": [
                {
                    "name": "Create Connection",
                    "value": "create_connection",
                    "description": "Create a new Clickzetta database connection"
                },
                {
                    "name": "Test Connection",
                    "value": "test_connection",
                    "description": "Test an existing Clickzetta connection"
                },
                {
                    "name": "Get Schema Info",
                    "value": "get_schema",
                    "description": "Get database schema information"
                }
            ]
        },
        # Clickzetta 连接配置 (instance.service/workspace 格式)
        {
            "displayName": "Instance",
            "name": "instance",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Clickzetta instance name"
        },
        {
            "displayName": "Service",
            "name": "service",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Clickzetta service name"
        },
        {
            "displayName": "Workspace",
            "name": "workspace",
            "type": NodePropertyTypes.STRING,
            "default": "default",
            "required": True,
            "description": "Workspace name"
        },
        {
            "displayName": "Username",
            "name": "username",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "required": True,
            "description": "Database username"
        },
        {
            "displayName": "Password",
            "name": "password",
            "type": NodePropertyTypes.STRING,
            "typeOptions": {
                "password": True
            },
            "default": "",
            "required": True,
            "description": "Database password"
        },
        # 可选连接参数
        {
            "displayName": "Virtual Cluster",
            "name": "virtualcluster",
            "type": NodePropertyTypes.STRING,
            "default": "",
            "description": "Virtual cluster name (optional)"
        },
        {
            "displayName": "Schema",
            "name": "schema",
            "type": NodePropertyTypes.STRING,
            "default": "public",
            "description": "Database schema"
        },
        # 高级配置
        {
            "displayName": "Connection Timeout",
            "name": "timeout",
            "type": NodePropertyTypes.NUMBER,
            "default": 30,
            "description": "Connection timeout in seconds"
        },
        {
            "displayName": "Query Timeout",
            "name": "query_timeout",
            "type": NodePropertyTypes.NUMBER,
            "default": 300,
            "description": "Query timeout in seconds"
        },
        {
            "displayName": "Enable Compression",
            "name": "enable_compression",
            "type": NodePropertyTypes.BOOLEAN,
            "default": True,
            "description": "Enable compression for better performance"
        }
    ]

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    async def execute(self, items: List[INodeExecutionData]) -> List[INodeExecutionData]:
        """
        执行节点逻辑
        """
        results = []

        for item_index, item in enumerate(items):
            try:
                # 获取参数
                operation = self.get_node_parameter("operation", item_index)

                # 构建连接配置
                connection_config = self._build_connection_config(item_index)

                # 根据操作类型执行对应逻辑
                if operation == "create_connection":
                    result = await self._create_connection(connection_config)
                elif operation == "test_connection":
                    result = await self._test_connection(connection_config)
                elif operation == "get_schema":
                    result = await self._get_schema_info(connection_config)
                else:
                    raise ValueError(f"Unknown operation: {operation}")

                # 构建输出数据
                output_data = {
                    "operation": operation,
                    "timestamp": datetime.now().isoformat(),
                    "connection_config": {
                        "instance": connection_config["instance"],
                        "service": connection_config["service"],
                        "workspace": connection_config["workspace"],
                        "username": connection_config["username"],
                        "schema": connection_config["schema"]
                        # 注意：不输出密码
                    },
                    "result": result
                }

                results.append({
                    "json": output_data
                })

            except Exception as e:
                self.logger.error(f"Error in ClickzettaConnector execution: {e}")
                results.append({
                    "json": {
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                })

        return results

    def _build_connection_config(self, item_index: int) -> Dict[str, Any]:
        """
        构建连接配置字典
        """
        return {
            "database_type": "clickzetta",
            "instance": self.get_node_parameter("instance", item_index),
            "service": self.get_node_parameter("service", item_index),
            "workspace": self.get_node_parameter("workspace", item_index),
            "username": self.get_node_parameter("username", item_index),
            "password": self.get_node_parameter("password", item_index),
            "virtualcluster": self.get_node_parameter("virtualcluster", item_index),
            "schema": self.get_node_parameter("schema", item_index),
            "timeout": self.get_node_parameter("timeout", item_index),
            "query_timeout": self.get_node_parameter("query_timeout", item_index),
            "enable_compression": self.get_node_parameter("enable_compression", item_index)
        }

    async def _create_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建 Clickzetta 连接
        """
        try:
            # 使用数据库注册表构建连接字符串
            from ...core.database_registry import database_registry

            connection_string = database_registry.build_connection_string("clickzetta", config)

            return {
                "status": "success",
                "message": "Clickzetta connection created successfully",
                "connection_id": f"clickzetta_{config['instance']}_{config['service']}_{config['workspace']}",
                "connection_string": connection_string
            }

        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to create connection: {str(e)}"
            }

    async def _test_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        测试 Clickzetta 连接（通过后端API）
        """
        api_url = os.getenv("CLICKZETTA_API_URL", "http://localhost:8080/api/v1/connections/test")
        try:
            # 使用 Clickzetta 适配器进行配置优化
            from ...core.clickzetta_adapter import ClickzettaAdapter

            adapter = ClickzettaAdapter()
            optimized_config = adapter.optimize_connection_config(config)

            # 验证配置
            validation_errors = adapter.validate_connection_config(optimized_config)
            if validation_errors:
                return {
                    "status": "error",
                    "message": f"Configuration validation failed: {'; '.join(validation_errors)}"
                }

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=optimized_config) as resp:
                    resp_json = await resp.json()
                    if resp.status == 200 and resp_json.get("success"):
                        return {
                            "status": "success",
                            "message": resp_json.get("message", "Connection test successful"),
                            "server_version": resp_json.get("server_version", "Clickzetta"),
                            "response_time_ms": resp_json.get("response_time_ms", None)
                        }
                    else:
                        return {
                            "status": "error",
                            "message": resp_json.get("detail") or resp_json.get("message") or str(resp_json)
                        }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Connection test failed: {str(e)}"
            }

    async def _get_schema_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取数据库架构信息（通过后端API）
        """
        api_url = os.getenv("CLICKZETTA_SCHEMA_API_URL", "http://localhost:8080/api/v1/tables/list")
        try:
            # 使用 Clickzetta 适配器进行配置优化
            from ...core.clickzetta_adapter import ClickzettaAdapter

            adapter = ClickzettaAdapter()
            optimized_config = adapter.optimize_connection_config(config)

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=optimized_config) as resp:
                    resp_json = await resp.json()
                    if resp.status == 200 and resp_json.get("status") == "success":
                        return resp_json
                    else:
                        return {
                            "status": "error",
                            "message": resp_json.get("detail") or resp_json.get("message") or str(resp_json)
                        }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to get schema info: {str(e)}"
            }
