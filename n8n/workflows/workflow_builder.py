"""
工作流构建器
动态构建 N8N 工作流
"""

import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import uuid


class WorkflowBuilder:
    """
    N8N 工作流构建器
    用于动态构建和配置数据比对工作流
    """

    def __init__(self, config_manager=None):
        self.logger = logging.getLogger(__name__)
        self.config_manager = config_manager

    def build_workflow(
        self,
        source_config: Optional[Dict[str, Any]] = None,
        target_config: Optional[Dict[str, Any]] = None,
        comparison_config: Optional[Dict[str, Any]] = None,
        workflow_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        构建完整的数据比对工作流

        Args:
            source_config: 源数据库配置
            target_config: 目标数据库配置
            comparison_config: 比对配置
            workflow_options: 工作流选项

        Returns:
            N8N 工作流定义
        """
        source_config = source_config or {}
        target_config = target_config or {}
        comparison_config = comparison_config or {}
        workflow_options = workflow_options or {}

        # 生成工作流基本信息
        workflow_id = str(uuid.uuid4())
        workflow_name = self._generate_workflow_name(source_config, target_config, comparison_config)

        # 构建节点
        nodes = self._build_workflow_nodes(
            source_config, target_config, comparison_config, workflow_options
        )

        # 构建连接
        connections = self._build_workflow_connections(nodes)

        # 构建完整工作流
        workflow = {
            "id": workflow_id,
            "name": workflow_name,
            "active": True,
            "createdAt": datetime.now().isoformat(),
            "updatedAt": datetime.now().isoformat(),
            "versionId": str(uuid.uuid4()),
            "tags": self._generate_workflow_tags(source_config, target_config),
            "settings": {
                "executionOrder": "v1",
                "saveManualExecutions": True,
                "callerPolicy": "workflowsFromSameOwner"
            },
            "staticData": {},
            "nodes": nodes,
            "connections": connections,
            "meta": {
                "created_by": "n8n-data-diff-integration",
                "version": "1.0",
                "description": f"Data comparison workflow between {source_config.get('database_type')} and {target_config.get('database_type')}"
            }
        }

        return workflow

    def _generate_workflow_name(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        comparison_config: Dict[str, Any]
    ) -> str:
        """
        生成工作流名称
        """
        source_db = source_config.get('database_type', 'unknown')
        target_db = target_config.get('database_type', 'unknown')
        source_table = comparison_config.get('source_table', 'table')
        target_table = comparison_config.get('target_table', 'table')

        # 提取表名（去掉schema前缀）
        source_table_name = source_table.split('.')[-1] if '.' in source_table else source_table
        target_table_name = target_table.split('.')[-1] if '.' in target_table else target_table

        if source_table_name == target_table_name:
            return f"{(source_db or '').title()} vs {(target_db or '').title()} - {source_table_name}"
        else:
            return f"{(source_db or '').title()}.{source_table_name} vs {(target_db or '').title()}.{target_table_name}"

    def _generate_workflow_tags(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any]
    ) -> List[str]:
        """
        生成工作流标签
        """
        tags = ["data-diff", "comparison"]

        # 添加数据库类型标签
        source_db = source_config.get('database_type')
        target_db = target_config.get('database_type')

        if source_db:
            tags.append(f"source-{source_db}")
        if target_db:
            tags.append(f"target-{target_db}")

        # 如果涉及 Clickzetta，添加特殊标签
        if source_db == 'clickzetta' or target_db == 'clickzetta':
            tags.append("clickzetta")

        return tags

    def _build_workflow_nodes(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        comparison_config: Dict[str, Any],
        workflow_options: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        构建工作流节点
        """
        nodes = []

        # 1. 触发节点
        trigger_node = self._create_trigger_node(workflow_options)
        nodes.append(trigger_node)

        # 2. 源数据库连接节点
        source_connection_node = self._create_connection_node(
            "source_connection", source_config, position=[300, 100]
        )
        nodes.append(source_connection_node)

        # 3. 目标数据库连接节点
        target_connection_node = self._create_connection_node(
            "target_connection", target_config, position=[300, 200]
        )
        nodes.append(target_connection_node)

        # 4. 比对配置节点
        config_node = self._create_comparison_config_node(
            comparison_config, position=[500, 150]
        )
        nodes.append(config_node)

        # 5. 数据比对执行节点
        comparison_node = self._create_comparison_node(
            workflow_options, position=[700, 150]
        )
        nodes.append(comparison_node)

        # 6. 结果分析节点
        analysis_node = self._create_analysis_node(
            workflow_options, position=[900, 150]
        )
        nodes.append(analysis_node)

        # 7. 可选的通知节点
        if workflow_options.get("enable_notifications"):
            notification_node = self._create_notification_node(
                workflow_options, position=[1100, 150]
            )
            nodes.append(notification_node)

        # 8. 可选的存储节点
        if workflow_options.get("save_results"):
            storage_node = self._create_storage_node(
                workflow_options, position=[1100, 250]
            )
            nodes.append(storage_node)

        return nodes

    def _create_trigger_node(self, workflow_options: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建触发节点
        """
        trigger_type = workflow_options.get("trigger_type", "manual")

        if trigger_type == "schedule":
            # 定时触发
            schedule = workflow_options.get("schedule", {"hour": 2, "minute": 0})
            return {
                "id": str(uuid.uuid4()),
                "name": "Schedule Trigger",
                "type": "n8n-nodes-base.cron",
                "position": [100, 150],
                "parameters": {
                    "rule": schedule
                },
                "typeVersion": 1
            }
        elif trigger_type == "webhook":
            # Webhook 触发
            return {
                "id": str(uuid.uuid4()),
                "name": "Webhook Trigger",
                "type": "n8n-nodes-base.webhook",
                "position": [100, 150],
                "parameters": {
                    "httpMethod": "POST",
                    "path": workflow_options.get("webhook_path", "data-diff-trigger"),
                    "responseMode": "onReceived"
                },
                "typeVersion": 1
            }
        else:
            # 手动触发
            return {
                "id": str(uuid.uuid4()),
                "name": "Manual Trigger",
                "type": "n8n-nodes-base.manualTrigger",
                "position": [100, 150],
                "parameters": {},
                "typeVersion": 1
            }

    def _create_connection_node(
        self,
        node_id: str,
        config: Dict[str, Any],
        position: List[int]
    ) -> Dict[str, Any]:
        """
        创建数据库连接节点
        """
        db_type = config.get('database_type')

        if db_type == 'clickzetta':
            node_type = "clickzetta-connector"
        elif db_type == 'postgresql':
            node_type = "n8n-nodes-base.postgres"
        elif db_type == 'mysql':
            node_type = "n8n-nodes-base.mysql"
        else:
            node_type = "database-connector"

        return {
            "id": str(uuid.uuid4()),
            "name": f"{(db_type or '').title()} Connection",
            "type": node_type,
            "position": position,
            "parameters": {
                "operation": "create_connection",
                **config
            },
            "typeVersion": 1
        }

    def _create_comparison_config_node(
        self,
        comparison_config: Dict[str, Any],
        position: List[int]
    ) -> Dict[str, Any]:
        """
        创建比对配置节点
        """
        return {
            "id": str(uuid.uuid4()),
            "name": "Comparison Config",
            "type": "data-diff-config",
            "position": position,
            "parameters": comparison_config,
            "typeVersion": 1
        }

    def _create_comparison_node(
        self,
        workflow_options: Dict[str, Any],
        position: List[int]
    ) -> Dict[str, Any]:
        """
        创建数据比对节点
        """
        return {
            "id": str(uuid.uuid4()),
            "name": "Data Comparison",
            "type": "data-diff-compare",
            "position": position,
            "parameters": {
                "execution_mode": workflow_options.get("execution_mode", "sync"),
                "timeout": workflow_options.get("timeout", 3600),
                "report_format": workflow_options.get("report_format", "json"),
                "include_sample_data": workflow_options.get("include_sample_data", True),
                "max_sample_size": workflow_options.get("max_sample_size", 100)
            },
            "typeVersion": 1
        }

    def _create_analysis_node(
        self,
        workflow_options: Dict[str, Any],
        position: List[int]
    ) -> Dict[str, Any]:
        """
        创建结果分析节点
        """
        return {
            "id": str(uuid.uuid4()),
            "name": "Result Analysis",
            "type": "data-diff-result",
            "position": position,
            "parameters": {
                "analysis_type": workflow_options.get("analysis_type", "full_analysis"),
                "report_template": workflow_options.get("report_template", "standard"),
                "alert_thresholds": workflow_options.get("alert_thresholds", {
                    "max_difference_rate": 0.05,
                    "min_match_rate": 0.95,
                    "max_missing_rows": 100
                }),
                "include_visualizations": workflow_options.get("include_visualizations", True),
                "include_recommendations": workflow_options.get("include_recommendations", True)
            },
            "typeVersion": 1
        }

    def _create_notification_node(
        self,
        workflow_options: Dict[str, Any],
        position: List[int]
    ) -> Dict[str, Any]:
        """
        创建通知节点
        """
        notification_type = workflow_options.get("notification_type", "email")

        if notification_type == "email":
            return {
                "id": str(uuid.uuid4()),
                "name": "Email Notification",
                "type": "n8n-nodes-base.emailSend",
                "position": position,
                "parameters": {
                    "to": workflow_options.get("notification_recipients", "admin@company.com"),
                    "subject": "Data Comparison Report",
                    "emailFormat": "html",
                    "message": "Data comparison completed. Please see attached report."
                },
                "typeVersion": 1
            }
        elif notification_type == "slack":
            return {
                "id": str(uuid.uuid4()),
                "name": "Slack Notification",
                "type": "n8n-nodes-base.slack",
                "position": position,
                "parameters": {
                    "channel": workflow_options.get("slack_channel", "#data-alerts"),
                    "text": "Data comparison completed",
                    "attachments": [
                        {
                            "color": "good",
                            "title": "Comparison Report",
                            "text": "={{$json.summary}}"
                        }
                    ]
                },
                "typeVersion": 1
            }
        else:
            return {
                "id": str(uuid.uuid4()),
                "name": "Webhook Notification",
                "type": "n8n-nodes-base.httpRequest",
                "position": position,
                "parameters": {
                    "method": "POST",
                    "url": workflow_options.get("webhook_url", ""),
                    "body": {
                        "event": "comparison_completed",
                        "data": "={{$json}}"
                    }
                },
                "typeVersion": 1
            }

    def _create_storage_node(
        self,
        workflow_options: Dict[str, Any],
        position: List[int]
    ) -> Dict[str, Any]:
        """
        创建存储节点
        """
        storage_type = workflow_options.get("storage_type", "file")

        if storage_type == "s3":
            return {
                "id": str(uuid.uuid4()),
                "name": "S3 Storage",
                "type": "n8n-nodes-base.awsS3",
                "position": position,
                "parameters": {
                    "operation": "upload",
                    "bucketName": workflow_options.get("s3_bucket", "data-diff-reports"),
                    "fileName": "comparison_report_{{$now.format('YYYY-MM-DD_HH-mm-ss')}}.json",
                    "fileContent": "={{JSON.stringify($json)}}"
                },
                "typeVersion": 1
            }
        elif storage_type == "database":
            return {
                "id": str(uuid.uuid4()),
                "name": "Database Storage",
                "type": "n8n-nodes-base.postgres",
                "position": position,
                "parameters": {
                    "operation": "insert",
                    "table": "comparison_results",
                    "columns": "timestamp,source_table,target_table,match_rate,differences,report_data",
                    "values": "={{$now}},={{$json.config.source_table}},={{$json.config.target_table}},={{$json.statistics.match_rate}},={{$json.statistics.differences.total_differences}},={{JSON.stringify($json)}}"
                },
                "typeVersion": 1
            }
        else:
            return {
                "id": str(uuid.uuid4()),
                "name": "File Storage",
                "type": "n8n-nodes-base.writeFile",
                "position": position,
                "parameters": {
                    "fileName": "/tmp/comparison_report_{{$now.format('YYYY-MM-DD_HH-mm-ss')}}.json",
                    "data": "={{JSON.stringify($json, null, 2)}}"
                },
                "typeVersion": 1
            }

    def _build_workflow_connections(self, nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        构建工作流连接
        """
        connections = {}

        # 按照节点顺序建立连接
        for i in range(len(nodes) - 1):
            current_node = nodes[i]
            next_node = nodes[i + 1]

            current_name = current_node["name"]

            if current_name not in connections:
                connections[current_name] = {"main": [[]]}

            connections[current_name]["main"][0].append({
                "node": next_node["name"],
                "type": "main",
                "index": 0
            })

        # 特殊连接处理
        # 源连接和目标连接都需要连接到比对配置节点
        trigger_node = nodes[0]
        source_node = nodes[1] if len(nodes) > 1 else None
        target_node = nodes[2] if len(nodes) > 2 else None
        config_node = nodes[3] if len(nodes) > 3 else None

        if source_node and target_node and config_node:
            # 重新配置连接
            connections = {}

            # 触发器连接到两个数据库连接节点
            connections[trigger_node["name"]] = {
                "main": [
                    [
                        {"node": source_node["name"], "type": "main", "index": 0},
                        {"node": target_node["name"], "type": "main", "index": 0}
                    ]
                ]
            }

            # 两个数据库连接节点都连接到配置节点
            connections[source_node["name"]] = {
                "main": [[{"node": config_node["name"], "type": "main", "index": 0}]]
            }
            connections[target_node["name"]] = {
                "main": [[{"node": config_node["name"], "type": "main", "index": 0}]]
            }

            # 后续节点线性连接
            for i in range(3, len(nodes) - 1):
                current_node = nodes[i]
                next_node = nodes[i + 1]

                connections[current_node["name"]] = {
                    "main": [[{"node": next_node["name"], "type": "main", "index": 0}]]
                }

        return connections

    def validate_workflow(self, workflow: Dict[str, Any]) -> List[str]:
        """
        验证工作流定义

        Args:
            workflow: 工作流定义

        Returns:
            验证错误列表
        """
        errors = []

        # 检查必需字段
        required_fields = ["id", "name", "nodes", "connections"]
        for field in required_fields:
            if field not in workflow:
                errors.append(f"Missing required field: {field}")

        # 检查节点
        if "nodes" in workflow:
            nodes = workflow["nodes"]
            if not nodes:
                errors.append("Workflow must have at least one node")

            node_names = set()
            for node in nodes:
                # 检查节点必需字段
                required_node_fields = ["id", "name", "type", "position"]
                for field in required_node_fields:
                    if field not in node:
                        errors.append(f"Node missing required field: {field}")

                # 检查节点名称唯一性
                node_name = node.get("name")
                if node_name in node_names:
                    errors.append(f"Duplicate node name: {node_name}")
                node_names.add(node_name)

        # 检查连接
        if "connections" in workflow and "nodes" in workflow:
            connections = workflow["connections"]
            nodes = workflow["nodes"]
            node_names = {node["name"] for node in nodes}

            for source_node, targets in connections.items():
                if source_node not in node_names:
                    errors.append(f"Connection source node not found: {source_node}")

                if "main" in targets:
                    for connection_list in targets["main"]:
                        for connection in connection_list:
                            target_node = connection.get("node")
                            if target_node not in node_names:
                                errors.append(f"Connection target node not found: {target_node}")

        return errors

    def export_workflow(self, workflow: Dict[str, Any], format: str = "json") -> str:
        """
        导出工作流定义

        Args:
            workflow: 工作流定义
            format: 导出格式 (json, yaml)

        Returns:
            导出的工作流字符串
        """
        if format.lower() == "yaml":
            import yaml
            return yaml.dump(workflow, default_flow_style=False, allow_unicode=True)
        else:
            return json.dumps(workflow, indent=2, ensure_ascii=False)

    def create_workflow(self, source_config=None, target_config=None, comparison_config=None, workflow_options=None):
        """
        高层封装，创建并返回 N8N 工作流定义
        """
        source_config = source_config or {}
        target_config = target_config or {}
        comparison_config = comparison_config or {}
        workflow_options = workflow_options or {}
        self.logger.info("Creating workflow with source/target config and options")
        workflow = self.build_workflow(
            source_config, target_config, comparison_config, workflow_options
        )
        workflow['created_at'] = datetime.utcnow().isoformat()
        workflow['workflow_id'] = str(uuid.uuid4())
        return workflow
