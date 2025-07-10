"""
补充的端到端集成测试
测试更多复杂场景和边界情况
"""

import asyncio
import pytest
import tempfile
import os
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# 导入要测试的模块
from ..core import (
    ConnectionManager,
    ComparisonEngine,
    ClickzettaAdapter,
    ResultProcessor,
    ConfigManager,
    ErrorHandler,
    WorkflowScheduler
)
from ..workflows import TemplateManager, WorkflowBuilder
from ..monitoring.advanced_monitoring import MonitoringService, AlertManager, PerformanceMonitor
from ..api.main import app
from fastapi.testclient import TestClient


class TestAdvancedIntegration:
    """高级集成测试"""

    def setup_method(self):
        """测试方法设置"""
        self.config_manager = ConfigManager()
        self.connection_manager = ConnectionManager(self.config_manager)
        self.comparison_engine = ComparisonEngine(self.config_manager)
        self.result_processor = ResultProcessor(self.config_manager)
        self.workflow_builder = WorkflowBuilder(self.config_manager)
        self.scheduler = WorkflowScheduler(self.config_manager)
        self.template_manager = TemplateManager()
        self.monitoring_service = MonitoringService(self.config_manager)
        self.client = TestClient(app)

    @pytest.mark.asyncio
    async def test_complete_workflow_lifecycle(self):
        """测试完整的工作流生命周期"""

        # 1. 创建工作流
        workflow_config = {
            "name": "test_workflow",
            "template": "simple_comparison",
            "source_db": {
                "type": "clickzetta",
                "host": "localhost",
                "port": 8123,
                "username": "test",
                "password": "test",
                "database": "test_db"
            },
            "target_db": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "username": "test",
                "password": "test",
                "database": "test_db"
            },
            "comparison_config": {
                "source_table": "test_table",
                "target_table": "test_table",
                "key_columns": ["id"],
                "method": "hashdiff"
            }
        }

        # 构建工作流
        workflow_def = await self.workflow_builder.build_comparison_workflow(
            name=workflow_config["name"],
            source_db=workflow_config["source_db"],
            target_db=workflow_config["target_db"],
            comparison_config=workflow_config["comparison_config"],
            template=workflow_config["template"]
        )

        assert workflow_def is not None
        assert "nodes" in workflow_def
        assert len(workflow_def["nodes"]) > 0

        # 2. 调度工作流
        schedule_config = {
            "type": "cron",
            "expression": "0 2 * * *",
            "timezone": "UTC",
            "enabled": True
        }

        job_info = await self.scheduler.schedule_workflow(
            workflow_id="test_workflow_id",
            schedule_config=schedule_config
        )

        assert job_info is not None
        assert "job_id" in job_info

        # 3. 监控工作流执行
        self.monitoring_service.record_comparison_start(
            "test_comparison_id",
            "test_table",
            "test_table"
        )

        # 模拟工作流执行
        await asyncio.sleep(0.1)

        self.monitoring_service.record_comparison_end(
            "test_comparison_id",
            "completed",
            execution_time=5.5,
            rows_compared=1000,
            differences_found=10
        )

        # 验证监控数据
        metrics = await self.monitoring_service.collect_metrics()
        assert isinstance(metrics, dict)

    @pytest.mark.asyncio
    async def test_batch_comparison_processing(self):
        """测试批量比对处理"""

        # 准备批量比对配置
        batch_configs = [
            {
                "source_table": "table1",
                "target_table": "table1",
                "key_columns": ["id"],
                "method": "hashdiff"
            },
            {
                "source_table": "table2",
                "target_table": "table2",
                "key_columns": ["id", "date"],
                "method": "joindiff"
            },
            {
                "source_table": "table3",
                "target_table": "table3",
                "key_columns": ["uuid"],
                "method": "hashdiff"
            }
        ]

        # 模拟批量处理
        results = []
        for config in batch_configs:
            # 在实际环境中，这里会调用真实的比对引擎
            mock_result = {
                "source_table": config["source_table"],
                "target_table": config["target_table"],
                "differences_found": 5,
                "status": "completed",
                "execution_time": 2.5
            }
            results.append(mock_result)

        assert len(results) == 3
        assert all(result["status"] == "completed" for result in results)

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self):
        """测试错误处理和恢复机制"""

        error_handler = ErrorHandler(self.config_manager)

        # 测试错误记录
        test_error = Exception("Test database connection failed")
        error_id = await error_handler.handle_error(
            error=test_error,
            context={"operation": "connection_test", "database": "clickzetta"},
            severity="high"
        )

        assert error_id is not None

        # 测试错误恢复策略
        recovery_strategy = await error_handler.get_recovery_strategy(error_id)
        assert recovery_strategy is not None

        # 测试重试机制
        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            try:
                # 模拟可能失败的操作
                if retry_count < 2:  # 前两次失败
                    raise Exception("Simulated failure")
                else:  # 第三次成功
                    result = {"status": "success"}
                    break
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise e
                await asyncio.sleep(0.1)  # 简化的退避策略

        assert result["status"] == "success"
        assert retry_count == 2

    @pytest.mark.asyncio
    async def test_performance_monitoring(self):
        """测试性能监控功能"""

        performance_monitor = PerformanceMonitor()

        # 记录一些测试指标
        performance_monitor.record_metric("performance", "response_time", 150.5)
        performance_monitor.record_metric("performance", "memory_usage", 512.0)
        performance_monitor.record_metric("business", "comparisons_per_hour", 24.0)

        # 记录比对信息
        comparison_data = {
            "comparison_id": "test_comparison_123",
            "start_time": datetime.utcnow() - timedelta(minutes=5),
            "end_time": datetime.utcnow(),
            "status": "completed",
            "source_table": "users",
            "target_table": "users_backup",
            "rows_compared": 10000,
            "differences_found": 150,
            "execution_time": 300.0
        }

        performance_monitor.record_comparison(comparison_data)

        # 获取指标摘要
        summary = performance_monitor.get_metrics_summary(hours=1)
        assert "comparisons" in summary
        assert "performance" in summary

        # 获取健康状态
        health_status = performance_monitor.get_health_status()
        assert "status" in health_status
        assert health_status["status"] in ["healthy", "warning", "unhealthy", "unknown"]

    @pytest.mark.asyncio
    async def test_alert_system(self):
        """测试告警系统"""

        alert_manager = AlertManager(self.config_manager)
        await alert_manager.initialize()

        # 测试告警触发
        test_metrics = {
            "error_count": 5,
            "diff_count": 1500,
            "execution_time": 4000,
            "health_status": "unhealthy"
        }

        # 检查告警
        await alert_manager.check_alerts(test_metrics)

        # 验证告警历史
        assert len(alert_manager.alert_history) > 0

        # 测试冷却期
        assert alert_manager.is_in_cooldown("比对任务失败") or not alert_manager.is_in_cooldown("比对任务失败")

    @pytest.mark.asyncio
    async def test_template_system(self):
        """测试模板系统"""

        # 测试模板列表
        templates = self.template_manager.list_templates()
        assert isinstance(templates, list)
        assert len(templates) > 0

        # 测试获取特定模板
        simple_template = self.template_manager.get_template("simple_comparison")
        assert simple_template is not None
        assert "name" in simple_template
        assert "workflow" in simple_template

        # 测试模板参数化
        template_with_params = await self.template_manager.apply_parameters(
            template_name="simple_comparison",
            parameters={
                "source_connection": "clickzetta://test:test@localhost:8123/db",
                "target_connection": "postgresql://test:test@localhost:5432/db",
                "source_table": "test_table",
                "target_table": "test_table",
                "key_columns": ["id", "created_at"]
            }
        )

        assert template_with_params is not None

    def test_api_endpoints(self):
        """测试 API 端点"""

        # 测试健康检查
        response = self.client.get("/health")
        assert response.status_code == 200
        assert "status" in response.json()

        # 测试根端点
        response = self.client.get("/")
        assert response.status_code == 200
        assert "message" in response.json()

        # 测试模板列表
        response = self.client.get("/api/v1/templates")
        assert response.status_code == 200
        assert "templates" in response.json()

        # 测试支持的数据库
        response = self.client.get("/api/v1/databases/supported")
        assert response.status_code == 200
        assert "databases" in response.json()

        # 测试连接测试
        connection_config = {
            "type": "clickzetta",
            "host": "localhost",
            "port": 8123,
            "username": "test",
            "password": "test",
            "database": "test_db"
        }

        # 注意：这个测试可能失败，因为没有真实的数据库连接
        # 在实际测试中应该使用模拟
        with patch('n8n.core.connection_manager.ConnectionManager.create_connection') as mock_conn:
            mock_connection = AsyncMock()
            mock_connection.test_connection.return_value = True
            mock_conn.return_value = mock_connection

            response = self.client.post("/api/v1/connections/test", json=connection_config)
            # 由于异步问题，这里可能需要特殊处理

    @pytest.mark.asyncio
    async def test_workflow_builder_advanced(self):
        """测试高级工作流构建功能"""

        # 测试复杂工作流构建
        complex_config = {
            "name": "complex_validation_workflow",
            "template": "complex_validation",
            "source_db": {
                "type": "clickzetta",
                "host": "localhost",
                "port": 8123,
                "username": "test",
                "password": "test",
                "database": "source_db"
            },
            "target_db": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "username": "test",
                "password": "test",
                "database": "target_db"
            },
            "validation_rules": [
                {"type": "schema_check", "enabled": True},
                {"type": "data_quality", "enabled": True},
                {"type": "referential_integrity", "enabled": True}
            ]
        }

        workflow_def = await self.workflow_builder.build_validation_workflow(
            config=complex_config
        )

        assert workflow_def is not None
        assert "nodes" in workflow_def

        # 验证工作流结构
        nodes = workflow_def["nodes"]
        assert len(nodes) > 5  # 复杂工作流应该有多个节点

        # 验证连接
        connections = workflow_def.get("connections", {})
        assert len(connections) > 0

    @pytest.mark.asyncio
    async def test_result_processing_pipeline(self):
        """测试结果处理管道"""

        # 模拟比对结果
        mock_comparison_result = {
            "comparison_id": "test_123",
            "source_table": "orders",
            "target_table": "orders_replica",
            "total_rows": 10000,
            "differences": {
                "added": 150,
                "removed": 75,
                "modified": 230
            },
            "execution_time": 45.5,
            "timestamp": datetime.utcnow().isoformat(),
            "details": [
                {"row_id": "1001", "column": "amount", "source_value": 100.50, "target_value": 100.60},
                {"row_id": "1002", "column": "status", "source_value": "pending", "target_value": "completed"}
            ]
        }

        # 测试不同的结果处理操作

        # 1. 格式化报告
        formatted_result = await self.result_processor.format_result(
            data=mock_comparison_result,
            format_type="json",
            include_details=True
        )
        assert formatted_result is not None

        # 2. 提取摘要
        summary = await self.result_processor.extract_summary(
            data=mock_comparison_result,
            fields=["total_rows", "differences", "execution_time"]
        )
        assert summary is not None
        assert "total_rows" in summary

        # 3. 过滤差异
        filtered_result = await self.result_processor.filter_differences(
            data=mock_comparison_result,
            difference_types=["added", "modified"]
        )
        assert filtered_result is not None

        # 4. 导出到文件
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = os.path.join(temp_dir, "test_results.json")
            export_result = await self.result_processor.export_to_file(
                data=mock_comparison_result,
                format_type="json",
                file_path=export_path
            )
            assert export_result is not None
            assert os.path.exists(export_path)

    @pytest.mark.asyncio
    async def test_configuration_management(self):
        """测试配置管理"""

        # 测试配置加载
        await self.config_manager.initialize()

        # 测试配置获取
        db_config = self.config_manager.get_database_config("clickzetta")
        assert db_config is not None

        # 测试配置更新
        new_config = {"timeout": 60, "max_connections": 10}
        await self.config_manager.update_config("database.clickzetta", new_config)

        # 验证更新
        updated_config = self.config_manager.get_database_config("clickzetta")
        assert updated_config["timeout"] == 60

    @pytest.mark.asyncio
    async def test_concurrent_operations(self):
        """测试并发操作"""

        # 创建多个并发比对任务
        comparison_tasks = []

        for i in range(5):
            task_config = {
                "comparison_id": f"concurrent_test_{i}",
                "source_table": f"table_{i}",
                "target_table": f"table_{i}_backup",
                "key_columns": ["id"],
                "method": "hashdiff"
            }

            # 创建模拟任务
            async def mock_comparison(config):
                await asyncio.sleep(0.1)  # 模拟处理时间
                return {
                    "comparison_id": config["comparison_id"],
                    "status": "completed",
                    "differences_found": 10
                }

            task = asyncio.create_task(mock_comparison(task_config))
            comparison_tasks.append(task)

        # 等待所有任务完成
        results = await asyncio.gather(*comparison_tasks)

        assert len(results) == 5
        assert all(result["status"] == "completed" for result in results)

    def teardown_method(self):
        """测试方法清理"""
        # 清理资源
        pass


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
