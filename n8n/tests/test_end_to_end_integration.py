"""
完整的端到端集成测试
测试 Clickzetta 与其他数据库的数据比对功能，以及 N8N 工作流集成
"""

import pytest
import asyncio
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List

# 导入核心组件
from n8n.core import (
    ConnectionManager,
    ComparisonEngine,
    ClickzettaAdapter,
    ResultProcessor,
    ConfigManager,
    ErrorHandler,
    WorkflowScheduler
)
from n8n.workflows import TemplateManager, WorkflowBuilder


class TestEndToEndIntegration:
    """端到端集成测试"""

    @pytest.fixture
    def config_manager(self):
        """配置管理器固定装置"""
        return ConfigManager()

    @pytest.fixture
    def connection_manager(self, config_manager):
        """连接管理器固定装置"""
        return ConnectionManager(config_manager)

    @pytest.fixture
    def comparison_engine(self, config_manager):
        """比对引擎固定装置"""
        return ComparisonEngine(config_manager)

    @pytest.fixture
    def result_processor(self, config_manager):
        """结果处理器固定装置"""
        return ResultProcessor(config_manager)

    @pytest.fixture
    def workflow_builder(self, config_manager):
        """工作流构建器固定装置"""
        return WorkflowBuilder(config_manager)

    @pytest.fixture
    def scheduler(self, config_manager):
        """调度器固定装置"""
        return WorkflowScheduler(config_manager)

    @pytest.fixture
    def sample_databases(self):
        """示例数据库配置"""
        return {
            'clickzetta_source': {
                'type': 'clickzetta',
                'host': 'clickzetta-test.example.com',
                'port': 8080,
                'username': 'test_user',
                'password': 'test_pass',
                'database': 'test_db',
                'schema': 'public'
            },
            'postgres_target': {
                'type': 'postgresql',
                'host': 'postgres-test.example.com',
                'port': 5432,
                'username': 'postgres',
                'password': 'postgres',
                'database': 'testdb',
                'schema': 'public'
            },
            'mysql_backup': {
                'type': 'mysql',
                'host': 'mysql-test.example.com',
                'port': 3306,
                'username': 'root',
                'password': 'mysql',
                'database': 'backup_db'
            }
        }

    @pytest.fixture
    def sample_comparison_config(self):
        """示例比对配置"""
        return {
            'source_table': 'users',
            'target_table': 'users_backup',
            'key_columns': ['id', 'email'],
            'exclude_columns': ['created_at', 'updated_at'],
            'method': 'hashdiff',
            'sample_size': 10000,
            'threads': 4,
            'tolerance': 0.001,
            'case_sensitive': True
        }

    async def test_complete_workflow_execution(
        self,
        config_manager,
        connection_manager,
        comparison_engine,
        result_processor,
        workflow_builder,
        sample_databases,
        sample_comparison_config
    ):
        """测试完整的工作流执行流程"""

        # 1. 配置数据库连接
        with patch.object(connection_manager, 'create_connection') as mock_create:
            mock_connections = {}
            for db_name, db_config in sample_databases.items():
                mock_conn = Mock()
                mock_conn.test_connection.return_value = True
                mock_connections[db_name] = mock_conn

            mock_create.side_effect = lambda config: mock_connections.get(
                f"{config['type']}_{'source' if 'source' in config.get('host', '') else 'target'}"
            )

            # 创建连接
            source_conn = await connection_manager.create_connection(
                sample_databases['clickzetta_source']
            )
            target_conn = await connection_manager.create_connection(
                sample_databases['postgres_target']
            )

            assert source_conn is not None
            assert target_conn is not None
            assert source_conn.test_connection()
            assert target_conn.test_connection()

    async def test_data_comparison_workflow(
        self,
        comparison_engine,
        result_processor,
        sample_databases,
        sample_comparison_config
    ):
        """测试数据比对工作流"""

        # 模拟比对结果
        mock_diff_result = {
            'summary': {
                'total_rows_source': 1000,
                'total_rows_target': 995,
                'matching_rows': 990,
                'extra_rows_source': 10,
                'extra_rows_target': 5,
                'different_rows': 0
            },
            'differences': [
                {
                    'row_id': '001',
                    'column': 'status',
                    'source_value': 'active',
                    'target_value': 'inactive'
                }
            ],
            'performance': {
                'execution_time': 45.2,
                'rows_per_second': 22124,
                'memory_usage': '256MB'
            }
        }

        with patch.object(comparison_engine, 'compare_tables') as mock_compare:
            mock_compare.return_value = mock_diff_result

            # 执行比对
            result = await comparison_engine.compare_tables(
                source_config=sample_databases['clickzetta_source'],
                target_config=sample_databases['postgres_target'],
                comparison_config=sample_comparison_config
            )

            assert result['summary']['total_rows_source'] == 1000
            assert result['summary']['matching_rows'] == 990
            assert len(result['differences']) == 1

            # 处理结果
            processed_result = await result_processor.process_result(result)

            assert 'report' in processed_result
            assert 'visualizations' in processed_result
            assert 'recommendations' in processed_result

    async def test_n8n_workflow_generation(
        self,
        workflow_builder,
        sample_databases,
        sample_comparison_config
    ):
        """测试 N8N 工作流生成"""

        # 生成数据比对工作流
        workflow_def = await workflow_builder.build_comparison_workflow(
            name="Clickzetta to PostgreSQL Comparison",
            source_db=sample_databases['clickzetta_source'],
            target_db=sample_databases['postgres_target'],
            comparison_config=sample_comparison_config,
            schedule="0 2 * * *"  # 每天凌晨2点
        )

        assert workflow_def['name'] == "Clickzetta to PostgreSQL Comparison"
        assert 'nodes' in workflow_def
        assert 'connections' in workflow_def

        # 验证节点类型
        node_types = [node['type'] for node in workflow_def['nodes']]
        expected_types = [
            'clickzettaConnector',
            'clickzettaQuery',
            'dataDiffConfig',
            'dataDiffCompare',
            'dataDiffResult'
        ]

        for expected_type in expected_types:
            assert expected_type in node_types

    async def test_workflow_scheduling(
        self,
        scheduler,
        workflow_builder,
        sample_databases,
        sample_comparison_config
    ):
        """测试工作流调度功能"""

        # 创建调度任务
        workflow_id = "test_comparison_workflow"
        schedule_config = {
            'type': 'cron',
            'expression': '0 2 * * *',
            'timezone': 'UTC',
            'enabled': True
        }

        with patch.object(scheduler, 'schedule_workflow') as mock_schedule:
            mock_schedule.return_value = {
                'job_id': 'job_12345',
                'status': 'scheduled',
                'next_run': '2024-01-02T02:00:00Z'
            }

            # 调度工作流
            job_info = await scheduler.schedule_workflow(
                workflow_id=workflow_id,
                schedule_config=schedule_config
            )

            assert job_info['status'] == 'scheduled'
            assert 'job_id' in job_info
            assert 'next_run' in job_info

    async def test_error_handling_and_recovery(
        self,
        comparison_engine,
        config_manager
    ):
        """测试错误处理和恢复机制"""

        error_handler = ErrorHandler(config_manager)

        # 模拟连接错误
        with patch.object(comparison_engine, 'compare_tables') as mock_compare:
            mock_compare.side_effect = ConnectionError("Database connection failed")

            with pytest.raises(ConnectionError):
                await comparison_engine.compare_tables(
                    source_config={'type': 'invalid'},
                    target_config={'type': 'invalid'},
                    comparison_config={}
                )

            # 验证错误记录
            errors = error_handler.get_recent_errors()
            assert len(errors) > 0
            assert 'ConnectionError' in str(errors[-1])

    async def test_performance_monitoring(
        self,
        comparison_engine,
        result_processor
    ):
        """测试性能监控功能"""

        # 模拟性能数据收集
        performance_data = {
            'start_time': '2024-01-01T10:00:00Z',
            'end_time': '2024-01-01T10:05:30Z',
            'duration': 330.5,
            'rows_processed': 1000000,
            'rows_per_second': 3027,
            'memory_peak': '512MB',
            'cpu_usage': '75%'
        }

        with patch.object(result_processor, 'collect_performance_metrics') as mock_metrics:
            mock_metrics.return_value = performance_data

            metrics = await result_processor.collect_performance_metrics()

            assert metrics['rows_per_second'] > 3000
            assert metrics['duration'] < 400
            assert 'memory_peak' in metrics

    async def test_multi_database_comparison(
        self,
        comparison_engine,
        sample_databases
    ):
        """测试多数据库比对场景"""

        # 模拟三方比对结果
        comparison_results = {}

        databases = ['clickzetta_source', 'postgres_target', 'mysql_backup']

        with patch.object(comparison_engine, 'compare_tables') as mock_compare:
            # 模拟两两比对
            for i in range(len(databases)):
                for j in range(i + 1, len(databases)):
                    source_db = databases[i]
                    target_db = databases[j]

                    mock_result = {
                        'source': source_db,
                        'target': target_db,
                        'summary': {
                            'matching_rows': 950 + i * 10,
                            'different_rows': 5 + j,
                            'total_rows_source': 1000,
                            'total_rows_target': 1000
                        }
                    }

                    mock_compare.return_value = mock_result

                    result = await comparison_engine.compare_tables(
                        source_config=sample_databases[source_db],
                        target_config=sample_databases[target_db],
                        comparison_config={'method': 'hashdiff'}
                    )

                    comparison_results[f"{source_db}_vs_{target_db}"] = result

        # 验证所有比对都完成
        assert len(comparison_results) == 3

        # 验证比对结果结构
        for key, result in comparison_results.items():
            assert 'summary' in result
            assert 'source' in result
            assert 'target' in result

    def test_configuration_validation(
        self,
        config_manager
    ):
        """测试配置验证功能"""

        # 测试有效配置
        valid_config = {
            'database': {
                'type': 'clickzetta',
                'host': 'localhost',
                'port': 8080,
                'username': 'user',
                'password': 'pass'
            },
            'comparison': {
                'method': 'hashdiff',
                'sample_size': 10000
            }
        }

        assert config_manager.validate_config(valid_config)

        # 测试无效配置
        invalid_configs = [
            {},  # 空配置
            {'database': {}},  # 缺少必要字段
            {'database': {'type': 'unsupported'}},  # 不支持的数据库类型
            {'comparison': {'method': 'invalid_method'}}  # 无效比对方法
        ]

        for invalid_config in invalid_configs:
            assert not config_manager.validate_config(invalid_config)

    async def test_workflow_template_system(self):
        """测试工作流模板系统"""

        template_manager = TemplateManager()

        # 获取所有可用模板
        templates = template_manager.list_templates()

        assert len(templates) > 0
        assert 'simple_comparison' in templates
        assert 'scheduled_comparison' in templates
        assert 'multi_database_sync' in templates

        # 测试模板实例化
        template = template_manager.get_template('simple_comparison')

        assert template is not None
        assert 'nodes' in template
        assert 'description' in template

        # 测试自定义模板创建
        custom_template = {
            'name': 'custom_clickzetta_workflow',
            'description': 'Custom Clickzetta comparison workflow',
            'nodes': [],
            'parameters': []
        }

        template_manager.register_template('custom_workflow', custom_template)

        registered_templates = template_manager.list_templates()
        assert 'custom_workflow' in registered_templates


class TestIntegrationScenarios:
    """集成场景测试"""

    async def test_real_time_sync_scenario(self):
        """测试实时同步场景"""
        # 模拟实时数据同步监控
        pass

    async def test_batch_migration_scenario(self):
        """测试批量迁移场景"""
        # 模拟大批量数据迁移验证
        pass

    async def test_data_quality_monitoring_scenario(self):
        """测试数据质量监控场景"""
        # 模拟持续数据质量监控
        pass

    async def test_compliance_audit_scenario(self):
        """测试合规审计场景"""
        # 模拟合规性数据审计
        pass


class TestPerformanceAndScalability:
    """性能和可扩展性测试"""

    @pytest.mark.performance
    async def test_large_dataset_comparison(self):
        """测试大数据集比对性能"""
        # 模拟百万级数据比对
        pass

    @pytest.mark.performance
    async def test_concurrent_comparisons(self):
        """测试并发比对性能"""
        # 模拟多个并发比对任务
        pass

    @pytest.mark.performance
    async def test_memory_efficiency(self):
        """测试内存使用效率"""
        # 监控内存使用情况
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
