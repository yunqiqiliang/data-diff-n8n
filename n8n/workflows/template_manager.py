"""
工作流模板管理器
管理预定义的数据比对工作流模板
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from .templates import WORKFLOW_TEMPLATES


class TemplateManager:
    """
    工作流模板管理器
    提供各种预定义的数据比对工作流模板
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.templates = {**WORKFLOW_TEMPLATES, **self._initialize_legacy_templates()}

    def _initialize_legacy_templates(self) -> Dict[str, Dict[str, Any]]:
        """
        初始化遗留工作流模板（保持向后兼容）
        """
        return {
            "clickzetta_postgresql_migration": self._clickzetta_postgresql_migration_template(),
            "clickzetta_mysql_sync": self._clickzetta_mysql_sync_template(),
            "clickzetta_oracle_migration": self._clickzetta_oracle_migration_template(),
            "clickzetta_snowflake_integration": self._clickzetta_snowflake_integration_template(),
            "clickzetta_etl_validation": self._clickzetta_etl_validation_template(),
            "clickzetta_quality_monitoring": self._clickzetta_quality_monitoring_template()
        }

    def get_template(self, template_name: str) -> Optional[Dict[str, Any]]:
        """
        获取指定的工作流模板

        Args:
            template_name: 模板名称

        Returns:
            工作流模板
        """
        return self.templates.get(template_name)

    def list_templates(self) -> List[Dict[str, Any]]:
        """
        列出所有可用的模板

        Returns:
            模板列表
        """
        template_list = []
        for name, template in self.templates.items():
            template_list.append({
                "name": name,
                "title": template.get("title", ""),
                "description": template.get("description", ""),
                "category": template.get("category", ""),
                "use_case": template.get("use_case", "")
            })
        return template_list

    def get_templates_by_category(self, category: str) -> List[Dict[str, Any]]:
        """
        按类别获取模板

        Args:
            category: 模板类别

        Returns:
            模板列表
        """
        templates = []
        for name, template in self.templates.items():
            if template.get("category") == category:
                templates.append({
                    "name": name,
                    "template": template
                })
        return templates

    def _clickzetta_postgresql_migration_template(self) -> Dict[str, Any]:
        """
        Clickzetta 与 PostgreSQL 数据迁移验证模板
        """
        return {
            "title": "Clickzetta ↔ PostgreSQL Data Migration Validation",
            "description": "Validate data migration between Clickzetta and PostgreSQL databases",
            "category": "migration",
            "use_case": "Data migration from PostgreSQL to Clickzetta or vice versa",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "workflow": {
                "nodes": [
                    {
                        "id": "trigger",
                        "type": "trigger",
                        "name": "Migration Trigger",
                        "parameters": {
                            "rule": {
                                "interval": [{"field": "hours", "hoursInterval": 6}]
                            }
                        },
                        "position": [100, 100]
                    },
                    {
                        "id": "source_config",
                        "type": "clickzetta_connector",
                        "name": "Source Clickzetta",
                        "parameters": {
                            "operation": "create_connection",
                            "host": "{{$env.CLICKZETTA_HOST}}",
                            "port": "{{$env.CLICKZETTA_PORT}}",
                            "database": "{{$env.CLICKZETTA_DB}}",
                            "username": "{{$env.CLICKZETTA_USER}}",
                            "password": "{{$env.CLICKZETTA_PASSWORD}}"
                        },
                        "position": [300, 100]
                    },
                    {
                        "id": "target_config",
                        "type": "postgresql_connector",
                        "name": "Target PostgreSQL",
                        "parameters": {
                            "host": "{{$env.POSTGRES_HOST}}",
                            "port": "{{$env.POSTGRES_PORT}}",
                            "database": "{{$env.POSTGRES_DB}}",
                            "username": "{{$env.POSTGRES_USER}}",
                            "password": "{{$env.POSTGRES_PASSWORD}}"
                        },
                        "position": [300, 200]
                    },
                    {
                        "id": "comparison_config",
                        "type": "data_diff_config",
                        "name": "Migration Comparison Config",
                        "parameters": {
                            "source_database": "clickzetta",
                            "target_database": "postgresql",
                            "source_table": "{{$env.SOURCE_TABLE}}",
                            "target_table": "{{$env.TARGET_TABLE}}",
                            "algorithm": "joindiff",
                            "key_columns": "{{$env.KEY_COLUMNS}}",
                            "enable_sampling": True,
                            "sample_size": 100000,
                            "confidence_level": 0.95,
                            "parallel_workers": 4
                        },
                        "position": [500, 150]
                    },
                    {
                        "id": "data_comparison",
                        "type": "data_diff_compare",
                        "name": "Execute Migration Validation",
                        "parameters": {
                            "execution_mode": "sync",
                            "timeout": 3600,
                            "report_format": "json",
                            "include_sample_data": True,
                            "max_sample_size": 50
                        },
                        "position": [700, 150]
                    },
                    {
                        "id": "result_analysis",
                        "type": "data_diff_result",
                        "name": "Migration Result Analysis",
                        "parameters": {
                            "analysis_type": "migration",
                            "report_template": "migration",
                            "alert_thresholds": {
                                "max_difference_rate": 0.01,
                                "min_match_rate": 0.99,
                                "max_missing_rows": 10
                            },
                            "include_visualizations": True,
                            "include_recommendations": True
                        },
                        "position": [900, 150]
                    },
                    {
                        "id": "notification",
                        "type": "email",
                        "name": "Migration Report Email",
                        "parameters": {
                            "to": "{{$env.NOTIFICATION_EMAIL}}",
                            "subject": "Clickzetta ↔ PostgreSQL Migration Validation Report",
                            "emailFormat": "html",
                            "message": "Migration validation completed. Please see attached report."
                        },
                        "position": [1100, 150]
                    }
                ],
                "connections": [
                    {"source": "trigger", "target": "source_config"},
                    {"source": "trigger", "target": "target_config"},
                    {"source": "source_config", "target": "comparison_config"},
                    {"source": "target_config", "target": "comparison_config"},
                    {"source": "comparison_config", "target": "data_comparison"},
                    {"source": "data_comparison", "target": "result_analysis"},
                    {"source": "result_analysis", "target": "notification"}
                ]
            },
            "environment_variables": {
                "CLICKZETTA_HOST": "your-clickzetta-host",
                "CLICKZETTA_PORT": "8123",
                "CLICKZETTA_DB": "default",
                "CLICKZETTA_USER": "default",
                "CLICKZETTA_PASSWORD": "your-password",
                "POSTGRES_HOST": "your-postgres-host",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "your_database",
                "POSTGRES_USER": "postgres",
                "POSTGRES_PASSWORD": "your-password",
                "SOURCE_TABLE": "users",
                "TARGET_TABLE": "users",
                "KEY_COLUMNS": "id",
                "NOTIFICATION_EMAIL": "admin@company.com"
            }
        }

    def _clickzetta_mysql_sync_template(self) -> Dict[str, Any]:
        """
        Clickzetta 与 MySQL 数据同步验证模板
        """
        return {
            "title": "Clickzetta ↔ MySQL Real-time Sync Validation",
            "description": "Monitor and validate real-time data synchronization between Clickzetta and MySQL",
            "category": "sync",
            "use_case": "Real-time data synchronization monitoring",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "workflow": {
                "nodes": [
                    {
                        "id": "trigger",
                        "type": "trigger",
                        "name": "Sync Monitor Trigger",
                        "parameters": {
                            "rule": {
                                "interval": [{"field": "minutes", "minutesInterval": 15}]
                            }
                        },
                        "position": [100, 100]
                    },
                    {
                        "id": "sync_config",
                        "type": "data_diff_config",
                        "name": "Sync Validation Config",
                        "parameters": {
                            "source_database": "mysql",
                            "target_database": "clickzetta",
                            "algorithm": "joindiff",
                            "enable_sampling": True,
                            "sample_size": 10000,
                            "time_range_filter": True,
                            "time_column": "updated_at",
                            "time_range": "15 minutes",
                            "tolerance": 0.0,
                            "parallel_workers": 2
                        },
                        "position": [300, 100]
                    },
                    {
                        "id": "sync_validation",
                        "type": "data_diff_compare",
                        "name": "Sync Validation",
                        "parameters": {
                            "execution_mode": "sync",
                            "timeout": 600,
                            "report_format": "json"
                        },
                        "position": [500, 100]
                    },
                    {
                        "id": "sync_analysis",
                        "type": "data_diff_result",
                        "name": "Sync Quality Analysis",
                        "parameters": {
                            "analysis_type": "quality_assessment",
                            "alert_thresholds": {
                                "max_difference_rate": 0.001,
                                "min_match_rate": 0.999,
                                "max_missing_rows": 5
                            }
                        },
                        "position": [700, 100]
                    },
                    {
                        "id": "alert_check",
                        "type": "if",
                        "name": "Check for Alerts",
                        "parameters": {
                            "conditions": {
                                "boolean": [
                                    {
                                        "value1": "={{$json.alerts.length}}",
                                        "operation": "larger",
                                        "value2": 0
                                    }
                                ]
                            }
                        },
                        "position": [900, 100]
                    },
                    {
                        "id": "alert_notification",
                        "type": "slack",
                        "name": "Sync Alert",
                        "parameters": {
                            "channel": "#data-alerts",
                            "text": "⚠️ Data sync issue detected between MySQL and Clickzetta",
                            "attachments": [
                                {
                                    "color": "warning",
                                    "title": "Sync Validation Alert",
                                    "text": "{{$json.alerts}}"
                                }
                            ]
                        },
                        "position": [1100, 50]
                    },
                    {
                        "id": "success_log",
                        "type": "set",
                        "name": "Log Success",
                        "parameters": {
                            "values": {
                                "status": "sync_validation_success",
                                "timestamp": "={{$now}}",
                                "match_rate": "={{$json.analysis.basic_metrics.match_percentage}}"
                            }
                        },
                        "position": [1100, 150]
                    }
                ],
                "connections": [
                    {"source": "trigger", "target": "sync_config"},
                    {"source": "sync_config", "target": "sync_validation"},
                    {"source": "sync_validation", "target": "sync_analysis"},
                    {"source": "sync_analysis", "target": "alert_check"},
                    {"source": "alert_check", "target": "alert_notification", "condition": "true"},
                    {"source": "alert_check", "target": "success_log", "condition": "false"}
                ]
            }
        }

    def _clickzetta_oracle_migration_template(self) -> Dict[str, Any]:
        """
        Clickzetta 与 Oracle 遗留系统迁移模板
        """
        return {
            "title": "Oracle → Clickzetta Legacy System Migration",
            "description": "Comprehensive validation for Oracle to Clickzetta migration projects",
            "category": "migration",
            "use_case": "Enterprise legacy system migration from Oracle to Clickzetta",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "workflow": {
                "nodes": [
                    {
                        "id": "migration_trigger",
                        "type": "webhook",
                        "name": "Migration Trigger",
                        "parameters": {
                            "httpMethod": "POST",
                            "path": "oracle-migration-validation"
                        },
                        "position": [100, 200]
                    },
                    {
                        "id": "pre_validation",
                        "type": "data_diff_config",
                        "name": "Pre-Migration Validation",
                        "parameters": {
                            "source_database": "oracle",
                            "target_database": "clickzetta",
                            "algorithm": "hashdiff",
                            "enable_sampling": True,
                            "sample_size": 50000,
                            "confidence_level": 0.99,
                            "parallel_workers": 6,
                            "tolerance": 0.0001
                        },
                        "position": [300, 200]
                    },
                    {
                        "id": "schema_validation",
                        "type": "custom_function",
                        "name": "Schema Compatibility Check",
                        "parameters": {
                            "function": "validate_schema_compatibility",
                            "source_schema": "oracle",
                            "target_schema": "clickzetta"
                        },
                        "position": [500, 150]
                    },
                    {
                        "id": "data_validation",
                        "type": "data_diff_compare",
                        "name": "Data Migration Validation",
                        "parameters": {
                            "execution_mode": "sync",
                            "timeout": 7200,
                            "report_format": "excel",
                            "include_sample_data": True
                        },
                        "position": [500, 250]
                    },
                    {
                        "id": "migration_analysis",
                        "type": "data_diff_result",
                        "name": "Migration Quality Analysis",
                        "parameters": {
                            "analysis_type": "full_analysis",
                            "report_template": "migration",
                            "alert_thresholds": {
                                "max_difference_rate": 0.005,
                                "min_match_rate": 0.995,
                                "max_missing_rows": 50
                            },
                            "include_recommendations": True
                        },
                        "position": [700, 200]
                    },
                    {
                        "id": "generate_report",
                        "type": "function",
                        "name": "Generate Migration Report",
                        "parameters": {
                            "function": "generate_comprehensive_report",
                            "include_executive_summary": True,
                            "include_technical_details": True,
                            "format": "pdf"
                        },
                        "position": [900, 200]
                    },
                    {
                        "id": "stakeholder_notification",
                        "type": "email",
                        "name": "Stakeholder Notification",
                        "parameters": {
                            "to": ["{{$env.PROJECT_MANAGER}}", "{{$env.DBA_TEAM}}", "{{$env.BUSINESS_OWNER}}"],
                            "subject": "Oracle → Clickzetta Migration Validation Complete",
                            "emailFormat": "html",
                            "attachments": "migration_report.pdf"
                        },
                        "position": [1100, 200]
                    }
                ],
                "connections": [
                    {"source": "migration_trigger", "target": "pre_validation"},
                    {"source": "pre_validation", "target": "schema_validation"},
                    {"source": "pre_validation", "target": "data_validation"},
                    {"source": "schema_validation", "target": "migration_analysis"},
                    {"source": "data_validation", "target": "migration_analysis"},
                    {"source": "migration_analysis", "target": "generate_report"},
                    {"source": "generate_report", "target": "stakeholder_notification"}
                ]
            }
        }

    def _clickzetta_snowflake_integration_template(self) -> Dict[str, Any]:
        """
        Clickzetta 与 Snowflake 云数仓集成模板
        """
        return {
            "title": "Clickzetta ↔ Snowflake Cloud Data Warehouse Integration",
            "description": "Validate data consistency between Clickzetta and Snowflake data warehouses",
            "category": "integration",
            "use_case": "Multi-cloud data warehouse integration and validation",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "workflow": {
                "nodes": [
                    {
                        "id": "schedule_trigger",
                        "type": "cron",
                        "name": "Daily Integration Check",
                        "parameters": {
                            "rule": {
                                "hour": 2,
                                "minute": 0,
                                "timezone": "UTC"
                            }
                        },
                        "position": [100, 200]
                    },
                    {
                        "id": "integration_config",
                        "type": "data_diff_config",
                        "name": "Cloud Integration Config",
                        "parameters": {
                            "source_database": "clickzetta",
                            "target_database": "snowflake",
                            "algorithm": "hashdiff",
                            "enable_sampling": True,
                            "sample_size": 200000,
                            "sampling_method": "stratified",
                            "confidence_level": 0.95,
                            "parallel_workers": 8,
                            "max_memory_mb": 4096
                        },
                        "position": [300, 200]
                    },
                    {
                        "id": "cloud_comparison",
                        "type": "data_diff_compare",
                        "name": "Cloud Data Validation",
                        "parameters": {
                            "execution_mode": "async",
                            "timeout": 10800,
                            "report_format": "json"
                        },
                        "position": [500, 200]
                    },
                    {
                        "id": "wait_completion",
                        "type": "wait",
                        "name": "Wait for Completion",
                        "parameters": {
                            "amount": 30,
                            "unit": "minutes"
                        },
                        "position": [700, 200]
                    },
                    {
                        "id": "integration_analysis",
                        "type": "data_diff_result",
                        "name": "Integration Analysis",
                        "parameters": {
                            "analysis_type": "full_analysis",
                            "report_template": "executive",
                            "include_visualizations": True
                        },
                        "position": [900, 200]
                    },
                    {
                        "id": "dashboard_update",
                        "type": "http_request",
                        "name": "Update Dashboard",
                        "parameters": {
                            "method": "POST",
                            "url": "{{$env.DASHBOARD_API}}/update",
                            "body": {
                                "source": "clickzetta_snowflake_integration",
                                "metrics": "={{$json.analysis}}"
                            }
                        },
                        "position": [1100, 150]
                    },
                    {
                        "id": "archive_results",
                        "type": "function",
                        "name": "Archive Results",
                        "parameters": {
                            "function": "archive_to_s3",
                            "bucket": "{{$env.ARCHIVE_BUCKET}}",
                            "key": "integration_reports/{{$now.format('YYYY/MM/DD')}}/report.json"
                        },
                        "position": [1100, 250]
                    }
                ],
                "connections": [
                    {"source": "schedule_trigger", "target": "integration_config"},
                    {"source": "integration_config", "target": "cloud_comparison"},
                    {"source": "cloud_comparison", "target": "wait_completion"},
                    {"source": "wait_completion", "target": "integration_analysis"},
                    {"source": "integration_analysis", "target": "dashboard_update"},
                    {"source": "integration_analysis", "target": "archive_results"}
                ]
            }
        }

    def _clickzetta_etl_validation_template(self) -> Dict[str, Any]:
        """
        Clickzetta ETL 流程验证模板
        """
        return {
            "title": "Clickzetta ETL Pipeline Validation",
            "description": "Validate ETL process results in Clickzetta data warehouse",
            "category": "etl",
            "use_case": "ETL pipeline quality assurance and validation",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "workflow": {
                "nodes": [
                    {
                        "id": "etl_completion_trigger",
                        "type": "webhook",
                        "name": "ETL Completion Webhook",
                        "parameters": {
                            "httpMethod": "POST",
                            "path": "etl-validation",
                            "authentication": "headerAuth"
                        },
                        "position": [100, 200]
                    },
                    {
                        "id": "extract_etl_info",
                        "type": "set",
                        "name": "Extract ETL Information",
                        "parameters": {
                            "values": {
                                "etl_job_id": "={{$json.job_id}}",
                                "source_tables": "={{$json.source_tables}}",
                                "target_table": "={{$json.target_table}}",
                                "execution_timestamp": "={{$json.timestamp}}"
                            }
                        },
                        "position": [300, 200]
                    },
                    {
                        "id": "etl_validation_config",
                        "type": "data_diff_config",
                        "name": "ETL Validation Config",
                        "parameters": {
                            "source_database": "{{$json.source_db_type}}",
                            "target_database": "clickzetta",
                            "algorithm": "joindiff",
                            "enable_sampling": False,
                            "time_range_filter": True,
                            "time_column": "etl_timestamp",
                            "time_range": "1 hour",
                            "parallel_workers": 4
                        },
                        "position": [500, 200]
                    },
                    {
                        "id": "etl_validation",
                        "type": "data_diff_compare",
                        "name": "ETL Result Validation",
                        "parameters": {
                            "execution_mode": "sync",
                            "timeout": 1800,
                            "report_format": "json",
                            "include_sample_data": True
                        },
                        "position": [700, 200]
                    },
                    {
                        "id": "etl_analysis",
                        "type": "data_diff_result",
                        "name": "ETL Quality Analysis",
                        "parameters": {
                            "analysis_type": "quality_assessment",
                            "report_template": "technical",
                            "alert_thresholds": {
                                "max_difference_rate": 0.01,
                                "min_match_rate": 0.99,
                                "max_missing_rows": 100
                            }
                        },
                        "position": [900, 200]
                    },
                    {
                        "id": "quality_gate",
                        "type": "if",
                        "name": "Quality Gate Check",
                        "parameters": {
                            "conditions": {
                                "boolean": [
                                    {
                                        "value1": "={{$json.analysis.quality_assessment.quality_grade}}",
                                        "operation": "equal",
                                        "value2": "A"
                                    }
                                ]
                            }
                        },
                        "position": [1100, 200]
                    },
                    {
                        "id": "etl_success",
                        "type": "http_request",
                        "name": "Mark ETL Success",
                        "parameters": {
                            "method": "POST",
                            "url": "{{$env.ETL_API}}/jobs/{{$json.etl_job_id}}/complete",
                            "body": {
                                "status": "success",
                                "quality_score": "={{$json.analysis.quality_assessment.quality_score}}"
                            }
                        },
                        "position": [1300, 150]
                    },
                    {
                        "id": "etl_failure",
                        "type": "http_request",
                        "name": "Mark ETL Failure",
                        "parameters": {
                            "method": "POST",
                            "url": "{{$env.ETL_API}}/jobs/{{$json.etl_job_id}}/fail",
                            "body": {
                                "status": "failed",
                                "reason": "Quality validation failed",
                                "details": "={{$json.alerts}}"
                            }
                        },
                        "position": [1300, 250]
                    }
                ],
                "connections": [
                    {"source": "etl_completion_trigger", "target": "extract_etl_info"},
                    {"source": "extract_etl_info", "target": "etl_validation_config"},
                    {"source": "etl_validation_config", "target": "etl_validation"},
                    {"source": "etl_validation", "target": "etl_analysis"},
                    {"source": "etl_analysis", "target": "quality_gate"},
                    {"source": "quality_gate", "target": "etl_success", "condition": "true"},
                    {"source": "quality_gate", "target": "etl_failure", "condition": "false"}
                ]
            }
        }

    def _clickzetta_quality_monitoring_template(self) -> Dict[str, Any]:
        """
        Clickzetta 数据质量监控模板
        """
        return {
            "title": "Clickzetta Data Quality Continuous Monitoring",
            "description": "Continuous monitoring of data quality in Clickzetta environment",
            "category": "monitoring",
            "use_case": "Production data quality monitoring and alerting",
            "version": "1.0",
            "created_date": datetime.now().isoformat(),
            "workflow": {
                "nodes": [
                    {
                        "id": "monitoring_trigger",
                        "type": "cron",
                        "name": "Quality Monitor Trigger",
                        "parameters": {
                            "rule": {
                                "hour": [8, 14, 20],
                                "minute": 0,
                                "timezone": "UTC"
                            }
                        },
                        "position": [100, 200]
                    },
                    {
                        "id": "quality_config",
                        "type": "data_diff_config",
                        "name": "Quality Check Config",
                        "parameters": {
                            "source_database": "clickzetta",
                            "target_database": "clickzetta",
                            "source_table": "prod.{{$env.TABLE_NAME}}",
                            "target_table": "backup.{{$env.TABLE_NAME}}",
                            "algorithm": "joindiff",
                            "enable_sampling": True,
                            "sample_size": 25000,
                            "time_range_filter": True,
                            "time_column": "created_at",
                            "time_range": "8 hours"
                        },
                        "position": [300, 200]
                    },
                    {
                        "id": "quality_validation",
                        "type": "data_diff_compare",
                        "name": "Quality Validation",
                        "parameters": {
                            "execution_mode": "sync",
                            "timeout": 900,
                            "report_format": "json"
                        },
                        "position": [500, 200]
                    },
                    {
                        "id": "quality_analysis",
                        "type": "data_diff_result",
                        "name": "Quality Analysis",
                        "parameters": {
                            "analysis_type": "quality_assessment",
                            "report_template": "quality",
                            "alert_thresholds": {
                                "max_difference_rate": 0.001,
                                "min_match_rate": 0.999,
                                "max_missing_rows": 5
                            }
                        },
                        "position": [700, 200]
                    },
                    {
                        "id": "trend_analysis",
                        "type": "function",
                        "name": "Trend Analysis",
                        "parameters": {
                            "function": "analyze_quality_trends",
                            "lookback_days": 7,
                            "metrics": ["match_rate", "difference_count", "processing_time"]
                        },
                        "position": [900, 150]
                    },
                    {
                        "id": "update_metrics",
                        "type": "http_request",
                        "name": "Update Quality Metrics",
                        "parameters": {
                            "method": "POST",
                            "url": "{{$env.METRICS_API}}/quality",
                            "body": {
                                "timestamp": "={{$now}}",
                                "table": "{{$env.TABLE_NAME}}",
                                "quality_score": "={{$json.analysis.quality_assessment.quality_score}}",
                                "match_rate": "={{$json.analysis.basic_metrics.match_rate}}",
                                "trend": "={{$json.trend_analysis}}"
                            }
                        },
                        "position": [900, 250]
                    },
                    {
                        "id": "alert_check",
                        "type": "if",
                        "name": "Check Quality Alerts",
                        "parameters": {
                            "conditions": {
                                "boolean": [
                                    {
                                        "value1": "={{$json.alerts.length}}",
                                        "operation": "larger",
                                        "value2": 0
                                    }
                                ]
                            }
                        },
                        "position": [1100, 200]
                    },
                    {
                        "id": "quality_alert",
                        "type": "teams",
                        "name": "Quality Alert",
                        "parameters": {
                            "webhook": "{{$env.TEAMS_WEBHOOK}}",
                            "title": "🚨 Data Quality Alert - Clickzetta",
                            "text": "Quality issues detected in table {{$env.TABLE_NAME}}",
                            "sections": [
                                {
                                    "activityTitle": "Quality Metrics",
                                    "facts": [
                                        {
                                            "name": "Match Rate",
                                            "value": "={{$json.analysis.basic_metrics.match_percentage}}%"
                                        },
                                        {
                                            "name": "Quality Score",
                                            "value": "={{$json.analysis.quality_assessment.quality_score}}"
                                        }
                                    ]
                                }
                            ]
                        },
                        "position": [1300, 200]
                    }
                ],
                "connections": [
                    {"source": "monitoring_trigger", "target": "quality_config"},
                    {"source": "quality_config", "target": "quality_validation"},
                    {"source": "quality_validation", "target": "quality_analysis"},
                    {"source": "quality_analysis", "target": "trend_analysis"},
                    {"source": "quality_analysis", "target": "update_metrics"},
                    {"source": "quality_analysis", "target": "alert_check"},
                    {"source": "alert_check", "target": "quality_alert", "condition": "true"}
                ]
            }
        }

    def customize_template(self, template_name: str, customizations: Dict[str, Any]) -> Dict[str, Any]:
        """
        自定义工作流模板

        Args:
            template_name: 模板名称
            customizations: 自定义配置

        Returns:
            自定义后的模板
        """
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template {template_name} not found")

        customized_template = template.copy()

        # 应用自定义配置
        if "environment_variables" in customizations:
            customized_template["environment_variables"].update(
                customizations["environment_variables"]
            )

        if "workflow_parameters" in customizations:
            # 更新工作流参数
            for node_id, params in customizations["workflow_parameters"].items():
                for node in customized_template["workflow"]["nodes"]:
                    if node["id"] == node_id:
                        node["parameters"].update(params)
                        break

        if "alert_thresholds" in customizations:
            # 更新告警阈值
            for node in customized_template["workflow"]["nodes"]:
                if node["type"] == "data_diff_result":
                    node["parameters"]["alert_thresholds"].update(
                        customizations["alert_thresholds"]
                    )

        # 添加自定义标记
        customized_template["customized"] = True
        customized_template["customization_date"] = datetime.now().isoformat()

        return customized_template
