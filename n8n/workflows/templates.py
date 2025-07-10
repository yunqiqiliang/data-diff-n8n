"""
示例工作流模板定义
"""

# 简单数据比对工作流模板
SIMPLE_COMPARISON_TEMPLATE = {
    "name": "simple_comparison",
    "version": "1.0.0",
    "description": "Simple data comparison workflow",
    "parameters": [
        {
            "name": "source_connection",
            "type": "string",
            "required": True,
            "description": "Source database connection string"
        },
        {
            "name": "target_connection",
            "type": "string",
            "required": True,
            "description": "Target database connection string"
        },
        {
            "name": "source_table",
            "type": "string",
            "required": True,
            "description": "Source table name"
        },
        {
            "name": "target_table",
            "type": "string",
            "required": True,
            "description": "Target table name"
        },
        {
            "name": "key_columns",
            "type": "array",
            "required": True,
            "description": "Primary key columns"
        }
    ],
    "workflow": {
        "nodes": [
            {
                "id": "start",
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [100, 100],
                "parameters": {}
            },
            {
                "id": "clickzetta_connection",
                "type": "clickzettaConnector",
                "typeVersion": 1,
                "position": [300, 100],
                "parameters": {
                    "connection": "{{$parameter.source_connection}}",
                    "operation": "testConnection"
                }
            },
            {
                "id": "data_comparison",
                "type": "dataComparison",
                "typeVersion": 1,
                "position": [500, 100],
                "parameters": {
                    "resource": "tableComparison",
                    "operation": "compareTables",
                    "sourceConnection": "{{$parameter.source_connection}}",
                    "targetConnection": "{{$parameter.target_connection}}",
                    "sourceTable": "{{$parameter.source_table}}",
                    "targetTable": "{{$parameter.target_table}}",
                    "primaryKeyColumns": "{{$parameter.key_columns.join(',')}}"
                }
            },
            {
                "id": "result_processor",
                "type": "resultProcessor",
                "typeVersion": 1,
                "position": [700, 100],
                "parameters": {
                    "operation": "formatReport",
                    "reportFormat": "json",
                    "includeDetails": True
                }
            },
            {
                "id": "notification",
                "type": "notificationHandler",
                "typeVersion": 1,
                "position": [900, 100],
                "parameters": {
                    "notificationType": "email",
                    "triggerConditions": ["differences_found", "error"],
                    "emailRecipients": "admin@example.com",
                    "emailSubject": "Data Comparison Results - {{$node.data_comparison.json.sourceTable}}"
                }
            }
        ],
        "connections": {
            "start": {
                "main": [
                    [
                        {
                            "node": "clickzetta_connection",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "clickzetta_connection": {
                "main": [
                    [
                        {
                            "node": "data_comparison",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "data_comparison": {
                "main": [
                    [
                        {
                            "node": "result_processor",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "result_processor": {
                "main": [
                    [
                        {
                            "node": "notification",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            }
        }
    }
}

# 调度数据比对工作流模板
SCHEDULED_COMPARISON_TEMPLATE = {
    "name": "scheduled_comparison",
    "version": "1.0.0",
    "description": "Scheduled data comparison workflow with monitoring",
    "parameters": [
        {
            "name": "cron_expression",
            "type": "string",
            "required": True,
            "default": "0 2 * * *",
            "description": "Cron expression for scheduling"
        },
        {
            "name": "source_connection",
            "type": "string",
            "required": True,
            "description": "Source database connection string"
        },
        {
            "name": "target_connection",
            "type": "string",
            "required": True,
            "description": "Target database connection string"
        },
        {
            "name": "tables_config",
            "type": "array",
            "required": True,
            "description": "List of table comparison configurations"
        }
    ],
    "workflow": {
        "nodes": [
            {
                "id": "scheduler",
                "type": "workflowScheduler",
                "typeVersion": 1,
                "position": [100, 100],
                "parameters": {
                    "scheduleType": "cron",
                    "cronExpression": "{{$parameter.cron_expression}}",
                    "workflowConfig": {
                        "workflowName": "Scheduled Data Comparison",
                        "maxRetries": 3,
                        "timeout": 7200,
                        "notifications": ["failure"]
                    }
                }
            },
            {
                "id": "batch_comparison",
                "type": "dataComparison",
                "typeVersion": 1,
                "position": [300, 100],
                "parameters": {
                    "resource": "tableComparison",
                    "operation": "compareTables",
                    "sourceConnection": "{{$parameter.source_connection}}",
                    "targetConnection": "{{$parameter.target_connection}}"
                }
            },
            {
                "id": "aggregate_results",
                "type": "resultProcessor",
                "typeVersion": 1,
                "position": [500, 100],
                "parameters": {
                    "operation": "extractSummary",
                    "summaryFields": ["total_rows", "rows_added", "rows_removed", "rows_modified", "execution_time"]
                }
            },
            {
                "id": "conditional_alert",
                "type": "n8n-nodes-base.if",
                "typeVersion": 1,
                "position": [700, 100],
                "parameters": {
                    "conditions": {
                        "number": [
                            {
                                "value1": "={{$json.summary.total_differences}}",
                                "operation": "larger",
                                "value2": 0
                            }
                        ]
                    }
                }
            },
            {
                "id": "alert_notification",
                "type": "notificationHandler",
                "typeVersion": 1,
                "position": [900, 50],
                "parameters": {
                    "notificationType": "slack",
                    "slackChannel": "#data-alerts",
                    "messageTemplate": "🚨 Data differences found: {{$json.summary.total_differences}} differences in scheduled comparison"
                }
            },
            {
                "id": "success_notification",
                "type": "notificationHandler",
                "typeVersion": 1,
                "position": [900, 150],
                "parameters": {
                    "notificationType": "email",
                    "emailRecipients": "team@example.com",
                    "emailSubject": "✅ Scheduled Data Comparison Completed Successfully",
                    "messageTemplate": "All scheduled comparisons completed successfully with no differences found."
                }
            }
        ],
        "connections": {
            "scheduler": {
                "main": [
                    [
                        {
                            "node": "batch_comparison",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "batch_comparison": {
                "main": [
                    [
                        {
                            "node": "aggregate_results",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "aggregate_results": {
                "main": [
                    [
                        {
                            "node": "conditional_alert",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "conditional_alert": {
                "main": [
                    [
                        {
                            "node": "alert_notification",
                            "type": "main",
                            "index": 0
                        }
                    ],
                    [
                        {
                            "node": "success_notification",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            }
        }
    }
}

# 复杂数据验证工作流模板
COMPLEX_VALIDATION_TEMPLATE = {
    "name": "complex_validation",
    "version": "1.0.0",
    "description": "Complex data validation workflow with multiple checks",
    "parameters": [
        {
            "name": "validation_config",
            "type": "object",
            "required": True,
            "description": "Validation configuration object"
        }
    ],
    "workflow": {
        "nodes": [
            {
                "id": "webhook_trigger",
                "type": "n8n-nodes-base.webhook",
                "typeVersion": 1,
                "position": [100, 100],
                "parameters": {
                    "path": "data-validation",
                    "httpMethod": "POST"
                }
            },
            {
                "id": "schema_comparison",
                "type": "dataComparison",
                "typeVersion": 1,
                "position": [300, 50],
                "parameters": {
                    "resource": "schemaComparison",
                    "operation": "compareSchemas"
                }
            },
            {
                "id": "data_comparison",
                "type": "dataComparison",
                "typeVersion": 1,
                "position": [300, 150],
                "parameters": {
                    "resource": "tableComparison",
                    "operation": "compareTables"
                }
            },
            {
                "id": "hash_validation",
                "type": "dataComparison",
                "typeVersion": 1,
                "position": [300, 250],
                "parameters": {
                    "resource": "tableComparison",
                    "operation": "hashDiff"
                }
            },
            {
                "id": "merge_results",
                "type": "n8n-nodes-base.merge",
                "typeVersion": 2,
                "position": [500, 150],
                "parameters": {
                    "mode": "combine",
                    "combinationMode": "mergeByPosition"
                }
            },
            {
                "id": "comprehensive_report",
                "type": "resultProcessor",
                "typeVersion": 1,
                "position": [700, 150],
                "parameters": {
                    "operation": "formatReport",
                    "reportFormat": "html",
                    "includeDetails": True,
                    "additionalOptions": {
                        "includeMetadata": True,
                        "customTemplate": "comprehensive_validation_report"
                    }
                }
            },
            {
                "id": "export_results",
                "type": "resultProcessor",
                "typeVersion": 1,
                "position": [900, 150],
                "parameters": {
                    "operation": "exportToFile",
                    "exportFormat": "xlsx",
                    "filePath": "/exports/validation_{{$now.format('YYYY-MM-DD_HH-mm-ss')}}.xlsx"
                }
            }
        ],
        "connections": {
            "webhook_trigger": {
                "main": [
                    [
                        {
                            "node": "schema_comparison",
                            "type": "main",
                            "index": 0
                        },
                        {
                            "node": "data_comparison",
                            "type": "main",
                            "index": 0
                        },
                        {
                            "node": "hash_validation",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "schema_comparison": {
                "main": [
                    [
                        {
                            "node": "merge_results",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "data_comparison": {
                "main": [
                    [
                        {
                            "node": "merge_results",
                            "type": "main",
                            "index": 1
                        }
                    ]
                ]
            },
            "hash_validation": {
                "main": [
                    [
                        {
                            "node": "merge_results",
                            "type": "main",
                            "index": 2
                        }
                    ]
                ]
            },
            "merge_results": {
                "main": [
                    [
                        {
                            "node": "comprehensive_report",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            },
            "comprehensive_report": {
                "main": [
                    [
                        {
                            "node": "export_results",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            }
        }
    }
}

# 所有模板的注册表
WORKFLOW_TEMPLATES = {
    "simple_comparison": SIMPLE_COMPARISON_TEMPLATE,
    "scheduled_comparison": SCHEDULED_COMPARISON_TEMPLATE,
    "complex_validation": COMPLEX_VALIDATION_TEMPLATE
}
