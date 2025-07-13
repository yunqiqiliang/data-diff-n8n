# n8n 节点高级选项实现指南

本文档说明如何在 n8n 节点中实现高级选项的用户交互。

## n8n 节点用户交互机制

### 1. 基本交互元素

n8n 提供了多种交互元素类型：

```typescript
// 基本输入类型
type: 'string' | 'number' | 'boolean' | 'options' | 'multiOptions' | 'collection' | 'fixedCollection'
```

### 2. 条件显示（displayOptions）

使用 `displayOptions` 实现动态显示/隐藏选项：

```typescript
displayOptions: {
    show: {
        operation: ['compareTable'],  // 当操作是 compareTable 时显示
        enableFeature: [true],       // 当 enableFeature 为 true 时显示
        '/parentOption': ['value']   // 引用父级选项
    },
    hide: {
        mode: ['simple']            // 当 mode 是 simple 时隐藏
    }
}
```

## 实现示例

### 1. 数据质量评估选项实现

```typescript
{
    displayName: 'Data Quality Assessment',
    name: 'dataQualityAssessment',
    type: 'collection',
    placeholder: 'Add Quality Checks',
    default: {},
    displayOptions: {
        show: {
            operation: ['compareTable']
        }
    },
    options: [
        {
            displayName: 'Enable Quality Assessment',
            name: 'enableQualityAssessment',
            type: 'boolean',
            default: false,
            description: 'Perform data quality checks during comparison'
        },
        {
            displayName: 'Quality Checks',
            name: 'qualityChecks',
            type: 'multiOptions',
            displayOptions: {
                show: {
                    enableQualityAssessment: [true]
                }
            },
            options: [
                {
                    name: 'Completeness Check',
                    value: 'completeness',
                    description: 'Analyze missing values and null percentages'
                },
                {
                    name: 'Uniqueness Check',
                    value: 'uniqueness',
                    description: 'Identify duplicate records'
                },
                {
                    name: 'Validity Check',
                    value: 'validity',
                    description: 'Validate data against rules'
                },
                {
                    name: 'Consistency Check',
                    value: 'consistency',
                    description: 'Check cross-field consistency'
                }
            ],
            default: ['completeness']
        },
        {
            displayName: 'Custom Validation Rules',
            name: 'customValidationRules',
            type: 'fixedCollection',
            typeOptions: {
                multipleValues: true,
                maxItems: 10
            },
            displayOptions: {
                show: {
                    enableQualityAssessment: [true],
                    qualityChecks: ['validity']
                }
            },
            placeholder: 'Add Validation Rule',
            default: {},
            options: [
                {
                    name: 'rules',
                    displayName: 'Rule',
                    values: [
                        {
                            displayName: 'Column',
                            name: 'column',
                            type: 'string',
                            default: '',
                            placeholder: 'email'
                        },
                        {
                            displayName: 'Rule Type',
                            name: 'ruleType',
                            type: 'options',
                            options: [
                                { name: 'Pattern Match', value: 'pattern' },
                                { name: 'Range Check', value: 'range' },
                                { name: 'Custom SQL', value: 'sql' }
                            ],
                            default: 'pattern'
                        },
                        {
                            displayName: 'Pattern',
                            name: 'pattern',
                            type: 'string',
                            displayOptions: {
                                show: {
                                    ruleType: ['pattern']
                                }
                            },
                            default: '',
                            placeholder: '^[^@]+@[^@]+\\.[^@]+$'
                        },
                        {
                            displayName: 'Min Value',
                            name: 'minValue',
                            type: 'number',
                            displayOptions: {
                                show: {
                                    ruleType: ['range']
                                }
                            },
                            default: 0
                        },
                        {
                            displayName: 'Max Value',
                            name: 'maxValue',
                            type: 'number',
                            displayOptions: {
                                show: {
                                    ruleType: ['range']
                                }
                            },
                            default: 100
                        },
                        {
                            displayName: 'SQL Expression',
                            name: 'sqlExpression',
                            type: 'string',
                            displayOptions: {
                                show: {
                                    ruleType: ['sql']
                                }
                            },
                            default: '',
                            placeholder: 'LENGTH(column) > 5'
                        }
                    ]
                }
            ]
        },
        {
            displayName: 'Quality Score Threshold',
            name: 'qualityScoreThreshold',
            type: 'number',
            displayOptions: {
                show: {
                    enableQualityAssessment: [true]
                }
            },
            default: 80,
            description: 'Minimum acceptable quality score (0-100)',
            hint: 'Comparison will flag results below this threshold'
        }
    ]
}
```

### 2. 性能优化选项实现

```typescript
{
    displayName: 'Performance Optimization',
    name: 'performanceOptimization',
    type: 'collection',
    placeholder: 'Add Performance Options',
    default: {},
    options: [
        {
            displayName: 'Optimization Mode',
            name: 'optimizationMode',
            type: 'options',
            options: [
                {
                    name: 'Balanced',
                    value: 'balanced',
                    description: 'Balance between speed and accuracy'
                },
                {
                    name: 'Speed Priority',
                    value: 'speed',
                    description: 'Optimize for speed, may reduce accuracy'
                },
                {
                    name: 'Accuracy Priority',
                    value: 'accuracy',
                    description: 'Ensure maximum accuracy, may be slower'
                },
                {
                    name: 'Custom',
                    value: 'custom',
                    description: 'Configure individual optimization settings'
                }
            ],
            default: 'balanced'
        },
        {
            displayName: 'Custom Settings',
            name: 'customSettings',
            type: 'collection',
            displayOptions: {
                show: {
                    optimizationMode: ['custom']
                }
            },
            placeholder: 'Configure Settings',
            default: {},
            options: [
                {
                    displayName: 'Enable Query Caching',
                    name: 'enableQueryCaching',
                    type: 'boolean',
                    default: false,
                    description: 'Cache query results for repeated comparisons'
                },
                {
                    displayName: 'Cache TTL (minutes)',
                    name: 'cacheTTL',
                    type: 'number',
                    displayOptions: {
                        show: {
                            enableQueryCaching: [true]
                        }
                    },
                    default: 60,
                    description: 'How long to keep cached results'
                },
                {
                    displayName: 'Enable Incremental Comparison',
                    name: 'incrementalComparison',
                    type: 'boolean',
                    default: false,
                    description: 'Only compare changes since last comparison'
                },
                {
                    displayName: 'Last Comparison Reference',
                    name: 'lastComparisonReference',
                    type: 'options',
                    displayOptions: {
                        show: {
                            incrementalComparison: [true]
                        }
                    },
                    options: [
                        {
                            name: 'Use Comparison ID',
                            value: 'id',
                            description: 'Reference a specific comparison by ID'
                        },
                        {
                            name: 'Use Timestamp Column',
                            value: 'timestamp',
                            description: 'Use a timestamp column to identify changes'
                        },
                        {
                            name: 'Use Change Tracking',
                            value: 'tracking',
                            description: 'Use database change tracking features'
                        }
                    ],
                    default: 'id'
                },
                {
                    displayName: 'Comparison ID',
                    name: 'lastComparisonId',
                    type: 'string',
                    displayOptions: {
                        show: {
                            incrementalComparison: [true],
                            lastComparisonReference: ['id']
                        }
                    },
                    default: '',
                    placeholder: 'comp_abc123'
                },
                {
                    displayName: 'Timestamp Column',
                    name: 'timestampColumn',
                    type: 'string',
                    displayOptions: {
                        show: {
                            incrementalComparison: [true],
                            lastComparisonReference: ['timestamp']
                        }
                    },
                    default: 'updated_at',
                    description: 'Column containing last update timestamp'
                }
            ]
        }
    ]
}
```

### 3. 通知和警报选项实现

```typescript
{
    displayName: 'Notifications & Alerts',
    name: 'notificationsAlerts',
    type: 'collection',
    placeholder: 'Add Notification Settings',
    default: {},
    options: [
        {
            displayName: 'Enable Alerts',
            name: 'enableAlerts',
            type: 'boolean',
            default: false,
            description: 'Send alerts when thresholds are exceeded'
        },
        {
            displayName: 'Alert Conditions',
            name: 'alertConditions',
            type: 'fixedCollection',
            typeOptions: {
                multipleValues: true
            },
            displayOptions: {
                show: {
                    enableAlerts: [true]
                }
            },
            placeholder: 'Add Alert Condition',
            default: {},
            options: [
                {
                    name: 'conditions',
                    displayName: 'Condition',
                    values: [
                        {
                            displayName: 'Alert Type',
                            name: 'alertType',
                            type: 'options',
                            options: [
                                { name: 'Difference Count', value: 'count' },
                                { name: 'Difference Percentage', value: 'percentage' },
                                { name: 'Quality Score', value: 'quality' },
                                { name: 'Execution Time', value: 'time' }
                            ],
                            default: 'count'
                        },
                        {
                            displayName: 'Operator',
                            name: 'operator',
                            type: 'options',
                            options: [
                                { name: 'Greater Than', value: 'gt' },
                                { name: 'Less Than', value: 'lt' },
                                { name: 'Equals', value: 'eq' }
                            ],
                            default: 'gt'
                        },
                        {
                            displayName: 'Threshold Value',
                            name: 'threshold',
                            type: 'number',
                            default: 1000,
                            description: 'Threshold value to trigger alert'
                        },
                        {
                            displayName: 'Severity',
                            name: 'severity',
                            type: 'options',
                            options: [
                                { name: 'Info', value: 'info' },
                                { name: 'Warning', value: 'warning' },
                                { name: 'Error', value: 'error' },
                                { name: 'Critical', value: 'critical' }
                            ],
                            default: 'warning'
                        }
                    ]
                }
            ]
        },
        {
            displayName: 'Notification Channels',
            name: 'notificationChannels',
            type: 'collection',
            displayOptions: {
                show: {
                    enableAlerts: [true]
                }
            },
            placeholder: 'Configure Channels',
            default: {},
            options: [
                {
                    displayName: 'Email Notifications',
                    name: 'emailConfig',
                    type: 'collection',
                    placeholder: 'Add Email Settings',
                    default: {},
                    options: [
                        {
                            displayName: 'Enable Email',
                            name: 'enabled',
                            type: 'boolean',
                            default: false
                        },
                        {
                            displayName: 'Recipients',
                            name: 'recipients',
                            type: 'string',
                            displayOptions: {
                                show: {
                                    enabled: [true]
                                }
                            },
                            default: '',
                            placeholder: 'user1@example.com, user2@example.com'
                        },
                        {
                            displayName: 'Include Report',
                            name: 'includeReport',
                            type: 'boolean',
                            displayOptions: {
                                show: {
                                    enabled: [true]
                                }
                            },
                            default: true,
                            description: 'Attach detailed comparison report'
                        }
                    ]
                },
                {
                    displayName: 'Slack Notifications',
                    name: 'slackConfig',
                    type: 'collection',
                    placeholder: 'Add Slack Settings',
                    default: {},
                    options: [
                        {
                            displayName: 'Enable Slack',
                            name: 'enabled',
                            type: 'boolean',
                            default: false
                        },
                        {
                            displayName: 'Webhook URL',
                            name: 'webhookUrl',
                            type: 'string',
                            typeOptions: {
                                password: true
                            },
                            displayOptions: {
                                show: {
                                    enabled: [true]
                                }
                            },
                            default: '',
                            placeholder: 'https://hooks.slack.com/services/...'
                        },
                        {
                            displayName: 'Channel',
                            name: 'channel',
                            type: 'string',
                            displayOptions: {
                                show: {
                                    enabled: [true]
                                }
                            },
                            default: '#data-quality',
                            placeholder: '#channel-name'
                        },
                        {
                            displayName: 'Message Format',
                            name: 'messageFormat',
                            type: 'options',
                            displayOptions: {
                                show: {
                                    enabled: [true]
                                }
                            },
                            options: [
                                { name: 'Simple', value: 'simple' },
                                { name: 'Detailed', value: 'detailed' },
                                { name: 'Custom', value: 'custom' }
                            ],
                            default: 'simple'
                        }
                    ]
                }
            ]
        }
    ]
}
```

### 4. 结果格式化选项实现

```typescript
{
    displayName: 'Output Formatting',
    name: 'outputFormatting',
    type: 'collection',
    placeholder: 'Add Formatting Options',
    default: {},
    options: [
        {
            displayName: 'Output Format',
            name: 'outputFormat',
            type: 'options',
            options: [
                {
                    name: 'Standard JSON',
                    value: 'json',
                    description: 'Default n8n JSON format'
                },
                {
                    name: 'CSV Export',
                    value: 'csv',
                    description: 'Export differences as CSV'
                },
                {
                    name: 'Excel Report',
                    value: 'excel',
                    description: 'Generate Excel workbook with multiple sheets'
                },
                {
                    name: 'HTML Report',
                    value: 'html',
                    description: 'Interactive HTML report with charts'
                },
                {
                    name: 'PDF Report',
                    value: 'pdf',
                    description: 'Professional PDF report'
                }
            ],
            default: 'json'
        },
        {
            displayName: 'Report Settings',
            name: 'reportSettings',
            type: 'collection',
            displayOptions: {
                show: {
                    outputFormat: ['excel', 'html', 'pdf']
                }
            },
            placeholder: 'Configure Report',
            default: {},
            options: [
                {
                    displayName: 'Report Title',
                    name: 'title',
                    type: 'string',
                    default: 'Data Comparison Report',
                    placeholder: 'Monthly Data Quality Report'
                },
                {
                    displayName: 'Include Visualizations',
                    name: 'includeVisualizations',
                    type: 'boolean',
                    default: true,
                    description: 'Generate charts and graphs'
                },
                {
                    displayName: 'Visualization Types',
                    name: 'visualizationTypes',
                    type: 'multiOptions',
                    displayOptions: {
                        show: {
                            includeVisualizations: [true]
                        }
                    },
                    options: [
                        { name: 'Difference Summary Pie Chart', value: 'summary_pie' },
                        { name: 'Column Difference Bar Chart', value: 'column_bar' },
                        { name: 'Timeline Trend Chart', value: 'timeline' },
                        { name: 'Data Quality Heatmap', value: 'quality_heatmap' }
                    ],
                    default: ['summary_pie', 'column_bar']
                },
                {
                    displayName: 'Summary Level',
                    name: 'summaryLevel',
                    type: 'options',
                    options: [
                        {
                            name: 'Executive Summary',
                            value: 'executive',
                            description: 'High-level overview for management'
                        },
                        {
                            name: 'Business Report',
                            value: 'business',
                            description: 'Detailed analysis for business users'
                        },
                        {
                            name: 'Technical Report',
                            value: 'technical',
                            description: 'Full technical details for developers'
                        }
                    ],
                    default: 'business'
                },
                {
                    displayName: 'Include Recommendations',
                    name: 'includeRecommendations',
                    type: 'boolean',
                    default: true,
                    description: 'Add AI-generated improvement recommendations'
                }
            ]
        }
    ]
}
```

## 处理用户输入的最佳实践

### 1. 输入验证

在节点的 `execute` 方法中验证用户输入：

```typescript
async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    
    // 获取用户配置
    const dataQualityAssessment = this.getNodeParameter('dataQualityAssessment', 0, {}) as IDataNodeOptions;
    
    // 验证输入
    if (dataQualityAssessment.enableQualityAssessment) {
        const qualityChecks = dataQualityAssessment.qualityChecks || [];
        
        if (qualityChecks.length === 0) {
            throw new NodeOperationError(
                this.getNode(),
                'At least one quality check must be selected when quality assessment is enabled'
            );
        }
        
        // 验证自定义规则
        if (qualityChecks.includes('validity')) {
            const rules = dataQualityAssessment.customValidationRules?.rules || [];
            for (const rule of rules) {
                if (!rule.column) {
                    throw new NodeOperationError(
                        this.getNode(),
                        'Column name is required for validation rules'
                    );
                }
            }
        }
    }
}
```

### 2. 动态选项加载

对于需要动态加载的选项（如数据库表列表），使用 `loadOptions` 方法：

```typescript
methods = {
    loadOptions: {
        async getTableColumns(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
            // 获取数据库连接
            const credentials = await this.getCredentials('postgres');
            
            // 查询列信息
            const columns = await queryDatabase(`
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = $1
            `, [tableName]);
            
            return columns.map(col => ({
                name: col.column_name,
                value: col.column_name,
                description: `Column: ${col.column_name}`
            }));
        }
    }
}
```

### 3. 条件逻辑处理

根据用户选择执行不同的逻辑：

```typescript
// 处理性能优化选项
const performanceOpts = this.getNodeParameter('performanceOptimization', 0, {}) as any;

if (performanceOpts.optimizationMode === 'custom' && performanceOpts.customSettings?.incrementalComparison) {
    // 实现增量比对逻辑
    const lastComparisonRef = performanceOpts.customSettings.lastComparisonReference;
    
    switch (lastComparisonRef) {
        case 'id':
            const comparisonId = performanceOpts.customSettings.lastComparisonId;
            // 基于上次比对ID的增量逻辑
            break;
        case 'timestamp':
            const timestampColumn = performanceOpts.customSettings.timestampColumn;
            // 基于时间戳的增量逻辑
            break;
        case 'tracking':
            // 使用数据库变更跟踪
            break;
    }
}
```

### 4. 用户友好的错误处理

提供清晰的错误信息和建议：

```typescript
try {
    // 执行比对
} catch (error) {
    if (error.code === 'TIMEOUT') {
        throw new NodeOperationError(
            this.getNode(),
            'Comparison timed out. Consider enabling performance optimization options:\n' +
            '- Enable sampling for large datasets\n' +
            '- Use incremental comparison\n' +
            '- Reduce the number of columns to compare',
            { description: 'Operation timeout' }
        );
    }
}
```

## 高级交互模式

### 1. 向导式配置

使用多个相关联的选项创建向导式体验：

```typescript
{
    displayName: 'Configuration Wizard',
    name: 'configWizard',
    type: 'options',
    options: [
        { name: 'Quick Setup', value: 'quick' },
        { name: 'Guided Setup', value: 'guided' },
        { name: 'Advanced Setup', value: 'advanced' }
    ],
    default: 'quick'
},
// 根据选择显示不同的选项集
{
    displayName: 'Quick Options',
    name: 'quickOptions',
    type: 'collection',
    displayOptions: {
        show: {
            configWizard: ['quick']
        }
    },
    // 简化的选项集
}
```

### 2. 预设模板

提供常用配置的预设：

```typescript
{
    displayName: 'Use Preset',
    name: 'usePreset',
    type: 'options',
    options: [
        {
            name: 'None',
            value: 'none',
            description: 'Configure manually'
        },
        {
            name: 'Data Migration Validation',
            value: 'migration',
            description: 'Optimized for validating data migrations'
        },
        {
            name: 'Daily Data Quality Check',
            value: 'daily_quality',
            description: 'Regular quality monitoring setup'
        },
        {
            name: 'Real-time Sync Monitoring',
            value: 'realtime_sync',
            description: 'Monitor real-time data synchronization'
        }
    ],
    default: 'none'
}
```

### 3. 上下文帮助

在复杂选项旁提供详细帮助：

```typescript
{
    displayName: 'Sampling Strategy',
    name: 'samplingStrategy',
    type: 'options',
    // ... options ...
    description: 'How rows are selected for sampling',
    hint: 'Random: Best for general use. Systematic: Good for ordered data. Stratified: Ensures all segments are represented.',
    // 可以添加链接到文档
    documentationUrl: 'https://docs.example.com/sampling-strategies'
}
```

## 总结

通过这些机制，我们可以在 n8n 节点中实现丰富的用户交互：

1. **分层配置**：使用 collection 类型组织相关选项
2. **条件显示**：通过 displayOptions 实现智能的选项显示/隐藏
3. **输入验证**：在执行时验证用户输入的有效性
4. **动态加载**：使用 loadOptions 提供动态选项
5. **用户引导**：通过预设、向导和帮助文本改善用户体验

这样可以让复杂的功能变得易于使用，同时为高级用户提供足够的灵活性。