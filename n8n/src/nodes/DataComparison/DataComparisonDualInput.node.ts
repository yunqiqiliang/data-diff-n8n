import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
	IDataObject,
} from 'n8n-workflow';
import { ensureSerializable } from '../utils/serialization';

export class DataComparisonDualInput implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Data Comparison (Dual Input)',
		name: 'dataComparisonDualInput',
		icon: 'fa:exchange-alt',
		group: ['transform'],
		version: 2,
		description: 'Compare data between two sources with automatic configuration detection',
		defaults: {
			name: 'Data Comparison',
		},
		// 定义两个输入
		inputs: [
			{
				displayName: 'Source',
				type: NodeConnectionType.Main,
			},
			{
				displayName: 'Target',
				type: NodeConnectionType.Main,
			}
		],
		outputs: [NodeConnectionType.Main],
		inputNames: ['Source', 'Target'],
		credentials: [
			{
				name: 'dataDiffConfig',
				required: false, // 可选，因为可以从上游节点获取
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Compare Table',
						value: 'compareTable',
						description: 'Compare data between two tables to find differences in rows',
						action: 'Compare two database tables',
					},
					{
						name: 'Compare Schema',
						value: 'compareSchema',
						description: 'Compare table structures, columns, and data types between databases',
						action: 'Compare database schemas',
					},
					{
						name: 'Get Comparison Result',
						value: 'getComparisonResult',
						description: 'Retrieve results from an asynchronous comparison using comparison ID',
						action: 'Get comparison result by ID',
					},
				],
				default: 'compareTable',
			},
			{
				displayName: 'Comparison ID',
				name: 'comparisonId',
				type: 'string',
				default: '',
				placeholder: 'e.g., 02f29186-e0c9-464c-8e7e-7ec66ac7c24d',
				description: 'The ID of the comparison task to get results for',
				displayOptions: {
					show: {
						operation: ['getComparisonResult'],
					},
				},
				required: true,
			},
			{
				displayName: 'Configuration Mode',
				name: 'configMode',
				type: 'options',
				options: [
					{
						name: 'Auto-detect from Connected Nodes',
						value: 'auto',
						description: 'Automatically detect configuration from DatabaseConnector/ClickzettaConnector nodes',
					},
					{
						name: 'Manual Configuration',
						value: 'manual',
						description: 'Manually specify connection and table information',
					},
				],
				default: 'auto',
				displayOptions: {
					show: {
						operation: ['compareTable', 'compareSchema'],
					},
				},
			},
			// 手动模式下的配置
			{
				displayName: 'Source Configuration',
				name: 'sourceConfig',
				type: 'collection',
				placeholder: 'Add Source Configuration',
				default: {},
				displayOptions: {
					show: {
						configMode: ['manual'],
					},
				},
				options: [
					{
						displayName: 'Connection String',
						name: 'connectionString',
						type: 'string',
						default: '',
						placeholder: 'postgresql://user:pass@host:port/db',
					},
					{
						displayName: 'Table Name',
						name: 'tableName',
						type: 'string',
						default: '',
						placeholder: 'schema.table_name',
					},
				],
			},
			{
				displayName: 'Target Configuration',
				name: 'targetConfig',
				type: 'collection',
				placeholder: 'Add Target Configuration',
				default: {},
				displayOptions: {
					show: {
						configMode: ['manual'],
					},
				},
				options: [
					{
						displayName: 'Connection String',
						name: 'connectionString',
						type: 'string',
						default: '',
						placeholder: 'clickzetta://user:pass@host:port/db',
					},
					{
						displayName: 'Table Name',
						name: 'tableName',
						type: 'string',
						default: '',
						placeholder: 'schema.table_name',
					},
				],
			},
			// 表选择策略
			{
				displayName: 'Table Selection',
				name: 'tableSelection',
				type: 'options',
				options: [
					{
						name: 'Use First Available Table',
						value: 'auto',
						description: 'Use the first table from each input (may not be the desired table)',
					},
					{
						name: 'Manually Specify Tables',
						value: 'specified',
						description: 'Choose specific tables to compare',
					},
					{
						name: 'Compare Query Results',
						value: 'all',
						description: 'Compare results from SQL queries or similar operations',
					},
				],
				default: 'specified',
				displayOptions: {
					show: {
						operation: ['compareTable'],
						configMode: ['auto'],
					},
				},
			},
			{
				displayName: 'Source Table',
				name: 'sourceTable',
				type: 'string',
				default: '',
				placeholder: 'e.g., public.accounts',
				description: 'The table to compare from the source database (Input 1). Include schema if needed.',
				hint: 'Format: schema.table_name (e.g., public.users) or just table_name if using default schema',
				displayOptions: {
					show: {
						operation: ['compareTable'],
						configMode: ['auto'],
						tableSelection: ['specified'],
					},
				},
			},
			{
				displayName: 'Target Table',
				name: 'targetTable',
				type: 'string',
				default: '',
				placeholder: 'e.g., from_pg.accounts',
				description: 'The table to compare from the target database (Input 2). Include schema if needed.',
				hint: 'Can be the same table name as source for comparing identical tables across databases, or a different name for renamed tables',
				displayOptions: {
					show: {
						operation: ['compareTable'],
						configMode: ['auto'],
						tableSelection: ['specified'],
					},
				},
			},
			// 关键列配置
			{
				displayName: 'Notice',
				name: 'notice',
				type: 'notice',
				default: '',
				description: 'When using auto-detect, the first table from each prepareComparison result will be used. For specific table selection, switch to "Use Table Names Below".',
				displayOptions: {
					show: {
						tableSelection: ['auto'],
					},
				},
			},
			{
				displayName: 'Key Columns',
				name: 'keyColumns',
				type: 'string',
				default: '',
				placeholder: 'id, created_at (auto-detect if empty)',
				description: 'Columns that uniquely identify each row. Used for matching rows between tables.',
				hint: 'Comma-separated list (e.g., "id" or "user_id, created_at"). Leave empty to auto-detect primary key. For best performance, use indexed columns.',
				displayOptions: {
					show: {
						operation: ['compareTable'],
					},
				},
			},
			{
				displayName: 'Columns to Compare',
				name: 'columnsToCompare',
				type: 'string',
				default: '',
				placeholder: 'Leave empty to compare all columns',
				description: 'Specific columns to check for differences. Leave empty to compare all columns automatically.',
				hint: 'Example: "name, email, price, updated_at". Limiting columns improves performance. Key columns are always included.',
				displayOptions: {
					show: {
						operation: ['compareTable'],
					},
				},
			},
			{
				displayName: 'Where Condition',
				name: 'whereCondition',
				type: 'string',
				default: '',
				placeholder: 'status = \'active\' AND created_date > \'2023-01-01\'',
				description: 'SQL WHERE condition to filter rows (optional)',
				displayOptions: {
					show: {
						operation: ['compareTable'],
					},
				},
			},
			// 高级选项
			{
				displayName: 'Advanced Options',
				name: 'advancedOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						operation: ['compareTable'],
					},
				},
				options: [
					{
						displayName: 'Algorithm',
						name: 'algorithm',
						type: 'options',
						default: 'auto',
						options: [
							{
								name: 'Auto (Recommended)',
								value: 'auto',
								description: 'Let the system choose based on database types',
							},
							{
								name: 'JoinDiff',
								value: 'joindiff',
								description: 'For same database comparisons',
							},
							{
								name: 'HashDiff',
								value: 'hashdiff',
								description: 'For cross-database comparisons',
							},
						],
					},
					{
						displayName: 'Enable Sampling',
						name: 'enableSampling',
						type: 'boolean',
						default: false,
						description: 'Enable statistical sampling to improve performance on large datasets. When enabled, only a subset of data will be compared, and results will include statistical estimates of total differences.',
						hint: 'Recommended for tables with more than 100,000 rows. Sampling can reduce comparison time from hours to minutes.',
					},
					{
						displayName: 'Sampling Type',
						name: 'samplingType',
						type: 'options',
						default: 'size',
						options: [
							{
								name: 'Fixed Size',
								value: 'size',
								description: 'Sample a specific number of rows (e.g., 10,000 rows)',
							},
							{
								name: 'Percentage',
								value: 'percent',
								description: 'Sample a percentage of total rows (e.g., 5% of all data)',
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
						description: 'Choose how to determine the sample size. Fixed size is predictable, percentage scales with your data.',
						hint: 'Use percentage for datasets that grow over time, fixed size for consistent performance.',
					},
					{
						displayName: 'Sample Size',
						name: 'sampleSize',
						type: 'number',
						default: 10000,
						description: 'Number of rows to sample from each table. Larger samples are more accurate but slower.',
						displayOptions: {
							show: {
								enableSampling: [true],
								samplingType: ['size'],
							},
						},
						hint: 'Common values: 1,000 (quick test), 10,000 (balanced), 100,000 (high accuracy). Both tables will sample the same rows when using DETERMINISTIC method.',
						typeOptions: {
							minValue: 100,
							numberStepSize: 1000,
						},
					},
					{
						displayName: 'Sample Percentage',
						name: 'samplePercent',
						type: 'number',
						default: 10,
						typeOptions: {
							minValue: 0.1,
							maxValue: 100,
							numberStepSize: 0.1,
							numberPrecision: 1,
						},
						description: 'Percentage of total rows to sample from each table (0.1% to 100%).',
						displayOptions: {
							show: {
								enableSampling: [true],
								samplingType: ['percent'],
							},
						},
						hint: 'Recommendations by table size: >10M rows (1-2%), 1M-10M rows (5-10%), 100K-1M rows (10-20%). Example: 5% of 1 million rows = 50,000 rows sampled.',
					},
					{
						displayName: 'Sampling Method',
						name: 'samplingMethod',
						type: 'options',
						default: 'DETERMINISTIC',
						options: [
							{
								name: 'Deterministic (Recommended)',
								value: 'DETERMINISTIC',
								description: '✅ Guarantees both tables sample the exact same rows using key-based selection',
							},
							{
								name: 'System',
								value: 'SYSTEM',
								description: '⚡ Fast block-level sampling, but may select different rows in each table',
							},
							{
								name: 'Bernoulli',
								value: 'BERNOULLI',
								description: '🎲 True random row-level sampling, accurate but may select different rows',
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
						description: 'How rows are selected for sampling. This affects both accuracy and consistency of results.',
						hint: '⚠️ IMPORTANT: Use DETERMINISTIC for cross-database comparisons or when you need reproducible results. Other methods are faster but may sample different rows from each table, leading to false positives.',
					},
					{
						displayName: 'Auto Sample Threshold',
						name: 'autoSampleThreshold',
						type: 'number',
						default: 100000,
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
						description: 'When table has more rows than this threshold, sampling will be automatically suggested. Set to 0 to disable auto-sampling.',
						hint: 'This helps prevent accidental full scans of very large tables. Default: 100,000 rows.',
						typeOptions: {
							minValue: 0,
							numberStepSize: 10000,
						},
					},
					{
						displayName: 'Sampling Confidence Level',
						name: 'samplingConfidence',
						type: 'options',
						default: 0.95,
						options: [
							{
								name: '90% - Lower confidence, smaller sample',
								value: 0.90,
								description: 'Faster but less accurate',
							},
							{
								name: '95% - Balanced (Recommended)',
								value: 0.95,
								description: 'Good balance of speed and accuracy',
							},
							{
								name: '99% - Higher confidence, larger sample',
								value: 0.99,
								description: 'More accurate but slower',
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
						description: 'How confident you want to be that the sample represents the full dataset. Higher confidence requires larger samples.',
						hint: 'Example: 95% confidence means there\'s a 95% probability that the true difference count is within the estimated range.',
					},
					{
						displayName: 'Sampling Margin of Error',
						name: 'samplingMarginOfError',
						type: 'options',
						default: 0.01,
						options: [
							{
								name: '±0.1% - Very precise, requires large sample',
								value: 0.001,
								description: 'For critical comparisons',
							},
							{
								name: '±1% - Balanced (Recommended)',
								value: 0.01,
								description: 'Good for most use cases',
							},
							{
								name: '±5% - Less precise, smaller sample',
								value: 0.05,
								description: 'For quick checks',
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
						description: 'Maximum acceptable error in the estimated results. Lower margin requires larger samples.',
						hint: 'Example: With 1% margin and 1000 estimated differences, the actual count is likely between 990-1010. This works with the confidence level to determine sample size.',
					},
					{
						displayName: 'Float Tolerance',
						name: 'floatTolerance',
						type: 'number',
						default: 0.0001,
						description: 'Maximum allowed difference between floating-point values to consider them equal.',
						hint: 'Example: With tolerance 0.01, values 10.001 and 10.009 are considered equal (diff=0.008 < 0.01). Set to 0 for exact comparison. Common values: 0.0001 (high precision), 0.01 (financial), 0.1 (general purpose).',
						typeOptions: {
							numberPrecision: 6,
							minValue: 0,
							numberStepSize: 0.0001,
						},
					},
					{
						displayName: 'Timestamp Precision',
						name: 'timestampPrecision',
						type: 'options',
						default: 'microsecond',
						options: [
							{
								name: 'Microsecond (Highest)',
								value: 'microsecond',
								description: 'Compare timestamps to microsecond precision (0.000001s)',
							},
							{
								name: 'Millisecond',
								value: 'millisecond',
								description: 'Compare timestamps to millisecond precision (0.001s)',
							},
							{
								name: 'Second',
								value: 'second',
								description: 'Compare timestamps to second precision (1s)',
							},
							{
								name: 'Minute',
								value: 'minute',
								description: 'Compare timestamps to minute precision (60s)',
							},
							{
								name: 'Hour',
								value: 'hour',
								description: 'Compare timestamps to hour precision (3600s)',
							},
							{
								name: 'Day',
								value: 'day',
								description: 'Compare timestamps to day precision',
							},
						],
						description: 'Precision level for timestamp comparisons. Lower precision ignores small time differences.',
						hint: '⚠️ WARNING: When comparing databases with different timezone support (e.g., ClickZetta), ensure all timestamps are in UTC. ClickZetta does not support timezone conversion.',
					},
					{
						displayName: 'JSON Comparison Mode',
						name: 'jsonComparisonMode',
						type: 'options',
						default: 'normalized',
						options: [
							{
								name: 'Exact Match',
								value: 'exact',
								description: 'JSON must be identical including whitespace and formatting',
							},
							{
								name: 'Normalized (Recommended)',
								value: 'normalized',
								description: 'Ignores whitespace and formatting differences',
							},
							{
								name: 'Semantic',
								value: 'semantic',
								description: 'Order-insensitive comparison for objects and arrays',
							},
							{
								name: 'Keys Only',
								value: 'keys_only',
								description: 'Compare only the structure (keys) without values',
							},
						],
						description: 'How to compare JSON/JSONB columns. Different modes offer trade-offs between strictness and flexibility.',
						hint: 'Normalized mode is recommended for most use cases. Semantic mode may not be supported by all databases.',
					},
					{
						displayName: 'Column Remapping',
						name: 'columnRemapping',
						type: 'string',
						default: '',
						placeholder: 'user_id:customer_id,created_at:creation_date',
						description: 'Map columns with different names between tables. Format: source1:target1,source2:target2',
						hint: 'Use this when comparing tables with different column names. Example: user_id:customer_id maps user_id from source to customer_id in target.',
					},
					{
						displayName: 'Case-Sensitive Column Remapping',
						name: 'caseSensitiveRemapping',
						type: 'boolean',
						default: true,
						description: 'Whether column name mapping should be case-sensitive',
						displayOptions: {
							show: {
								columnRemapping: ['!', ''],
							},
						},
					},
					{
						displayName: 'Thread Count',
						name: 'threads',
						type: 'number',
						default: 1,
						description: 'Number of parallel threads',
					},
					{
						displayName: 'Bisection Factor',
						name: 'bisectionFactor',
						type: 'number',
						default: 32,
						displayOptions: {
							show: {
								algorithm: ['hashdiff', 'auto'],
							},
						},
						description: 'Number of segments per iteration (only for HashDiff)',
					},
					{
						displayName: 'Bisection Threshold',
						name: 'bisectionThreshold',
						type: 'number',
						default: 16384,
						displayOptions: {
							show: {
								algorithm: ['hashdiff', 'auto'],
							},
						},
						description: 'Minimum rows before using bisection (only for HashDiff)',
					},
					{
						displayName: 'Case Sensitive',
						name: 'caseSensitive',
						type: 'boolean',
						default: true,
						description: 'Whether string comparisons are case sensitive',
					},
					{
						displayName: 'Strict Type Checking',
						name: 'strictTypeChecking',
						type: 'boolean',
						default: false,
						description: 'Whether to strictly check data types',
					},
					{
						displayName: 'Enable Column Statistics',
						name: 'enableColumnStatistics',
						type: 'boolean',
						default: false,
						description: 'Collect detailed statistics for each column including null rates, distributions, and value ranges',
						hint: '📊 Column statistics provide insights into data quality but may increase comparison time. Useful for data profiling and quality assessment.',
					},
					{
						displayName: 'Timeline Analysis',
						name: 'timelineAnalysis',
						type: 'collection',
						placeholder: 'Add Timeline Settings',
						default: {},
						options: [
							{
								displayName: 'Time Column',
								name: 'timelineColumn',
								type: 'string',
								default: '',
								placeholder: 'created_at',
								description: 'Column containing timestamps for timeline analysis',
								hint: '📈 Specify a timestamp column to analyze differences over time. This helps identify patterns and trends.',
							},
							{
								displayName: 'Start Time',
								name: 'timelineStartTime',
								type: 'string',
								default: '',
								placeholder: '2024-01-01T00:00:00',
								description: 'Start time for analysis (ISO format). Leave empty to auto-detect.',
							},
							{
								displayName: 'End Time',
								name: 'timelineEndTime',
								type: 'string',
								default: '',
								placeholder: '2024-12-31T23:59:59',
								description: 'End time for analysis (ISO format). Leave empty to auto-detect.',
							},
							{
								displayName: 'Number of Time Buckets',
								name: 'timelineBuckets',
								type: 'number',
								default: 20,
								typeOptions: {
									minValue: 5,
									maxValue: 100,
								},
								description: 'Number of time periods to analyze',
							},
							{
								displayName: 'Max Differences to Analyze',
								name: 'timelineMaxDifferences',
								type: 'number',
								default: 10000,
								description: 'Maximum number of differences to include in timeline analysis',
								hint: 'Limiting this improves performance for large datasets',
							},
						],
					},
					{
						displayName: 'Difference Classification',
						name: 'differenceClassification',
						type: 'collection',
						placeholder: 'Add Classification Settings',
						default: {},
						description: 'Configure how differences are classified and assessed',
						options: [
							{
								displayName: 'Enable Classification',
								name: 'enableClassification',
								type: 'boolean',
								default: true,
								description: 'Classify differences by type and severity',
							},
							{
								displayName: 'Treat NULL as Critical',
								name: 'treatNullAsCritical',
								type: 'boolean',
								default: false,
								description: 'Consider NULL mismatches as critical severity',
							},
							{
								displayName: 'Case Sensitive Classification',
								name: 'caseSensitiveClassification',
								type: 'boolean',
								default: true,
								description: 'Consider case differences as significant',
							},
						],
					},
					{
						displayName: 'Result Materialization',
						name: 'resultMaterialization',
						type: 'collection',
						placeholder: 'Add Materialization Settings',
						default: {},
						description: 'Configure how comparison results are stored in database',
						options: [
							{
								displayName: 'Enable Materialization',
								name: 'materializeResults',
								type: 'boolean',
								default: true,
								description: 'Store comparison results in PostgreSQL database for historical analysis',
							},
							{
								displayName: 'Store Difference Details',
								name: 'storeDifferenceDetails',
								type: 'boolean',
								default: true,
								description: 'Include detailed difference records (limited to 1000 rows)',
								displayOptions: {
									show: {
										materializeResults: [true],
									},
								},
							},
							{
								displayName: 'Store Column Statistics',
								name: 'storeColumnStats',
								type: 'boolean',
								default: true,
								description: 'Include column-level statistics in materialized results',
								displayOptions: {
									show: {
										materializeResults: [true],
									},
								},
							},
							{
								displayName: 'Store Timeline Analysis',
								name: 'storeTimelineAnalysis',
								type: 'boolean',
								default: true,
								description: 'Include timeline analysis results if enabled',
								displayOptions: {
									show: {
										materializeResults: [true],
									},
								},
							},
						],
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;
		
		// 处理获取比对结果的操作
		if (operation === 'getComparisonResult') {
			const comparisonId = this.getNodeParameter('comparisonId', 0) as string;
			try {
				const result = await DataComparisonDualInput.getComparisonResult(comparisonId);
				returnData.push({
					json: ensureSerializable({
						success: true,
						operation,
						comparisonId,
						result,
					}),
				});
			} catch (error: any) {
				returnData.push({
					json: {
						success: false,
						operation,
						comparisonId,
						error: error?.message || 'Failed to get comparison result',
					},
				});
			}
			return [returnData];
		}
		
		// 对于比对操作，需要两个输入
		let sourceItems: INodeExecutionData[] = [];
		let targetItems: INodeExecutionData[] = [];
		
		// 尝试不同的方法获取输入数据
		try {
			// 方法1: 使用索引获取
			sourceItems = this.getInputData(0); // Source input
		} catch (e) {
			console.log('[DataComparisonDualInput] Failed to get source input with index 0:', e);
			// 方法2: 尝试使用默认方法
			try {
				const allItems = this.getInputData();
				if (allItems && allItems.length > 0) {
					sourceItems = [allItems[0]]; // 假设第一个是源
					targetItems = allItems.length > 1 ? [allItems[1]] : []; // 假设第二个是目标
				}
			} catch (e2) {
				console.log('[DataComparisonDualInput] Failed to get input data with default method:', e2);
			}
		}
		
		try {
			// 如果源成功，尝试获取目标
			if (sourceItems.length > 0 && targetItems.length === 0) {
				targetItems = this.getInputData(1); // Target input
			}
		} catch (e) {
			console.log('[DataComparisonDualInput] Failed to get target input with index 1:', e);
		}
		
		// 调试信息
		console.log(`[DataComparisonDualInput] Source items: ${sourceItems.length}, Target items: ${targetItems.length}`);
		
		// 提供更详细的错误信息
		if (sourceItems.length === 0 && targetItems.length === 0) {
			throw new Error('Both source and target inputs are missing. Please connect database nodes to both input ports.');
		} else if (sourceItems.length === 0) {
			throw new Error('Source input is missing. Please connect a database node to the "Source" input port.');
		} else if (targetItems.length === 0) {
			throw new Error('Target input is missing. Please connect a database node to the "Target" input port.');
		}
		
		// 获取配置模式
		const configMode = this.getNodeParameter('configMode', 0) as string;
		const tableSelection = this.getNodeParameter('tableSelection', 0, 'auto') as string;
		
		// 声明在 try 外部，以便错误处理时访问
		let sourceTable = '';
		let targetTable = '';
		
		try {
			let sourceConfig: IDataObject = {};
			let targetConfig: IDataObject = {};
			
			if (configMode === 'auto') {
				// 自动检测模式：从连接的节点提取配置
				sourceConfig = DataComparisonDualInput.extractNodeConfig(sourceItems[0]);
				targetConfig = DataComparisonDualInput.extractNodeConfig(targetItems[0]);
				
				// 自动检测表名
				if (tableSelection === 'auto') {
					sourceTable = DataComparisonDualInput.detectTableName(sourceItems[0]);
					targetTable = DataComparisonDualInput.detectTableName(targetItems[0]);
				}
			} else {
				// 手动配置模式
				const manualSourceConfig = this.getNodeParameter('sourceConfig', 0) as IDataObject;
				const manualTargetConfig = this.getNodeParameter('targetConfig', 0) as IDataObject;
				
				sourceConfig = {
					connectionString: manualSourceConfig.connectionString,
					type: DataComparisonDualInput.detectDatabaseType(manualSourceConfig.connectionString as string),
				};
				targetConfig = {
					connectionString: manualTargetConfig.connectionString,
					type: DataComparisonDualInput.detectDatabaseType(manualTargetConfig.connectionString as string),
				};
				
				sourceTable = manualSourceConfig.tableName as string || '';
				targetTable = manualTargetConfig.tableName as string || '';
			}
			
			// 如果指定了表名，使用指定的
			if (tableSelection === 'specified') {
				const specifiedSource = this.getNodeParameter('sourceTable', 0, '') as string;
				const specifiedTarget = this.getNodeParameter('targetTable', 0, '') as string;
				if (specifiedSource) sourceTable = specifiedSource;
				if (specifiedTarget) targetTable = specifiedTarget;
			}
			
			// 获取其他参数
			const keyColumns = this.getNodeParameter('keyColumns', 0, '') as string;
			const columnsToCompare = this.getNodeParameter('columnsToCompare', 0, '') as string;
			const whereCondition = this.getNodeParameter('whereCondition', 0, '') as string;
			const advancedOptions = this.getNodeParameter('advancedOptions', 0, {}) as IDataObject;
			
			let result: any;
			
			if (operation === 'compareTable') {
				// 构建表比对请求参数
				// 处理采样参数
				let samplingConfig: any = {
					enable_sampling: advancedOptions.enableSampling === true,
				};
				
				if (advancedOptions.enableSampling) {
					// 根据采样类型设置参数
					if (advancedOptions.samplingType === 'percent') {
						samplingConfig.sampling_percent = advancedOptions.samplePercent || 10;
					} else {
						samplingConfig.sample_size = advancedOptions.sampleSize || 10000;
					}
					
					samplingConfig.sampling_method = advancedOptions.samplingMethod || 'DETERMINISTIC';
					samplingConfig.auto_sample_threshold = advancedOptions.autoSampleThreshold || 100000;
					samplingConfig.sampling_confidence = advancedOptions.samplingConfidence || 0.95;
					samplingConfig.sampling_margin_of_error = advancedOptions.samplingMarginOfError || 0.01;
				}

				const requestData = {
					source_config: DataComparisonDualInput.convertToApiConfig(sourceConfig),
					target_config: DataComparisonDualInput.convertToApiConfig(targetConfig),
					comparison_config: {
						source_table: sourceTable,
						target_table: targetTable,
						key_columns: keyColumns ? keyColumns.split(',').map(c => c.trim()) : ['id'],
						columns_to_compare: columnsToCompare ? columnsToCompare.split(',').map(c => c.trim()) : undefined,
						where_condition: whereCondition || undefined,
						algorithm: advancedOptions.algorithm || 'auto',
						...samplingConfig,
						threads: advancedOptions.threads || 1,
						float_tolerance: advancedOptions.floatTolerance,
						timestamp_precision: advancedOptions.timestampPrecision,
						json_comparison_mode: advancedOptions.jsonComparisonMode,
						column_remapping: advancedOptions.columnRemapping,
						case_sensitive_remapping: advancedOptions.caseSensitiveRemapping !== false,
						bisection_factor: advancedOptions.bisectionFactor,
						bisection_threshold: advancedOptions.bisectionThreshold,
						case_sensitive: advancedOptions.caseSensitive !== false,
						strict_type_checking: advancedOptions.strictTypeChecking || false,
						enable_column_statistics: advancedOptions.enableColumnStatistics || false,
						// Timeline analysis configuration
						timeline_column: (advancedOptions.timelineAnalysis as IDataObject)?.timelineColumn as string || undefined,
						timeline_start_time: (advancedOptions.timelineAnalysis as IDataObject)?.timelineStartTime as string || undefined,
						timeline_end_time: (advancedOptions.timelineAnalysis as IDataObject)?.timelineEndTime as string || undefined,
						timeline_buckets: (advancedOptions.timelineAnalysis as IDataObject)?.timelineBuckets as number || 20,
						timeline_max_differences: (advancedOptions.timelineAnalysis as IDataObject)?.timelineMaxDifferences as number || 10000,
						// Difference classification configuration
						enable_classification: (advancedOptions.differenceClassification as IDataObject)?.enableClassification !== false,
						treat_null_as_critical: (advancedOptions.differenceClassification as IDataObject)?.treatNullAsCritical || false,
						// Result materialization configuration
						materialize_results: (advancedOptions.resultMaterialization as IDataObject)?.materializeResults !== false,
						store_difference_details: (advancedOptions.resultMaterialization as IDataObject)?.storeDifferenceDetails !== false,
						store_column_stats: (advancedOptions.resultMaterialization as IDataObject)?.storeColumnStats !== false,
						store_timeline_analysis: (advancedOptions.resultMaterialization as IDataObject)?.storeTimelineAnalysis !== false,
					}
				};
				
				// 调用表比对 API 启动异步任务
				console.log('[DataComparisonDualInput] Starting async comparison with request:', JSON.stringify(requestData, null, 2));
				const startResult = await DataComparisonDualInput.callComparisonAPI(requestData, 'tables');
				
				// 如果返回的是异步任务信息
				if (startResult.comparison_id && startResult.status === 'started') {
					console.log('[DataComparisonDualInput] Async comparison started with ID:', startResult.comparison_id);
					result = {
						...startResult,
						message: 'Comparison task started. Use "Get Comparison Result" operation to retrieve results.',
						async: true
					};
				} else {
					// 兼容同步返回的情况
					result = startResult;
				}
			} else if (operation === 'compareSchema') {
				// 构建模式比对请求参数
				const requestData = {
					source_config: DataComparisonDualInput.convertToApiConfig(sourceConfig),
					target_config: DataComparisonDualInput.convertToApiConfig(targetConfig),
				};
				
				// 调用模式比对 API
				result = await DataComparisonDualInput.callComparisonAPI(requestData, 'schemas');
			}
			
			// 构建输出
			if (result.async) {
				// 异步任务输出
				returnData.push({
					json: ensureSerializable({
						success: true,
						async: true,
						comparison_id: result.comparison_id,
						status: result.status,
						message: result.message,
						metadata: {
							source: {
								type: sourceConfig.type,
								table: sourceTable,
								itemCount: sourceItems.length,
							},
							target: {
								type: targetConfig.type,
								table: targetTable,
								itemCount: targetItems.length,
							},
							comparison_config: {
								algorithm: advancedOptions.algorithm,
								enableSampling: (advancedOptions.sampling as IDataObject)?.enableSampling,
								columnStatistics: (advancedOptions.columnStatistics as IDataObject)?.enableStatistics,
								materializeResults: (advancedOptions.resultMaterialization as IDataObject)?.materializeResults !== false,
							},
						},
						next_steps: {
							description: 'Use the comparison_id with "Get Comparison Result" operation to retrieve results',
							check_interval_seconds: 5,
							max_wait_time_seconds: 300,
						},
					}),
				});
			} else {
				// 同步结果输出（向后兼容）
				returnData.push({
					json: ensureSerializable({
						success: true,
						comparison_result: result,
						metadata: {
							source: {
								type: sourceConfig.type,
								table: sourceTable,
								itemCount: sourceItems.length,
							},
							target: {
								type: targetConfig.type,
								table: targetTable,
								itemCount: targetItems.length,
							},
							algorithm_used: result.config?.algorithm || advancedOptions.algorithm,
							sampling_applied: result.config?._sampling_applied || false,
						},
					}),
				});
			}
			
		} catch (error: any) {
			console.error('[DataComparisonDualInput] Error occurred:', error);
			console.error('[DataComparisonDualInput] Error type:', typeof error);
			console.error('[DataComparisonDualInput] Error message:', error?.message);
			console.error('[DataComparisonDualInput] Error stack:', error?.stack);
			
			// 更好的错误处理
			let errorMessage = 'Unknown error occurred';
			if (error instanceof Error) {
				errorMessage = error.message;
			} else if (typeof error === 'string') {
				errorMessage = error;
			} else if (error && typeof error === 'object') {
				// 尝试序列化错误对象
				try {
					errorMessage = JSON.stringify(error);
				} catch (e) {
					errorMessage = 'Complex error object that cannot be serialized';
				}
			}
			
			returnData.push({
				json: {
					success: false,
					error: errorMessage,
					errorType: error?.constructor?.name || typeof error,
					details: {
						sourceItemsCount: sourceItems.length,
						targetItemsCount: targetItems.length,
						// 添加更多调试信息
						sourceTableDetected: sourceTable || 'not detected',
						targetTableDetected: targetTable || 'not detected',
						configMode: configMode,
						tableSelection: tableSelection,
					},
				},
			});
		}
		
		return [returnData];
	}

	// 从节点数据中提取配置
	private static extractNodeConfig(item: INodeExecutionData): IDataObject {
		const json = item.json;
		
		// 调试：打印输入数据
		console.log('[DataComparisonDualInput] Extracting config from:', JSON.stringify(json, null, 2));
		
		// 检查是否来自 prepareComparison 操作
		if (json.comparisonReady === true && json.comparisonConfig) {
			const compConfig = json.comparisonConfig as IDataObject;
			const extracted = {
				connectionUrl: json.connectionUrl,
				connectionConfig: compConfig.source_config || json.connectionConfig,
				type: compConfig.database_type || json.databaseType,
				isPrepared: true,
				availableTables: compConfig.available_tables || json.tables || [],
			};
			console.log('[DataComparisonDualInput] Extracted prepareComparison config:', JSON.stringify(extracted, null, 2));
			return extracted;
		}
		
		// 检查是否来自 DatabaseConnector 或 ClickzettaConnector
		if (json.connectionConfig) {
			return json.connectionConfig as IDataObject;
		}
		
		// 检查是否有 connectionUrl
		if (json.connectionUrl) {
			return {
				connectionString: json.connectionUrl,
				type: json.databaseType || DataComparisonDualInput.detectDatabaseType(json.connectionUrl as string),
			};
		}
		
		// 尝试从其他可能的字段提取
		const config: IDataObject = {};
		
		// 连接字符串的可能字段
		const connFields = ['connectionString', 'connection', 'dsn', 'url'];
		for (const field of connFields) {
			if (json[field]) {
				config.connectionString = json[field];
				break;
			}
		}
		
		// 数据库类型
		config.type = json.databaseType || json.type || json.dbType || 'unknown';
		
		return config;
	}

	// 检测表名
	private static detectTableName(item: INodeExecutionData): string {
		const json = item.json;
		
		// 检查是否来自 prepareComparison 操作
		if (json.comparisonReady === true && json.tables && Array.isArray(json.tables)) {
			// 返回第一个表作为默认选项
			if (json.tables.length > 0) {
				const firstTable = json.tables[0];
				return firstTable.value || firstTable.name || firstTable;
			}
		}
		
		// 直接的表名字段
		const tableFields = ['tableName', 'table', 'table_name', 'source_table', 'target_table'];
		for (const field of tableFields) {
			if (json[field]) {
				return String(json[field]);
			}
		}
		
		// 从 _meta 中提取
		if (json._meta && typeof json._meta === 'object') {
			const meta = json._meta as IDataObject;
			for (const field of tableFields) {
				if (meta[field]) {
					return String(meta[field]);
				}
			}
			
			// 从查询中提取
			if (meta.query) {
				const match = String(meta.query).match(/FROM\s+([^\s]+)/i);
				if (match) {
					return match[1];
				}
			}
		}
		
		// 从 SQL 查询中提取
		if (json.query || json.sqlQuery) {
			const query = String(json.query || json.sqlQuery);
			const match = query.match(/FROM\s+([^\s]+)/i);
			if (match) {
				return match[1];
			}
		}
		
		// 从 tables 列表中获取第一个
		if (json.tables && Array.isArray(json.tables) && json.tables.length > 0) {
			const firstTable = json.tables[0];
			if (typeof firstTable === 'string') {
				return firstTable;
			} else if (firstTable.name) {
				return firstTable.name;
			}
		}
		
		return '';
	}

	// 检测数据库类型
	private static detectDatabaseType(connectionString: string): string {
		if (connectionString.startsWith('postgresql://') || connectionString.startsWith('postgres://')) {
			return 'postgresql';
		} else if (connectionString.startsWith('mysql://')) {
			return 'mysql';
		} else if (connectionString.startsWith('clickzetta://')) {
			return 'clickzetta';
		} else if (connectionString.startsWith('clickhouse://')) {
			return 'clickhouse';
		} else if (connectionString.startsWith('mssql://') || connectionString.startsWith('sqlserver://')) {
			return 'sqlserver';
		} else if (connectionString.includes('oracle')) {
			return 'oracle';
		}
		return 'unknown';
	}

	// 转换为 API 配置格式
	private static convertToApiConfig(config: IDataObject): IDataObject {
		console.log('[DataComparisonDualInput] Converting config to API format:', JSON.stringify(config, null, 2));
		
		// 如果来自 prepareComparison，已经有正确的配置
		if (config.isPrepared && config.connectionConfig) {
			console.log('[DataComparisonDualInput] Using prepared config');
			const preparedConfig = config.connectionConfig as IDataObject;
			// 确保有 database_type 字段，并转换类型名称
			const dbType = preparedConfig.type || config.type;
			return {
				...preparedConfig,
				database_type: dbType === 'postgres' ? 'postgresql' : dbType,
			};
		}
		
		// 如果已经是完整配置，确保有database_type字段
		if (config.host && config.type) {
			console.log('[DataComparisonDualInput] Config already complete');
			const dbType = config.type as string;
			return {
				...config,
				database_type: dbType === 'postgres' ? 'postgresql' : dbType,  // API需要database_type字段
			};
		}
		
		// 从连接字符串解析
		const connectionString = config.connectionString as string;
		if (!connectionString) {
			throw new Error('Connection configuration is missing');
		}
		
		// 简单的连接字符串解析
		const urlPattern = /^(\w+):\/\/([^:]+):([^@]+)@([^:\/]+):?(\d+)?\/(.+)$/;
		const match = connectionString.match(urlPattern);
		
		if (match) {
			const dbType = config.type || match[1];
			const normalizedType = dbType === 'postgres' ? 'postgresql' : dbType;
			return {
				type: dbType,
				database_type: normalizedType,  // API需要database_type字段
				host: match[4],
				port: match[5] ? parseInt(match[5]) : DataComparisonDualInput.getDefaultPort(match[1]),
				username: match[2],
				password: match[3],
				database: match[6],
			};
		}
		
		// 如果解析失败，返回原始配置
		const dbType = config.type || 'unknown';
		const normalizedType = dbType === 'postgres' ? 'postgresql' : dbType;
		return {
			type: dbType,
			database_type: normalizedType,  // API需要database_type字段
			connection_string: connectionString,
		};
	}

	// 获取默认端口
	private static getDefaultPort(dbType: string): number {
		const defaultPorts: { [key: string]: number } = {
			postgresql: 5432,
			postgres: 5432,
			mysql: 3306,
			clickzetta: 9000,
			clickhouse: 9000,
			sqlserver: 1433,
			mssql: 1433,
			oracle: 1521,
		};
		return defaultPorts[dbType.toLowerCase()] || 5432;
	}

	// 调用比对 API
	private static async callComparisonAPI(requestData: any, type: string = 'tables'): Promise<any> {
		const fetch = require('node-fetch');
		const apiUrl = type === 'schemas' 
			? 'http://data-diff-api:8000/api/v1/compare/schemas/nested'
			: 'http://data-diff-api:8000/api/v1/compare/tables/nested';
		
		const response = await fetch(apiUrl, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(requestData),
		});
		
		const data = await response.json();
		
		if (!response.ok) {
			console.error('[DataComparisonDualInput] API error response:', JSON.stringify(data, null, 2));
			
			// 更好地处理错误信息
			let errorMessage = 'Comparison failed';
			if (data.detail) {
				if (typeof data.detail === 'string') {
					errorMessage = data.detail;
				} else if (Array.isArray(data.detail)) {
					// FastAPI 验证错误通常返回数组
					errorMessage = data.detail.map((err: any) => {
						if (typeof err === 'string') {
							return err;
						} else if (err.loc && err.msg) {
							return `${err.loc.join('.')} - ${err.msg}`;
						} else {
							return JSON.stringify(err);
						}
					}).join('; ');
				} else if (typeof data.detail === 'object') {
					errorMessage = JSON.stringify(data.detail);
				}
			} else if (data.message) {
				errorMessage = data.message;
			} else if (data.error) {
				errorMessage = data.error;
			}
			
			throw new Error(errorMessage);
		}
		
		return data;
	}
	
	// 获取比对结果
	private static async getComparisonResult(comparisonId: string): Promise<any> {
		const fetch = require('node-fetch');
		const apiUrl = `http://data-diff-api:8000/api/v1/compare/results/${comparisonId}`;
		
		const response = await fetch(apiUrl, {
			method: 'GET',
			headers: { 'Content-Type': 'application/json' },
		});
		
		const data = await response.json();
		
		if (!response.ok) {
			// 如果是404，可能是任务不存在
			if (response.status === 404) {
				throw new Error(`Comparison ID '${comparisonId}' not found. The comparison task may not exist or has expired.`);
			}
			throw new Error(data.detail || data.message || 'Failed to get comparison result');
		}
		
		// 检查任务状态
		if (data.status === 'pending' || data.status === 'running') {
			console.log(`[DataComparisonDualInput] Comparison ${comparisonId} is still ${data.status}`);
		}
		
		return data;
	}

}