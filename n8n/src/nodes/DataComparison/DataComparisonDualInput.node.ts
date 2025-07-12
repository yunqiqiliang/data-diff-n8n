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
						description: 'Compare two database tables',
						action: 'Compare two database tables',
					},
					{
						name: 'Compare Schema',
						value: 'compareSchema',
						description: 'Compare database schemas',
						action: 'Compare database schemas',
					},
					{
						name: 'Get Comparison Result',
						value: 'getComparisonResult',
						description: 'Get the result of a previously started comparison',
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
				description: 'Full table name including schema',
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
				description: 'Full table name including schema',
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
				description: 'Comma-separated list of columns to use as keys for comparison',
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
				description: 'Specific columns to compare (comma-separated)',
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
						displayName: 'Enable Smart Sampling',
						name: 'enableSampling',
						type: 'boolean',
						default: true,
						description: 'Automatically sample large datasets for better performance',
					},
					{
						displayName: 'Sample Size',
						name: 'sampleSize',
						type: 'number',
						default: 0,
						description: 'Number of rows to sample (0 for auto-sampling based on data size)',
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
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
						description: 'Minimum rows before auto-sampling kicks in',
					},
					{
						displayName: 'Sampling Method',
						name: 'samplingMethod',
						type: 'options',
						default: 'SYSTEM',
						options: [
							{
								name: 'System (Fast)',
								value: 'SYSTEM',
								description: 'File-level sampling - faster but less accurate',
							},
							{
								name: 'Row (Accurate)',
								value: 'ROW',
								description: 'Row-level sampling - slower but more accurate',
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
					},
					{
						displayName: 'Sampling Confidence',
						name: 'samplingConfidence',
						type: 'options',
						default: 0.95,
						options: [
							{
								name: '90%',
								value: 0.90,
							},
							{
								name: '95%',
								value: 0.95,
							},
							{
								name: '99%',
								value: 0.99,
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
					},
					{
						displayName: 'Sampling Tolerance',
						name: 'samplingTolerance',
						type: 'options',
						default: 0.01,
						options: [
							{
								name: '0.1%',
								value: 0.001,
							},
							{
								name: '1%',
								value: 0.01,
							},
							{
								name: '5%',
								value: 0.05,
							},
						],
						displayOptions: {
							show: {
								enableSampling: [true],
							},
						},
						description: 'Acceptable margin of error for sampling',
					},
					{
						displayName: 'Float Tolerance',
						name: 'floatTolerance',
						type: 'number',
						default: 0.0001,
						description: 'Tolerance for floating-point comparisons',
						typeOptions: {
							numberPrecision: 6,
						},
					},
					{
						displayName: 'Timestamp Precision',
						name: 'timestampPrecision',
						type: 'options',
						default: 'second',
						options: [
							{
								name: 'Microsecond',
								value: 'microsecond',
							},
							{
								name: 'Millisecond',
								value: 'millisecond',
							},
							{
								name: 'Second',
								value: 'second',
							},
							{
								name: 'Minute',
								value: 'minute',
							},
						],
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
						enable_sampling: advancedOptions.enableSampling !== false,
						sample_size: advancedOptions.sampleSize || 0,
						auto_sample_threshold: advancedOptions.autoSampleThreshold || 100000,
						sampling_method: advancedOptions.samplingMethod || 'SYSTEM',
						sampling_confidence: advancedOptions.samplingConfidence || 0.95,
						sampling_tolerance: advancedOptions.samplingTolerance || 0.01,
						threads: advancedOptions.threads || 1,
						float_tolerance: advancedOptions.floatTolerance,
						timestamp_precision: advancedOptions.timestampPrecision,
						bisection_factor: advancedOptions.bisectionFactor,
						bisection_threshold: advancedOptions.bisectionThreshold,
						case_sensitive: advancedOptions.caseSensitive !== false,
						strict_type_checking: advancedOptions.strictTypeChecking || false,
					}
				};
				
				// 调用表比对 API
				console.log('[DataComparisonDualInput] Calling comparison API with request:', JSON.stringify(requestData, null, 2));
				result = await DataComparisonDualInput.callComparisonAPI(requestData, 'tables');
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
			? 'http://data-diff-api:8000/api/v1/compare/schemas'
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