import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
	IDataObject,
} from 'n8n-workflow';

export class DataComparison implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Data Comparison',
		name: 'dataComparison',
		icon: 'fa:exchange-alt',
		group: ['transform'],
		version: 1,
		description: 'Compare data between different databases using data-diff',
		defaults: {
			name: 'Data Comparison',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'dataDiffConfig',
				required: true,
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
						value: 'compareTables',
						description: 'Compare two database tables',
						action: 'Compare two database tables',
					},
					{
						name: 'Compare Schema',
						value: 'compareSchemas',
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
				default: 'compareTables',
			},
			{
				displayName: 'Source Connection',
				name: 'sourceConnection',
				type: 'string',
				default: '',
				placeholder: 'postgresql://user:pass@host:port/db or from upstream node',
				description: 'Source database connection string (can be filled from upstream DatabaseConnector/ClickzettaConnector node)',
				displayOptions: {
					show: {
						operation: ['compareTables', 'compareSchemas'],
					},
				},
				required: true,
			},
			{
				displayName: 'Target Connection',
				name: 'targetConnection',
				type: 'string',
				default: '',
				placeholder: 'clickzetta://user:pass@host:port/db or from upstream node',
				description: 'Target database connection string (can be filled from upstream DatabaseConnector/ClickzettaConnector node)',
				displayOptions: {
					show: {
						operation: ['compareTables', 'compareSchemas'],
					},
				},
				required: true,
			},
			{
				displayName: 'Source Table',
				name: 'sourceTable',
				type: 'string',
				default: '',
				placeholder: 'schema.table_name or select from upstream node',
				description: 'Source table name (can be selected from upstream DatabaseConnector/ClickzettaConnector node list)',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
				required: true,
			},
			{
				displayName: 'Target Table',
				name: 'targetTable',
				type: 'string',
				default: '',
				placeholder: 'schema.table_name or select from upstream node',
				description: 'Target table name (can be selected from upstream DatabaseConnector/ClickzettaConnector node list)',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
				required: true,
			},
			{
				displayName: 'Key Columns',
				name: 'keyColumns',
				type: 'string',
				default: '',
				placeholder: 'id,user_id (leave empty to use credential default)',
				description: 'Primary key columns (comma-separated). Leave empty to use the default from credentials.',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Columns to Compare',
				name: 'columnsToCompare',
				type: 'string',
				default: '',
				placeholder: 'name,email,status (leave empty for all columns)',
				description: 'Specific columns to compare (comma-separated, leave empty to compare all columns except excluded ones)',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Where Condition',
				name: 'whereCondition',
				type: 'string',
				default: '',
				placeholder: 'status = \'active\' AND created_date > \'2023-01-01\'',
				description: 'SQL WHERE condition to filter rows (optional, specific to this comparison)',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Auto-fill from upstream',
				name: 'autoFillFromUpstream',
				type: 'boolean',
				default: true,
				description: 'Automatically fill connection URLs and table lists from upstream DatabaseConnector/ClickzettaConnector nodes',
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
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;
		const autoFillFromUpstream = this.getNodeParameter('autoFillFromUpstream', 0) as boolean;

		for (let i = 0; i < items.length; i++) {
			try {
				let result: any = {};

				// 尝试从上游节点获取连接信息
				const upstreamData = DataComparison.extractUpstreamData(items, autoFillFromUpstream);

				switch (operation) {
					case 'compareTables':
						result = await DataComparison.compareTables(this, i, upstreamData);
						break;
					case 'compareSchemas':
						result = await DataComparison.compareSchemas(this, i, upstreamData);
						break;
					case 'getComparisonResult':
						result = await DataComparison.getComparisonResult(this, i);
						break;
					default:
						throw new Error(`Unknown operation: ${operation}`);
				}

				returnData.push({
					json: {
						operation,
						success: true,
						upstreamData: upstreamData,
						data: result,
						timestamp: new Date().toISOString(),
					},
				});
			} catch (error: any) {
				returnData.push({
					json: {
						operation,
						success: false,
						error: error?.message || 'Unknown error',
						timestamp: new Date().toISOString(),
					},
				});
			}
		}

		return [returnData];
	}

	private static async compareTables(context: IExecuteFunctions, itemIndex: number, upstreamData: any): Promise<any> {
		let sourceConnection = context.getNodeParameter('sourceConnection', itemIndex) as string;
		let targetConnection = context.getNodeParameter('targetConnection', itemIndex) as string;
		let sourceTable = context.getNodeParameter('sourceTable', itemIndex) as string;
		let targetTable = context.getNodeParameter('targetTable', itemIndex) as string;
		const keyColumns = context.getNodeParameter('keyColumns', itemIndex) as string;
		const columnsToCompare = context.getNodeParameter('columnsToCompare', itemIndex) as string;
		const whereCondition = context.getNodeParameter('whereCondition', itemIndex) as string;

		// 如果连接字符串为空，尝试从上游数据获取
		if (!sourceConnection && upstreamData.connections.length > 0) {
			sourceConnection = upstreamData.connections[0].url;
		}
		if (!targetConnection && upstreamData.connections.length > 1) {
			targetConnection = upstreamData.connections[1].url;
		} else if (!targetConnection && upstreamData.connections.length > 0) {
			targetConnection = upstreamData.connections[0].url;
		}

		// 如果表名为空，尝试从上游数据获取
		if (!sourceTable && upstreamData.tables.length > 0) {
			sourceTable = upstreamData.tables[0].value;
		}
		if (!targetTable && upstreamData.tables.length > 1) {
			targetTable = upstreamData.tables[1].value;
		} else if (!targetTable && upstreamData.tables.length > 0) {
			targetTable = upstreamData.tables[0].value;
		}

		// 获取凭证
		const credentials = await context.getCredentials('dataDiffConfig');

		// 参数优先级：节点表单参数 > 凭据配置 > 默认值
		// 只有特定的参数可以在节点表单中覆盖，其他参数直接从凭据获取
		const mergedKeyColumns = keyColumns || (typeof credentials?.keyColumns === 'string' ? credentials.keyColumns : '') || 'id';

		// 所有系统性参数直接从凭据获取，有默认值保底
		const mergedSampleSize = (typeof credentials?.sampleSize === 'number' ? credentials.sampleSize : 0) || 10000;
		const mergedThreads = (typeof credentials?.threads === 'number' ? credentials.threads : 0) || 4;
		const mergedCaseSensitive = credentials?.caseSensitive !== undefined ? credentials.caseSensitive : true;
		const mergedTolerance = (typeof credentials?.tolerance === 'number' ? credentials.tolerance : 0) || 0.001;
		const mergedMethod = (typeof credentials?.method === 'string' ? credentials.method : '') || 'hashdiff';
		const mergedExcludeColumns = (typeof credentials?.excludeColumns === 'string' ? credentials.excludeColumns : '') || '';
		const mergedBisectionThreshold = (typeof credentials?.bisectionThreshold === 'number' ? credentials.bisectionThreshold : 0) || 1024;
		const mergedStrictTypeChecking = credentials?.strictTypeChecking !== undefined ? credentials.strictTypeChecking : false;

		console.log('参数合并结果:', {
			keyColumns: mergedKeyColumns,
			sampleSize: mergedSampleSize,
			threads: mergedThreads,
			caseSensitive: mergedCaseSensitive,
			tolerance: mergedTolerance,
			method: mergedMethod,
			excludeColumns: mergedExcludeColumns,
			bisectionThreshold: mergedBisectionThreshold,
			strictTypeChecking: mergedStrictTypeChecking,
			source: {
				formKeyColumns: keyColumns,
				credentialsKeyColumns: credentials?.keyColumns,
				credentialsSampleSize: credentials?.sampleSize
			}
		});

		if (!sourceConnection) {
			throw new Error('Source connection string is required');
		}
		if (!targetConnection) {
			throw new Error('Target connection string is required');
		}
		if (!sourceTable) {
			throw new Error('Source table name is required');
		}
		if (!targetTable) {
			throw new Error('Target table name is required');
		}

		// 调用API
		try {
			const apiUrl = 'http://data-diff-api:8000/api/v1/compare/tables/nested';

			// 解析列名 - 使用合并后的参数，确保类型安全
			const keyColumnsList = typeof mergedKeyColumns === 'string' && mergedKeyColumns ?
				mergedKeyColumns.split(',').map(col => col.trim()).filter(col => col) :
				['id'];

			const columnsToCompareList = typeof columnsToCompare === 'string' && columnsToCompare ?
				columnsToCompare.split(',').map(col => col.trim()).filter(col => col) :
				[];

			// 处理排除列（来自凭据），确保类型安全
			const excludeColumnsList = typeof mergedExcludeColumns === 'string' && mergedExcludeColumns ?
				mergedExcludeColumns.split(',').map(col => col.trim()).filter(col => col) :
				[];

			// 解析连接字符串为配置对象
			const sourceConfig = DataComparison.parseConnectionString(sourceConnection);
			const targetConfig = DataComparison.parseConnectionString(targetConnection);

			// 使用嵌套的 JSON 请求方式，与 nested 端点保持一致
			const requestData = {
				source_config: sourceConfig,
				target_config: targetConfig,
				comparison_config: {
					source_table: sourceTable,
					target_table: targetTable,
					key_columns: keyColumnsList,
					columns_to_compare: columnsToCompareList.length > 0 ? columnsToCompareList : undefined,
					exclude_columns: excludeColumnsList.length > 0 ? excludeColumnsList : undefined,
					sample_size: mergedSampleSize,
					threads: mergedThreads,
					case_sensitive: mergedCaseSensitive,
					tolerance: mergedTolerance,
					algorithm: mergedMethod,
					bisection_threshold: mergedBisectionThreshold,
					where_condition: whereCondition || undefined,
					strict_type_checking: mergedStrictTypeChecking
				}
			};

			console.log('发送API请求 (纯JSON方式)');
			console.log('请求URL:', apiUrl);
			console.log('请求体:', JSON.stringify(requestData, null, 2));

			// 发送请求 - 使用重试机制
			try {
				const response = await DataComparison.httpRequestWithRetry(
					context,
					{
						method: 'POST',
						url: apiUrl,
						headers: {
							'Content-Type': 'application/json',
						},
						body: requestData,
						json: true,
						returnFullResponse: true,
					},
					'TableComparison'
				);

				console.log('API请求成功，响应:', JSON.stringify(response.body));

				// 检查 API 是否返回错误
				if (response.body.error) {
					throw new Error(`API返回错误: ${response.body.error}`);
				}

				// API 返回的是 comparison_id，直接返回而不等待结果
				const comparisonId = response.body.comparison_id;
				if (!comparisonId) {
					throw new Error('API 未返回比对ID');
				}

				return {
					comparisonId: comparisonId,
					status: response.body.status || 'started',
					message: response.body.message || '表比对任务已启动',
					requestData: requestData,
					apiUrl: apiUrl,
					timestamp: new Date().toISOString(),
					retrieveResultUrl: `http://data-diff-api:8000/api/v1/compare/results/${comparisonId}`,
					note: 'Use the "Get Comparison Result" operation with this comparison ID to retrieve the results'
				};
			} catch (error: any) {
				console.error('API请求失败:', error.message);
				if (error.response) {
					console.error('错误响应状态:', error.response.statusCode);
					console.error('错误响应内容:', JSON.stringify(error.response.body));
				}
				throw new Error(`启动表比对失败: ${error.message}`);
			}
		} catch (error: any) {
			throw new Error(`Data comparison API request failed: ${error.message}`);
		}
	}

	private static async compareSchemas(context: IExecuteFunctions, itemIndex: number, upstreamData: any): Promise<any> {
		let sourceConnection = context.getNodeParameter('sourceConnection', itemIndex) as string;
		let targetConnection = context.getNodeParameter('targetConnection', itemIndex) as string;

		// 如果连接字符串为空，尝试从上游数据获取
		if (!sourceConnection && upstreamData.connections.length > 0) {
			sourceConnection = upstreamData.connections[0].url;
		}
		if (!targetConnection && upstreamData.connections.length > 1) {
			targetConnection = upstreamData.connections[1].url;
		} else if (!targetConnection && upstreamData.connections.length > 0) {
			targetConnection = upstreamData.connections[0].url;
		}

		if (!sourceConnection) {
			throw new Error('Source connection string is required');
		}
		if (!targetConnection) {
			throw new Error('Target connection string is required');
		}

		// 调用API - 使用嵌套端点
		try {
			const apiUrl = 'http://data-diff-api:8000/api/v1/compare/schemas/nested';

			// 解析连接字符串为配置对象
			const sourceConfig = DataComparison.parseConnectionString(sourceConnection);
			const targetConfig = DataComparison.parseConnectionString(targetConnection);

			// 使用嵌套的 JSON 请求方式
			const requestData = {
				source_config: sourceConfig,
				target_config: targetConfig
			};

			console.log('发送模式比对API请求 (嵌套JSON方式)');
			console.log('请求URL:', apiUrl);
			console.log('请求体:', JSON.stringify(requestData, null, 2));

			try {
				const response = await DataComparison.httpRequestWithRetry(
					context,
					{
						method: 'POST',
						url: apiUrl,
						headers: {
							'Content-Type': 'application/json',
						},
						body: requestData,
						json: true,
						returnFullResponse: true,
					},
					'SchemaComparison'
				);

				console.log('模式比对API响应:', JSON.stringify(response.body));

				// 检查 API 是否返回错误
				if (response.body.error) {
					throw new Error(`API返回错误: ${response.body.error}`);
				}

				// 格式化返回结果
				const result = response.body.result;
				const summary = result.summary || {};
				const diff = result.diff || {};

				// 生成详细的差异明细
				const detailedDifferences = DataComparison.generateDetailedDifferences(diff);
				const executionSummary = DataComparison.generateSchemaSummary(summary, diff);

				return {
					status: response.body.status || 'completed',
					sourceType: response.body.source_type,
					targetType: response.body.target_type,
					summary: {
						identical: summary.schemas_identical || false,
						totalDifferences: summary.total_differences || 0,
						tableDifferences: summary.table_differences || 0,
						columnDifferences: summary.column_differences || 0,
						typeDifferences: summary.type_differences || 0
					},
					differences: {
						tablesOnlyInSource: diff.tables_only_in_source || [],
						tablesOnlyInTarget: diff.tables_only_in_target || [],
						commonTables: diff.common_tables || [],
						columnDifferences: diff.column_diffs || {},
						typeDifferences: diff.type_diffs || {}
					},
					// 添加详细的差异明细
					detailedDifferences: detailedDifferences,
					sourceSchema: {
						databaseType: result.source_schema?.database_type,
						schemaName: result.source_schema?.schema_name,
						totalTables: result.source_schema?.tables?.length || 0,
						tables: result.source_schema?.tables || []
					},
					targetSchema: {
						databaseType: result.target_schema?.database_type,
						schemaName: result.target_schema?.schema_name,
						totalTables: result.target_schema?.tables?.length || 0,
						tables: result.target_schema?.tables || []
					},
					// 执行摘要（友好的文本描述）
					executionSummary: executionSummary,
					requestData: requestData,
					apiUrl: apiUrl,
					timestamp: result.timestamp || new Date().toISOString(),
					// 正确使用executionTime字段
					executionTime: '模式比对已完成',
					// 添加处理时间信息
					processedAt: new Date().toISOString(),
					duration: 'instant' // 模式比对通常很快
				};
			} catch (error: any) {
				console.error('模式比对API请求失败:', error.message);
				if (error.response) {
					console.error('错误响应状态:', error.response.statusCode);
					console.error('错误响应内容:', JSON.stringify(error.response.body));
				}
				throw new Error(`模式比对失败: ${error.message}`);
			}
		} catch (error: any) {
			throw new Error(`Schema comparison API request failed: ${error.message}`);
		}
	}

	private static async getComparisonResult(context: IExecuteFunctions, itemIndex: number): Promise<any> {
		const comparisonId = context.getNodeParameter('comparisonId', itemIndex) as string;

		if (!comparisonId) {
			throw new Error('Comparison ID is required');
		}

		// 调用 API 获取比对结果
		const resultUrl = `http://data-diff-api:8000/api/v1/compare/results/${comparisonId}`;

		try {
			const response = await DataComparison.httpRequestWithRetry(
				context,
				{
					method: 'GET',
					url: resultUrl,
					headers: {
						'Content-Type': 'application/json',
					},
					json: true,
					returnFullResponse: true,
				},
				'GetComparisonResult'
			);

			if (response.statusCode === 404) {
				throw new Error(`Comparison result not found for ID: ${comparisonId}. The comparison may still be running or the ID is invalid.`);
			}

			if (response.statusCode !== 200) {
				throw new Error(`Failed to get comparison result: ${response.statusCode} ${response.statusMessage}`);
			}

			return {
				comparisonId,
				resultUrl,
				status: 'completed',
				data: response.body,
				retrievedAt: new Date().toISOString(),
			};
		} catch (error: any) {
			if (error.message.includes('ECONNREFUSED') || error.message.includes('connect')) {
				throw new Error(`Failed to connect to data-diff API at http://data-diff-api:8000. Please check if the API is running and accessible.`);
			}
			throw error;
		}
	}

	private static extractUpstreamData(items: INodeExecutionData[], autoFill: boolean): any {
		if (!autoFill || items.length === 0) {
			return {};
		}

		const upstreamData: any = {
			connections: [],
			tables: [],
		};

		// 从上游节点提取连接信息和表列表
		for (const item of items) {
			if (item.json) {
				// 提取连接 URL
				if (item.json.connectionUrl) {
					upstreamData.connections.push({
						url: item.json.connectionUrl,
						type: item.json.databaseType || 'unknown',
						config: item.json.connectionConfig,
					});
				}

				// 提取表列表
				if (item.json.tables && Array.isArray(item.json.tables)) {
					upstreamData.tables = upstreamData.tables.concat(item.json.tables);
				}
			}
		}

		return upstreamData;
	}

	private static parseConnectionString(connectionString: string): any {
		/**
		 * 解析数据库连接字符串为配置对象
		 * 支持 PostgreSQL 和 ClickZetta 格式
		 */

		// 如果传入的已经是对象（可能是从上游节点传递过来的）
		if (typeof connectionString === 'object') {
			return connectionString;
		}

		// PostgreSQL: postgresql://user:pass@host:port/database
		if (connectionString.startsWith('postgresql://')) {
			const url = new URL(connectionString);
			return {
				database_type: 'postgresql',
				host: url.hostname,
				port: parseInt(url.port) || 5432,
				username: url.username,
				password: url.password,
				database: url.pathname.substring(1), // 移除开头的 /
				db_schema: 'public'
			};
		}

		// ClickZetta: clickzetta://user:pass@host/database?virtualcluster=xxx&schema=xxx
		if (connectionString.startsWith('clickzetta://')) {
			const url = new URL(connectionString);
			const params = new URLSearchParams(url.search);

			// 从主机名中提取 instance 和 service 信息
			let instance = '';
			let service = '';

			if (url.hostname.includes('.')) {
				// 格式如 instance.service.com
				const hostParts = url.hostname.split('.');
				instance = hostParts[0];
				service = hostParts.slice(1).join('.');
			} else {
				// 如果没有点分隔，假设整个是 instance
				instance = url.hostname;
				service = 'uat-api.clickzetta.com'; // 默认服务地址
			}

			return {
				database_type: 'clickzetta',
				username: url.username,
				password: url.password,
				instance: instance,
				service: service,
				workspace: url.pathname.substring(1) || 'default', // 移除开头的 /
				db_schema: params.get('schema') || 'public', // 修改为 db_schema 以匹配 API 期望
				vcluster: params.get('virtualcluster') || 'default_ap' // 默认虚拟集群
			};
		}

		// 如果是其他格式，尝试作为JSON解析
		try {
			const parsed = JSON.parse(connectionString);

			// 确保 Clickzetta 对象有正确的字段名
			if (parsed.database_type === 'clickzetta') {
				// 如果使用 schema，转换为 db_schema
				if (parsed.schema && !parsed.db_schema) {
					parsed.db_schema = parsed.schema;
					delete parsed.schema; // 删除原来的 schema 字段，避免混淆
				}

				// 确保必要字段有默认值
				parsed.service = parsed.service || 'uat-api.clickzetta.com';
				parsed.vcluster = parsed.vcluster || 'default_ap';
				parsed.db_schema = parsed.db_schema || 'public';
			}

			return parsed;
		} catch {
			// 如果解析失败，返回原始字符串（向后兼容）
			throw new Error(`Unsupported connection string format: ${connectionString}`);
		}
	}

	private static generateSchemaSummary(summary: any, diff: any): string {
		try {
			const identical = summary?.schemas_identical || false;
			const totalDiffs = summary?.total_differences || 0;

			if (identical) {
				return "✅ 模式完全相同 - 两个数据库的模式结构一致";
			}

			const parts: string[] = [];

			// 总览
			parts.push(`📊 发现 ${totalDiffs} 个差异`);

			// 表级差异
			const tablesOnlySource = diff?.tables_only_in_source || [];
			const tablesOnlyTarget = diff?.tables_only_in_target || [];

			if (tablesOnlySource.length > 0) {
				parts.push(`📤 仅在源数据库: ${tablesOnlySource.join(', ')}`);
			}

			if (tablesOnlyTarget.length > 0) {
				parts.push(`📥 仅在目标数据库: ${tablesOnlyTarget.join(', ')}`);
			}

			// 列级差异
			const columnDiffs = diff?.column_diffs || {};
			const columnDiffCount = Object.keys(columnDiffs).length;
			if (columnDiffCount > 0) {
				parts.push(`📋 ${columnDiffCount} 个表有列差异`);
			}

			// 类型差异
			const typeDiffs = diff?.type_diffs || {};
			const typeDiffCount = Object.keys(typeDiffs).length;
			if (typeDiffCount > 0) {
				parts.push(`🔄 ${typeDiffCount} 个表有类型差异`);
			}

			return parts.join(' | ');

		} catch (error) {
			return "⚠️ 模式比对完成但摘要生成失败";
		}
	}

	private static generateDetailedDifferences(diff: any): any {
		const detailed: any = {
			tableLevelDifferences: [],
			columnLevelDifferences: [],
			typeLevelDifferences: [],
			summary: {
				hasTableDifferences: false,
				hasColumnDifferences: false,
				hasTypeDifferences: false
			}
		};

		// 表级差异
		const tablesOnlySource = diff?.tables_only_in_source || [];
		const tablesOnlyTarget = diff?.tables_only_in_target || [];

		if (tablesOnlySource.length > 0 || tablesOnlyTarget.length > 0) {
			detailed.summary.hasTableDifferences = true;
		}

		tablesOnlySource.forEach((table: string) => {
			detailed.tableLevelDifferences.push({
				type: 'missing_in_target',
				table: table,
				description: `表 "${table}" 仅存在于源数据库中`,
				impact: 'high',
				recommendation: '在目标数据库中创建此表'
			});
		});

		tablesOnlyTarget.forEach((table: string) => {
			detailed.tableLevelDifferences.push({
				type: 'missing_in_source',
				table: table,
				description: `表 "${table}" 仅存在于目标数据库中`,
				impact: 'medium',
				recommendation: '检查是否需要删除此表或在源数据库中添加'
			});
		});

		// 列级差异
		const columnDiffs = diff?.column_diffs || {};
		Object.entries(columnDiffs).forEach(([table, diffs]: [string, any]) => {
			detailed.summary.hasColumnDifferences = true;

			const colsOnlySource = diffs.columns_only_in_source || [];
			const colsOnlyTarget = diffs.columns_only_in_target || [];

			colsOnlySource.forEach((column: string) => {
				detailed.columnLevelDifferences.push({
					type: 'column_missing_in_target',
					table: table,
					column: column,
					description: `表 "${table}" 中的列 "${column}" 仅存在于源数据库`,
					impact: 'high',
					recommendation: '在目标数据库的此表中添加该列'
				});
			});

			colsOnlyTarget.forEach((column: string) => {
				detailed.columnLevelDifferences.push({
					type: 'column_missing_in_source',
					table: table,
					column: column,
					description: `表 "${table}" 中的列 "${column}" 仅存在于目标数据库`,
					impact: 'medium',
					recommendation: '检查是否需要删除此列或在源数据库中添加'
				});
			});
		});

		// 类型差异
		const typeDiffs = diff?.type_diffs || {};
		Object.entries(typeDiffs).forEach(([table, changes]: [string, any]) => {
			detailed.summary.hasTypeDifferences = true;

			if (Array.isArray(changes)) {
				changes.forEach((change: any) => {
					detailed.typeLevelDifferences.push({
						type: 'type_mismatch',
						table: table,
						column: change.column,
						sourceType: change.source_type,
						targetType: change.target_type,
						description: `表 "${table}" 中列 "${change.column}" 的类型不匹配: ${change.source_type} vs ${change.target_type}`,
						impact: 'high',
						recommendation: '检查数据兼容性并考虑类型转换'
					});
				});
			}
		});

		return detailed;
	}

	/**
	 * 带重试机制的 API 调用包装器
	 * 专门处理数据库连接错误和网络问题
	 */
	private static async executeWithRetry<T>(
		operation: () => Promise<T>,
		context: {
			operationName: string;
			maxRetries?: number;
			baseDelay?: number;
			maxDelay?: number;
			retryCondition?: (error: any) => boolean;
		}
	): Promise<T> {
		const {
			operationName,
			maxRetries = 3,
			baseDelay = 1000,
			maxDelay = 10000,
			retryCondition = DataComparison.isRetryableError
		} = context;

		let lastError: any;
		let attempt = 0;

		while (attempt <= maxRetries) {
			try {
				console.log(`[${operationName}] 尝试 ${attempt + 1}/${maxRetries + 1}`);
				const result = await operation();

				if (attempt > 0) {
					console.log(`[${operationName}] 重试成功，总共尝试了 ${attempt + 1} 次`);
				}

				return result;
			} catch (error: any) {
				lastError = error;
				console.error(`[${operationName}] 尝试 ${attempt + 1} 失败:`, error.message);

				// 如果是最后一次尝试，或者错误不可重试，直接抛出
				if (attempt >= maxRetries || !retryCondition(error)) {
					console.error(`[${operationName}] 不再重试，原因: ${attempt >= maxRetries ? '达到最大重试次数' : '错误不可重试'}`);
					throw error;
				}

				// 计算延迟时间 (指数退避 + 随机抖动)
				const delay = Math.min(
					baseDelay * Math.pow(2, attempt) + Math.random() * 1000,
					maxDelay
				);

				console.log(`[${operationName}] ${delay}ms 后重试...`);
				await DataComparison.sleep(delay);
				attempt++;
			}
		}

		throw lastError;
	}

	/**
	 * 判断错误是否可以重试
	 */
	private static isRetryableError(error: any): boolean {
		const errorMessage = error.message?.toLowerCase() || '';

		// 可重试的错误类型
		const retryablePatterns = [
			'connection already closed',
			'connection reset',
			'connection refused',
			'connection timeout',
			'connection lost',
			'timeout',
			'network error',
			'econnrefused',
			'etimedout',
			'socket hang up',
			'socket timeout',
			'read timeout',
			'write timeout',
			'connection pool exhausted',
			'max connections reached',
			'server temporarily unavailable'
		];

		// 不可重试的错误类型
		const nonRetryablePatterns = [
			'authentication failed',
			'invalid credentials',
			'permission denied',
			'access denied',
			'not found',
			'table does not exist',
			'database does not exist',
			'invalid query',
			'syntax error',
			'column does not exist',
			'invalid configuration',
			'malformed request'
		];

		// 检查是否是不可重试的错误
		for (const pattern of nonRetryablePatterns) {
			if (errorMessage.includes(pattern)) {
				return false;
			}
		}

		// 检查是否是可重试的错误
		for (const pattern of retryablePatterns) {
			if (errorMessage.includes(pattern)) {
				return true;
			}
		}

		// HTTP 状态码检查
		if (error.response) {
			const statusCode = error.response.statusCode;
			// 5xx 错误和某些 4xx 错误可以重试
			return statusCode >= 500 || statusCode === 408 || statusCode === 429;
		}

		// 默认不重试未知错误
		return false;
	}

	/**
	 * 睡眠函数
	 */
	private static sleep(ms: number): Promise<void> {
		return new Promise(resolve => setTimeout(resolve, ms));
	}

	/**
	 * 增强的 HTTP 请求包装器，内置重试机制
	 */
	private static async httpRequestWithRetry(
		context: IExecuteFunctions,
		requestOptions: any,
		operationName: string
	): Promise<any> {
		return DataComparison.executeWithRetry(
			async () => {
				console.log(`[${operationName}] 发送 HTTP 请求到:`, requestOptions.url);
				const response = await context.helpers.httpRequest(requestOptions);

				// 检查响应是否包含应用级错误
				if (response.body && response.body.error) {
					throw new Error(`API 返回错误: ${response.body.error}`);
				}

				return response;
			},
			{
				operationName: `HTTP-${operationName}`,
				maxRetries: 3,
				baseDelay: 2000,
				maxDelay: 15000
			}
		);
	}
}
