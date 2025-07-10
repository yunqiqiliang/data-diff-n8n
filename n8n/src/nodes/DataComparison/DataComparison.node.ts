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
		icon: 'file:dataComparison.svg',
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
						name: 'Compare Tables',
						value: 'compareTables',
						description: 'Compare two database tables',
						action: 'Compare two database tables',
					},
					{
						name: 'Compare Schemas',
						value: 'compareSchemas',
						description: 'Compare database schemas',
						action: 'Compare database schemas',
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
				required: true,
			},
			{
				displayName: 'Target Connection',
				name: 'targetConnection',
				type: 'string',
				default: '',
				placeholder: 'clickzetta://user:pass@host:port/db or from upstream node',
				description: 'Target database connection string (can be filled from upstream DatabaseConnector/ClickzettaConnector node)',
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
				default: 'id',
				placeholder: 'id,user_id',
				description: 'Primary key columns (comma-separated)',
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
				description: 'Specific columns to compare (comma-separated, leave empty to compare all columns)',
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
				description: 'SQL WHERE condition to filter rows (optional)',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Sample Size',
				name: 'sampleSize',
				type: 'number',
				default: 10000,
				description: 'Number of rows to sample for comparison',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Number of Threads',
				name: 'threads',
				type: 'number',
				default: 4,
				description: 'Number of parallel threads for comparison',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Case Sensitive',
				name: 'caseSensitive',
				type: 'boolean',
				default: true,
				description: 'Whether string comparisons should be case sensitive',
				displayOptions: {
					show: {
						operation: ['compareTables'],
					},
				},
			},
			{
				displayName: 'Bisection Threshold',
				name: 'bisectionThreshold',
				type: 'number',
				default: 1024,
				description: 'Threshold for bisection algorithm optimization',
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
		const sampleSize = context.getNodeParameter('sampleSize', itemIndex) as number;
		const threads = context.getNodeParameter('threads', itemIndex) as number;
		const caseSensitive = context.getNodeParameter('caseSensitive', itemIndex) as boolean;
		const bisectionThreshold = context.getNodeParameter('bisectionThreshold', itemIndex) as number;

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
			const apiUrl = 'http://localhost:8000/api/v1/compare/tables';

			// 解析列名
			const keyColumnsList = keyColumns.split(',').map(col => col.trim()).filter(col => col);
			const columnsToCompareList = columnsToCompare ?
				columnsToCompare.split(',').map(col => col.trim()).filter(col => col) :
				[];

			// 使用简化的 JSON 请求方式，与API测试保持一致
			const requestData = {
				source_connection: sourceConnection,
				target_connection: targetConnection,
				source_table: sourceTable,
				target_table: targetTable,
				key_columns: keyColumnsList,  // 使用正确的参数名
				columns_to_compare: columnsToCompareList.length > 0 ? columnsToCompareList : undefined,
				sample_size: sampleSize,
				threads: threads,
				case_sensitive: caseSensitive,
				bisection_threshold: bisectionThreshold,
				where_condition: whereCondition || undefined
			};

			console.log('发送API请求 (纯JSON方式)');
			console.log('请求URL:', apiUrl);
			console.log('请求体:', JSON.stringify(requestData, null, 2));

			// 发送请求
			try {
				const response = await context.helpers.httpRequest({
					method: 'POST',
					url: apiUrl,
					headers: {
						'Content-Type': 'application/json',
					},
					body: requestData,
					json: true,
					returnFullResponse: true,
				});

				console.log('API请求成功，响应:', JSON.stringify(response.body));

				// API 返回的是 comparison_id，需要查询结果
				const comparisonId = response.body.comparison_id;
				if (!comparisonId) {
					throw new Error('API 未返回比对ID');
				}

				// 等待比对完成并获取结果
				let comparisonResult;
				let attempts = 0;
				const maxAttempts = 30; // 最多等待30次，每次2秒

				while (attempts < maxAttempts) {
					await new Promise(resolve => setTimeout(resolve, 2000)); // 等待2秒
					attempts++;

					try {
						const resultResponse = await context.helpers.httpRequest({
							method: 'GET',
							url: `http://localhost:8000/api/v1/compare/results/${comparisonId}`,
							json: true,
							returnFullResponse: true,
						});

						if (resultResponse.body.status === 'completed' && resultResponse.body.result) {
							comparisonResult = resultResponse.body.result;
							break;
						} else if (resultResponse.body.status === 'error') {
							throw new Error(`比对失败: ${resultResponse.body.message || '未知错误'}`);
						}
						// 如果状态是 running 或其他，继续等待
					} catch (error: any) {
						console.error(`查询比对结果失败 (尝试 ${attempts}):`, error.message);
						if (attempts >= maxAttempts) {
							throw new Error(`比对超时：经过 ${maxAttempts} 次尝试仍未完成`);
						}
					}
				}

				if (!comparisonResult) {
					throw new Error('比对超时：未能在预期时间内完成');
				}

				// 返回成功结果
				return {
					sourceConnection,
					targetConnection,
					sourceTable,
					targetTable,
					keyColumns: keyColumnsList,
					columnsToCompare: columnsToCompareList,
					whereCondition: whereCondition || null,
					sampleSize,
					threads,
					caseSensitive,
					bisectionThreshold,
					result: comparisonResult.status || 'completed',
					summary: comparisonResult.summary || {},
					statistics: comparisonResult.statistics || {},
					config: comparisonResult.config || {},
					sample_differences: comparisonResult.sample_differences || [],
					availableConnections: upstreamData.connections,
					availableTables: upstreamData.tables,
					executionTime: comparisonResult.execution_time_seconds ? `${comparisonResult.execution_time_seconds}s` : '0s',
					jobId: comparisonResult.job_id || comparisonId,
					comparisonId: comparisonId,
					rawResponse: comparisonResult // 保留原始响应，用于调试
				};
			} catch (error: any) {
				console.error('API请求失败:', error.message);
				if (error.response) {
					console.error('错误响应状态:', error.response.statusCode);
					console.error('错误响应内容:', JSON.stringify(error.response.body));
				}

				// 返回错误结果
				return {
					sourceConnection,
					targetConnection,
					sourceTable,
					targetTable,
					keyColumns: keyColumnsList,
					columnsToCompare: columnsToCompareList,
					whereCondition: whereCondition || null,
					sampleSize,
					threads,
					caseSensitive,
					bisectionThreshold,
					result: 'error',
					summary: {
						status: '请求失败',
						error: error.message,
						statusCode: error.response?.statusCode,
						statusMessage: error.response?.statusMessage
					},
					details: error.response?.body || {},
					availableConnections: upstreamData.connections,
					availableTables: upstreamData.tables,
					executionTime: '0s',
					jobId: 'error-job-id',
				};
			}
		} catch (error: any) {
			throw new Error(`Data comparison API request failed: ${error}`);
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

		// 调用API
		try {
			const apiUrl = 'http://localhost:8000/api/v1/compare/schemas';

			// 查询参数
			const queryParams = {
				source_connection: sourceConnection,
				target_connection: targetConnection,
				operation_type: 'compareSchemas',
				kwargs: '{}'  // 添加关键的 kwargs 参数
			};

			// 构建完整的URL，包含查询参数
			const queryUrl = new URL(apiUrl);
			Object.entries(queryParams).forEach(([key, value]) => {
				queryUrl.searchParams.append(key, String(value));
			});

			console.log('发送模式比较API请求 (查询参数+请求体方式)');
			console.log('请求URL:', queryUrl.toString());

			try {
				const response = await context.helpers.httpRequest({
					method: 'POST',
					url: queryUrl.toString(),
					headers: {
						'Content-Type': 'application/json',
					},
					body: {},  // 空请求体，所有参数在URL中
					json: true,
					returnFullResponse: true,
				});

				console.log('模式比较API响应:', JSON.stringify(response.body));

				// 返回API响应和其他上下文信息
				return {
					sourceConnection,
					targetConnection,
					result: response.body.result || 'completed',
					differences: response.body.differences || [],
					availableConnections: upstreamData.connections,
					executionTime: response.body.execution_time || '0s',
					jobId: response.body.comparison_id || response.body.job_id || 'schema-job',
					rawResponse: response.body // 保留原始响应，用于调试
				};
			} catch (error: any) {
				console.error('模式比较API请求失败:', error.message);
				if (error.response) {
					console.error('错误响应状态:', error.response.statusCode);
					console.error('错误响应内容:', JSON.stringify(error.response.body));
				}

				// 返回错误结果
				return {
					sourceConnection,
					targetConnection,
					result: 'error',
					summary: {
						status: '请求失败',
						error: error.message,
						statusCode: error.response?.statusCode,
						statusMessage: error.response?.statusMessage
					},
					differences: [],
					availableConnections: upstreamData.connections,
					executionTime: '0s',
					jobId: 'error-schema-job',
				};
			}
		} catch (error: any) {
			throw new Error(`Schema comparison API request failed: ${error}`);
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
}
