import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
} from 'n8n-workflow';
import fetch from 'node-fetch';

export class ClickzettaConnector implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Clickzetta Connector',
		name: 'clickzettaConnector',
		icon: 'fa:plug',
		group: ['transform'],
		version: 1,
		description: 'Connect to Clickzetta database for data operations',
		defaults: {
			name: 'Clickzetta Connector',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'clickzettaApi',
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
						name: 'Test Connection',
						value: 'testConnection',
						description: 'Test the database connection',
						action: 'Test the database connection',
					},
					{
						name: 'Get Schema Info',
						value: 'getSchema',
						description: 'Retrieve database schema information',
						action: 'Get database schema information',
					},
					{
						name: 'List Tables',
						value: 'listTables',
						description: 'List all tables in the database',
						action: 'List all tables in the database',
					},
					{
						name: 'Execute Query',
						value: 'executeQuery',
						description: 'Execute custom SQL query',
						action: 'Execute custom SQL query',
					},
					{
						name: 'Prepare for Comparison',
						value: 'prepareComparison',
						description: 'Get connection info and tables for data comparison',
						action: 'Prepare Clickzetta for comparison',
					},
				],
				default: 'testConnection',
			},
			{
				displayName: 'Schema Name',
				name: 'schemaName',
				type: 'string',
				default: '',
				placeholder: 'public',
				description: 'Name of the schema to query (optional)',
				displayOptions: {
					show: {
						operation: ['getSchema', 'listTables', 'prepareComparison'],
					},
				},
			},
			{
				displayName: 'SQL Query',
				name: 'sqlQuery',
				type: 'string',
				typeOptions: {
					rows: 4,
				},
				default: '',
				placeholder: 'SELECT * FROM table_name LIMIT 10',
				description: 'SQL query to execute',
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
			},
			{
				displayName: 'Return Raw Results',
				name: 'returnRawResults',
				type: 'boolean',
				default: false,
				description: 'Whether to return raw query results or formatted data',
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
			},
			{
				displayName: 'Include Sample Data',
				name: 'includeSampleData',
				type: 'boolean',
				default: false,
				description: 'Include sample rows from each table',
				displayOptions: {
					show: {
						operation: ['prepareComparison'],
					},
				},
			},
			{
				displayName: 'Sample Size',
				name: 'sampleSize',
				type: 'number',
				default: 5,
				description: 'Number of sample rows to include per table',
				displayOptions: {
					show: {
						operation: ['prepareComparison'],
						includeSampleData: [true],
					},
				},
			},
			{
				displayName: 'Table Filter',
				name: 'tableFilter',
				type: 'string',
				default: '',
				placeholder: 'e.g., user%, order_*',
				description: 'Filter tables by pattern (SQL LIKE syntax)',
				displayOptions: {
					show: {
						operation: ['prepareComparison'],
					},
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;
		let credentials: any = null;

		for (let i = 0; i < items.length; i++) {
			try {
				// 获取凭据
				credentials = await this.getCredentials('clickzettaApi', i);
				let result: any = {};

				switch (operation) {
					case 'testConnection':
						result = await ClickzettaConnector.testConnectionMethod(credentials);
						// 生成 Clickzetta 连接 URL 供下游节点使用
						const connectionUrl = ClickzettaConnector.buildConnectionUrl(credentials);
						returnData.push({
							json: {
								operation,
								success: true,
								connectionUrl,
								connectionConfig: { ...credentials, type: 'clickzetta' },
								data: result,
								timestamp: new Date().toISOString(),
							},
						});
						break;
					case 'getSchema':
						const schemaName = this.getNodeParameter('schemaName', i) as string;
						result = await ClickzettaConnector.getSchemaInfoMethod(credentials, schemaName);
						returnData.push({
							json: {
								operation,
								success: true,
								data: result,
								timestamp: new Date().toISOString(),
							},
						});
						break;
					case 'listTables':
						const schema = this.getNodeParameter('schemaName', i) as string;
						result = await ClickzettaConnector.listTablesMethod(credentials, schema);
						// 格式化表列表供下游节点选择
						const tables = ClickzettaConnector.formatTablesForSelection(result, credentials);
						returnData.push({
							json: {
								operation,
								success: true,
								tables,
								data: result,
								timestamp: new Date().toISOString(),
							},
						});
						break;
					case 'prepareComparison':
						// 使用默认值避免参数错误
						const schemaForComparison = this.getNodeParameter('schemaName', i, '') as string;
						const includeSampleData = this.getNodeParameter('includeSampleData', i, false) as boolean;
						const sampleSize = this.getNodeParameter('sampleSize', i, 5) as number;
						const tableFilter = this.getNodeParameter('tableFilter', i, '') as string;
						
						// 获取连接信息和表列表
						result = await ClickzettaConnector.prepareForComparisonMethod(
							credentials,
							schemaForComparison,
							includeSampleData,
							sampleSize,
							tableFilter
						);
						
						// 生成 Clickzetta 连接 URL
						const comparisonUrl = ClickzettaConnector.buildConnectionUrl(credentials);
						
						// 生成比对专用的输出格式
						returnData.push({
							json: {
								operation,
								success: true,
								connectionUrl: comparisonUrl,
								connectionConfig: { ...credentials, type: 'clickzetta' },
								tables: result.tables || [],
								schema: schemaForComparison || credentials.schema,
								statistics: result.statistics || {},
								sampleData: result.sampleData || {},
								metadata: {
									totalTables: result.tables?.length || 0,
									includedSampleData: includeSampleData,
									preparedAt: new Date().toISOString(),
								},
								// 为数据比对节点准备的特殊字段
								comparisonReady: true,
								comparisonConfig: {
									// 避免循环引用，创建新对象
									source_config: { ...credentials, type: 'clickzetta' },
									available_tables: [...(result.tables || [])],
									database_type: 'clickzetta',
								},
								timestamp: new Date().toISOString(),
							},
						});
						break;
					case 'executeQuery':
						const sqlQuery = this.getNodeParameter('sqlQuery', i) as string;
						const returnRawResults = this.getNodeParameter('returnRawResults', i) as boolean;
						if (!sqlQuery.trim()) {
							throw new Error('SQL query is required');
						}
						result = await ClickzettaConnector.executeQueryMethod(credentials, sqlQuery);

						// 处理查询结果
						if (returnRawResults) {
							returnData.push({
								json: {
									operation,
									success: true,
									query: sqlQuery,
									data: result,
									timestamp: new Date().toISOString(),
								},
							});
						} else {
							// 格式化结果为多个输出项
							const formattedResults = ClickzettaConnector.formatQueryResults(result);
							if (formattedResults.length > 0) {
								formattedResults.forEach(row => {
									returnData.push({
										json: {
											...row,
											_meta: {
												operation,
												query: sqlQuery,
												timestamp: new Date().toISOString(),
											},
										},
									});
								});
							} else {
								returnData.push({
									json: {
										operation,
										success: true,
										query: sqlQuery,
										message: 'Query executed successfully but returned no results',
										data: result,
										timestamp: new Date().toISOString(),
									},
								});
							}
						}
						break;
					default:
						throw new Error(`Unknown operation: ${operation}`);
				}

			} catch (error: any) {
				// 提供更详细的错误信息
				const errorData: any = {
					operation,
					success: false,
					error: error?.message || 'Unknown error',
					timestamp: new Date().toISOString(),
				};
				
				// 如果是凭据错误，添加提示
				if (error?.message?.includes('Could not get parameter') || error?.message?.includes('credentials')) {
					errorData.hint = 'Please ensure Clickzetta credentials are properly configured';
					errorData.errorType = 'credentials';
					errorData.credentialName = 'clickzettaApi';
					errorData.requiredFields = ['username', 'password', 'instance', 'service', 'workspace', 'vcluster'];
				}
				
				// 如果有凭据信息，添加部分调试信息（不包含敏感信息）
				if (credentials) {
					errorData.connectionInfo = {
						instance: credentials.instance || 'not set',
						service: credentials.service || 'not set',
						workspace: credentials.workspace || 'not set',
						vcluster: credentials.vcluster || 'not set',
						schema: credentials.schema || 'not set',
					};
				}
				
				returnData.push({
					json: errorData,
				});
			}
		}

		return [returnData];
	}

	private static async testConnectionMethod(credentials: any): Promise<any> {
		// 通过后端 API 测试 Clickzetta 连接，传递 type 字段
		try {
			const apiUrl = process.env.CLICKZETTA_API_URL || 'http://data-diff-api:8000/api/v1/connections/test';
			const body = { ...credentials, type: 'clickzetta' };

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(body),
			});

			const data = await response.json();
			if (!response.ok || !data.success) {
				throw new Error(data.detail || data.message || 'Connection test failed');
			}
			return data;
		} catch (error: any) {
			throw new Error(`Connection test failed: ${error.message}`);
		}
	}

	private static async getSchemaInfoMethod(credentials: any, schemaName: string): Promise<any> {
		// 通过后端 API 获取真实 schema 信息
		try {
			const targetSchema = schemaName || credentials.schema;
			const apiUrl = process.env.CLICKZETTA_SCHEMA_API_URL || process.env.CLICKZETTA_API_URL || 'http://data-diff-api:8000/api/v1/tables/list';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					...credentials,
					type: 'clickzetta',
					schema: targetSchema,
				}),
			});

			if (!response.ok) {
				const errorText = await response.text();
				throw new Error(`API error: ${response.status} ${errorText}`);
			}

			const data = await response.json();
			return data;
		} catch (error: any) {
			throw new Error(`Failed to get schema info: ${error.message}`);
		}
	}

	private static async listTablesMethod(credentials: any, schema: string): Promise<any> {
		// 通过后端 API 获取真实表列表
		try {
			const targetSchema = schema || credentials.schema;
			const apiUrl = process.env.CLICKZETTA_SCHEMA_API_URL || process.env.CLICKZETTA_API_URL || 'http://data-diff-api:8000/api/v1/tables/list';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					...credentials,
					type: 'clickzetta',
					schema: targetSchema,
				}),
			});

			if (!response.ok) {
				const errorText = await response.text();
				throw new Error(`API error: ${response.status} ${errorText}`);
			}

			const data = await response.json();
			// 转换为 N8N 选项格式
			if (data.tables) {
				return data.tables.map((table: string) => ({
					name: table,
					value: table,
				}));
			}
			return [];
		} catch (error: any) {
			throw new Error(`Failed to list tables: ${error.message}`);
		}
	}

	private static async executeQueryMethod(credentials: any, sqlQuery: string): Promise<any> {
		// 通过后端 API 执行 SQL 查询
		try {
			const apiUrl = process.env.CLICKZETTA_QUERY_API_URL || 'http://data-diff-api:8000/api/v1/query/execute';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					connection: {
						...credentials,
						type: 'clickzetta',
					},
					query: sqlQuery,
				}),
			});

			if (!response.ok) {
				const errorText = await response.text();
				throw new Error(`API error: ${response.status} ${errorText}`);
			}

			const data = await response.json();
			if (!data.success) {
				throw new Error(data.error || 'Query execution failed');
			}

			return data.result || data.data || [];
		} catch (error: any) {
			throw new Error(`Failed to execute query: ${error.message}`);
		}
	}

	private static formatQueryResults(result: any): any[] {
		try {
			// 处理不同的结果格式
			if (Array.isArray(result)) {
				return result;
			} else if (result.rows && Array.isArray(result.rows)) {
				return result.rows;
			} else if (result.data && Array.isArray(result.data)) {
				return result.data;
			} else if (result.result && Array.isArray(result.result)) {
				return result.result;
			}

			// 如果结果不是数组，将其包装为数组
			return [result];
		} catch (error) {
			console.error('Error formatting query results:', error);
			return [];
		}
	}

	private static buildConnectionUrl(credentials: any): string {
		// Clickzetta 连接 URL 格式: clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>
		// 参数: virtualcluster, schema
		return `clickzetta://${credentials.username}:${credentials.password}@${credentials.instance}.${credentials.service}/${credentials.workspace}?virtualcluster=${credentials.vcluster}&schema=${credentials.schema}`;
	}

	private static formatTablesForSelection(result: any, credentials: any): any[] {
		try {
			let tables: any[] = [];

			// 处理不同 API 返回格式
			if (Array.isArray(result)) {
				tables = result;
			} else if (result.tables && Array.isArray(result.tables)) {
				tables = result.tables;
			} else if (result.data && Array.isArray(result.data)) {
				tables = result.data;
			}

			// 转换为选择选项格式
			return tables.map(table => {
				// 处理表名可能是对象的情况
				let tableName: string;
				if (typeof table === 'string') {
					tableName = table;
				} else if (table && typeof table === 'object') {
					// 尝试从对象中提取表名
					tableName = table.name || table.tableName || table.table_name || 
							   table.TABLE_NAME || table.Name || String(table);
				} else {
					tableName = String(table);
				}
				
				const fullTableName = credentials.schema ? `${credentials.schema}.${tableName}` : tableName;
				return {
					name: fullTableName,
					value: fullTableName,
					description: `Clickzetta table: ${tableName}`,
				};
			});
		} catch (error) {
			console.error('Error formatting Clickzetta tables for selection:', error);
			return [];
		}
	}

	private static async prepareForComparisonMethod(
		credentials: any,
		schema: string,
		includeSampleData: boolean,
		sampleSize: number,
		tableFilter: string
	): Promise<any> {
		try {
			// 1. 首先获取表列表
			const tablesResult = await ClickzettaConnector.listTablesMethod(credentials, schema);
			
			// 调试：查看原始返回数据
			console.log('ClickZetta listTables 原始返回:', JSON.stringify(tablesResult, null, 2));
			
			let tables = ClickzettaConnector.formatTablesForSelection(tablesResult, credentials);
			
			// 2. 应用表过滤器
			if (tableFilter) {
				const filterPattern = tableFilter.replace(/%/g, '.*').replace(/_/g, '.');
				const filterRegex = new RegExp(filterPattern, 'i');
				tables = tables.filter(table => filterRegex.test(table.name));
			}
			
			// 3. 获取 Clickzetta 特定的统计信息
			const statistics: any = {
				totalTables: tables.length,
				schema: schema || credentials.schema || 'default',
				databaseType: 'clickzetta',
				workspace: credentials.workspace,
				vcluster: credentials.vcluster,
			};
			
			// 4. 如果需要，获取示例数据
			let sampleData: any = {};
			if (includeSampleData && tables.length > 0) {
				// 限制采样的表数量，避免性能问题
				const tablesToSample = tables.slice(0, 10);
				
				for (const table of tablesToSample) {
					try {
						// 使用 TABLESAMPLE 进行高效采样
						const query = `SELECT * FROM ${table.value} TABLESAMPLE (${sampleSize} ROWS)`;
						const queryResult = await ClickzettaConnector.executeQueryMethod(credentials, query);
						
						if (queryResult && queryResult.rows) {
							sampleData[table.value] = {
								rows: queryResult.rows,
								rowCount: queryResult.rows.length,
								columns: queryResult.rows.length > 0 ? Object.keys(queryResult.rows[0]) : [],
								samplingMethod: 'TABLESAMPLE',
							};
						}
					} catch (error) {
						// 如果 TABLESAMPLE 失败，尝试使用 LIMIT
						try {
							const fallbackQuery = `SELECT * FROM ${table.value} LIMIT ${sampleSize}`;
							const fallbackResult = await ClickzettaConnector.executeQueryMethod(credentials, fallbackQuery);
							
							if (fallbackResult && fallbackResult.rows) {
								sampleData[table.value] = {
									rows: fallbackResult.rows,
									rowCount: fallbackResult.rows.length,
									columns: fallbackResult.rows.length > 0 ? Object.keys(fallbackResult.rows[0]) : [],
									samplingMethod: 'LIMIT',
								};
							}
						} catch (fallbackError) {
							console.warn(`Failed to get sample data for ${table.value}:`, fallbackError);
							sampleData[table.value] = {
								error: 'Failed to retrieve sample data',
								rowCount: 0,
								columns: [],
							};
						}
					}
				}
			}
			
			// 5. 获取 Clickzetta 特定的元数据
			try {
				// 获取版本信息
				const versionQuery = 'SELECT version() as version';
				const versionResult = await ClickzettaConnector.executeQueryMethod(credentials, versionQuery);
				if (versionResult && versionResult.rows && versionResult.rows.length > 0) {
					statistics.clickzettaVersion = versionResult.rows[0].version || 'Unknown';
				}
				
				// 可以添加更多 Clickzetta 特定的元数据查询
			} catch (error) {
				console.warn('Failed to get Clickzetta metadata:', error);
			}
			
			return {
				tables,
				statistics,
				sampleData,
				connectionValid: true,
				preparedAt: new Date().toISOString(),
				clickzettaSpecific: {
					supportsTablesample: true,
					supportedSamplingMethods: ['TABLESAMPLE', 'SYSTEM', 'ROW'],
				},
			};
		} catch (error: any) {
			throw new Error(`Failed to prepare Clickzetta for comparison: ${error.message}`);
		}
	}
}
