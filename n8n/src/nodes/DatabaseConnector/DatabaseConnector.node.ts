import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
} from 'n8n-workflow';
import fetch from 'node-fetch';
import { ensureSerializable } from '../utils/serialization';

export class DatabaseConnector implements INodeType {
	// 辅助方法：安全地添加数据到返回数组
	private static pushSafeData(returnData: INodeExecutionData[], data: any): void {
		const cleanedData = ensureSerializable(data);
		returnData.push({
			json: cleanedData,
		});
	}
	description: INodeTypeDescription = {
		displayName: 'Database Connector',
		name: 'databaseConnector',
		icon: 'fa:database',
		group: ['transform'],
		version: 1,
		description: 'Connect to various databases for data operations',
		defaults: {
			name: 'Database Connector',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'databaseConnectorCredentials',
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
						action: 'Prepare database for comparison',
					},
				],
				default: 'testConnection',
			},
			{
				displayName: 'Schema Name (for operations)',
				name: 'schemaName',
				type: 'string',
				default: '',
				placeholder: 'Leave empty to use default schema from credentials',
				description: 'Name of the schema to query (optional, overrides credential schema)',
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

		for (let i = 0; i < items.length; i++) {
			let connectionConfig: any = null;
			try {
				// 从凭证获取连接配置
				const credentials = await this.getCredentials('databaseConnectorCredentials', i);
				connectionConfig = DatabaseConnector.buildConnectionConfig(credentials, this, i);
				let result: any = {};

				switch (operation) {
					case 'testConnection':
						result = await DatabaseConnector.testConnectionMethod(connectionConfig);
						// 生成连接 URL 供下游节点使用
						const connectionUrl = DatabaseConnector.buildConnectionUrl(connectionConfig);
						DatabaseConnector.pushSafeData(returnData, {
							operation,
							databaseType: connectionConfig.type,
							success: true,
							connectionUrl,
							connectionConfig: connectionConfig,
							data: result,
							timestamp: new Date().toISOString(),
						});
						break;
					case 'getSchema':
						const schemaName = this.getNodeParameter('schemaName', i) as string;
						result = await DatabaseConnector.getSchemaInfoMethod(connectionConfig, schemaName);
						returnData.push({
							json: {
								operation,
								databaseType: connectionConfig.type,
								success: true,
								data: result,
								timestamp: new Date().toISOString(),
							},
						});
						break;
					case 'listTables':
						const schema = this.getNodeParameter('schemaName', i) as string;
						result = await DatabaseConnector.listTablesMethod(connectionConfig, schema);
						// 格式化表列表供下游节点选择
						const tables = DatabaseConnector.formatTablesForSelection(result, connectionConfig);
						returnData.push({
							json: {
								operation,
								databaseType: connectionConfig.type,
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
						result = await DatabaseConnector.prepareForComparisonMethod(
							connectionConfig,
							schemaForComparison,
							includeSampleData,
							sampleSize,
							tableFilter
						);
						
						// 生成比对专用的输出格式
						const comparisonData = {
							operation,
							databaseType: connectionConfig.type,
							success: true,
							connectionUrl: DatabaseConnector.buildConnectionUrl(connectionConfig),
							connectionConfig: connectionConfig,
							tables: result.tables || [],
							schema: schemaForComparison || connectionConfig.schema,
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
								source_config: { ...connectionConfig },
								// 深拷贝表数组，避免循环引用
								available_tables: (result.tables || []).map((table: any) => ({
									name: table.name,
									value: table.value,
									description: table.description
								})),
								database_type: connectionConfig.type,
							},
						};
						
						DatabaseConnector.pushSafeData(returnData, comparisonData);
						break;
					case 'executeQuery':
						const sqlQuery = this.getNodeParameter('sqlQuery', i) as string;
						const returnRawResults = this.getNodeParameter('returnRawResults', i) as boolean;
						if (!sqlQuery.trim()) {
							throw new Error('SQL query is required');
						}
						result = await DatabaseConnector.executeQueryMethod(connectionConfig, sqlQuery);

						// 处理查询结果
						if (returnRawResults) {
							returnData.push({
								json: {
									operation,
									databaseType: connectionConfig.type,
									success: true,
									query: sqlQuery,
									data: result,
									timestamp: new Date().toISOString(),
								},
							});
						} else {
							// 格式化结果为多个输出项
							const formattedResults = DatabaseConnector.formatQueryResults(result);
							if (formattedResults.length > 0) {
								formattedResults.forEach(row => {
									returnData.push({
										json: {
											...row,
											_meta: {
												operation,
												databaseType: connectionConfig.type,
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
										databaseType: connectionConfig.type,
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
					databaseType: connectionConfig?.type || 'unknown',
					success: false,
					error: error?.message || 'Unknown error',
					timestamp: new Date().toISOString(),
				};
				
				// 如果是凭据错误，添加提示
				if (error?.message?.includes('Could not get parameter') || error?.message?.includes('credentials')) {
					errorData.hint = 'Please ensure database credentials are properly configured';
					errorData.errorType = 'credentials';
					errorData.credentialName = 'databaseConnectorCredentials';
				}
				
				// 如果是连接错误，添加调试信息
				if (connectionConfig) {
					errorData.attemptedConnection = {
						type: connectionConfig.type,
						host: connectionConfig.host,
						database: connectionConfig.database,
						schema: connectionConfig.schema,
					};
				}
				
				returnData.push({
					json: ensureSerializable(errorData),
				});
			}
		}

		return [returnData];
	}

	private static buildConnectionConfig(credentials: any, context: IExecuteFunctions, itemIndex: number): any {
		const databaseType = credentials.databaseType;
		const baseConfig = {
			type: databaseType,
		};

		if (databaseType === 'clickzetta') {
			return {
				...baseConfig,
				username: credentials.username,
				password: credentials.password,
				instance: credentials.instance,
				service: credentials.service,
				workspace: credentials.workspace,
				vcluster: credentials.vcluster,
				schema: credentials.schema,
			};
		} else if (databaseType === 'duckdb') {
			return {
				...baseConfig,
				database: credentials.database,
				schema: credentials.schema,
				filepath: credentials.filepath,
			};
		} else if (['trino', 'presto'].includes(databaseType)) {
			return {
				...baseConfig,
				host: credentials.host,
				port: credentials.port,
				username: credentials.username,
				password: credentials.password,
				catalog: credentials.catalog,
				schema: credentials.schema,
			};
		} else if (databaseType === 'snowflake') {
			return {
				...baseConfig,
				username: credentials.username,
				password: credentials.password,
				account: credentials.account,
				database: credentials.database,
				warehouse: credentials.warehouse,
				schema: credentials.schema,
			};
		} else if (databaseType === 'bigquery') {
			return {
				...baseConfig,
				project: credentials.project,
				dataset: credentials.dataset,
				schema: credentials.schema,
			};
		} else if (databaseType === 'databricks') {
			return {
				...baseConfig,
				access_token: credentials.access_token,
				server_hostname: credentials.server_hostname,
				http_path: credentials.http_path,
				schema: credentials.schema,
			};
		} else {
			// postgresql, mysql, clickhouse, redshift, oracle, mssql, vertica
			return {
				...baseConfig,
				host: credentials.host,
				port: credentials.port,
				username: credentials.username,
				password: credentials.password,
				database: credentials.database,
				schema: credentials.schema,
				ssl: credentials.ssl,
			};
		}
	}

	private static async testConnectionMethod(config: any): Promise<any> {
		try {
			const apiUrl = process.env.DATABASE_API_URL || 'http://data-diff-api:8000/api/v1/connections/test';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(config),
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

	private static async getSchemaInfoMethod(config: any, schemaName: string): Promise<any> {
		try {
			// 使用传入的 schema 参数，如果为空则使用配置中的默认 schema
			const targetSchema = schemaName || config.schema;
			const apiUrl = process.env.DATABASE_SCHEMA_API_URL || 'http://data-diff-api:8000/api/v1/tables/list';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					...config,
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

	private static async listTablesMethod(config: any, schema: string): Promise<any> {
		try {
			// 使用传入的 schema 参数，如果为空则使用配置中的默认 schema
			const targetSchema = schema || config.schema;
			const apiUrl = process.env.DATABASE_SCHEMA_API_URL || 'http://data-diff-api:8000/api/v1/tables/list';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					...config,
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
			throw new Error(`Failed to list tables: ${error.message}`);
		}
	}

	private static async executeQueryMethod(config: any, sqlQuery: string): Promise<any> {
		// 通过后端 API 执行 SQL 查询
		try {
			const apiUrl = process.env.DATABASE_QUERY_API_URL || 'http://data-diff-api:8000/api/v1/query/execute';

			const response = await fetch(apiUrl, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					connection: config,
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

	private static buildConnectionUrl(config: any): string {
		const type = config.type;

		switch (type) {
			case 'clickzetta':
				// Clickzetta 连接 URL 格式: clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>
				// 参数: virtualcluster, schema
				return `clickzetta://${config.username}:${config.password}@${config.instance}.${config.service}/${config.workspace}?virtualcluster=${config.vcluster}&schema=${config.schema}`;

			case 'postgres':
				// postgresql://<user>:<password>@<host>/<database>
				const sslParam = config.ssl ? '?sslmode=require' : '';
				return `postgresql://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}${sslParam}`;

			case 'mysql':
				// mysql://<user>:<password>@<host>/<database>
				const sslQuery = config.ssl ? '?ssl=true' : '';
				return `mysql://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}${sslQuery}`;

			case 'clickhouse':
				return `clickhouse://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}`;

			case 'snowflake':
				// snowflake://<user>:<password>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>
				return `snowflake://${config.username}:${config.password}@${config.account || 'account'}/${config.database || 'database'}/${config.schema}?warehouse=${config.warehouse || 'warehouse'}`;

			case 'bigquery':
				// bigquery://<project>/<dataset>
				return `bigquery://${config.project || 'project'}/${config.dataset || 'dataset'}`;

			case 'redshift':
				return `redshift://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}`;

			case 'oracle':
				return `oracle://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}`;

			case 'mssql':
				return `mssql://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}`;

			case 'duckdb':
				// duckdb://<dbname>@<filepath>
				return `duckdb://${config.database || 'main'}@${config.filepath || ':memory:'}`;

			case 'databricks':
				// databricks://:<access_token>@<server_hostname>/<http_path>
				return `databricks://:${config.access_token || config.password}@${config.server_hostname || config.host}/${config.http_path || config.database}`;

			case 'trino':
				// trino://<user>@<host>/<catalog>/<schema>
				return `trino://${config.username}@${config.host}:${config.port}/${config.catalog}/${config.schema}`;

			case 'presto':
				// presto://<user>@<host>/<catalog>/<schema>
				return `presto://${config.username}@${config.host}:${config.port}/${config.catalog}/${config.schema}`;

			case 'vertica':
				return `vertica://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}`;

			default:
				return `${type}://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}`;
		}
	}

	private static formatTablesForSelection(result: any, config: any): any[] {
		try {
			// 处理不同 API 返回格式
			let tables: string[] = [];

			if (Array.isArray(result)) {
				tables = result;
			} else if (result.tables && Array.isArray(result.tables)) {
				tables = result.tables;
			} else if (result.data && Array.isArray(result.data)) {
				tables = result.data;
			}

			// 转换为选择选项格式
			return tables.map(table => {
				const fullTableName = config.schema ? `${config.schema}.${table}` : table;
				return {
					name: fullTableName,
					value: fullTableName,
					description: `Table: ${table}`,
				};
			});
		} catch (error) {
			console.error('Error formatting tables for selection:', error);
			return [];
		}
	}

	private static async prepareForComparisonMethod(
		config: any,
		schema: string,
		includeSampleData: boolean,
		sampleSize: number,
		tableFilter: string
	): Promise<any> {
		try {
			// 1. 首先获取表列表
			const tablesResult = await DatabaseConnector.listTablesMethod(config, schema);
			let tables = DatabaseConnector.formatTablesForSelection(tablesResult, config);
			
			// 2. 应用表过滤器
			if (tableFilter) {
				const filterPattern = tableFilter.replace(/%/g, '.*').replace(/_/g, '.');
				const filterRegex = new RegExp(filterPattern, 'i');
				tables = tables.filter(table => filterRegex.test(table.name));
			}
			
			// 3. 获取数据库统计信息
			const statistics: any = {
				totalTables: tables.length,
				schema: schema || config.schema || 'default',
				databaseType: config.type,
			};
			
			// 4. 如果需要，获取示例数据
			let sampleData: any = {};
			if (includeSampleData && tables.length > 0) {
				// 限制采样的表数量，避免性能问题
				const tablesToSample = tables.slice(0, 10);
				
				for (const table of tablesToSample) {
					try {
						const query = `SELECT * FROM ${table.value} LIMIT ${sampleSize}`;
						const queryResult = await DatabaseConnector.executeQueryMethod(config, query);
						
						if (queryResult && queryResult.rows) {
							sampleData[table.value] = {
								rows: queryResult.rows,
								rowCount: queryResult.rows.length,
								columns: queryResult.rows.length > 0 ? Object.keys(queryResult.rows[0]) : [],
							};
						}
					} catch (error) {
						// 如果单个表查询失败，继续处理其他表
						console.warn(`Failed to get sample data for ${table.value}:`, error);
						sampleData[table.value] = {
							error: 'Failed to retrieve sample data',
							rowCount: 0,
							columns: [],
						};
					}
				}
			}
			
			// 5. 获取额外的元数据
			try {
				// 可以添加更多元数据查询，如：
				// - 数据库版本
				// - 表大小估算
				// - 索引信息等
				const versionQuery = DatabaseConnector.getVersionQuery(config.type);
				if (versionQuery) {
					const versionResult = await DatabaseConnector.executeQueryMethod(config, versionQuery);
					statistics.databaseVersion = DatabaseConnector.extractVersion(versionResult, config.type);
				}
			} catch (error) {
				// 元数据查询失败不应该影响主要功能
				console.warn('Failed to get additional metadata:', error);
			}
			
			return {
				tables,
				statistics,
				sampleData,
				connectionValid: true,
				preparedAt: new Date().toISOString(),
			};
		} catch (error: any) {
			throw new Error(`Failed to prepare for comparison: ${error.message}`);
		}
	}
	
	private static getVersionQuery(dbType: string): string | null {
		const versionQueries: { [key: string]: string } = {
			mysql: 'SELECT VERSION() as version',
			postgresql: 'SELECT version() as version',
			postgres: 'SELECT version() as version',
			clickzetta: 'SELECT version() as version',
			sqlserver: 'SELECT @@VERSION as version',
			mssql: 'SELECT @@VERSION as version',
		};
		
		return versionQueries[dbType.toLowerCase()] || null;
	}
	
	private static extractVersion(result: any, dbType: string): string {
		try {
			if (result && result.rows && result.rows.length > 0) {
				const row = result.rows[0];
				return row.version || row.VERSION || row.Version || 'Unknown';
			}
			return 'Unknown';
		} catch (error) {
			return 'Unknown';
		}
	}
}
