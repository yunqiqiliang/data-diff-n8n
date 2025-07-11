import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
} from 'n8n-workflow';
import fetch from 'node-fetch';

export class DatabaseConnector implements INodeType {
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
						operation: ['getSchema', 'listTables'],
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
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;

		for (let i = 0; i < items.length; i++) {
			try {
				// 从凭证获取连接配置
				const credentials = await this.getCredentials('databaseConnectorCredentials', i);
				const connectionConfig = DatabaseConnector.buildConnectionConfig(credentials, this, i);
				let result: any = {};

				switch (operation) {
					case 'testConnection':
						result = await DatabaseConnector.testConnectionMethod(connectionConfig);
						// 生成连接 URL 供下游节点使用
						const connectionUrl = DatabaseConnector.buildConnectionUrl(connectionConfig);
						returnData.push({
							json: {
								operation,
								databaseType: connectionConfig.type,
								success: true,
								connectionUrl,
								connectionConfig: connectionConfig,
								data: result,
								timestamp: new Date().toISOString(),
							},
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
				returnData.push({
					json: {
						operation,
						databaseType: 'unknown',
						success: false,
						error: error?.message || 'Unknown error',
						timestamp: new Date().toISOString(),
					},
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
}
