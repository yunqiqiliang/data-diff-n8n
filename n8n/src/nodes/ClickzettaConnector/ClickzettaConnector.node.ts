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
		icon: 'file:clickzetta.svg',
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
						operation: ['getSchema', 'listTables'],
					},
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;
		const credentials = await this.getCredentials('clickzettaApi');

		for (let i = 0; i < items.length; i++) {
			try {
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
					default:
						throw new Error(`Unknown operation: ${operation}`);
				}

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

	private static buildConnectionUrl(credentials: any): string {
		// Clickzetta 连接 URL 格式: clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>
		// 参数: virtualcluster, schema
		return `clickzetta://${credentials.username}:${credentials.password}@${credentials.instance}.${credentials.service}/${credentials.workspace}?virtualcluster=${credentials.vcluster}&schema=${credentials.schema}`;
	}

	private static formatTablesForSelection(result: any, credentials: any): any[] {
		try {
			let tables: string[] = [];

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
				const fullTableName = credentials.schema ? `${credentials.schema}.${table}` : table;
				return {
					name: fullTableName,
					value: fullTableName,
					description: `Clickzetta table: ${table}`,
				};
			});
		} catch (error) {
			console.error('Error formatting Clickzetta tables for selection:', error);
			return [];
		}
	}
}
