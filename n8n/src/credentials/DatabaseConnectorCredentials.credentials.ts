import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class DatabaseConnectorCredentials implements ICredentialType {
	name = 'databaseConnectorCredentials';
	displayName = 'Database Connector Credentials';
	documentationUrl = 'https://github.com/datafold/data-diff';
	properties: INodeProperties[] = [
		{
			displayName: 'Database Type',
			name: 'databaseType',
			type: 'options',
			options: [
				{
					name: 'Clickzetta',
					value: 'clickzetta',
					description: 'Connect to Clickzetta database',
				},
				{
					name: 'PostgreSQL',
					value: 'postgres',
					description: 'Connect to PostgreSQL database',
				},
				{
					name: 'MySQL',
					value: 'mysql',
					description: 'Connect to MySQL database',
				},
				{
					name: 'ClickHouse',
					value: 'clickhouse',
					description: 'Connect to ClickHouse database',
				},
				{
					name: 'Snowflake',
					value: 'snowflake',
					description: 'Connect to Snowflake database',
				},
				{
					name: 'BigQuery',
					value: 'bigquery',
					description: 'Connect to Google BigQuery',
				},
				{
					name: 'Redshift',
					value: 'redshift',
					description: 'Connect to Amazon Redshift',
				},
				{
					name: 'Oracle',
					value: 'oracle',
					description: 'Connect to Oracle database',
				},
				{
					name: 'SQL Server',
					value: 'mssql',
					description: 'Connect to Microsoft SQL Server',
				},
				{
					name: 'DuckDB',
					value: 'duckdb',
					description: 'Connect to DuckDB database',
				},
				{
					name: 'Databricks',
					value: 'databricks',
					description: 'Connect to Databricks database',
				},
				{
					name: 'Trino',
					value: 'trino',
					description: 'Connect to Trino database',
				},
				{
					name: 'Presto',
					value: 'presto',
					description: 'Connect to Presto database',
				},
				{
					name: 'Vertica',
					value: 'vertica',
					description: 'Connect to Vertica database',
				},
			],
			default: 'postgres',
			description: 'Type of database to connect to',
		},
		// 通用连接字段
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: 'localhost',
			description: 'Database host',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'mysql', 'clickhouse', 'redshift', 'oracle', 'mssql', 'trino', 'presto', 'vertica'],
				},
			},
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 5432,
			description: 'Database port',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'mysql', 'clickhouse', 'redshift', 'oracle', 'mssql', 'trino', 'presto', 'vertica'],
				},
			},
		},
		{
			displayName: 'Database',
			name: 'database',
			type: 'string',
			default: '',
			description: 'Database name',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'mysql', 'clickhouse', 'redshift', 'oracle', 'mssql', 'duckdb', 'vertica'],
				},
			},
		},
		{
			displayName: 'Username',
			name: 'username',
			type: 'string',
			default: '',
			description: 'Database username',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'mysql', 'clickhouse', 'redshift', 'oracle', 'mssql', 'trino', 'presto', 'vertica', 'clickzetta', 'snowflake', 'bigquery', 'databricks'],
				},
			},
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Database password',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'mysql', 'clickhouse', 'redshift', 'oracle', 'mssql', 'trino', 'presto', 'vertica', 'clickzetta', 'snowflake', 'bigquery', 'databricks'],
				},
			},
		},
		{
			displayName: 'Schema',
			name: 'schema',
			type: 'string',
			default: 'public',
			description: 'Database schema name',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'oracle', 'mssql', 'vertica'],
				},
			},
		},
		// Clickzetta 专用字段
		{
			displayName: 'Instance',
			name: 'instance',
			type: 'string',
			default: '',
			description: 'Clickzetta instance name',
			displayOptions: {
				show: {
					databaseType: ['clickzetta'],
				},
			},
		},
		{
			displayName: 'Service',
			name: 'service',
			type: 'string',
			default: '',
			description: 'Clickzetta service name',
			displayOptions: {
				show: {
					databaseType: ['clickzetta'],
				},
			},
		},
		{
			displayName: 'Workspace',
			name: 'workspace',
			type: 'string',
			default: '',
			description: 'Clickzetta workspace name',
			displayOptions: {
				show: {
					databaseType: ['clickzetta'],
				},
			},
		},
		{
			displayName: 'VCluster',
			name: 'vcluster',
			type: 'string',
			default: '',
			description: 'Clickzetta virtual cluster name',
			displayOptions: {
				show: {
					databaseType: ['clickzetta'],
				},
			},
		},
		{
			displayName: 'Schema',
			name: 'schema',
			type: 'string',
			default: 'public',
			description: 'Database schema name',
			displayOptions: {
				show: {
					databaseType: ['clickzetta'],
				},
			},
		},
		// Trino/Presto 专用字段
		{
			displayName: 'Catalog',
			name: 'catalog',
			type: 'string',
			default: 'hive',
			description: 'Catalog name',
			displayOptions: {
				show: {
					databaseType: ['trino', 'presto'],
				},
			},
		},
		// Snowflake 专用字段
		{
			displayName: 'Account',
			name: 'account',
			type: 'string',
			default: '',
			description: 'Snowflake account identifier',
			displayOptions: {
				show: {
					databaseType: ['snowflake'],
				},
			},
		},
		{
			displayName: 'Warehouse',
			name: 'warehouse',
			type: 'string',
			default: '',
			description: 'Snowflake warehouse name',
			displayOptions: {
				show: {
					databaseType: ['snowflake'],
				},
			},
		},
		// BigQuery 专用字段
		{
			displayName: 'Project',
			name: 'project',
			type: 'string',
			default: '',
			description: 'Google Cloud project ID',
			displayOptions: {
				show: {
					databaseType: ['bigquery'],
				},
			},
		},
		{
			displayName: 'Dataset',
			name: 'dataset',
			type: 'string',
			default: '',
			description: 'BigQuery dataset name',
			displayOptions: {
				show: {
					databaseType: ['bigquery'],
				},
			},
		},
		// Databricks 专用字段
		{
			displayName: 'Server Hostname',
			name: 'server_hostname',
			type: 'string',
			default: '',
			description: 'Databricks server hostname',
			displayOptions: {
				show: {
					databaseType: ['databricks'],
				},
			},
		},
		{
			displayName: 'HTTP Path',
			name: 'http_path',
			type: 'string',
			default: '',
			description: 'Databricks HTTP path',
			displayOptions: {
				show: {
					databaseType: ['databricks'],
				},
			},
		},
		{
			displayName: 'Access Token',
			name: 'access_token',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'Databricks access token',
			displayOptions: {
				show: {
					databaseType: ['databricks'],
				},
			},
		},
		// DuckDB 专用字段
		{
			displayName: 'File Path',
			name: 'filepath',
			type: 'string',
			default: ':memory:',
			description: 'DuckDB database file path (:memory: for in-memory database)',
			displayOptions: {
				show: {
					databaseType: ['duckdb'],
				},
			},
		},
		// SSL 选项
		{
			displayName: 'Use SSL',
			name: 'ssl',
			type: 'boolean',
			default: false,
			description: 'Use SSL/TLS connection',
			displayOptions: {
				show: {
					databaseType: ['postgres', 'mysql', 'oracle', 'mssql', 'vertica'],
				},
			},
		},
	];
}
