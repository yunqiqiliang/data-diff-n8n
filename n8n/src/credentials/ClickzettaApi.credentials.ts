import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class ClickzettaApi implements ICredentialType {
	name = 'clickzettaApi';
	displayName = 'Clickzetta API';
	documentationUrl = 'https://docs.clickzetta.com/';
	properties: INodeProperties[] = [
		{
			displayName: 'Username',
			name: 'username',
			type: 'string',
			default: '',
			required: true,
			description: 'Clickzetta username for authentication',
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			required: true,
			description: 'Clickzetta password for authentication',
		},
		{
			displayName: 'Service',
			name: 'service',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'clickzetta-serverless',
			description: 'Clickzetta service name (e.g., clickzetta-serverless)',
		},
		{
			displayName: 'Instance',
			name: 'instance',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'your-instance-name',
			description: 'Clickzetta instance name',
		},
		{
			displayName: 'Workspace',
			name: 'workspace',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'default',
			description: 'Clickzetta workspace name',
		},
		{
			displayName: 'VCluster',
			name: 'vcluster',
			type: 'string',
			default: '',
			required: true,
			placeholder: 'default',
			description: 'Clickzetta virtual cluster name',
		},
		{
			displayName: 'Schema',
			name: 'schema',
			type: 'string',
			default: 'public',
			required: true,
			description: 'Default schema name to use',
		},
	];

	// 注意：Clickzetta 使用数据库协议连接，不是 HTTP API
	// 连接测试将在 Clickzetta Connector 节点中进行，而不是在凭证级别
}
