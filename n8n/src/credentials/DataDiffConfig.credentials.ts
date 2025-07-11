import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class DataDiffConfig implements ICredentialType {
	name = 'dataDiffConfig';
	displayName = 'Data-Diff Configuration';
	documentationUrl = 'https://github.com/datafold/data-diff';
	properties: INodeProperties[] = [
		{
			displayName: 'System Configuration',
			name: 'systemConfigSection',
			type: 'notice',
			default: '',
			description: 'Configure system-wide default settings for data comparison operations. These can be overridden in individual nodes.',
		},
		{
			displayName: 'Comparison Method',
			name: 'method',
			type: 'options',
			options: [
				{
					name: 'Hash Diff',
					value: 'hashdiff',
					description: 'Use hash-based comparison for large datasets',
				},
				{
					name: 'Join Diff',
					value: 'joindiff',
					description: 'Use join-based comparison for detailed analysis',
				},
			],
			default: 'hashdiff',
			description: 'Default method to use for data comparison',
		},
		{
			displayName: 'Default Sample Size',
			name: 'sampleSize',
			type: 'number',
			default: 10000,
			description: 'Default number of rows to sample for comparison (0 for all rows, can be overridden in node configuration)',
		},
		{
			displayName: 'Default Number of Threads',
			name: 'threads',
			type: 'number',
			default: 4,
			description: 'Default number of parallel threads to use (can be overridden in node configuration)',
		},
		{
			displayName: 'Default Key Columns',
			name: 'keyColumns',
			type: 'string',
			default: 'id',
			placeholder: 'id,user_id',
			description: 'Default comma-separated list of key columns for comparison (can be overridden in node configuration)',
		},
		{
			displayName: 'Default Exclude Columns',
			name: 'excludeColumns',
			type: 'string',
			default: '',
			placeholder: 'created_at,updated_at',
			description: 'Default comma-separated list of columns to exclude from comparison (can be overridden in node configuration)',
		},
		{
			displayName: 'Default Case Sensitive',
			name: 'caseSensitive',
			type: 'boolean',
			default: true,
			description: 'Default setting for whether string comparisons should be case sensitive (can be overridden in node configuration)',
		},
		{
			displayName: 'Default Tolerance',
			name: 'tolerance',
			type: 'number',
			default: 0.001,
			description: 'Numerical tolerance for floating point comparisons',
		},
		{
			displayName: 'Default Bisection Threshold',
			name: 'bisectionThreshold',
			type: 'number',
			default: 1024,
			description: 'Default threshold for bisection algorithm optimization',
		},
		{
			displayName: 'Default Strict Type Checking',
			name: 'strictTypeChecking',
			type: 'boolean',
			default: false,
			description: 'Default setting for strict type checking - when enabled, comparison will fail if unsupported data types are detected',
		},
	];
}
