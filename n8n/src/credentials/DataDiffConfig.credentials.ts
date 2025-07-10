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
			description: 'Method to use for data comparison',
		},
		{
			displayName: 'Sample Size',
			name: 'sampleSize',
			type: 'number',
			default: 10000,
			description: 'Number of rows to sample for comparison (0 for all rows)',
		},
		{
			displayName: 'Threads',
			name: 'threads',
			type: 'number',
			default: 4,
			description: 'Number of parallel threads to use',
		},
		{
			displayName: 'Key Columns',
			name: 'keyColumns',
			type: 'string',
			default: '',
			placeholder: 'id,user_id',
			description: 'Comma-separated list of key columns for comparison',
		},
		{
			displayName: 'Exclude Columns',
			name: 'excludeColumns',
			type: 'string',
			default: '',
			placeholder: 'created_at,updated_at',
			description: 'Comma-separated list of columns to exclude from comparison',
		},
		{
			displayName: 'Case Sensitive',
			name: 'caseSensitive',
			type: 'boolean',
			default: true,
			description: 'Whether string comparisons should be case sensitive',
		},
		{
			displayName: 'Tolerance',
			name: 'tolerance',
			type: 'number',
			default: 0.001,
			description: 'Numerical tolerance for floating point comparisons',
		},
	];
}
