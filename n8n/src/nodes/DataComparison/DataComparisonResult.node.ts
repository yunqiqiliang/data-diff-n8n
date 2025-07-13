import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
} from 'n8n-workflow';

export class DataComparisonResult implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Data Comparison Result',
		name: 'dataComparisonResult',
		icon: 'fa:check-circle',
		group: ['transform'],
		version: 1,
		description: 'Get the result of a data comparison task (supports both table and schema comparisons)',
		defaults: {
			name: 'Get Comparison Result',
		},
		inputs: [NodeConnectionType.Main],  // 接受输入以获取 comparison ID
		outputs: [NodeConnectionType.Main],
		properties: [
			{
				displayName: 'Comparison ID',
				name: 'comparisonId',
				type: 'string',
				default: '',
				placeholder: 'e.g., {{ $json.comparison_id }}',
				description: 'The ID of the comparison task. Can be provided manually or extracted from input data (comparison_id field).',
				hint: 'Leave empty to automatically use comparison_id from the input data',
				required: false,
			},
			{
				displayName: 'Wait for Completion',
				name: 'waitForCompletion',
				type: 'boolean',
				default: false,
				description: 'Poll until the comparison is complete (blocking)',
			},
			{
				displayName: 'Max Wait Time',
				name: 'maxWaitTime',
				type: 'number',
				typeOptions: {
					minValue: 10,
					maxValue: 3600,
				},
				default: 300,
				description: 'Maximum time to wait for results in seconds',
				displayOptions: {
					show: {
						waitForCompletion: [true],
					},
				},
			},
			{
				displayName: 'Poll Interval',
				name: 'pollInterval',
				type: 'number',
				typeOptions: {
					minValue: 1,
					maxValue: 60,
				},
				default: 5,
				description: 'How often to check for results in seconds',
				displayOptions: {
					show: {
						waitForCompletion: [true],
					},
				},
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		let comparisonId = this.getNodeParameter('comparisonId', 0, '') as string;
		const waitForCompletion = this.getNodeParameter('waitForCompletion', 0, false) as boolean;
		
		// 如果没有手动输入 comparison ID，尝试从输入数据中获取
		if (!comparisonId && items.length > 0) {
			const firstItem = items[0].json;
			// 尝试多个可能的字段名
			const possibleId = firstItem.comparison_id || firstItem.comparisonId || firstItem.id || '';
			comparisonId = String(possibleId);
		}
		
		if (!comparisonId) {
			throw new Error('Comparison ID is required. Please provide it manually or ensure the input contains a comparison_id field.');
		}

		try {
			let result = await DataComparisonResult.getComparisonResult(comparisonId);
			
			// 检查初始状态是否失败
			if (result.status === 'failed') {
				const errorMessage = result.error || result.message || 'Comparison failed';
				throw new Error(`Comparison failed: ${errorMessage}`);
			}
			
			// 如果需要等待完成（支持多种状态）
			if (waitForCompletion && (result.status === 'running' || result.status === 'pending' || result.status === 'started')) {
				const maxWaitTime = this.getNodeParameter('maxWaitTime', 0, 300) as number;
				const pollInterval = this.getNodeParameter('pollInterval', 0, 5) as number;
				const startTime = Date.now();
				
				while (result.status === 'running' || result.status === 'pending' || result.status === 'started') {
					// 检查超时
					if (Date.now() - startTime > maxWaitTime * 1000) {
						throw new Error(`Comparison timed out after ${maxWaitTime} seconds`);
					}
					
					// 等待
					await new Promise(resolve => setTimeout(resolve, pollInterval * 1000));
					
					// 重新获取结果
					result = await DataComparisonResult.getComparisonResult(comparisonId);
					
					// 检查是否失败
					if (result.status === 'failed') {
						const errorMessage = result.error || result.message || 'Comparison failed';
						throw new Error(`Comparison failed: ${errorMessage}`);
					}
				}
			}
			
			// 添加结果类型标识
			if (result.result) {
				// 如果有 result 字段，说明是完整的结果
				const comparisonType = result.result.comparison_type || 
					(result.result.schemas ? 'schema' : 'table');
				
				return [this.helpers.returnJsonArray([{
					...result,
					comparison_type: comparisonType
				}])];
			}
			
			return [this.helpers.returnJsonArray([result])];
			
		} catch (error: any) {
			throw new Error(`Failed to get comparison result: ${error.message}`);
		}
	}

	private static async getComparisonResult(comparisonId: string): Promise<any> {
		const fetch = require('node-fetch');
		const apiUrl = `http://data-diff-api:8000/api/v1/compare/results/${comparisonId}`;
		
		const response = await fetch(apiUrl, {
			method: 'GET',
			headers: {
				'Content-Type': 'application/json',
			},
		});
		
		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`API Error: ${response.status} - ${errorText}`);
		}
		
		return response.json();
	}
}