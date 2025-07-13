import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeConnectionType,
} from 'n8n-workflow';

export class DataComparisonMaintenance implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Data Comparison Maintenance',
		name: 'dataComparisonMaintenance',
		icon: 'fa:tools',
		group: ['transform'],
		version: 1,
		description: 'Maintenance operations for data comparison system',
		defaults: {
			name: 'Data Comparison Maintenance',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				options: [
					{
						name: 'Clear Materialized Records',
						value: 'clearRecords',
						description: 'Clear comparison history from database',
					},
					{
						name: 'Get System Statistics',
						value: 'getStatistics',
						description: 'Get statistics about stored comparisons',
					},
					{
						name: 'Clean Up Failed Tasks',
						value: 'cleanupFailed',
						description: 'Remove failed or stuck comparison tasks',
					},
					{
						name: 'Export Comparison History',
						value: 'exportHistory',
						description: 'Export comparison history to CSV/JSON',
					},
					{
						name: 'Database Maintenance',
						value: 'dbMaintenance',
						description: 'Perform database maintenance tasks (VACUUM, ANALYZE)',
					},
				],
				default: 'getStatistics',
				description: 'The maintenance operation to perform',
			},
			// Clear Records Options
			{
				displayName: 'Clear Options',
				name: 'clearOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						operation: ['clearRecords'],
					},
				},
				options: [
					{
						displayName: 'Record Type',
						name: 'recordType',
						type: 'options',
						options: [
							{
								name: 'All Records',
								value: 'all',
								description: 'Clear all comparison records',
							},
							{
								name: 'Table Comparisons Only',
								value: 'table',
								description: 'Clear only table comparison records',
							},
							{
								name: 'Schema Comparisons Only',
								value: 'schema',
								description: 'Clear only schema comparison records',
							},
						],
						default: 'all',
						description: 'Which type of records to clear',
					},
					{
						displayName: 'Filter',
						name: 'filter',
						type: 'options',
						options: [
							{
								name: 'All Records',
								value: 'all',
								description: 'Clear all records of selected type',
							},
							{
								name: 'Failed Only',
								value: 'failed',
								description: 'Clear only failed comparisons',
							},
							{
								name: 'Older Than Days',
								value: 'age',
								description: 'Clear records older than specified days',
							},
						],
						default: 'all',
						description: 'Filter criteria for clearing records',
					},
					{
						displayName: 'Days to Keep',
						name: 'daysToKeep',
						type: 'number',
						typeOptions: {
							minValue: 0,
						},
						default: 30,
						description: 'Number of days of records to keep (0 = clear all)',
						displayOptions: {
							show: {
								filter: ['age'],
							},
						},
					},
					{
						displayName: 'Confirm Clear',
						name: 'confirmClear',
						type: 'boolean',
						default: false,
						description: 'Confirm that you want to clear the records (safety check)',
						required: true,
					},
				],
			},
			// Export Options
			{
				displayName: 'Export Options',
				name: 'exportOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						operation: ['exportHistory'],
					},
				},
				options: [
					{
						displayName: 'Export Format',
						name: 'format',
						type: 'options',
						options: [
							{
								name: 'JSON',
								value: 'json',
								description: 'Export as JSON format',
							},
							{
								name: 'CSV',
								value: 'csv',
								description: 'Export as CSV format',
							},
						],
						default: 'json',
						description: 'Format for exported data',
					},
					{
						displayName: 'Date Range',
						name: 'dateRange',
						type: 'options',
						options: [
							{
								name: 'All Time',
								value: 'all',
								description: 'Export all records',
							},
							{
								name: 'Last 7 Days',
								value: 'week',
								description: 'Export last 7 days',
							},
							{
								name: 'Last 30 Days',
								value: 'month',
								description: 'Export last 30 days',
							},
							{
								name: 'Custom Range',
								value: 'custom',
								description: 'Specify custom date range',
							},
						],
						default: 'month',
						description: 'Date range for export',
					},
					{
						displayName: 'Start Date',
						name: 'startDate',
						type: 'dateTime',
						default: '',
						description: 'Start date for export',
						displayOptions: {
							show: {
								dateRange: ['custom'],
							},
						},
					},
					{
						displayName: 'End Date',
						name: 'endDate',
						type: 'dateTime',
						default: '',
						description: 'End date for export',
						displayOptions: {
							show: {
								dateRange: ['custom'],
							},
						},
					},
					{
						displayName: 'Include Details',
						name: 'includeDetails',
						type: 'boolean',
						default: false,
						description: 'Include difference details in export',
					},
				],
			},
			// Cleanup Options
			{
				displayName: 'Cleanup Options',
				name: 'cleanupOptions',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				displayOptions: {
					show: {
						operation: ['cleanupFailed'],
					},
				},
				options: [
					{
						displayName: 'Status to Clean',
						name: 'statusToClean',
						type: 'multiOptions',
						options: [
							{
								name: 'Failed',
								value: 'failed',
								description: 'Clean failed comparisons',
							},
							{
								name: 'Running (Stuck)',
								value: 'running',
								description: 'Clean comparisons stuck in running state',
							},
							{
								name: 'Pending (Stuck)',
								value: 'pending',
								description: 'Clean comparisons stuck in pending state',
							},
						],
						default: ['failed'],
						description: 'Which statuses to clean up',
					},
					{
						displayName: 'Stuck Threshold (Hours)',
						name: 'stuckThreshold',
						type: 'number',
						typeOptions: {
							minValue: 1,
						},
						default: 24,
						description: 'Consider tasks stuck if running/pending for more than this many hours',
					},
				],
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;

		try {
			let result: any = {};

			switch (operation) {
				case 'clearRecords':
					result = await DataComparisonMaintenance.clearRecords(this);
					break;
				case 'getStatistics':
					result = await DataComparisonMaintenance.getStatistics(this);
					break;
				case 'cleanupFailed':
					result = await DataComparisonMaintenance.cleanupFailed(this);
					break;
				case 'exportHistory':
					result = await DataComparisonMaintenance.exportHistory(this);
					break;
				case 'dbMaintenance':
					result = await DataComparisonMaintenance.performDbMaintenance(this);
					break;
			}

			returnData.push({
				json: {
					success: true,
					operation,
					result,
					timestamp: new Date().toISOString(),
				},
			});

		} catch (error: any) {
			returnData.push({
				json: {
					success: false,
					operation,
					error: error.message || 'Unknown error',
					timestamp: new Date().toISOString(),
				},
			});
		}

		return [returnData];
	}

	private static async clearRecords(context: IExecuteFunctions): Promise<any> {
		const clearOptions = context.getNodeParameter('clearOptions', 0) as any;
		const confirmClear = clearOptions.confirmClear || false;

		if (!confirmClear) {
			throw new Error('Please confirm that you want to clear records by setting "Confirm Clear" to true');
		}

		const fetch = require('node-fetch');
		const apiUrl = 'http://data-diff-api:8000/api/v1/maintenance/clear-records';

		const response = await fetch(apiUrl, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				record_type: clearOptions.recordType || 'all',
				filter: clearOptions.filter || 'all',
				days_to_keep: clearOptions.filter === 'age' ? (clearOptions.daysToKeep || 0) : null,
			}),
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`API Error: ${response.status} - ${errorText}`);
		}

		return response.json();
	}

	private static async getStatistics(_context: IExecuteFunctions): Promise<any> {
		const fetch = require('node-fetch');
		const apiUrl = 'http://data-diff-api:8000/api/v1/maintenance/statistics';

		const response = await fetch(apiUrl, {
			method: 'GET',
			headers: { 'Content-Type': 'application/json' },
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`API Error: ${response.status} - ${errorText}`);
		}

		return response.json();
	}

	private static async cleanupFailed(context: IExecuteFunctions): Promise<any> {
		const cleanupOptions = context.getNodeParameter('cleanupOptions', 0) as any;
		const fetch = require('node-fetch');
		const apiUrl = 'http://data-diff-api:8000/api/v1/maintenance/cleanup-failed';

		const response = await fetch(apiUrl, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				status_to_clean: cleanupOptions.statusToClean || ['failed'],
				stuck_threshold_hours: cleanupOptions.stuckThreshold || 24,
			}),
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`API Error: ${response.status} - ${errorText}`);
		}

		return response.json();
	}

	private static async exportHistory(context: IExecuteFunctions): Promise<any> {
		const exportOptions = context.getNodeParameter('exportOptions', 0) as any;
		const fetch = require('node-fetch');
		const apiUrl = 'http://data-diff-api:8000/api/v1/maintenance/export-history';

		const params = new URLSearchParams({
			format: exportOptions.format || 'json',
			date_range: exportOptions.dateRange || 'month',
			include_details: exportOptions.includeDetails || false,
		});

		if (exportOptions.dateRange === 'custom') {
			if (exportOptions.startDate) params.append('start_date', exportOptions.startDate);
			if (exportOptions.endDate) params.append('end_date', exportOptions.endDate);
		}

		const response = await fetch(`${apiUrl}?${params}`, {
			method: 'GET',
			headers: { 'Content-Type': 'application/json' },
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`API Error: ${response.status} - ${errorText}`);
		}

		const contentType = response.headers.get('content-type');
		if (contentType && contentType.includes('text/csv')) {
			const csvText = await response.text();
			return {
				format: 'csv',
				data: csvText,
				message: 'CSV data exported successfully',
			};
		} else {
			return response.json();
		}
	}

	private static async performDbMaintenance(_context: IExecuteFunctions): Promise<any> {
		const fetch = require('node-fetch');
		const apiUrl = 'http://data-diff-api:8000/api/v1/maintenance/db-maintenance';

		const response = await fetch(apiUrl, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				operations: ['vacuum', 'analyze'],
			}),
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`API Error: ${response.status} - ${errorText}`);
		}

		return response.json();
	}
}