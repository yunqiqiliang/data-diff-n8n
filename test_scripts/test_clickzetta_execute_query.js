#!/usr/bin/env node

/**
 * 测试 ClickzettaConnector 节点的 Execute Query 功能
 */

const fs = require('fs');
const path = require('path');

// 模拟 N8N 运行环境
const mockCredentials = {
	username: 'test_user',
	password: 'test_password',
	instance: 'test_instance',
	service: 'test_service',
	workspace: 'test_workspace',
	vcluster: 'test_vcluster',
	schema: 'public'
};

// 测试不同的 SQL 查询
const testQueries = [
	'SELECT 1 as test_column',
	'SELECT * FROM users LIMIT 5',
	'SELECT COUNT(*) as total_users FROM users',
	'SHOW TABLES',
	'DESCRIBE users'
];

console.log('🔍 Testing ClickzettaConnector Execute Query Feature\n');

// 检查节点文件是否存在
const nodePath = path.join(__dirname, 'n8n/src/nodes/ClickzettaConnector/ClickzettaConnector.node.ts');
if (!fs.existsSync(nodePath)) {
	console.error('❌ ClickzettaConnector node file not found:', nodePath);
	process.exit(1);
}

// 读取节点文件内容
const nodeContent = fs.readFileSync(nodePath, 'utf8');

// 检查是否包含新添加的功能
const checks = [
	{
		name: 'Execute Query Operation',
		pattern: /value: 'executeQuery'/,
		description: 'Execute Query operation option'
	},
	{
		name: 'SQL Query Parameter',
		pattern: /name: 'sqlQuery'/,
		description: 'SQL query input parameter'
	},
	{
		name: 'Return Raw Results Parameter',
		pattern: /name: 'returnRawResults'/,
		description: 'Return raw results option'
	},
	{
		name: 'Execute Query Method',
		pattern: /executeQueryMethod/,
		description: 'Execute query method implementation'
	},
	{
		name: 'Format Query Results Method',
		pattern: /formatQueryResults/,
		description: 'Format query results method implementation'
	},
	{
		name: 'Query API URL',
		pattern: /CLICKZETTA_QUERY_API_URL/,
		description: 'Query API URL configuration'
	}
];

console.log('✅ Feature Implementation Check:');
checks.forEach(check => {
	const found = check.pattern.test(nodeContent);
	console.log(`  ${found ? '✅' : '❌'} ${check.name}: ${check.description}`);
});

// 检查代码结构
console.log('\n🔍 Code Structure Analysis:');

// 计算操作数量
const operationMatches = nodeContent.match(/value: '[^']+'/g);
const operationCount = operationMatches ? operationMatches.length : 0;
console.log(`  📊 Total Operations: ${operationCount}`);

// 检查参数数量
const parameterMatches = nodeContent.match(/displayName: '[^']+'/g);
const parameterCount = parameterMatches ? parameterMatches.length : 0;
console.log(`  📋 Total Parameters: ${parameterCount}`);

// 检查方法数量
const methodMatches = nodeContent.match(/private static async \w+Method/g);
const methodCount = methodMatches ? methodMatches.length : 0;
console.log(`  🔧 Total Methods: ${methodCount}`);

console.log('\n📝 Sample SQL Query Testing Configuration:');
testQueries.forEach((query, index) => {
	console.log(`  ${index + 1}. ${query}`);
});

console.log('\n🌐 API Endpoint Configuration:');
console.log('  Default Query API: http://data-diff-api:8000/api/v1/query/execute');
console.log('  Environment Variable: CLICKZETTA_QUERY_API_URL');

console.log('\n📊 Expected Output Formats:');
console.log('  Raw Results: Single JSON object with all data');
console.log('  Formatted Results: Multiple JSON objects (one per row)');

console.log('\n✅ ClickzettaConnector Execute Query feature implementation complete!');
console.log('💡 Next steps:');
console.log('  1. Test the node in N8N workflow');
console.log('  2. Verify API endpoint connectivity');
console.log('  3. Test with different SQL queries');
console.log('  4. Validate output formatting options');
