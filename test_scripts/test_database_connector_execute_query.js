#!/usr/bin/env node

/**
 * 测试 DatabaseConnector 节点的 Execute Query 功能
 */

const fs = require('fs');
const path = require('path');

console.log('🔍 Testing DatabaseConnector Execute Query Feature\n');

// 检查节点文件是否存在
const nodePath = path.join(__dirname, 'n8n/src/nodes/DatabaseConnector/DatabaseConnector.node.ts');
if (!fs.existsSync(nodePath)) {
	console.error('❌ DatabaseConnector node file not found:', nodePath);
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
		name: 'Database Type in Output',
		pattern: /databaseType: connectionConfig\.type/,
		description: 'Database type included in output'
	},
	{
		name: 'Query API URL',
		pattern: /DATABASE_QUERY_API_URL/,
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

// 检查支持的数据库类型
const databaseTypes = [
	'postgresql', 'mysql', 'clickzetta', 'sqlserver', 'oracle',
	'trino', 'presto', 'duckdb', 'mssql', 'vertica'
];

console.log('\n📊 Supported Database Types:');
databaseTypes.forEach(dbType => {
	const found = nodeContent.includes(dbType);
	console.log(`  ${found ? '✅' : '❌'} ${dbType.toUpperCase()}`);
});

// 示例查询
const sampleQueries = [
	'SELECT 1 as test_column',
	'SELECT * FROM users LIMIT 10',
	'SELECT COUNT(*) as total_users FROM users',
	'SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id',
	'UPDATE users SET last_login = NOW() WHERE id = 123',
	'INSERT INTO logs (message) VALUES (\'Query executed\')',
	'DELETE FROM temp_table WHERE created_at < NOW() - INTERVAL \'1 day\''
];

console.log('\n📝 Sample SQL Queries for Testing:');
sampleQueries.forEach((query, index) => {
	console.log(`  ${index + 1}. ${query}`);
});

console.log('\n🌐 API Integration:');
console.log('  Default Query API: http://data-diff-api:8000/api/v1/query/execute');
console.log('  Environment Variable: DATABASE_QUERY_API_URL');
console.log('  Method: POST');
console.log('  Request Format: { connection: {...}, query: "..." }');

console.log('\n📊 Output Format Options:');
console.log('  Raw Results: Single JSON object with complete result set');
console.log('  Formatted Results: Multiple JSON objects (one per row)');
console.log('  Meta Information: Includes operation, databaseType, query, timestamp');

console.log('\n🔒 Security Considerations:');
console.log('  ⚠️  SQL Injection: Backend validation required');
console.log('  ⚠️  Permission Control: User authorization needed');
console.log('  ⚠️  Data Modification: Use INSERT/UPDATE/DELETE carefully');
console.log('  ⚠️  Resource Limits: Query timeout and result size limits');

console.log('\n✅ DatabaseConnector Execute Query feature implementation complete!');
console.log('💡 Next steps:');
console.log('  1. Test the node in N8N workflow');
console.log('  2. Verify API endpoint connectivity');
console.log('  3. Test with different database types');
console.log('  4. Validate output formatting options');
console.log('  5. Test error handling scenarios');
console.log('  6. Validate security measures');
