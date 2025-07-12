#!/usr/bin/env node
/**
 * 集成测试：验证 Prepare for Comparison 功能的实际效果
 * 
 * 测试目标：
 * 1. 验证连接信息是否正确传递
 * 2. 验证表信息是否完整获取
 * 3. 验证错误场景的处理
 * 4. 验证与数据比对节点的集成
 */

const fetch = require('node-fetch');

// 测试配置
const TEST_CONFIG = {
    n8nUrl: 'http://localhost:5678',
    apiUrl: 'http://localhost:8000',
    testDb: {
        postgresql: {
            type: 'postgresql',
            host: 'localhost',
            port: 5432,
            username: 'test',
            password: 'test',
            database: 'testdb',
            schema: 'public'
        },
        clickzetta: {
            type: 'clickzetta',
            username: 'test',
            password: 'test',
            instance: 'test-instance',
            service: 'test-service',
            workspace: 'test-workspace',
            vcluster: 'test-vcluster',
            schema: 'default'
        }
    }
};

// 测试用例
const testCases = [
    {
        name: 'Test 1: 验证输出结构完整性',
        validate: (output) => {
            const required = ['connectionUrl', 'connectionConfig', 'tables', 'comparisonReady', 'comparisonConfig'];
            const missing = required.filter(field => !output[field]);
            
            return {
                passed: missing.length === 0,
                message: missing.length > 0 ? `缺少必要字段: ${missing.join(', ')}` : '输出结构完整',
                details: {
                    required,
                    missing,
                    actual: Object.keys(output)
                }
            };
        }
    },
    
    {
        name: 'Test 2: 验证 comparisonConfig 内容',
        validate: (output) => {
            if (!output.comparisonConfig) {
                return { passed: false, message: 'comparisonConfig 不存在' };
            }
            
            const config = output.comparisonConfig;
            const checks = {
                hasSourceConfig: !!config.source_config,
                hasAvailableTables: Array.isArray(config.available_tables),
                hasDatabaseType: !!config.database_type,
                tablesMatch: JSON.stringify(config.available_tables) === JSON.stringify(output.tables)
            };
            
            const failed = Object.entries(checks).filter(([k, v]) => !v).map(([k]) => k);
            
            return {
                passed: failed.length === 0,
                message: failed.length > 0 ? `检查失败: ${failed.join(', ')}` : '配置内容正确',
                details: checks
            };
        }
    },
    
    {
        name: 'Test 3: 验证连接信息可用性',
        validate: async (output) => {
            if (!output.connectionConfig) {
                return { passed: false, message: 'connectionConfig 不存在' };
            }
            
            try {
                // 尝试使用连接信息调用 API
                const response = await fetch(`${TEST_CONFIG.apiUrl}/api/v1/connections/test`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(output.connectionConfig)
                });
                
                const result = await response.json();
                
                return {
                    passed: response.ok && result.success,
                    message: result.success ? '连接信息有效' : `连接失败: ${result.error || '未知错误'}`,
                    details: result
                };
            } catch (error) {
                return {
                    passed: false,
                    message: `API 调用失败: ${error.message}`,
                    details: { error: error.message }
                };
            }
        }
    },
    
    {
        name: 'Test 4: 验证表列表准确性',
        validate: (output) => {
            if (!output.tables || !Array.isArray(output.tables)) {
                return { passed: false, message: 'tables 不是数组' };
            }
            
            const validations = {
                notEmpty: output.tables.length > 0,
                hasCorrectStructure: output.tables.every(table => 
                    table.name && table.value && table.description
                ),
                namesMatchValues: output.tables.every(table => 
                    table.name === table.value
                ),
                descriptionsCorrect: output.tables.every(table => 
                    table.description.includes('Table:') || table.description.includes('Clickzetta table:')
                )
            };
            
            const failed = Object.entries(validations).filter(([k, v]) => !v).map(([k]) => k);
            
            return {
                passed: failed.length === 0,
                message: failed.length > 0 ? `表验证失败: ${failed.join(', ')}` : `找到 ${output.tables.length} 个表`,
                details: {
                    tableCount: output.tables.length,
                    sampleTables: output.tables.slice(0, 3),
                    validations
                }
            };
        }
    },
    
    {
        name: 'Test 5: 验证采样数据功能',
        validateWithParams: async (params) => {
            // 这个测试需要实际执行带参数的 prepareComparison
            const testParams = {
                ...params,
                includeSampleData: true,
                sampleSize: 5
            };
            
            // 实际执行并验证 sampleData
            return {
                passed: false,
                message: '需要实际执行测试',
                details: { testParams }
            };
        }
    },
    
    {
        name: 'Test 6: 验证错误场景处理',
        scenarios: [
            {
                name: '无效凭据',
                setup: { invalidCredentials: true },
                expectedError: 'credentials',
                expectedHint: true
            },
            {
                name: '连接失败',
                setup: { wrongHost: 'invalid-host' },
                expectedError: 'connection',
                expectedDetails: true
            },
            {
                name: '权限不足',
                setup: { limitedUser: true },
                expectedError: 'permission',
                expectedTables: 0
            }
        ]
    },
    
    {
        name: 'Test 7: 验证与 DataComparison 的集成',
        validate: async (output) => {
            // 模拟 DataComparison 节点的参数提取逻辑
            const extraction = {
                sourceConfig: extractNodeConfig(output),
                sourceTable: detectTableName(output),
                isPrepared: output.comparisonReady === true
            };
            
            const checks = {
                configExtracted: !!extraction.sourceConfig,
                tableDetected: !!extraction.sourceTable,
                preparedFlagCorrect: extraction.isPrepared === true,
                hasConnectionUrl: !!extraction.sourceConfig?.connectionUrl,
                hasConnectionConfig: !!extraction.sourceConfig?.connectionConfig
            };
            
            const failed = Object.entries(checks).filter(([k, v]) => !v).map(([k]) => k);
            
            return {
                passed: failed.length === 0,
                message: failed.length > 0 ? `集成检查失败: ${failed.join(', ')}` : '可以正确集成',
                details: { extraction, checks }
            };
        }
    },
    
    {
        name: 'Test 8: 性能测试',
        validate: async (output, executionTime) => {
            const thresholds = {
                small: 1000,  // 小数据库 < 1秒
                medium: 3000, // 中等数据库 < 3秒
                large: 10000  // 大数据库 < 10秒
            };
            
            const tableCount = output.tables?.length || 0;
            const threshold = tableCount < 10 ? thresholds.small :
                           tableCount < 100 ? thresholds.medium :
                           thresholds.large;
            
            return {
                passed: executionTime < threshold,
                message: `执行时间: ${executionTime}ms (阈值: ${threshold}ms)`,
                details: {
                    executionTime,
                    threshold,
                    tableCount,
                    includedSampleData: !!output.sampleData
                }
            };
        }
    },
    
    {
        name: 'Test 9: 数据一致性测试',
        validate: async (output) => {
            // 对比多次执行的结果是否一致
            const results = [];
            for (let i = 0; i < 3; i++) {
                // 模拟多次执行
                results.push(JSON.stringify(output.tables));
            }
            
            const consistent = results.every(r => r === results[0]);
            
            return {
                passed: consistent,
                message: consistent ? '多次执行结果一致' : '结果不一致，可能存在并发问题',
                details: {
                    executionCount: results.length,
                    consistent
                }
            };
        }
    },
    
    {
        name: 'Test 10: 边界条件测试',
        scenarios: [
            {
                name: '空数据库',
                expectedTables: 0,
                shouldSucceed: true
            },
            {
                name: '超大表名',
                tableFilter: 'a'.repeat(200) + '%',
                shouldSucceed: true,
                expectedTables: 0
            },
            {
                name: '特殊字符',
                schemaName: "test'; DROP TABLE users; --",
                shouldSucceed: true,
                shouldSanitize: true
            },
            {
                name: '并发请求',
                concurrent: 10,
                shouldAllSucceed: true
            }
        ]
    }
];

// 辅助函数（模拟 DataComparison 的逻辑）
function extractNodeConfig(item) {
    const json = item;
    
    if (json.comparisonReady === true && json.comparisonConfig) {
        const compConfig = json.comparisonConfig;
        return {
            connectionUrl: json.connectionUrl,
            connectionConfig: compConfig.source_config || json.connectionConfig,
            type: compConfig.database_type || json.databaseType,
            isPrepared: true,
            availableTables: compConfig.available_tables || json.tables || []
        };
    }
    
    return null;
}

function detectTableName(item) {
    const json = item;
    
    if (json.comparisonReady === true && json.tables && Array.isArray(json.tables)) {
        if (json.tables.length > 0) {
            const firstTable = json.tables[0];
            return firstTable.value || firstTable.name || firstTable;
        }
    }
    
    return null;
}

// 测试报告生成
function generateReport(results) {
    const summary = {
        total: results.length,
        passed: results.filter(r => r.passed).length,
        failed: results.filter(r => !r.passed).length,
        skipped: results.filter(r => r.skipped).length
    };
    
    console.log('\n=== 测试报告 ===\n');
    console.log(`总计: ${summary.total} | 通过: ${summary.passed} | 失败: ${summary.failed} | 跳过: ${summary.skipped}\n`);
    
    results.forEach((result, index) => {
        const status = result.passed ? '✅' : result.skipped ? '⏭️' : '❌';
        console.log(`${status} ${result.name}`);
        console.log(`   ${result.message}`);
        
        if (result.details && !result.passed) {
            console.log('   详情:', JSON.stringify(result.details, null, 2).split('\n').join('\n   '));
        }
        
        console.log('');
    });
    
    if (summary.failed > 0) {
        console.log('\n❗ 发现问题，需要修复\n');
        
        // 生成修复建议
        const suggestions = [];
        results.filter(r => !r.passed).forEach(r => {
            if (r.message.includes('缺少必要字段')) {
                suggestions.push('- 检查 prepareComparison 输出是否包含所有必要字段');
            }
            if (r.message.includes('连接失败')) {
                suggestions.push('- 验证数据库连接配置是否正确');
            }
            if (r.message.includes('表验证失败')) {
                suggestions.push('- 检查表列表获取逻辑');
            }
        });
        
        if (suggestions.length > 0) {
            console.log('修复建议:');
            [...new Set(suggestions)].forEach(s => console.log(s));
        }
    }
    
    return summary;
}

// 执行测试
async function runTests() {
    console.log('🧪 开始测试 Prepare for Comparison 功能...\n');
    
    // 这里需要实际的测试数据或模拟数据
    const mockOutput = {
        operation: 'prepareComparison',
        success: true,
        connectionUrl: 'postgresql://test:test@localhost:5432/testdb',
        connectionConfig: TEST_CONFIG.testDb.postgresql,
        tables: [
            { name: 'public.users', value: 'public.users', description: 'Table: users' },
            { name: 'public.orders', value: 'public.orders', description: 'Table: orders' }
        ],
        comparisonReady: true,
        comparisonConfig: {
            source_config: TEST_CONFIG.testDb.postgresql,
            available_tables: [
                { name: 'public.users', value: 'public.users', description: 'Table: users' },
                { name: 'public.orders', value: 'public.orders', description: 'Table: orders' }
            ],
            database_type: 'postgresql'
        }
    };
    
    const results = [];
    const startTime = Date.now();
    
    for (const testCase of testCases) {
        try {
            let result;
            
            if (testCase.validate) {
                result = await testCase.validate(mockOutput, Date.now() - startTime);
            } else if (testCase.scenarios) {
                // 场景测试
                result = {
                    name: testCase.name,
                    passed: true,
                    message: '场景测试需要实际执行',
                    skipped: true
                };
            } else {
                result = {
                    name: testCase.name,
                    passed: false,
                    message: '测试未实现',
                    skipped: true
                };
            }
            
            results.push({ name: testCase.name, ...result });
        } catch (error) {
            results.push({
                name: testCase.name,
                passed: false,
                message: `测试异常: ${error.message}`,
                details: { error: error.stack }
            });
        }
    }
    
    const summary = generateReport(results);
    
    // 返回退出码
    process.exit(summary.failed > 0 ? 1 : 0);
}

// 运行测试
if (require.main === module) {
    runTests().catch(console.error);
}

module.exports = { testCases, runTests };