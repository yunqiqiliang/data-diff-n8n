#!/usr/bin/env node
/**
 * n8n 节点执行调试工具
 * 用于诊断 "Could not get parameter" 等错误
 */

const util = require('util');

// 模拟 n8n 的执行环境
class MockExecuteFunctions {
    constructor(nodeType, credentials, parameters) {
        this.nodeType = nodeType;
        this.credentials = credentials;
        this.parameters = parameters;
        this.errors = [];
        this.logs = [];
    }
    
    // 模拟 getCredentials
    async getCredentials(type, itemIndex = 0) {
        this.log(`getCredentials('${type}', ${itemIndex})`);
        
        if (!this.credentials) {
            const error = new Error('Could not get credentials');
            this.errors.push({ method: 'getCredentials', error: error.message });
            throw error;
        }
        
        if (!this.credentials[type]) {
            const error = new Error(`Credentials of type "${type}" not found`);
            this.errors.push({ method: 'getCredentials', error: error.message });
            throw error;
        }
        
        return this.credentials[type];
    }
    
    // 模拟 getNodeParameter
    getNodeParameter(parameterName, itemIndex, defaultValue) {
        this.log(`getNodeParameter('${parameterName}', ${itemIndex}, ${defaultValue})`);
        
        if (!this.parameters) {
            const error = new Error('Could not get parameter');
            this.errors.push({ 
                method: 'getNodeParameter', 
                parameter: parameterName,
                error: error.message 
            });
            
            if (defaultValue !== undefined) {
                this.log(`  -> 使用默认值: ${defaultValue}`);
                return defaultValue;
            }
            throw error;
        }
        
        if (!(parameterName in this.parameters)) {
            if (defaultValue !== undefined) {
                this.log(`  -> 参数不存在，使用默认值: ${defaultValue}`);
                return defaultValue;
            }
            
            const error = new Error(`Could not get parameter "${parameterName}"`);
            this.errors.push({ 
                method: 'getNodeParameter', 
                parameter: parameterName,
                error: error.message,
                availableParameters: Object.keys(this.parameters)
            });
            throw error;
        }
        
        const value = this.parameters[parameterName];
        this.log(`  -> 返回值: ${JSON.stringify(value)}`);
        return value;
    }
    
    // 模拟 getInputData
    getInputData(inputIndex = 0) {
        this.log(`getInputData(${inputIndex})`);
        return [{ json: { test: true } }];
    }
    
    log(message) {
        const timestamp = new Date().toISOString();
        const logEntry = `[${timestamp}] ${message}`;
        this.logs.push(logEntry);
        console.log(logEntry);
    }
    
    // 生成调试报告
    generateReport() {
        console.log('\n=== 调试报告 ===\n');
        
        console.log('节点类型:', this.nodeType);
        console.log('参数:', util.inspect(this.parameters, { depth: null, colors: true }));
        console.log('凭据:', this.credentials ? Object.keys(this.credentials) : 'None');
        
        if (this.errors.length > 0) {
            console.log('\n错误列表:');
            this.errors.forEach((err, index) => {
                console.log(`\n错误 ${index + 1}:`);
                console.log('  方法:', err.method);
                if (err.parameter) console.log('  参数:', err.parameter);
                console.log('  错误:', err.error);
                if (err.availableParameters) {
                    console.log('  可用参数:', err.availableParameters.join(', '));
                }
            });
        } else {
            console.log('\n✅ 没有错误');
        }
        
        console.log('\n执行日志:');
        this.logs.forEach(log => console.log(log));
    }
}

// 测试场景
const testScenarios = [
    {
        name: '场景 1: 缺少凭据',
        nodeType: 'DatabaseConnector',
        credentials: null,
        parameters: {
            operation: 'prepareComparison',
            schemaName: 'public'
        }
    },
    {
        name: '场景 2: 错误的凭据类型',
        nodeType: 'DatabaseConnector',
        credentials: {
            wrongType: { user: 'test' }
        },
        parameters: {
            operation: 'prepareComparison'
        }
    },
    {
        name: '场景 3: 缺少必要参数',
        nodeType: 'DatabaseConnector',
        credentials: {
            databaseConnectorCredentials: {
                type: 'postgresql',
                host: 'localhost',
                port: 5432,
                username: 'test',
                password: 'test',
                database: 'testdb'
            }
        },
        parameters: {
            // operation 参数缺失
        }
    },
    {
        name: '场景 4: 正常情况',
        nodeType: 'DatabaseConnector',
        credentials: {
            databaseConnectorCredentials: {
                type: 'postgresql',
                host: 'localhost',
                port: 5432,
                username: 'test',
                password: 'test',
                database: 'testdb',
                schema: 'public'
            }
        },
        parameters: {
            operation: 'prepareComparison',
            schemaName: '',
            includeSampleData: false,
            sampleSize: 5,
            tableFilter: ''
        }
    }
];

// 模拟 DatabaseConnector 的执行逻辑
async function simulateDatabaseConnectorExecution(context) {
    console.log('\n模拟 DatabaseConnector.execute()...\n');
    
    try {
        const operation = context.getNodeParameter('operation', 0);
        console.log('操作类型:', operation);
        
        if (operation === 'prepareComparison') {
            // 获取凭据
            const credentials = await context.getCredentials('databaseConnectorCredentials', 0);
            console.log('凭据获取成功');
            
            // 构建连接配置
            const connectionConfig = {
                type: credentials.type || credentials.databaseType,
                host: credentials.host,
                port: credentials.port,
                username: credentials.username,
                password: credentials.password,
                database: credentials.database,
                schema: credentials.schema
            };
            console.log('连接配置:', connectionConfig);
            
            // 获取参数
            const schemaName = context.getNodeParameter('schemaName', 0, '');
            const includeSampleData = context.getNodeParameter('includeSampleData', 0, false);
            const sampleSize = context.getNodeParameter('sampleSize', 0, 5);
            const tableFilter = context.getNodeParameter('tableFilter', 0, '');
            
            console.log('参数获取成功:');
            console.log('  - schemaName:', schemaName);
            console.log('  - includeSampleData:', includeSampleData);
            console.log('  - sampleSize:', sampleSize);
            console.log('  - tableFilter:', tableFilter);
            
            return {
                success: true,
                operation: 'prepareComparison',
                connectionConfig
            };
        }
    } catch (error) {
        console.error('执行失败:', error.message);
        throw error;
    }
}

// 运行测试
async function runTests() {
    console.log('🔍 n8n 节点执行调试工具\n');
    
    for (const scenario of testScenarios) {
        console.log('\n' + '='.repeat(50));
        console.log(`🧪 ${scenario.name}`);
        console.log('='.repeat(50));
        
        const context = new MockExecuteFunctions(
            scenario.nodeType,
            scenario.credentials,
            scenario.parameters
        );
        
        try {
            const result = await simulateDatabaseConnectorExecution(context);
            console.log('\n✅ 执行成功');
            console.log('结果:', result);
        } catch (error) {
            console.log('\n❌ 执行失败');
        }
        
        context.generateReport();
    }
    
    // 诊断建议
    console.log('\n\n=== 诊断建议 ===\n');
    console.log('如果看到 "Could not get parameter" 错误：');
    console.log('1. 检查是否已创建并选择了正确的凭据');
    console.log('2. 确认凭据类型是否正确 (databaseConnectorCredentials 或 clickzettaApi)');
    console.log('3. 验证所有必填字段是否已填写');
    console.log('4. 检查 n8n 节点定义中的 displayOptions 是否正确');
    console.log('5. 确保参数名称拼写正确');
}

// 导出用于其他脚本
module.exports = {
    MockExecuteFunctions,
    simulateDatabaseConnectorExecution
};

// 如果直接运行
if (require.main === module) {
    runTests().catch(console.error);
}