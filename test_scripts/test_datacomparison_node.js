#!/usr/bin/env node
/**
 * 测试 DataComparison 节点的连接信息提取功能
 */

const fs = require('fs');
const path = require('path');

// 模拟 N8N 的输入数据
const mockInputData = [
    {
        json: {
            connectionUrl: "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
            databaseType: "postgresql",
            operation: "connect",
            tables: ["accounts", "invoices", "people", "products"],
            success: true
        }
    },
    {
        json: {
            connectionUrl: "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
            databaseType: "clickzetta",
            operation: "connect",
            tables: ["accounts", "invoices", "test_table"],
            success: true
        }
    }
];

// 模拟 extractUpstreamData 方法
function extractUpstreamData(items, autoFill) {
    const upstreamData = {
        connections: [],
        tables: [],
    };

    if (!autoFill || items.length === 0) {
        return upstreamData;
    }

    // 从上游节点提取连接信息和表列表
    for (const item of items) {
        if (item.json) {
            // 提取连接 URL - 支持多种字段名
            if (item.json.connectionUrl) {
                upstreamData.connections.push({
                    url: item.json.connectionUrl,
                    type: item.json.databaseType || 'unknown',
                    config: item.json.connectionConfig,
                });
            }

            // 支持 connectionString 字段
            if (item.json.connectionString) {
                upstreamData.connections.push({
                    url: item.json.connectionString,
                    type: item.json.databaseType || 'unknown',
                    config: item.json.connectionConfig,
                });
            }

            // 支持其他可能的连接字段
            if (item.json.connection) {
                upstreamData.connections.push({
                    url: item.json.connection,
                    type: item.json.databaseType || 'unknown',
                    config: item.json.connectionConfig,
                });
            }

            // 提取表列表 - 支持多种格式
            if (item.json.tables && Array.isArray(item.json.tables)) {
                const processedTables = item.json.tables.map((table) => {
                    if (typeof table === 'string') {
                        return {
                            name: table,
                            value: table,
                            description: `Table: ${table}`
                        };
                    } else if (table && typeof table === 'object') {
                        // 安全地处理对象格式
                        const tableName = table.name || table.value || table.table_name;
                        if (tableName && typeof tableName === 'string') {
                            return {
                                name: tableName,
                                value: table.value || tableName,
                                description: table.description || `Table: ${tableName}`
                            };
                        } else {
                            // 如果无法提取有效的表名，跳过这个条目
                            console.warn('无法提取有效的表名:', table);
                            return null;
                        }
                    }
                    return table;
                }).filter(table => table !== null); // 过滤掉无效的表

                upstreamData.tables = upstreamData.tables.concat(processedTables);
            }

            // 支持 data 字段中的表列表（某些节点使用这种格式）
            if (item.json.data && Array.isArray(item.json.data)) {
                // 检查是否是表名数组
                const dataAsTableList = item.json.data.map((dataItem) => {
                    if (typeof dataItem === 'string') {
                        // 简单字符串格式
                        return {
                            name: dataItem,
                            value: dataItem,
                            description: `Table: ${dataItem}`
                        };
                    } else if (dataItem && typeof dataItem === 'object') {
                        // 对象格式，确保有标准字段
                        const tableName = dataItem.name || dataItem.value || dataItem.table_name;
                        if (tableName && typeof tableName === 'string') {
                            return {
                                name: tableName,
                                value: dataItem.value || tableName,
                                description: dataItem.description || `Table: ${tableName}`
                            };
                        } else {
                            // 如果无法提取有效的表名，跳过这个条目
                            console.warn('无法从 data 字段提取有效的表名:', dataItem);
                            return null;
                        }
                    }
                    return dataItem;
                }).filter(table => table !== null); // 过滤掉无效的表

                upstreamData.tables = upstreamData.tables.concat(dataAsTableList);
            }
        }
    }

    return upstreamData;
}

// 测试函数
function testConnectionExtraction() {
    console.log('🔍 测试 DataComparison 节点连接信息提取...');
    console.log('=' * 50);

    // 测试 extractUpstreamData 方法
    const upstreamData = extractUpstreamData(mockInputData, true);

    console.log('📋 测试结果:');
    console.log('输入数据项数:', mockInputData.length);
    console.log('提取的连接数:', upstreamData.connections.length);
    console.log('提取的表数:', upstreamData.tables.length);
    console.log('');

    console.log('📱 连接信息:');
    upstreamData.connections.forEach((conn, index) => {
        console.log(`  ${index + 1}. 类型: ${conn.type}`);
        console.log(`     URL: ${conn.url.substring(0, 50)}...`);
    });
    console.log('');

    console.log('📋 表信息:');
    upstreamData.tables.slice(0, 10).forEach((table, index) => {
        console.log(`  ${index + 1}. ${table.name} (${table.description})`);
    });
    console.log('');

    // 测试连接分配逻辑
    console.log('🔗 连接分配测试:');
    let sourceConnection = '';
    let targetConnection = '';

    if (upstreamData.connections && upstreamData.connections.length > 0) {
        sourceConnection = upstreamData.connections[0].url;
        console.log('✅ 源连接已分配:', sourceConnection.substring(0, 50) + '...');
    }

    if (upstreamData.connections && upstreamData.connections.length > 1) {
        targetConnection = upstreamData.connections[1].url;
        console.log('✅ 目标连接已分配:', targetConnection.substring(0, 50) + '...');
    } else if (upstreamData.connections && upstreamData.connections.length > 0) {
        console.log('⚠️ 只有一个连接，无法分配目标连接');
    }

    console.log('');

    // 测试表名分配
    console.log('📋 表名分配测试:');
    let sourceTable = '';
    let targetTable = '';

    if (upstreamData.tables && upstreamData.tables.length > 0) {
        const firstTable = upstreamData.tables[0];
        sourceTable = firstTable?.value || firstTable?.name || firstTable;
        console.log('✅ 源表已分配:', sourceTable);
    }

    if (upstreamData.tables && upstreamData.tables.length > 1) {
        const secondTable = upstreamData.tables[1];
        targetTable = secondTable?.value || secondTable?.name || secondTable;
        console.log('✅ 目标表已分配:', targetTable);
    } else if (upstreamData.tables && upstreamData.tables.length > 0) {
        const firstTable = upstreamData.tables[0];
        targetTable = firstTable?.value || firstTable?.name || firstTable;
        console.log('⚠️ 使用相同表名作为目标表:', targetTable);
    }

    console.log('');

    // 总结
    console.log('🎯 测试总结:');
    const hasSourceConnection = !!sourceConnection;
    const hasTargetConnection = !!targetConnection;
    const hasSourceTable = !!sourceTable;
    const hasTargetTable = !!targetTable;

    console.log('连接信息:', hasSourceConnection && hasTargetConnection ? '✅ 完整' : '❌ 不完整');
    console.log('表信息:', hasSourceTable && hasTargetTable ? '✅ 完整' : '❌ 不完整');

    const allGood = hasSourceConnection && hasTargetConnection && hasSourceTable && hasTargetTable;
    console.log('总体状态:', allGood ? '✅ 成功' : '❌ 需要修复');

    console.log('');
    console.log('🛠️ 修复建议:');
    if (!hasSourceConnection) {
        console.log('- 确保第一个上游节点包含 connectionUrl 字段');
    }
    if (!hasTargetConnection) {
        console.log('- 确保第二个上游节点包含 connectionUrl 字段');
    }
    if (!hasSourceTable) {
        console.log('- 确保上游节点包含 tables 数组');
    }
    if (!hasTargetTable) {
        console.log('- 确保有足够的表数据进行比较');
    }

    return allGood;
}

// 运行测试
if (require.main === module) {
    testConnectionExtraction();
}
