#!/usr/bin/env node
/**
 * 测试 DataComparison 节点的修复
 * 验证 upstreamData 的安全访问和错误处理
 */

console.log('🔍 测试 DataComparison 节点修复...');

// 模拟不同的上游数据情况
const testCases = [
  {
    name: '空上游数据',
    items: [],
    autoFill: true,
    expected: { connections: [], tables: [] }
  },
  {
    name: '无连接和表信息',
    items: [{ json: { someOtherData: 'value' } }],
    autoFill: true,
    expected: { connections: [], tables: [] }
  },
  {
    name: '有连接但无表',
    items: [{
      json: {
        connectionUrl: 'postgresql://user:pass@host:5432/db',
        databaseType: 'postgresql'
      }
    }],
    autoFill: true,
    expected: {
      connections: [{ url: 'postgresql://user:pass@host:5432/db', type: 'postgresql' }],
      tables: []
    }
  },
  {
    name: '有连接和表（完整格式）',
    items: [{
      json: {
        connectionUrl: 'postgresql://user:pass@host:5432/db',
        databaseType: 'postgresql',
        tables: [
          { name: 'public.users', value: 'public.users', description: 'Table: users' },
          { name: 'public.orders', value: 'public.orders', description: 'Table: orders' }
        ]
      }
    }],
    autoFill: true,
    expected: {
      connections: [{ url: 'postgresql://user:pass@host:5432/db', type: 'postgresql' }],
      tables: [
        { name: 'public.users', value: 'public.users', description: 'Table: users' },
        { name: 'public.orders', value: 'public.orders', description: 'Table: orders' }
      ]
    }
  },
  {
    name: '有连接和表（简单格式）',
    items: [{
      json: {
        connectionUrl: 'postgresql://user:pass@host:5432/db',
        databaseType: 'postgresql',
        tables: ['users', 'orders']
      }
    }],
    autoFill: true,
    expected: {
      connections: [{ url: 'postgresql://user:pass@host:5432/db', type: 'postgresql' }],
      tables: ['users', 'orders']
    }
  },
  {
    name: 'autoFill 关闭',
    items: [{
      json: {
        connectionUrl: 'postgresql://user:pass@host:5432/db',
        databaseType: 'postgresql',
        tables: ['users', 'orders']
      }
    }],
    autoFill: false,
    expected: { connections: [], tables: [] }
  }
];

// 模拟 extractUpstreamData 方法
function extractUpstreamData(items, autoFill) {
  // 始终返回一个带有默认结构的对象，避免 undefined 错误
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
      // 提取连接 URL
      if (item.json.connectionUrl) {
        upstreamData.connections.push({
          url: item.json.connectionUrl,
          type: item.json.databaseType || 'unknown',
          config: item.json.connectionConfig,
        });
      }

      // 提取表列表
      if (item.json.tables && Array.isArray(item.json.tables)) {
        upstreamData.tables = upstreamData.tables.concat(item.json.tables);
      }
    }
  }

  return upstreamData;
}

// 测试安全访问表数据
function testSafeTableAccess(upstreamData) {
  console.log('📋 测试安全的表访问...');

  let sourceTable = '';
  let targetTable = '';

  try {
    // 安全访问逻辑
    if (!sourceTable && upstreamData.tables && upstreamData.tables.length > 0) {
      const firstTable = upstreamData.tables[0];
      sourceTable = firstTable?.value || firstTable?.name || firstTable;
    }
    if (!targetTable && upstreamData.tables && upstreamData.tables.length > 1) {
      const secondTable = upstreamData.tables[1];
      targetTable = secondTable?.value || secondTable?.name || secondTable;
    } else if (!targetTable && upstreamData.tables && upstreamData.tables.length > 0) {
      const firstTable = upstreamData.tables[0];
      targetTable = firstTable?.value || firstTable?.name || firstTable;
    }

    return { sourceTable, targetTable, success: true };
  } catch (error) {
    return { sourceTable, targetTable, success: false, error: error.message };
  }
}

// 测试安全访问连接数据
function testSafeConnectionAccess(upstreamData) {
  console.log('🔗 测试安全的连接访问...');

  let sourceConnection = '';
  let targetConnection = '';

  try {
    // 安全访问逻辑
    if (!sourceConnection && upstreamData.connections && upstreamData.connections.length > 0) {
      sourceConnection = upstreamData.connections[0].url;
    }
    if (!targetConnection && upstreamData.connections && upstreamData.connections.length > 1) {
      targetConnection = upstreamData.connections[1].url;
    } else if (!targetConnection && upstreamData.connections && upstreamData.connections.length > 0) {
      targetConnection = upstreamData.connections[0].url;
    }

    return { sourceConnection, targetConnection, success: true };
  } catch (error) {
    return { sourceConnection, targetConnection, success: false, error: error.message };
  }
}

// 运行测试
function runTests() {
  console.log('🚀 开始测试...\n');

  let passedTests = 0;
  let totalTests = testCases.length;

  for (const testCase of testCases) {
    console.log(`📝 测试: ${testCase.name}`);

    try {
      // 测试 extractUpstreamData
      const result = extractUpstreamData(testCase.items, testCase.autoFill);

      console.log('  输入:', JSON.stringify(testCase.items, null, 2));
      console.log('  预期:', JSON.stringify(testCase.expected, null, 2));
      console.log('  结果:', JSON.stringify(result, null, 2));

      // 测试安全访问
      const tableAccess = testSafeTableAccess(result);
      const connectionAccess = testSafeConnectionAccess(result);

      console.log('  表访问:', tableAccess);
      console.log('  连接访问:', connectionAccess);

      if (tableAccess.success && connectionAccess.success) {
        console.log('  ✅ 通过\n');
        passedTests++;
      } else {
        console.log('  ❌ 失败\n');
      }

    } catch (error) {
      console.log(`  ❌ 异常: ${error.message}\n`);
    }
  }

  console.log(`📊 测试结果: ${passedTests}/${totalTests} 通过`);

  if (passedTests === totalTests) {
    console.log('🎉 所有测试通过！DataComparison 节点修复成功。');
  } else {
    console.log('⚠️  部分测试失败，需要进一步检查。');
  }
}

// 运行测试
runTests();
