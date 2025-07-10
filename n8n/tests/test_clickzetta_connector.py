#!/usr/bin/env python3
"""
测试 clickzetta-connector 的 API

这个测试验证了 clickzetta-connector 的基本功能和与 Clickzetta 数据库的连接
"""
import json

def load_connection_config():
    """加载 uat 连接配置"""
    try:
        with open('/Users/liangmo/.clickzetta/connections.json', 'r') as f:
            config = json.load(f)

        # 查找 uat 连接配置
        for conn in config.get('connections', []):
            if conn.get('name') == 'uat':
                return conn

        print("❌ 未找到 uat 连接配置")
        return None
    except Exception as e:
        print(f"❌ 读取配置文件失败: {e}")
        return None

try:
    import clickzetta
    print("✅ clickzetta 导入成功")

    # 加载连接配置
    conn_config = load_connection_config()
    if not conn_config:
        exit(1)

    print(f"✅ 找到 uat 连接配置:")
    for key, value in conn_config.items():
        if key == 'password':
            print(f"  - {key}: ***")
        else:
            print(f"  - {key}: {value}")

    # 尝试真实连接
    print("\n🔗 尝试连接到 Clickzetta uat...")
    try:
        conn = clickzetta.connect(
            username=conn_config['username'],
            password=conn_config['password'],
            service=conn_config['service'],
            instance=conn_config['instance'],
            workspace=conn_config['workspace'],
            vcluster=conn_config['vcluster'],
            schema=conn_config['schema']
        )
        print("✅ 连接成功!")

        # 测试基本查询
        print("\n📊 测试基本查询...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test_column")
            result = cursor.fetchone()
            print(f"基本查询结果: {result}")

        # 测试 show tables
        print("\n📋 测试 show tables...")
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"找到 {len(tables)} 张表:")
            for i, table in enumerate(tables[:5]):  # 只显示前5张表
                print(f"  {i+1}. {table}")
            if len(tables) > 5:
                print(f"  ... 和其他 {len(tables) - 5} 张表")

        # 测试 show columns (如果有表的话)
        if tables:
            # 修正：SHOW TABLES 返回的是 6 列元组，第二列是表名
            table_name = tables[0][1]  # 使用第二列作为表名
            print(f"\n🔍 测试表结构查询 (表: {table_name})...")
            try:
                with conn.cursor() as cursor:
                    # 测试不同的查询格式
                    queries_to_try = [
                        f"SHOW COLUMNS IN {table_name}",
                        f"SHOW COLUMNS IN {conn_config['schema']}.{table_name}",
                        f"DESCRIBE {table_name}",
                        f"DESCRIBE {conn_config['schema']}.{table_name}"
                    ]

                    for query in queries_to_try:
                        try:
                            print(f"  尝试查询: {query}")
                            cursor.execute(query)
                            columns = cursor.fetchall()
                            print(f"  ✅ 成功! 找到 {len(columns)} 列")
                            for i, col in enumerate(columns[:3]):  # 只显示前3列
                                print(f"    {i+1}. {col}")
                            if len(columns) > 3:
                                print(f"    ... 和其他 {len(columns) - 3} 列")
                            break  # 成功后跳出循环
                        except Exception as e:
                            print(f"  ❌ 失败: {e}")
                    else:
                        print("  ⚠️ 所有查询格式都失败了")
            except Exception as e:
                print(f"⚠️ 查询表结构失败: {e}")

        conn.close()
        print("\n✅ 所有测试完成，连接已关闭")

    except Exception as e:
        print(f"❌ 连接失败: {e}")
        print(f"错误类型: {type(e)}")

except ImportError as e:
    print(f"❌ clickzetta 导入失败: {e}")
except Exception as e:
    print(f"❌ 其他错误: {e}")
