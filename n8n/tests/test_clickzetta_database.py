#!/usr/bin/env python3
"""
测试修正后的 n8n.databases.clickzetta 模块

这个测试验证了我们修正后的 Clickzetta 数据库驱动是否能正常工作
"""
import json
import sys
import os

# 添加项目路径到 Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def load_connection_config():
    """加载 uat 连接配置"""
    try:
        with open('/Users/liangmo/.clickzetta/connections.json', 'r') as f:
            config = json.load(f)

        for conn in config.get('connections', []):
            if conn.get('name') == 'uat':
                return conn

        print("❌ 未找到 uat 连接配置")
        return None
    except Exception as e:
        print(f"❌ 读取配置文件失败: {e}")
        return None

def test_clickzetta_database():
    """测试 n8n 版本的 Clickzetta 数据库类"""
    try:
        from n8n.databases.clickzetta import Clickzetta

        # 加载连接配置
        conn_config = load_connection_config()
        if not conn_config:
            return False

        print("📊 测试 n8n.databases.clickzetta.Clickzetta 类...")

        # 创建 Clickzetta 实例
        db = Clickzetta(
            thread_count=1,
            username=conn_config['username'],
            password=conn_config['password'],
            service=conn_config['service'],
            instance=conn_config['instance'],
            workspace=conn_config['workspace'],
            virtualcluster=conn_config['vcluster'],
            schema=conn_config['schema']
        )

        print("✅ Clickzetta 实例创建成功")

        # 测试连接
        print("\n🔗 测试连接...")
        conn = db.create_connection()
        print("✅ 连接创建成功")
        conn.close()

        # 测试查询表结构
        print("\n🔍 测试查询表结构...")
        try:
            # 测试一个简单的表路径
            table_path = ("ab_test_config",)  # 单表名
            schema = db.query_table_schema(table_path)

            print(f"✅ 成功查询表结构，找到 {len(schema)} 列:")
            for i, (col_name, col_info) in enumerate(list(schema.items())[:5]):
                print(f"  {i+1}. {col_name}: {col_info}")
            if len(schema) > 5:
                print(f"  ... 和其他 {len(schema) - 5} 列")

        except Exception as e:
            print(f"❌ 查询表结构失败: {e}")
            print(f"错误类型: {type(e)}")

            # 尝试调试
            print("\n🔧 尝试调试路径规范化...")
            try:
                normalized = db._normalize_table_path(("ab_test_config",))
                print(f"规范化路径: {normalized}")
            except Exception as e2:
                print(f"路径规范化失败: {e2}")

        # 测试处理表结构
        print("\n⚙️ 测试处理表结构...")
        try:
            table_path = ("ab_test_config",)
            raw_schema = db.query_table_schema(table_path)
            processed_schema = db._process_table_schema(
                table_path,
                raw_schema,
                filter_columns=list(raw_schema.keys())[:3]  # 只测试前3列
            )

            print(f"✅ 成功处理表结构，处理了 {len(processed_schema)} 列:")
            for col_name, col_type in processed_schema.items():
                print(f"  - {col_name}: {col_type}")

        except Exception as e:
            print(f"❌ 处理表结构失败: {e}")

        return True

    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_clickzetta_database()
    if success:
        print("\n🎉 所有测试完成!")
    else:
        print("\n💥 测试失败")
        sys.exit(1)
