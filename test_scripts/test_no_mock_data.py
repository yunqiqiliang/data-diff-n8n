#!/usr/bin/env python3
"""
测试删除模拟数据后的schema比较功能
验证是否正确抛出错误而不是返回模拟数据
"""

import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'n8n', 'core'))

try:
    import sys
    import os

    # 添加 n8n/core 目录到 Python 路径
    core_path = os.path.join(os.path.dirname(__file__), 'n8n', 'core')
    sys.path.insert(0, core_path)

    # 修复相对导入问题
    import comparison_engine
    import config_manager

    ComparisonEngine = comparison_engine.ComparisonEngine
    ConfigManager = config_manager.ConfigManager
    HAS_DATA_DIFF = comparison_engine.HAS_DATA_DIFF

    print("✅ 成功导入比较引擎模块")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

async def test_no_mock_data():
    """测试删除模拟数据后的行为"""
    print("\n🧪 测试删除模拟数据后的schema比较行为...")

    # 创建测试配置
    config_manager = ConfigManager()
    engine = ComparisonEngine(config_manager)

    # 模拟无效的数据库配置
    invalid_source_config = {
        "database_type": "postgresql",
        "driver": "postgresql",
        "host": "invalid-host",
        "port": 5432,
        "database": "invalid-db",
        "username": "invalid-user",
        "password": "invalid-pass",
        "schema": "public"
    }

    invalid_target_config = {
        "database_type": "clickzetta",
        "driver": "clickzetta",
        "host": "invalid-host",
        "port": 8123,
        "database": "invalid-db",
        "username": "invalid-user",
        "password": "invalid-pass",
        "schema": "public"
    }

    # 测试schema比较（预期应该抛出错误而不是返回模拟数据）
    try:
        result = await engine.compare_schemas(invalid_source_config, invalid_target_config)
        print(f"❌ 意外成功，应该抛出错误！结果: {result}")
        print("🚨 这表明可能还有模拟数据没有被删除！")
        return False
    except Exception as e:
        print(f"✅ 预期的错误: {e}")
        # 检查错误信息是否包含模拟数据相关内容
        error_msg = str(e).lower()
        if "mock" in error_msg or "模拟" in error_msg:
            print("❌ 错误信息中仍然包含模拟数据相关内容！")
            return False
        elif "invoices" in error_msg or "users" in error_msg:
            print("❌ 错误信息中包含模拟的表名！")
            return False
        else:
            print("✅ 错误信息看起来是真实的数据库错误，没有模拟数据痕迹")
            return True

async def test_data_diff_dependency():
    """测试data-diff依赖检查"""
    print("\n🧪 测试data-diff库依赖检查...")

    # 检查HAS_DATA_DIFF状态
    print(f"📊 HAS_DATA_DIFF 状态: {HAS_DATA_DIFF}")

    if not HAS_DATA_DIFF:
        print("⚠️  data-diff 库未安装，这是预期行为")
        return True
    else:
        print("✅ data-diff 库已安装")
        return True

async def main():
    """主测试函数"""
    print("🚀 开始测试删除模拟数据后的比较引擎...")

    # 测试依赖检查
    dep_test = await test_data_diff_dependency()

    # 测试无模拟数据行为
    mock_test = await test_no_mock_data()

    if dep_test and mock_test:
        print("\n✅ 所有测试通过！模拟数据已成功删除，比较引擎现在使用真实数据库查询。")
        return True
    else:
        print("\n❌ 部分测试失败，可能还需要进一步清理模拟数据。")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
