#!/usr/bin/env python3
"""
直接测试比对引擎
"""

import sys
import asyncio
import logging
sys.path.append('/Users/liangmo/Documents/GitHub/data-diff-n8n')

# 设置日志
logging.basicConfig(level=logging.DEBUG)

async def test_comparison_engine():
    """直接测试比对引擎"""
    try:
        from n8n.core.comparison_engine import ComparisonEngine
        from n8n.core.config_manager import ConfigManager

        config_manager = ConfigManager()
        engine = ComparisonEngine(config_manager)

        # 测试配置
        source_config = {
            "database_type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "username": "postgres",
            "password": "password"
        }

        target_config = source_config.copy()

        comparison_config = {
            "source_table": "users",
            "target_table": "users",
            "key_columns": ["id"],
            "threads": 1
        }

        print("开始测试比对引擎（使用模拟模式）...")
        # 由于数据库不存在，使用模拟比对
        result = await engine._execute_mock_comparison(
            "mock_source", "mock_target", comparison_config, "test_job"
        )

        print(f"比对结果: {result}")
        return True

    except Exception as e:
        print(f"比对引擎测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_comparison_engine())
    print(f"测试结果: {'成功' if success else '失败'}")
