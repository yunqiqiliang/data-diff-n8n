#!/usr/bin/env python3
"""
直接测试模式比对中的事务问题
"""

import os
import sys
import logging

# 添加项目根目录到 Python 路径
sys.path.insert(0, '/Users/liangmo/Documents/GitHub/data-diff-n8n')

from n8n.core.connection_manager import ConnectionManager
from data_diff import connect_to_table

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_direct_schema_access():
    """直接测试模式访问"""

    # 测试连接配置
    config = {
        "database_type": "postgresql",
        "username": "postgres",
        "password": "password",
        "host": "localhost",
        "port": 5432,
        "database": "test_source",
        "schema": "public"
    }

    # 创建连接管理器
    connection_manager = ConnectionManager()

    # 构建连接字符串
    connection_string = connection_manager._build_connection_string(config)
    logger.info(f"连接字符串: {connection_string}")

    # 测试连接到每个表
    tables = ['users', 'orders', 'products']

    for table_name in tables:
        logger.info(f"\n=== 测试表: {table_name} ===")

        try:
            # 创建表连接
            table_segment = connect_to_table(
                db_info=connection_string,
                table_name=table_name,
                key_columns=("id",),
                thread_count=1
            )

            logger.info(f"✅ 成功连接到表 {table_name}")

            # 尝试获取 schema
            try:
                schema = table_segment.get_schema()
                logger.info(f"✅ 获取到 {table_name} 的 schema: {schema}")

                # 显示列信息
                for col_name, col_info in schema.items():
                    logger.info(f"  - {col_name}: {col_info}")

            except Exception as schema_error:
                logger.error(f"❌ 获取 {table_name} schema 失败: {schema_error}")

        except Exception as connect_error:
            logger.error(f"❌ 连接表 {table_name} 失败: {connect_error}")

if __name__ == "__main__":
    test_direct_schema_access()
