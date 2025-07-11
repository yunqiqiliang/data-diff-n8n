#!/usr/bin/env python3
"""
验证模式比对返回的表数量与真实数据库一致的测试脚本
"""

import requests
import json
import time
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API 基础URL
API_BASE_URL = "http://localhost:8000"

def test_schema_comparison():
    """测试模式比对功能"""

    # 测试连接配置
    source_config = {
        "database_type": "postgresql",
        "username": "postgres",
        "password": "password",
        "host": "postgres",  # 使用Docker内部服务名
        "port": 5432,
        "database": "test_source",
        "schema": "public"  # 修改为 schema
    }

    target_config = {
        "database_type": "clickzetta",
        "username": "test_user",
        "password": "test_password",
        "instance": "test_instance",
        "service": "test_service",
        "workspace": "test_workspace",
        "vcluster": "test_vcluster",
        "schema": "test_schema"  # 修改为 schema
    }

    # 准备请求数据
    request_data = {
        "source_config": source_config,
        "target_config": target_config
    }

    logger.info("=== 开始测试模式比对 ===")
    logger.info(f"源数据库: PostgreSQL (database: {source_config['database']}, schema: {source_config['schema']})")
    logger.info(f"目标数据库: Clickzetta (workspace: {target_config['workspace']}, schema: {target_config['schema']})")

    try:
        # 发起模式比对请求
        logger.info("发起模式比对请求...")
        response = requests.post(
            f"{API_BASE_URL}/api/v1/compare/schemas/nested",
            json=request_data,
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            logger.info("✅ 模式比对成功")

            # 分析结果
            result_data = result.get("result", {})
            source_schema = result_data.get("source_schema", {})
            target_schema = result_data.get("target_schema", {})
            diff_result = result_data.get("diff", {})

            logger.info(f"📊 源数据库表数量: {len(source_schema.get('tables', []))}")
            logger.info(f"📊 目标数据库表数量: {len(target_schema.get('tables', []))}")

            # 显示源数据库的表
            source_tables = source_schema.get('tables', [])
            if source_tables:
                logger.info(f"📋 源数据库表列表 ({len(source_tables)}张):")
                for i, table in enumerate(source_tables[:10], 1):  # 只显示前10张
                    logger.info(f"  {i}. {table}")
                if len(source_tables) > 10:
                    logger.info(f"  ... 还有 {len(source_tables) - 10} 张表")

            # 显示目标数据库的表
            target_tables = target_schema.get('tables', [])
            if target_tables:
                logger.info(f"📋 目标数据库表列表 ({len(target_tables)}张):")
                for i, table in enumerate(target_tables[:10], 1):  # 只显示前10张
                    logger.info(f"  {i}. {table}")
                if len(target_tables) > 10:
                    logger.info(f"  ... 还有 {len(target_tables) - 10} 张表")

            # 显示差异摘要
            if diff_result:
                logger.info("📊 差异摘要:")
                logger.info(f"  • 仅存在于源数据库的表: {len(diff_result.get('tables_only_in_source', []))}")
                logger.info(f"  • 仅存在于目标数据库的表: {len(diff_result.get('tables_only_in_target', []))}")
                logger.info(f"  • 共同表: {len(diff_result.get('common_tables', []))}")

                # 显示仅在源数据库的表
                source_only = diff_result.get('tables_only_in_source', [])
                if source_only:
                    logger.info(f"  仅在源数据库的表 ({len(source_only)}张):")
                    for table in source_only[:5]:
                        logger.info(f"    - {table}")
                    if len(source_only) > 5:
                        logger.info(f"    ... 还有 {len(source_only) - 5} 张表")

                # 显示仅在目标数据库的表
                target_only = diff_result.get('tables_only_in_target', [])
                if target_only:
                    logger.info(f"  仅在目标数据库的表 ({len(target_only)}张):")
                    for table in target_only[:5]:
                        logger.info(f"    - {table}")
                    if len(target_only) > 5:
                        logger.info(f"    ... 还有 {len(target_only) - 5} 张表")

            # 检查是否存在特定的表
            logger.info("\n🔍 检查特定表的存在:")
            expected_tables = ['users', 'orders', 'products', 'invoices', 'accounts', 'people', 'reviews']
            for table in expected_tables:
                if table in source_tables:
                    logger.info(f"  ✅ {table} - 存在于源数据库")
                else:
                    logger.info(f"  ❌ {table} - 不存在于源数据库")

            # 检查是否为真实数据还是mock数据
            logger.info("\n🔍 检查数据真实性:")
            if len(source_tables) >= 10:
                logger.info("  ✅ 源数据库表数量较多(≥10张)，可能是真实数据")
            else:
                logger.info("  ⚠️ 源数据库表数量较少(<10张)，可能是mock数据或有遗漏")

            # 检查表结构详情
            source_columns = source_schema.get('columns', {})
            if source_columns:
                logger.info(f"\n📋 源数据库表结构详情 (共{len(source_columns)}张表有结构信息):")
                for table_name, cols in list(source_columns.items())[:3]:  # 显示前3张表的结构
                    logger.info(f"  表 {table_name} ({len(cols)}列):")
                    for col in cols[:5]:  # 显示前5列
                        logger.info(f"    - {col.get('name', 'unknown')}: {col.get('type', 'unknown')}")
                    if len(cols) > 5:
                        logger.info(f"    ... 还有 {len(cols) - 5} 列")

                if len(source_columns) > 3:
                    logger.info(f"  ... 还有 {len(source_columns) - 3} 张表有结构信息")

            return result

        else:
            logger.error(f"❌ 模式比对失败: HTTP {response.status_code}")
            logger.error(f"错误信息: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ 请求失败: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ 发生错误: {e}")
        return None

def main():
    """主函数"""
    logger.info("开始验证模式比对功能...")

    # 检查API服务状态
    try:
        health_response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if health_response.status_code == 200:
            logger.info("✅ API服务正常运行")
        else:
            logger.error(f"❌ API服务异常: {health_response.status_code}")
            return
    except Exception as e:
        logger.error(f"❌ API服务不可用: {e}")
        return

    # 执行测试
    result = test_schema_comparison()

    if result:
        logger.info("\n✅ 模式比对功能验证完成")
        logger.info("注意：如果源数据库表数量少于预期，请检查:")
        logger.info("  1. 数据库连接配置是否正确")
        logger.info("  2. schema名称是否正确")
        logger.info("  3. 数据库中是否确实存在相应的表")
        logger.info("  4. 用户权限是否足够")
    else:
        logger.error("❌ 模式比对功能验证失败")

if __name__ == "__main__":
    main()
