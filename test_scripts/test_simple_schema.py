#!/usr/bin/env python3
"""
简单的模式比对测试
"""
import requests
import json
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_simple_schema_comparison():
    """测试简单的模式比对"""

    # API端点
    url = "http://localhost:8000/api/v1/compare/schemas/nested"

    # 测试数据
    payload = {
        "source_config": {
            "database_type": "postgresql",
            "host": "postgres",
            "port": 5432,
            "username": "postgres",
            "password": "password",
            "database": "test_source",
            "schema": "public"
        },
        "target_config": {
            "database_type": "clickzetta",
            "username": "test_user",
            "password": "test_password",
            "instance": "test_instance",
            "service": "test_service",
            "workspace": "test_workspace",
            "schema": "test_schema",
            "vcluster": "test_vcluster"
        }
    }

    logger.info("发送模式比对请求...")
    logger.info(f"源数据库: {payload['source_config']['database_type']}")
    logger.info(f"目标数据库: {payload['target_config']['database_type']}")

    try:
        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
            result = response.json()
            logger.info("✅ 模式比对成功")

            # 检查结果结构
            if "result" in result and "source_schema" in result["result"] and "target_schema" in result["result"]:
                source_tables = result["result"]["source_schema"].get("tables", [])
                target_tables = result["result"]["target_schema"].get("tables", [])

                logger.info(f"源数据库表数量: {len(source_tables)}")
                logger.info(f"目标数据库表数量: {len(target_tables)}")

                if len(source_tables) > 0:
                    logger.info(f"源数据库表列表: {source_tables}")

                    # 检查每个表的详细信息
                    for table in source_tables:
                        columns = result["result"]["source_schema"].get("columns", {}).get(table, [])
                        logger.info(f"表 {table} 有 {len(columns)} 列")
                        if columns:
                            logger.info(f"  列名: {[col['name'] for col in columns]}")
                else:
                    logger.warning("⚠️ 源数据库没有找到表")

                if len(target_tables) > 0:
                    logger.info(f"目标数据库表列表: {target_tables}")
                else:
                    logger.warning("⚠️ 目标数据库没有找到表")

            else:
                logger.error("❌ 返回结果格式不正确")
                logger.error(f"返回结果: {json.dumps(result, indent=2)}")

        else:
            logger.error(f"❌ 请求失败，状态码: {response.status_code}")
            logger.error(f"错误信息: {response.text}")

    except Exception as e:
        logger.error(f"❌ 请求异常: {e}")

if __name__ == "__main__":
    test_simple_schema_comparison()
