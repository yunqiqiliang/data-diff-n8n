#!/usr/bin/env python3
"""
检查模式比对 API 响应结构
"""

import requests
import json
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_api_response_structure():
    """检查 API 响应结构"""

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

    try:
        response = requests.post(url, json=payload, timeout=10)

        if response.status_code == 200:
            result = response.json()
            logger.info("✅ 模式比对成功")

            # 打印完整的响应结构
            logger.info(f"完整响应结构: {json.dumps(result, indent=2, ensure_ascii=False)}")

        else:
            logger.error(f"❌ 模式比对失败，状态码: {response.status_code}")
            logger.error(f"响应内容: {response.text}")

    except requests.RequestException as e:
        logger.error(f"❌ 请求失败: {e}")

if __name__ == "__main__":
    test_api_response_structure()
