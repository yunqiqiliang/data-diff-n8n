#!/usr/bin/env python3
"""
测试脚本：模拟 n8n 节点的 API 调用方式
"""

import requests
import json
import time

# 测试 API 端点
API_URL = "http://localhost:8000/api/v1/compare/tables"
RESULTS_URL = "http://localhost:8000/api/v1/compare/results"

def test_n8n_style_api_call():
    """模拟 n8n 节点的 API 调用方式"""
    print("测试 n8n 节点风格的 API 调用...")

    # 使用修复后的 JSON 请求方式
    request_data = {
        "source_connection": "postgresql://postgres:password@postgres:5432/postgres",
        "target_connection": "postgresql://postgres:password@postgres:5432/postgres",
        "source_table": "users",
        "target_table": "users_copy",
        "key_columns": ["id"],
        "columns_to_compare": ["name", "email"],
        "sample_size": 10000,
        "threads": 1,
        "case_sensitive": True,
        "bisection_threshold": 1024
    }

    try:
        # 1. 发起比对请求
        print("发送比对请求...")
        print("请求数据:", json.dumps(request_data, indent=2))

        response = requests.post(
            API_URL,
            json=request_data,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"响应状态码: {response.status_code}")
        print(f"响应内容: {response.text}")

        if response.status_code != 200:
            print("❌ 比对请求失败")
            return False

        result = response.json()
        comparison_id = result.get("comparison_id")
        
        if not comparison_id:
            print("❌ 未获取到比对ID")
            return False

        print(f"✅ 比对任务已启动，ID: {comparison_id}")

        # 2. 等待并查询结果
        print("等待比对完成...")
        max_attempts = 15
        attempts = 0

        while attempts < max_attempts:
            time.sleep(2)  # 等待2秒
            attempts += 1

            try:
                result_response = requests.get(f"{RESULTS_URL}/{comparison_id}")
                print(f"查询结果 (尝试 {attempts}): 状态码 {result_response.status_code}")

                if result_response.status_code == 200:
                    result_data = result_response.json()

                    if result_data.get("status") == "completed" and result_data.get("result"):
                        print("✅ 比对成功完成!")
                        comparison_result = result_data["result"]
                        
                        # 输出关键信息
                        statistics = comparison_result.get("statistics", {})
                        summary = comparison_result.get("summary", {})
                        
                        print(f"比对统计:")
                        print(f"  - 总差异数: {statistics.get('differences', {}).get('total_differences', 0)}")
                        print(f"  - 匹配率: {statistics.get('match_rate', 0) * 100:.1f}%")
                        print(f"  - 有差异: {summary.get('has_differences', False)}")
                        print(f"  - 数据质量评分: {summary.get('data_quality_score', 'N/A')}")
                        
                        return True
                    elif result_data.get("status") == "error":
                        print(f"❌ 比对失败: {result_data.get('message', '未知错误')}")
                        return False
                    else:
                        print(f"比对进行中，状态: {result_data.get('status', 'unknown')}")
                        continue
                else:
                    print(f"查询结果失败: {result_response.text}")

            except Exception as e:
                print(f"查询结果出错: {e}")

        print("❌ 比对超时")
        return False

    except Exception as e:
        print(f"❌ 请求失败: {e}")
        return False

if __name__ == "__main__":
    success = test_n8n_style_api_call()
    print(f"\n测试结果: {'✅ 成功' if success else '❌ 失败'}")
