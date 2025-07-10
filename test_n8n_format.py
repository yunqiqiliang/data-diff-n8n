#!/usr/bin/env python3
"""
测试脚本：模拟n8n节点格式的API请求
"""

import requests
import json
from urllib.parse import quote

# 测试 API 端点
API_URL = "http://localhost:8000/api/v1/compare/tables"

def test_n8n_format_request():
    """测试n8n格式的API请求"""
    print("测试n8n格式的API请求...")

    # 构建查询参数（模拟你提供的请求）
    params = {
        "source_connection": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
        "target_connection": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
        "source_table": "invoices",
        "target_table": "invoices",
        "primary_key_columns": "id",  # 单个值，不是数组
        "operation_type": "compareTables",
        "sample_size": "10000",
        "threads": "4",
        "columns_to_compare": "",  # 空字符串
        "case_sensitive": "true"
    }

    try:
        # 发送POST请求，参数在URL中
        response = requests.post(
            API_URL,
            params=params,
            headers={"Content-Type": "application/json"},
            json={}  # 空的JSON体
        )
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")

        if response.status_code == 200:
            print("✅ 测试成功!")
            return True
        else:
            print(f"❌ 测试失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ 请求失败: {e}")
        return False

def test_with_multiple_columns():
    """测试多个主键列的情况"""
    print("\n测试多个主键列...")

    params = {
        "source_connection": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
        "target_connection": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
        "source_table": "invoices",
        "target_table": "invoices",
        "primary_key_columns": "id,user_id",  # 多个列
        "operation_type": "compareTables",
        "sample_size": "10000",
        "threads": "4",
        "columns_to_compare": "id,total,status",  # 指定要比较的列
        "case_sensitive": "true"
    }

    try:
        response = requests.post(API_URL, params=params, json={})
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text[:500]}...")  # 只显示前500字符

        if response.status_code == 200:
            print("✅ 多列测试成功!")
            return True
        else:
            print(f"❌ 多列测试失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ 多列测试请求失败: {e}")
        return False

def test_with_array_format():
    """测试数组格式的参数（[]后缀）"""
    print("\n测试数组格式参数...")

    params = {
        "source_connection": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
        "target_connection": "clickzetta://qiliang:Ql123456!@jnsxwfyr.uat-api.clickzetta.com/quick_start?virtualcluster=default_ap&schema=from_pg",
        "source_table": "invoices",
        "target_table": "invoices",
        "primary_key_columns[]": "id",  # 使用[]格式
        "operation_type": "compareTables",
        "sample_size": "10000",
        "threads": "4",
        "columns_to_compare[]": "id,total",  # 使用[]格式
        "case_sensitive": "true"
    }

    try:
        response = requests.post(API_URL, params=params, json={})
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text[:500]}...")

        if response.status_code == 200:
            print("✅ 数组格式测试成功!")
            return True
        else:
            print(f"❌ 数组格式测试失败，状态码: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ 数组格式测试请求失败: {e}")
        return False

if __name__ == "__main__":
    print("开始测试API参数处理...")

    success_count = 0
    total_tests = 3

    if test_n8n_format_request():
        success_count += 1

    if test_with_multiple_columns():
        success_count += 1

    if test_with_array_format():
        success_count += 1

    print(f"\n测试完成: {success_count}/{total_tests} 成功")

    if success_count == total_tests:
        print("🎉 所有测试都通过了!")
    else:
        print("⚠️ 部分测试失败，请检查API实现")
