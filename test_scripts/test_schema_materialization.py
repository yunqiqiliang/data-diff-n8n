#!/usr/bin/env python3
"""
测试模式比对结果物化功能
"""
import asyncio
import requests
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor

# API基础URL
BASE_URL = "http://localhost:8001"

# 测试数据库配置
source_config = {
    "database_type": "postgresql",
    "host": "postgres",
    "port": 5432,
    "database": "test_source",
    "username": "postgres",
    "password": "password",
    "db_schema": "public"
}

target_config = {
    "database_type": "postgresql", 
    "host": "postgres",
    "port": 5432,
    "database": "test_target",
    "username": "postgres",
    "password": "password",
    "db_schema": "public"
}

def test_schema_comparison_with_materialization():
    """测试模式比对并检查物化结果"""
    print("\n=== 测试模式比对结果物化 ===")
    
    # 1. 执行异步模式比对
    payload = {
        "source_config": source_config,
        "target_config": target_config,
        "async_mode": True,
        "comparison_config": {
            "materialize_results": True
        }
    }
    
    print("发起异步模式比对请求...")
    response = requests.post(f"{BASE_URL}/api/v1/compare/schemas/nested", json=payload)
    
    if response.status_code != 200:
        print(f"❌ 请求失败: {response.status_code}")
        print(f"响应: {response.text}")
        return
    
    result = response.json()
    comparison_id = result.get("comparison_id")
    print(f"✅ 比对任务已创建，ID: {comparison_id}")
    
    # 2. 等待任务完成
    print("\n等待任务完成...")
    max_attempts = 30
    for i in range(max_attempts):
        time.sleep(2)
        status_response = requests.get(f"{BASE_URL}/api/v1/result/{comparison_id}")
        
        if status_response.status_code == 200:
            status_data = status_response.json()
            if status_data.get("status") == "completed":
                print("✅ 任务已完成")
                break
            elif status_data.get("status") == "failed":
                print(f"❌ 任务失败: {status_data.get('error')}")
                return
        
        print(f"  进度: {i+1}/{max_attempts}")
    else:
        print("❌ 任务超时")
        return
    
    # 3. 检查物化结果
    print("\n检查数据库中的物化结果...")
    
    # 连接到数据库
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="datadiff",
        user="postgres",
        password="password"
    )
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 检查 schema_comparison_summary 表
            cursor.execute("""
                SELECT * FROM data_diff_results.schema_comparison_summary 
                WHERE comparison_id = %s
            """, (comparison_id,))
            
            summary = cursor.fetchone()
            if summary:
                print("\n✅ 找到模式比对汇总记录:")
                print(f"  - 比对ID: {summary['comparison_id']}")
                print(f"  - 状态: {summary['status']}")
                print(f"  - 总差异: {summary['total_differences']}")
                print(f"  - 表差异: {summary['table_differences']}")
                print(f"  - 列差异: {summary['column_differences']}")
                print(f"  - 类型差异: {summary['type_differences']}")
                print(f"  - 模式相同: {summary['schemas_identical']}")
                print(f"  - 执行时间: {summary['execution_time_seconds']}秒")
            else:
                print("❌ 未找到模式比对汇总记录")
                return
            
            # 检查表差异
            cursor.execute("""
                SELECT * FROM data_diff_results.schema_table_differences 
                WHERE comparison_id = %s
                ORDER BY table_name
            """, (comparison_id,))
            
            table_diffs = cursor.fetchall()
            if table_diffs:
                print(f"\n找到 {len(table_diffs)} 个表差异:")
                for diff in table_diffs:
                    print(f"  - 表 '{diff['table_name']}' {diff['difference_type']}")
            
            # 检查列差异
            cursor.execute("""
                SELECT * FROM data_diff_results.schema_column_differences 
                WHERE comparison_id = %s
                ORDER BY table_name, column_name
            """, (comparison_id,))
            
            column_diffs = cursor.fetchall()
            if column_diffs:
                print(f"\n找到 {len(column_diffs)} 个列差异:")
                for diff in column_diffs[:5]:  # 只显示前5个
                    if diff['difference_type'] == 'type_mismatch':
                        print(f"  - 表 '{diff['table_name']}' 列 '{diff['column_name']}': "
                              f"{diff['source_data_type']} vs {diff['target_data_type']}")
                    else:
                        print(f"  - 表 '{diff['table_name']}' 列 '{diff['column_name']}' "
                              f"{diff['difference_type']}")
                
                if len(column_diffs) > 5:
                    print(f"  ... 还有 {len(column_diffs) - 5} 个差异")
            
            # 检查视图
            cursor.execute("""
                SELECT * FROM data_diff_results.recent_schema_comparisons
                WHERE comparison_id = %s
            """, (comparison_id,))
            
            view_result = cursor.fetchone()
            if view_result:
                print("\n✅ 视图查询成功")
            
            print("\n✅ 模式比对结果物化测试通过！")
            
    except Exception as e:
        print(f"\n❌ 数据库查询失败: {e}")
    finally:
        conn.close()

def test_sync_schema_comparison():
    """测试同步模式比对（也应该物化结果）"""
    print("\n\n=== 测试同步模式比对结果物化 ===")
    
    payload = {
        "source_config": source_config,
        "target_config": target_config,
        "comparison_config": {
            "materialize_results": True
        }
    }
    
    print("发起同步模式比对请求...")
    response = requests.post(f"{BASE_URL}/api/v1/compare/schemas/nested", json=payload)
    
    if response.status_code != 200:
        print(f"❌ 请求失败: {response.status_code}")
        print(f"响应: {response.text}")
        return
    
    result = response.json()
    comparison_id = result.get("comparison_id")
    print(f"✅ 同步比对完成，ID: {comparison_id}")
    
    # 检查物化结果
    print("\n检查同步比对的物化结果...")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="datadiff",
        user="postgres",
        password="password"
    )
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT comparison_id, status, total_differences, schemas_identical
                FROM data_diff_results.schema_comparison_summary 
                WHERE comparison_id = %s
            """, (comparison_id,))
            
            summary = cursor.fetchone()
            if summary:
                print("✅ 同步比对结果也已物化到数据库")
                print(f"  - 状态: {summary['status']}")
                print(f"  - 总差异: {summary['total_differences']}")
            else:
                print("❌ 同步比对结果未物化")
    finally:
        conn.close()

if __name__ == "__main__":
    # 等待服务启动
    print("等待服务启动...")
    time.sleep(5)
    
    # 运行测试
    test_schema_comparison_with_materialization()
    test_sync_schema_comparison()
    
    print("\n\n=== 所有测试完成 ===")