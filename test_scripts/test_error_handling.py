#!/usr/bin/env python3
"""
测试 DataComparison 节点的错误处理改进
"""

import requests
import json
import time
import sys
from typing import Dict, Any, Optional

def test_n8n_workflow_error_handling():
    """测试 n8n 工作流的错误处理"""
    
    # n8n API 配置
    n8n_base_url = "http://localhost:5678"
    
    # 测试不同类型的错误场景
    test_cases = [
        {
            "name": "数据库连接错误",
            "workflow": {
                "nodes": [
                    {
                        "id": "start",
                        "type": "n8n-nodes-base.manualTrigger",
                        "name": "Manual Trigger",
                        "parameters": {},
                        "position": [250, 300],
                        "typeVersion": 1
                    },
                    {
                        "id": "datacomparison",
                        "type": "n8n-nodes-data-diff.dataComparison",
                        "name": "Data Comparison",
                        "parameters": {
                            "operation": "compareSchemas",
                            "autoFillFromUpstream": False,
                            "sourceConnection": "postgresql://invalid_user:wrong_password@localhost:5432/test_db",
                            "targetConnection": "clickzetta://invalid_user:wrong_password@invalid_host:8123/test_db"
                        },
                        "position": [450, 300],
                        "typeVersion": 1
                    }
                ],
                "connections": {
                    "Manual Trigger": {
                        "main": [
                            [
                                {
                                    "node": "Data Comparison",
                                    "type": "main",
                                    "index": 0
                                }
                            ]
                        ]
                    }
                }
            }
        },
        {
            "name": "API服务不可用",
            "workflow": {
                "nodes": [
                    {
                        "id": "start",
                        "type": "n8n-nodes-base.manualTrigger",
                        "name": "Manual Trigger",
                        "parameters": {},
                        "position": [250, 300],
                        "typeVersion": 1
                    },
                    {
                        "id": "datacomparison",
                        "type": "n8n-nodes-data-diff.dataComparison",
                        "name": "Data Comparison",
                        "parameters": {
                            "operation": "getComparisonResult",
                            "comparisonId": "non_existent_id_12345"
                        },
                        "position": [450, 300],
                        "typeVersion": 1
                    }
                ],
                "connections": {
                    "Manual Trigger": {
                        "main": [
                            [
                                {
                                    "node": "Data Comparison",
                                    "type": "main",
                                    "index": 0
                                }
                            ]
                        ]
                    }
                }
            }
        },
        {
            "name": "表不存在错误",
            "workflow": {
                "nodes": [
                    {
                        "id": "start",
                        "type": "n8n-nodes-base.manualTrigger",
                        "name": "Manual Trigger",
                        "parameters": {},
                        "position": [250, 300],
                        "typeVersion": 1
                    },
                    {
                        "id": "datacomparison",
                        "type": "n8n-nodes-data-diff.dataComparison",
                        "name": "Data Comparison",
                        "parameters": {
                            "operation": "compareTables",
                            "autoFillFromUpstream": False,
                            "sourceConnection": "postgresql://postgres:password@localhost:5432/test_db",
                            "targetConnection": "postgresql://postgres:password@localhost:5432/test_db",
                            "sourceTable": "non_existent_table_source",
                            "targetTable": "non_existent_table_target"
                        },
                        "position": [450, 300],
                        "typeVersion": 1
                    }
                ],
                "connections": {
                    "Manual Trigger": {
                        "main": [
                            [
                                {
                                    "node": "Data Comparison",
                                    "type": "main",
                                    "index": 0
                                }
                            ]
                        ]
                    }
                }
            }
        }
    ]
    
    print("🔍 开始测试 DataComparison 节点错误处理...")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📋 测试 {i}: {test_case['name']}")
        print("-" * 40)
        
        try:
            # 创建并执行测试工作流
            result = execute_test_workflow(test_case['workflow'])
            
            if result:
                print("✅ 测试完成")
                analyze_error_handling(result)
            else:
                print("❌ 测试失败")
                
        except Exception as e:
            print(f"❌ 测试执行错误: {str(e)}")
    
    print("\n" + "=" * 60)
    print("🏁 错误处理测试完成")

def execute_test_workflow(workflow: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """执行测试工作流"""
    
    n8n_base_url = "http://localhost:5678"
    
    try:
        # 创建工作流
        create_response = requests.post(
            f"{n8n_base_url}/api/v1/workflows",
            json={
                "name": f"Error Test {int(time.time())}",
                "nodes": workflow["nodes"],
                "connections": workflow["connections"],
                "active": False
            },
            headers={"Content-Type": "application/json"}
        )
        
        if create_response.status_code not in [200, 201]:
            print(f"❌ 创建工作流失败: {create_response.status_code}")
            print(f"响应内容: {create_response.text}")
            return None
        
        workflow_data = create_response.json()
        workflow_id = workflow_data["id"]
        print(f"✅ 工作流创建成功: {workflow_id}")
        
        # 执行工作流
        execute_response = requests.post(
            f"{n8n_base_url}/api/v1/workflows/{workflow_id}/execute",
            json={},
            headers={"Content-Type": "application/json"}
        )
        
        if execute_response.status_code not in [200, 201]:
            print(f"❌ 执行工作流失败: {execute_response.status_code}")
            print(f"响应内容: {execute_response.text}")
            return None
        
        execution_data = execute_response.json()
        print(f"✅ 工作流执行完成")
        
        # 删除测试工作流
        requests.delete(f"{n8n_base_url}/api/v1/workflows/{workflow_id}")
        
        return execution_data
        
    except Exception as e:
        print(f"❌ 执行工作流时发生错误: {str(e)}")
        return None

def analyze_error_handling(result: Dict[str, Any]) -> None:
    """分析错误处理结果"""
    
    print("\n📊 错误处理分析:")
    
    if "data" in result and "resultData" in result["data"]:
        result_data = result["data"]["resultData"]
        
        for node_name, node_data in result_data.items():
            if isinstance(node_data, list) and len(node_data) > 0:
                for item in node_data:
                    if isinstance(item, dict) and "json" in item:
                        json_data = item["json"]
                        
                        if "success" in json_data:
                            if json_data["success"]:
                                print(f"  ✅ {node_name}: 执行成功")
                            else:
                                print(f"  ❌ {node_name}: 执行失败")
                                
                                # 分析错误信息
                                if "error" in json_data:
                                    print(f"     错误信息: {json_data['error']}")
                                
                                if "errorCategory" in json_data:
                                    print(f"     错误类别: {json_data['errorCategory']}")
                                
                                if "improvementSuggestions" in json_data:
                                    print(f"     改进建议:")
                                    for suggestion in json_data["improvementSuggestions"]:
                                        print(f"       - {suggestion}")
                                
                                if "debugInfo" in json_data:
                                    debug_info = json_data["debugInfo"]
                                    if "errorAnalysis" in debug_info:
                                        analysis = debug_info["errorAnalysis"]
                                        print(f"     错误分析: {analysis}")
    
    else:
        print("  ⚠️  未找到执行结果数据")

def check_api_service():
    """检查 API 服务状态"""
    
    print("🔍 检查 data-diff API 服务状态...")
    
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("✅ data-diff API 服务正常")
            return True
        else:
            print(f"⚠️  data-diff API 服务状态异常: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ data-diff API 服务不可用: {str(e)}")
        return False

def check_n8n_service():
    """检查 n8n 服务状态"""
    
    print("🔍 检查 n8n 服务状态...")
    
    try:
        response = requests.get("http://localhost:5678/api/v1/workflows", timeout=5)
        if response.status_code == 200:
            print("✅ n8n 服务正常")
            return True
        else:
            print(f"⚠️  n8n 服务状态异常: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ n8n 服务不可用: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 DataComparison 错误处理测试")
    print("=" * 60)
    
    # 检查服务状态
    n8n_ok = check_n8n_service()
    api_ok = check_api_service()
    
    if not n8n_ok:
        print("\n❌ n8n 服务不可用，请先启动 n8n 服务")
        sys.exit(1)
    
    if not api_ok:
        print("\n⚠️  data-diff API 服务不可用，某些测试可能失败")
    
    print("\n🎯 开始错误处理测试...")
    test_n8n_workflow_error_handling()
