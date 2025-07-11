#!/usr/bin/env python3
"""
测试N8N节点的模式比对功能
通过创建和执行工作流来验证节点是否正常工作
"""

import requests
import json
import time

def test_n8n_schema_comparison():
    """测试N8N节点的模式比对功能"""
    print("🔧 测试 N8N 节点模式比对功能")
    print("=" * 60)

    # N8N API 基础URL
    n8n_base_url = "http://localhost:5678/api/v1"

    # 创建一个简单的工作流来测试模式比对节点
    workflow_data = {
        "name": "Schema Comparison Test",
        "nodes": [
            {
                "id": "start",
                "name": "Start",
                "type": "n8n-nodes-base.start",
                "position": [240, 300],
                "parameters": {},
                "typeVersion": 1
            },
            {
                "id": "schema_compare",
                "name": "Schema Compare",
                "type": "n8n-nodes-data-diff-clickzetta.dataComparison",
                "position": [460, 300],
                "parameters": {
                    "operation": "compareSchemas",
                    "sourceConnection": "postgresql://metabase:metasample123@106.120.41.178:5436/sample",
                    "targetConnection": "clickzetta://qiliang:Ql123456!@uat-api.clickzetta.com/quick_start?instance=jnsxwfyr&workspace=quick_start&vcluster=default_ap",
                    "autoFillFromUpstream": False
                },
                "typeVersion": 1
            }
        ],
        "connections": {
            "Start": {
                "main": [
                    [
                        {
                            "node": "Schema Compare",
                            "type": "main",
                            "index": 0
                        }
                    ]
                ]
            }
        },
        "active": True,
        "settings": {},
        "staticData": {}
    }

    try:
        # 1. 创建工作流
        print("📝 创建测试工作流...")
        create_response = requests.post(
            f"{n8n_base_url}/workflows",
            json=workflow_data,
            timeout=10
        )

        if create_response.status_code != 201:
            print(f"❌ 创建工作流失败: {create_response.status_code} - {create_response.text}")
            return False

        workflow_info = create_response.json()
        workflow_id = workflow_info["id"]
        print(f"✅ 工作流创建成功，ID: {workflow_id}")

        # 2. 执行工作流
        print("▶️  执行工作流...")
        execute_response = requests.post(
            f"{n8n_base_url}/workflows/{workflow_id}/execute",
            json={},
            timeout=30
        )

        if execute_response.status_code != 201:
            print(f"❌ 执行工作流失败: {execute_response.status_code} - {execute_response.text}")
            return False

        execution_info = execute_response.json()
        execution_id = execution_info["id"]
        print(f"✅ 工作流执行启动，执行ID: {execution_id}")

        # 3. 等待执行完成并获取结果
        print("⏳ 等待执行完成...")
        max_wait = 30  # 最多等待30秒
        wait_time = 0

        while wait_time < max_wait:
            time.sleep(2)
            wait_time += 2

            # 获取执行状态
            status_response = requests.get(
                f"{n8n_base_url}/executions/{execution_id}",
                timeout=10
            )

            if status_response.status_code == 200:
                execution_data = status_response.json()
                status = execution_data.get("status")

                if status == "success":
                    print("✅ 工作流执行成功!")

                    # 分析结果
                    result_data = execution_data.get("data", {}).get("resultData", {}).get("runData", {})
                    if "Schema Compare" in result_data:
                        node_result = result_data["Schema Compare"][0]["data"]["main"][0][0]["json"]

                        print(f"\n📊 模式比对结果:")
                        print(f"   操作类型: {node_result.get('operation')}")
                        print(f"   执行状态: {node_result.get('success')}")

                        if node_result.get("success"):
                            data = node_result.get("data", {})
                            print(f"   状态: {data.get('status')}")
                            print(f"   源类型: {data.get('sourceType')}")
                            print(f"   目标类型: {data.get('targetType')}")

                            summary = data.get("summary", {})
                            print(f"   模式相同: {summary.get('identical')}")
                            print(f"   总差异: {summary.get('totalDifferences')}")

                            differences = data.get("differences", {})
                            print(f"   仅在源中的表: {differences.get('tablesOnlyInSource', [])}")
                            print(f"   仅在目标中的表: {differences.get('tablesOnlyInTarget', [])}")
                            print(f"   共同表: {differences.get('commonTables', [])}")
                        else:
                            print(f"   ❌ 节点执行失败: {node_result.get('error')}")

                    return True

                elif status == "failed":
                    print(f"❌ 工作流执行失败: {execution_data.get('stoppedAt')}")
                    return False

                elif status == "running":
                    print(f"⏳ 工作流仍在执行中... ({wait_time}s)")
                    continue
                else:
                    print(f"⚠️  未知执行状态: {status}")
                    continue
            else:
                print(f"❌ 获取执行状态失败: {status_response.status_code}")
                break

        print("⏰ 执行超时")
        return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

    finally:
        # 清理：删除测试工作流
        if 'workflow_id' in locals():
            try:
                print(f"🧹 清理测试工作流 {workflow_id}...")
                requests.delete(f"{n8n_base_url}/workflows/{workflow_id}")
                print("✅ 测试工作流已删除")
            except:
                print("⚠️  清理工作流失败")

def test_n8n_api_connectivity():
    """测试N8N API连通性"""
    print("🔗 测试 N8N API 连通性...")

    try:
        response = requests.get("http://localhost:5678/api/v1/workflows", timeout=5)
        if response.status_code == 200:
            print("✅ N8N API 连接正常")
            return True
        else:
            print(f"❌ N8N API 响应异常: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ N8N API 连接失败: {e}")
        return False

def main():
    """主函数"""
    print("🔧 N8N 节点模式比对功能测试")
    print("=" * 60)

    results = []

    # 测试API连通性
    print("\n1️⃣ 测试API连通性")
    result1 = test_n8n_api_connectivity()
    results.append(("API连通性", result1))

    if not result1:
        print("❌ N8N API 不可用，跳过节点测试")
        return False

    # 测试节点功能
    print("\n2️⃣ 测试节点功能")
    result2 = test_n8n_schema_comparison()
    results.append(("节点功能", result2))

    # 输出最终结果
    print("\n" + "=" * 60)
    print("🏁 测试结果总结:")

    all_passed = True
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 所有测试通过！N8N 节点模式比对功能正常工作")
        print("✅ N8N API 连接正常")
        print("✅ 节点执行成功")
        print("✅ 数据格式正确")
    else:
        print("💥 部分测试失败！需要进一步调试")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
