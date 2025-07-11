#!/usr/bin/env python3
"""
简单验证测试：确认模式比对功能已完全实现并可用
"""

import requests
import json

def test_api_directly():
    """直接测试API功能"""
    print("🔧 直接测试模式比对API")
    print("=" * 50)

    # 测试数据
    test_data = {
        "source_config": {
            "database_type": "postgresql",
            "host": "106.120.41.178",
            "port": 5436,
            "username": "metabase",
            "password": "metasample123",
            "database": "sample",
            "db_schema": "public"
        },
        "target_config": {
            "database_type": "clickzetta",
            "username": "qiliang",
            "password": "Ql123456!",
            "instance": "jnsxwfyr",
            "service": "uat-api.clickzetta.com",
            "workspace": "quick_start",
            "db_schema": "from_pg",
            "vcluster": "default_ap"
        }
    }

    print("📤 测试嵌套端点...")
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/schemas/nested",
            json=test_data,
            timeout=20
        )

        if response.status_code == 200:
            result = response.json()
            print("✅ 嵌套端点工作正常")
            print(f"   状态: {result.get('status')}")
            print(f"   源类型: {result.get('source_type')}")
            print(f"   目标类型: {result.get('target_type')}")

            # 检查结果结构
            comparison_result = result.get("result", {})
            if "summary" in comparison_result:
                summary = comparison_result["summary"]
                print(f"   总差异: {summary.get('total_differences', 0)}")
                print(f"   模式相同: {summary.get('schemas_identical', False)}")

            return True
        else:
            print(f"❌ 端点测试失败: {response.status_code}")
            print(f"   响应: {response.text}")
            return False

    except Exception as e:
        print(f"❌ 测试异常: {e}")
        return False

def verify_node_structure():
    """验证节点结构"""
    print("\n🔍 验证节点结构...")

    node_file = "/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/src/nodes/DataComparison/DataComparison.node.ts"

    try:
        with open(node_file, 'r') as f:
            content = f.read()

        checks = [
            ("Compare Schema", "模式比对操作名称"),
            ("compareSchemas", "模式比对操作值"),
            ("Compare database schemas", "模式比对描述"),
            ("compareSchemas(", "模式比对方法"),
            ("schemas/nested", "嵌套端点URL")
        ]

        all_checks_passed = True
        for check, description in checks:
            if check in content:
                print(f"   ✅ {description}")
            else:
                print(f"   ❌ 缺少{description}")
                all_checks_passed = False

        return all_checks_passed

    except Exception as e:
        print(f"❌ 验证异常: {e}")
        return False

def check_available_endpoints():
    """检查可用端点"""
    print("\n📡 检查可用的API端点...")

    try:
        response = requests.get("http://localhost:8000/openapi.json", timeout=5)
        if response.status_code == 200:
            openapi_data = response.json()
            paths = openapi_data.get("paths", {})

            schema_endpoints = [path for path in paths.keys() if "schema" in path.lower()]

            print("   可用的模式比对端点:")
            for endpoint in schema_endpoints:
                print(f"     - {endpoint}")

            return len(schema_endpoints) > 0
        else:
            print(f"❌ 获取端点列表失败: {response.status_code}")
            return False

    except Exception as e:
        print(f"❌ 检查端点异常: {e}")
        return False

def main():
    """主测试函数"""
    print("🎯 模式比对功能验证测试")
    print("=" * 60)

    tests = [
        ("API端点可用性", check_available_endpoints),
        ("节点结构完整性", verify_node_structure),
        ("API功能测试", test_api_directly)
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n{len(results)+1}️⃣ {test_name}")
        result = test_func()
        results.append((test_name, result))

    # 输出总结
    print("\n" + "=" * 60)
    print("🏁 验证结果总结:")

    all_passed = True
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
        if not result:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 模式比对功能完全实现且可用！")
        print("✅ API端点正常工作")
        print("✅ 节点结构完整")
        print("✅ 功能测试通过")
        print("\n📝 手动测试指南:")
        print("1. 访问 http://localhost:5678")
        print("2. 创建新工作流")
        print("3. 添加 'Data Comparison' 节点")
        print("4. 选择 'Compare Schema' 操作")
        print("5. 配置源和目标数据库连接")
        print("6. 执行工作流查看结果")
    else:
        print("💥 部分功能未完成，需要进一步修复")

    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
