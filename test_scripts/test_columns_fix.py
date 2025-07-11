#!/usr/bin/env python3
"""
验证 columns_to_compare 参数传递修复
"""

import requests
import json
import time

def test_columns_to_compare_fix():
    """测试 columns_to_compare 参数是否正确传递到后端"""

    # API 端点
    api_url = "http://localhost:8000/api/v1/compare/tables"

    # 测试数据
    test_data = {
        "source_connection": "postgresql://test_user:test_password@localhost:5432/test_db",
        "target_connection": "clickzetta://test_user:test_password@example.clickzetta.com:9000/test_db?schema=test_schema",
        "source_table": "test_table",
        "target_table": "test_table",
        "key_columns": ["id"],
        "columns_to_compare": ["plan"]  # 指定只比较 plan 列
    }

    print("🧪 测试 columns_to_compare 参数传递...")
    print(f"📤 发送的参数: columns_to_compare = {test_data['columns_to_compare']}")

    # 发送比对请求
    try:
        response = requests.post(api_url, json=test_data, timeout=10)

        if response.status_code == 200:
            result = response.json()
            comparison_id = result.get("comparison_id")
            print(f"✅ 比对任务创建成功，ID: {comparison_id}")

            # 等待一段时间让比对完成
            print("⏳ 等待比对完成...")
            time.sleep(3)

            # 获取比对结果
            result_url = f"http://localhost:8000/api/v1/compare/results/{comparison_id}"
            result_response = requests.get(result_url, timeout=10)

            if result_response.status_code == 200:
                result_data = result_response.json()
                print("📊 比对结果:")
                print(json.dumps(result_data, indent=2, ensure_ascii=False))

                # 检查结果中的配置
                config = result_data.get("config", {})
                compare_columns = config.get("compare_columns", [])
                columns_to_compare = config.get("columns_to_compare", [])

                print(f"\n🔍 配置检查:")
                print(f"   compare_columns: {compare_columns}")
                print(f"   columns_to_compare: {columns_to_compare}")

                if compare_columns == ["plan"] and columns_to_compare == ["plan"]:
                    print("✅ 参数传递成功！columns_to_compare 已正确传递到后端")
                    return True
                else:
                    print("❌ 参数传递失败！参数未正确传递")
                    return False
            else:
                print(f"❌ 获取比对结果失败: {result_response.status_code}")
                print(f"   错误信息: {result_response.text}")
                return False
        else:
            print(f"❌ 比对请求失败: {response.status_code}")
            print(f"   错误信息: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ 请求异常: {e}")
        return False
    except Exception as e:
        print(f"❌ 其他错误: {e}")
        return False

if __name__ == "__main__":
    print("🔧 验证 columns_to_compare 参数传递修复")
    print("=" * 50)

    success = test_columns_to_compare_fix()

    print("\n" + "=" * 50)
    if success:
        print("🎉 测试通过！columns_to_compare 参数传递已修复")
    else:
        print("💥 测试失败！参数传递仍有问题")
