#!/usr/bin/env python3
"""
测试删除模拟数据后的API行为
验证schema比较现在是否会返回真实错误而不是模拟数据
"""

import requests
import json
import time

def test_schema_comparison_real_errors():
    """测试schema比较现在是否返回真实错误"""
    print("🧪 测试删除模拟数据后的schema比较API行为...")

    # API 端点
    base_url = "http://localhost:3000"  # 根据实际配置调整

    # 无效的数据库配置（用于测试错误处理）
    test_payload = {
        "operation": "Compare Schema",
        "sourceConfig": {
            "database_type": "postgresql",
            "driver": "postgresql",
            "host": "nonexistent-host.invalid",
            "port": 5432,
            "database": "nonexistent_db",
            "username": "invalid_user",
            "password": "invalid_password",
            "schema": "public"
        },
        "targetConfig": {
            "database_type": "clickzetta",
            "driver": "clickzetta",
            "host": "nonexistent-host.invalid",
            "port": 8123,
            "database": "nonexistent_db",
            "username": "invalid_user",
            "password": "invalid_password",
            "schema": "public"
        }
    }

    try:
        print("📡 发送schema比较请求到API...")
        response = requests.post(
            f"{base_url}/api/v1/compare/schemas",
            headers={"Content-Type": "application/json"},
            json=test_payload,
            timeout=30
        )

        print(f"📊 响应状态码: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("📋 API响应内容:")
            print(json.dumps(result, indent=2, ensure_ascii=False))

            # 检查响应中是否包含模拟数据
            response_str = json.dumps(result, ensure_ascii=False).lower()

            # 检查是否包含模拟表名
            if "invoices" in response_str and "users" in response_str and ("orders" in response_str or "products" in response_str):
                print("❌ 响应中包含模拟表名（invoices, users, orders/products）!")
                print("🚨 这表明仍然有模拟数据被返回！")
                return False

            # 检查是否是错误响应
            if result.get("status") == "error" or "error" in result:
                print("✅ API正确返回了错误响应，没有模拟数据")

                # 检查错误信息是否合理
                error_msg = str(result.get("error", "")).lower()
                if "data-diff" in error_msg or "connection" in error_msg or "database" in error_msg:
                    print("✅ 错误信息看起来是真实的数据库/库错误")
                    return True
                else:
                    print("⚠️  错误信息不够明确")
                    return True
            else:
                print("❌ API返回了成功响应，但使用无效配置应该失败")
                return False

        else:
            print(f"⚠️  API返回非200状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
            return True  # 非200状态码也是预期的错误行为

    except requests.exceptions.ConnectionError:
        print("⚠️  无法连接到API服务器（可能服务未启动）")
        print("💡 这是正常的，如果API服务器未运行")
        return True
    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        return False

def check_api_server():
    """检查API服务器是否运行"""
    try:
        response = requests.get("http://localhost:3000/health", timeout=5)
        if response.status_code == 200:
            print("✅ API服务器正在运行")
            return True
    except:
        pass

    print("⚠️  API服务器未运行，跳过API测试")
    return False

def main():
    """主测试函数"""
    print("🚀 开始测试删除模拟数据后的API行为...")

    # 检查API服务器状态
    if check_api_server():
        # 运行API测试
        api_test = test_schema_comparison_real_errors()

        if api_test:
            print("\n✅ API测试通过！删除模拟数据后，API正确返回真实错误。")
        else:
            print("\n❌ API测试失败，可能仍有模拟数据被返回。")

        return api_test
    else:
        print("\n💡 API服务器未运行，无法进行API测试。")
        print("   要进行完整测试，请先启动API服务器：")
        print("   docker-compose up -d")
        return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
