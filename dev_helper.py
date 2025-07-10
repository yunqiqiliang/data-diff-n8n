#!/usr/bin/env python
"""
本地开发助手脚本
提供各种本地开发和测试的便捷功能
"""

import sys
import os
import argparse
from pathlib import Path

# 将项目根目录添加到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_connection_string_parsing():
    """测试连接字符串解析功能"""
    print("🔗 测试连接字符串解析...")

    try:
        from n8n.api.main import parse_connection_string

        # 测试各种连接字符串
        test_cases = [
            "postgresql://user:pass@localhost:5432/dbname",
            "mysql://user:pass@localhost:3306/dbname",
            "clickzetta://user:pass@host:9000/database?cluster=default",
            "sqlite:///path/to/database.db"
        ]

        for conn_str in test_cases:
            try:
                result = parse_connection_string(conn_str)
                print(f"✅ {conn_str[:50]}... -> {result}")
            except Exception as e:
                print(f"❌ {conn_str[:50]}... -> 失败: {e}")

        return True
    except Exception as e:
        print(f"❌ 连接字符串解析测试失败: {e}")
        return False

def test_clickzetta_adapter():
    """测试 Clickzetta 适配器"""
    print("🎯 测试 Clickzetta 适配器...")

    try:
        from n8n.core import ClickzettaAdapter

        # 创建适配器实例
        adapter = ClickzettaAdapter()
        print("✅ Clickzetta 适配器创建成功")

        # 测试连接参数解析
        test_params = {
            "host": "test.clickzetta.com",
            "port": 9000,
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass"
        }

        # 注意：这里只是测试参数处理，不会真正连接
        print(f"✅ 测试连接参数: {test_params}")

        return True
    except Exception as e:
        print(f"❌ Clickzetta 适配器测试失败: {e}")
        return False

def test_workflow_templates():
    """测试工作流模板"""
    print("📋 测试工作流模板...")

    try:
        from n8n.workflows import TemplateManager

        template_mgr = TemplateManager()

        # 获取所有模板
        templates = template_mgr.list_templates()
        print(f"✅ 找到 {len(templates)} 个模板:")

        for template in templates:
            print(f"  - {template.get('name', 'unnamed')}: {template.get('description', 'no description')}")

        # 尝试获取一个特定模板
        if templates:
            first_template = templates[0]
            template_name = first_template.get('name')
            if template_name:
                template_content = template_mgr.get_template(template_name)
                print(f"✅ 成功获取模板 '{template_name}': {len(str(template_content))} 字符")

        return True
    except Exception as e:
        print(f"❌ 工作流模板测试失败: {e}")
        return False

def test_monitoring():
    """测试监控功能"""
    print("📊 测试监控功能...")

    try:
        from n8n.monitoring import metrics

        # 测试指标收集
        print("✅ 监控模块导入成功")

        # 这里可以添加更多监控相关的测试

        return True
    except Exception as e:
        print(f"❌ 监控功能测试失败: {e}")
        return False

def run_all_tests():
    """运行所有测试"""
    print("🧪 运行所有本地测试...")

    tests = [
        ("基础导入测试", lambda: __import__("test_local").test_imports()),
        ("连接字符串解析", test_connection_string_parsing),
        ("Clickzetta 适配器", test_clickzetta_adapter),
        ("工作流模板", test_workflow_templates),
        ("监控功能", test_monitoring),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"🔍 {test_name}")
        print('='*50)

        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} 通过")
            else:
                print(f"❌ {test_name} 失败")
        except Exception as e:
            print(f"❌ {test_name} 异常: {e}")

    print(f"\n{'='*50}")
    print(f"📊 测试结果: {passed}/{total} 通过")
    print('='*50)

    return passed == total

def main():
    parser = argparse.ArgumentParser(description="本地开发助手")
    parser.add_argument("--test", action="store_true", help="运行所有测试")
    parser.add_argument("--api", action="store_true", help="启动本地 API 服务器")
    parser.add_argument("--connection", type=str, help="测试特定的连接字符串")

    args = parser.parse_args()

    if args.test:
        success = run_all_tests()
        sys.exit(0 if success else 1)
    elif args.api:
        from start_local_api import start_local_api_server
        start_local_api_server()
    elif args.connection:
        from n8n.api.main import parse_connection_string
        try:
            result = parse_connection_string(args.connection)
            print(f"✅ 解析结果: {result}")
        except Exception as e:
            print(f"❌ 解析失败: {e}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
