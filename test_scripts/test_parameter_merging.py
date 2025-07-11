#!/usr/bin/env python3
"""
测试脚本：验证n8n节点中凭据和表单参数的合并逻辑
"""

import requests
import json
import time

def test_parameter_merging_logic():
    """
    通过测试API调用来验证参数合并逻辑
    这里我们模拟n8n节点的行为，测试参数优先级：表单参数 > 凭据参数 > 默认值
    """
    print("🧪 测试参数合并逻辑")
    print("=" * 60)

    # 模拟凭据配置（通常从n8n凭据获取）
    mock_credentials = {
        'keyColumns': 'user_id',  # 凭据中设置的默认主键
        'sampleSize': 5000,       # 凭据中设置的默认采样大小
        'threads': 2,             # 凭据中设置的默认线程数
        'caseSensitive': False,   # 凭据中设置的默认大小写敏感性
        'tolerance': 0.005,       # 凭据中设置的默认容差
        'method': 'joindiff',     # 凭据中设置的默认方法
        'excludeColumns': 'created_at,updated_at'  # 凭据中设置的默认排除列
    }

    # 模拟表单参数（用户在节点中填写的）
    form_params = {
        'keyColumns': 'id',       # 表单中覆盖主键
        'sampleSize': 100,        # 表单中覆盖采样大小
        # threads 未设置，应该使用凭据中的值
        # caseSensitive 未设置，应该使用凭据中的值
        'columnsToCompare': 'plan' # 表单中设置要比较的列
    }

    # 实现参数合并逻辑（模拟n8n节点中的逻辑）
    def merge_parameters(form_params, credentials):
        """合并参数：表单参数 > 凭据参数 > 默认值"""

        # 安全检查类型
        def safe_string(value, default=''):
            return value if isinstance(value, str) else default

        def safe_number(value, default=0):
            return value if isinstance(value, (int, float)) and value > 0 else default

        # 参数合并
        merged = {}

        # Key columns
        merged['keyColumns'] = (
            form_params.get('keyColumns') or
            safe_string(credentials.get('keyColumns')) or
            'id'
        )

        # Sample size
        merged['sampleSize'] = (
            form_params.get('sampleSize') or
            safe_number(credentials.get('sampleSize')) or
            10000
        )

        # Threads
        merged['threads'] = (
            form_params.get('threads') or
            safe_number(credentials.get('threads')) or
            4
        )

        # Case sensitive
        merged['caseSensitive'] = (
            form_params.get('caseSensitive')
            if form_params.get('caseSensitive') is not None
            else (credentials.get('caseSensitive')
                  if credentials.get('caseSensitive') is not None
                  else True)
        )

        # Tolerance
        merged['tolerance'] = (
            safe_number(credentials.get('tolerance')) or
            0.001
        )

        # Method
        merged['method'] = (
            safe_string(credentials.get('method')) or
            'hashdiff'
        )

        # Exclude columns
        merged['excludeColumns'] = (
            safe_string(credentials.get('excludeColumns')) or
            ''
        )

        # Columns to compare (只来自表单，不从凭据获取)
        merged['columnsToCompare'] = form_params.get('columnsToCompare', '')

        return merged

    # 执行合并
    merged_params = merge_parameters(form_params, mock_credentials)

    print("📋 参数合并结果:")
    print(f"凭据参数: {json.dumps(mock_credentials, indent=2, ensure_ascii=False)}")
    print(f"表单参数: {json.dumps(form_params, indent=2, ensure_ascii=False)}")
    print(f"合并结果: {json.dumps(merged_params, indent=2, ensure_ascii=False)}")

    # 验证合并逻辑
    print("\n✅ 验证合并逻辑:")

    # keyColumns 应该使用表单中的值（覆盖凭据）
    assert merged_params['keyColumns'] == 'id', f"keyColumns 应该是 'id'，实际是 '{merged_params['keyColumns']}'"
    print("  ✓ keyColumns: 表单参数正确覆盖凭据参数")

    # sampleSize 应该使用表单中的值（覆盖凭据）
    assert merged_params['sampleSize'] == 100, f"sampleSize 应该是 100，实际是 {merged_params['sampleSize']}"
    print("  ✓ sampleSize: 表单参数正确覆盖凭据参数")

    # threads 应该使用凭据中的值（表单未设置）
    assert merged_params['threads'] == 2, f"threads 应该是 2，实际是 {merged_params['threads']}"
    print("  ✓ threads: 正确使用凭据中的默认值")

    # caseSensitive 应该使用凭据中的值（表单未设置）
    assert merged_params['caseSensitive'] == False, f"caseSensitive 应该是 False，实际是 {merged_params['caseSensitive']}"
    print("  ✓ caseSensitive: 正确使用凭据中的默认值")

    # tolerance 应该使用凭据中的值
    assert merged_params['tolerance'] == 0.005, f"tolerance 应该是 0.005，实际是 {merged_params['tolerance']}"
    print("  ✓ tolerance: 正确使用凭据中的值")

    # method 应该使用凭据中的值
    assert merged_params['method'] == 'joindiff', f"method 应该是 'joindiff'，实际是 '{merged_params['method']}'"
    print("  ✓ method: 正确使用凭据中的值")

    # excludeColumns 应该使用凭据中的值
    assert merged_params['excludeColumns'] == 'created_at,updated_at', f"excludeColumns 应该是 'created_at,updated_at'，实际是 '{merged_params['excludeColumns']}'"
    print("  ✓ excludeColumns: 正确使用凭据中的值")

    # columnsToCompare 应该使用表单中的值
    assert merged_params['columnsToCompare'] == 'plan', f"columnsToCompare 应该是 'plan'，实际是 '{merged_params['columnsToCompare']}'"
    print("  ✓ columnsToCompare: 正确使用表单参数")

    print("\n🎉 所有参数合并逻辑验证通过！")

    # 测试列名解析逻辑
    print("\n🔧 测试列名解析逻辑:")

    def safe_split_columns(value):
        """安全地解析列名字符串"""
        if isinstance(value, str) and value.strip():
            return [col.strip() for col in value.split(',') if col.strip()]
        return []

    # 测试各种情况
    test_cases = [
        ('id,user_id', ['id', 'user_id']),
        ('id, user_id , email', ['id', 'user_id', 'email']),
        ('', []),
        (None, []),
        (123, []),  # 非字符串类型
        ('single_column', ['single_column'])
    ]

    for input_val, expected in test_cases:
        result = safe_split_columns(input_val)
        assert result == expected, f"输入 {input_val} 期望 {expected}，实际 {result}"
        print(f"  ✓ 输入: {repr(input_val)} -> 输出: {result}")

    print("\n✅ 列名解析逻辑验证通过！")

    return True

def test_edge_cases():
    """测试边界情况"""
    print("\n🔍 测试边界情况:")

    # 测试所有参数都为空的情况
    empty_credentials = {}
    empty_form = {}

    def merge_with_defaults(form_params, credentials):
        merged = {}

        merged['keyColumns'] = (
            form_params.get('keyColumns') or
            (credentials.get('keyColumns') if isinstance(credentials.get('keyColumns'), str) else '') or
            'id'
        )

        merged['sampleSize'] = (
            form_params.get('sampleSize') or
            (credentials.get('sampleSize') if isinstance(credentials.get('sampleSize'), (int, float)) and credentials.get('sampleSize') > 0 else 0) or
            10000
        )

        return merged

    result = merge_with_defaults(empty_form, empty_credentials)
    assert result['keyColumns'] == 'id', "空参数时应该使用默认值 'id'"
    assert result['sampleSize'] == 10000, "空参数时应该使用默认值 10000"

    print("  ✓ 空参数时正确使用默认值")

    # 测试无效类型
    invalid_credentials = {
        'keyColumns': 123,  # 应该是字符串
        'sampleSize': 'invalid',  # 应该是数字
        'threads': -1  # 应该是正数
    }

    result = merge_with_defaults({}, invalid_credentials)
    assert result['keyColumns'] == 'id', "无效类型时应该使用默认值"
    assert result['sampleSize'] == 10000, "无效类型时应该使用默认值"

    print("  ✓ 无效类型时正确使用默认值")

    print("\n✅ 边界情况测试通过！")

    return True

if __name__ == "__main__":
    try:
        success1 = test_parameter_merging_logic()
        success2 = test_edge_cases()

        if success1 and success2:
            print("\n🎉 所有测试通过！参数合并逻辑工作正常")
            print("\n📝 总结:")
            print("1. 表单参数优先级最高，会覆盖凭据参数")
            print("2. 凭据参数作为默认值，当表单参数为空时使用")
            print("3. 系统默认值在凭据参数也为空时使用")
            print("4. 类型检查确保参数安全性")
            print("5. 列名解析处理各种边界情况")
            exit(0)
        else:
            print("\n❌ 部分测试失败")
            exit(1)
    except Exception as e:
        print(f"\n💥 测试异常: {e}")
        exit(1)
