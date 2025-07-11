#!/usr/bin/env python3
"""
连接重试机制测试脚本
用于验证 DataComparison 节点的连接重试功能
"""

import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConnectionRetryTester:
    def __init__(self):
        self.test_results = []
        self.api_base_url = "http://data-diff-api:8000"
        
    async def test_connection_retry_scenarios(self):
        """测试各种连接重试场景"""
        logger.info("开始连接重试机制测试...")
        
        # 测试场景
        scenarios = [
            {
                "name": "正常连接测试",
                "description": "测试正常的数据库连接",
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
                },
                "expected_success": True
            },
            {
                "name": "连接超时测试",
                "description": "测试连接超时情况",
                "source_config": {
                    "database_type": "postgresql",
                    "host": "192.168.1.999",  # 不存在的IP
                    "port": 5432,
                    "username": "test",
                    "password": "test",
                    "database": "test",
                    "db_schema": "public"
                },
                "target_config": {
                    "database_type": "postgresql",
                    "host": "106.120.41.178",
                    "port": 5436,
                    "username": "metabase",
                    "password": "metasample123",
                    "database": "sample",
                    "db_schema": "public"
                },
                "expected_success": False,
                "expected_error_patterns": ["timeout", "connection refused", "econnrefused"]
            },
            {
                "name": "认证失败测试",
                "description": "测试错误的认证信息",
                "source_config": {
                    "database_type": "postgresql",
                    "host": "106.120.41.178",
                    "port": 5436,
                    "username": "wrong_user",
                    "password": "wrong_pass",
                    "database": "sample",
                    "db_schema": "public"
                },
                "target_config": {
                    "database_type": "postgresql",
                    "host": "106.120.41.178",
                    "port": 5436,
                    "username": "metabase",
                    "password": "metasample123",
                    "database": "sample",
                    "db_schema": "public"
                },
                "expected_success": False,
                "expected_error_patterns": ["authentication", "invalid credentials", "permission denied"]
            }
        ]
        
        for scenario in scenarios:
            await self.run_scenario(scenario)
            
        # 生成测试报告
        self.generate_test_report()
        
    async def run_scenario(self, scenario: Dict[str, Any]):
        """运行单个测试场景"""
        logger.info(f"运行测试场景: {scenario['name']}")
        logger.info(f"描述: {scenario['description']}")
        
        start_time = time.time()
        test_result = {
            "scenario_name": scenario["name"],
            "description": scenario["description"],
            "start_time": datetime.now().isoformat(),
            "success": False,
            "error": None,
            "duration": 0,
            "retry_attempts": 0,
            "expected_success": scenario.get("expected_success", True),
            "expected_error_patterns": scenario.get("expected_error_patterns", []),
            "details": {}
        }
        
        try:
            # 模拟数据比对请求
            result = await self.simulate_comparison_request(
                scenario["source_config"],
                scenario["target_config"]
            )
            
            test_result["success"] = True
            test_result["details"] = result
            
        except Exception as e:
            test_result["error"] = str(e)
            test_result["success"] = False
            
            # 检查是否是预期的错误
            if not scenario.get("expected_success", True):
                error_lower = str(e).lower()
                expected_patterns = scenario.get("expected_error_patterns", [])
                
                if any(pattern in error_lower for pattern in expected_patterns):
                    test_result["expected_error_occurred"] = True
                    logger.info(f"预期错误正确发生: {e}")
                else:
                    test_result["unexpected_error"] = True
                    logger.warning(f"发生了非预期错误: {e}")
            else:
                logger.error(f"测试场景失败: {e}")
                
        test_result["duration"] = time.time() - start_time
        self.test_results.append(test_result)
        
        logger.info(f"测试场景完成: {scenario['name']}, 用时: {test_result['duration']:.2f}s")
        
    async def simulate_comparison_request(self, source_config: Dict, target_config: Dict) -> Dict:
        """模拟数据比对请求"""
        import aiohttp
        
        # 构建请求数据
        request_data = {
            "source_config": source_config,
            "target_config": target_config,
            "comparison_config": {
                "source_table": "invoices",
                "target_table": "invoices",
                "key_columns": ["id"],
                "sample_size": 1000,
                "threads": 2,
                "algorithm": "hashdiff",
                "case_sensitive": True,
                "tolerance": 0.001,
                "bisection_threshold": 1024,
                "strict_type_checking": False
            }
        }
        
        # 模拟重试机制
        max_retries = 3
        base_delay = 1.0
        
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"尝试 {attempt + 1}/{max_retries + 1}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.api_base_url}/api/v1/compare/tables/nested",
                        json=request_data,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        
                        if response.status == 200:
                            result = await response.json()
                            if attempt > 0:
                                logger.info(f"重试成功，总共尝试了 {attempt + 1} 次")
                            return result
                        else:
                            error_text = await response.text()
                            raise Exception(f"HTTP {response.status}: {error_text}")
                            
            except Exception as e:
                error_msg = str(e).lower()
                
                # 检查是否是可重试的错误
                retryable_patterns = [
                    'connection already closed',
                    'connection reset',
                    'connection refused',
                    'connection timeout',
                    'connection lost',
                    'timeout',
                    'network error',
                    'econnrefused',
                    'etimedout',
                    'socket hang up',
                    'socket timeout',
                    'read timeout',
                    'write timeout'
                ]
                
                is_retryable = any(pattern in error_msg for pattern in retryable_patterns)
                
                if attempt >= max_retries or not is_retryable:
                    logger.error(f"不再重试，原因: {'达到最大重试次数' if attempt >= max_retries else '错误不可重试'}")
                    raise e
                
                # 计算延迟时间
                delay = min(base_delay * (2 ** attempt), 10.0)
                logger.info(f"等待 {delay:.1f}s 后重试...")
                await asyncio.sleep(delay)
                
        raise Exception("所有重试都失败了")
        
    def generate_test_report(self):
        """生成测试报告"""
        report = {
            "test_summary": {
                "total_scenarios": len(self.test_results),
                "successful_scenarios": sum(1 for r in self.test_results if r["success"]),
                "failed_scenarios": sum(1 for r in self.test_results if not r["success"]),
                "expected_failures": sum(1 for r in self.test_results if not r["success"] and not r.get("expected_success", True)),
                "unexpected_failures": sum(1 for r in self.test_results if not r["success"] and r.get("expected_success", True)),
                "test_date": datetime.now().isoformat()
            },
            "detailed_results": self.test_results
        }
        
        # 保存报告
        report_file = f"connection_retry_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        logger.info(f"测试报告已保存到: {report_file}")
        
        # 打印摘要
        print("\n" + "="*50)
        print("连接重试机制测试报告")
        print("="*50)
        print(f"总测试场景: {report['test_summary']['total_scenarios']}")
        print(f"成功场景: {report['test_summary']['successful_scenarios']}")
        print(f"失败场景: {report['test_summary']['failed_scenarios']}")
        print(f"预期失败: {report['test_summary']['expected_failures']}")
        print(f"意外失败: {report['test_summary']['unexpected_failures']}")
        print("="*50)
        
        # 打印详细结果
        for result in self.test_results:
            status = "✅ 成功" if result["success"] else "❌ 失败"
            print(f"{status} - {result['scenario_name']}: {result['description']}")
            if result.get("error"):
                print(f"   错误: {result['error']}")
            print(f"   用时: {result['duration']:.2f}s")
            
        return report

async def main():
    """主函数"""
    tester = ConnectionRetryTester()
    await tester.test_connection_retry_scenarios()

if __name__ == "__main__":
    asyncio.run(main())
