#!/usr/bin/env python3
"""
测试连接重试机制的脚本
验证DataComparison节点的连接重试功能是否正常工作
"""

import requests
import time
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
import threading
import random

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConnectionRetryTester:
    """连接重试机制测试器"""
    
    def __init__(self, n8n_webhook_url: str = "http://localhost:5678/webhook/test-datacomparison"):
        self.n8n_webhook_url = n8n_webhook_url
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'tests': {},
            'summary': {}
        }
    
    def test_normal_connection(self) -> Dict[str, Any]:
        """测试正常连接情况"""
        logger.info("测试正常连接情况...")
        
        test_result = {
            'name': 'Normal Connection Test',
            'success': False,
            'duration': 0,
            'error': None,
            'details': {}
        }
        
        start_time = time.time()
        
        try:
            # 构造正常的请求数据
            test_data = {
                'source_connection': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'target_connection': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'comparison_type': 'schema',
                'source_table': 'test_table',
                'target_table': 'test_table'
            }
            
            # 发送请求
            response = requests.post(
                self.n8n_webhook_url,
                json=test_data,
                timeout=30
            )
            
            test_result['success'] = response.status_code == 200
            test_result['details'] = {
                'status_code': response.status_code,
                'response_size': len(response.content),
                'response_time': response.elapsed.total_seconds()
            }
            
            if response.status_code == 200:
                response_data = response.json()
                test_result['details']['response_data'] = response_data
                logger.info("正常连接测试成功")
            else:
                test_result['error'] = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"正常连接测试失败: {test_result['error']}")
                
        except Exception as e:
            test_result['error'] = str(e)
            logger.error(f"正常连接测试异常: {e}")
        
        test_result['duration'] = time.time() - start_time
        return test_result
    
    def test_connection_failure_recovery(self) -> Dict[str, Any]:
        """测试连接失败后的恢复"""
        logger.info("测试连接失败后的恢复...")
        
        test_result = {
            'name': 'Connection Failure Recovery Test',
            'success': False,
            'duration': 0,
            'error': None,
            'details': {}
        }
        
        start_time = time.time()
        
        try:
            # 构造会导致连接失败的请求数据
            test_data = {
                'source_connection': {
                    'type': 'postgresql',
                    'host': 'invalid_host',  # 无效的主机名
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'target_connection': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'comparison_type': 'schema',
                'source_table': 'test_table',
                'target_table': 'test_table'
            }
            
            # 发送请求
            response = requests.post(
                self.n8n_webhook_url,
                json=test_data,
                timeout=60  # 给重试足够的时间
            )
            
            test_result['details'] = {
                'status_code': response.status_code,
                'response_size': len(response.content),
                'response_time': response.elapsed.total_seconds()
            }
            
            if response.status_code == 200:
                response_data = response.json()
                test_result['details']['response_data'] = response_data
                
                # 检查是否包含重试信息
                if 'error' in response_data:
                    test_result['success'] = 'retry' in response_data['error'].lower()
                    test_result['details']['retry_detected'] = test_result['success']
                else:
                    test_result['success'] = False
                    test_result['error'] = "期望的连接错误未发生"
            else:
                test_result['error'] = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"连接失败恢复测试失败: {test_result['error']}")
                
        except Exception as e:
            test_result['error'] = str(e)
            logger.error(f"连接失败恢复测试异常: {e}")
        
        test_result['duration'] = time.time() - start_time
        return test_result
    
    def test_timeout_handling(self) -> Dict[str, Any]:
        """测试超时处理"""
        logger.info("测试超时处理...")
        
        test_result = {
            'name': 'Timeout Handling Test',
            'success': False,
            'duration': 0,
            'error': None,
            'details': {}
        }
        
        start_time = time.time()
        
        try:
            # 构造会导致超时的请求数据
            test_data = {
                'source_connection': {
                    'type': 'postgresql',
                    'host': '10.255.255.1',  # 不可达的IP地址
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'target_connection': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'comparison_type': 'schema',
                'source_table': 'test_table',
                'target_table': 'test_table'
            }
            
            # 发送请求
            response = requests.post(
                self.n8n_webhook_url,
                json=test_data,
                timeout=90  # 给重试足够的时间
            )
            
            test_result['details'] = {
                'status_code': response.status_code,
                'response_size': len(response.content),
                'response_time': response.elapsed.total_seconds()
            }
            
            if response.status_code == 200:
                response_data = response.json()
                test_result['details']['response_data'] = response_data
                
                # 检查是否包含超时信息
                if 'error' in response_data:
                    error_msg = response_data['error'].lower()
                    test_result['success'] = 'timeout' in error_msg or 'retry' in error_msg
                    test_result['details']['timeout_detected'] = test_result['success']
                else:
                    test_result['success'] = False
                    test_result['error'] = "期望的超时错误未发生"
            else:
                test_result['error'] = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"超时处理测试失败: {test_result['error']}")
                
        except requests.exceptions.Timeout:
            test_result['success'] = True
            test_result['details']['timeout_occurred'] = True
            logger.info("超时处理测试成功（请求超时）")
        except Exception as e:
            test_result['error'] = str(e)
            logger.error(f"超时处理测试异常: {e}")
        
        test_result['duration'] = time.time() - start_time
        return test_result
    
    def test_concurrent_requests(self, num_requests: int = 5) -> Dict[str, Any]:
        """测试并发请求"""
        logger.info(f"测试并发请求 ({num_requests} 个请求)...")
        
        test_result = {
            'name': 'Concurrent Requests Test',
            'success': False,
            'duration': 0,
            'error': None,
            'details': {
                'num_requests': num_requests,
                'successful_requests': 0,
                'failed_requests': 0,
                'request_results': []
            }
        }
        
        start_time = time.time()
        
        def single_request(request_id: int) -> Dict[str, Any]:
            """单个请求"""
            req_result = {
                'id': request_id,
                'success': False,
                'error': None,
                'duration': 0,
                'status_code': None
            }
            
            req_start = time.time()
            
            try:
                # 随机选择连接参数以增加变化
                test_data = {
                    'source_connection': {
                        'type': 'postgresql',
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'username': 'postgres',
                        'password': 'password'
                    },
                    'target_connection': {
                        'type': 'postgresql',
                        'host': 'localhost',
                        'port': 5432,
                        'database': 'test_db',
                        'username': 'postgres',
                        'password': 'password'
                    },
                    'comparison_type': 'schema',
                    'source_table': f'test_table_{request_id}',
                    'target_table': f'test_table_{request_id}'
                }
                
                response = requests.post(
                    self.n8n_webhook_url,
                    json=test_data,
                    timeout=30
                )
                
                req_result['success'] = response.status_code == 200
                req_result['status_code'] = response.status_code
                
                if not req_result['success']:
                    req_result['error'] = f"HTTP {response.status_code}: {response.text}"
                    
            except Exception as e:
                req_result['error'] = str(e)
                logger.error(f"请求 {request_id} 失败: {e}")
            
            req_result['duration'] = time.time() - req_start
            return req_result
        
        # 启动并发请求
        threads = []
        results = []
        
        for i in range(num_requests):
            thread = threading.Thread(
                target=lambda i=i: results.append(single_request(i))
            )
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 分析结果
        successful_requests = sum(1 for r in results if r['success'])
        failed_requests = len(results) - successful_requests
        
        test_result['success'] = successful_requests > 0
        test_result['details']['successful_requests'] = successful_requests
        test_result['details']['failed_requests'] = failed_requests
        test_result['details']['request_results'] = results
        
        test_result['duration'] = time.time() - start_time
        
        logger.info(f"并发请求测试完成: {successful_requests}/{num_requests} 成功")
        return test_result
    
    def test_retry_mechanism_effectiveness(self) -> Dict[str, Any]:
        """测试重试机制的有效性"""
        logger.info("测试重试机制的有效性...")
        
        test_result = {
            'name': 'Retry Mechanism Effectiveness Test',
            'success': False,
            'duration': 0,
            'error': None,
            'details': {}
        }
        
        start_time = time.time()
        
        try:
            # 构造偶尔会失败的请求（模拟网络不稳定）
            test_data = {
                'source_connection': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'target_connection': {
                    'type': 'postgresql',
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'test_db',
                    'username': 'postgres',
                    'password': 'password'
                },
                'comparison_type': 'schema',
                'source_table': 'test_table',
                'target_table': 'test_table'
            }
            
            # 执行多次请求，观察重试机制
            attempts = 3
            success_count = 0
            retry_detected = False
            
            for attempt in range(attempts):
                try:
                    response = requests.post(
                        self.n8n_webhook_url,
                        json=test_data,
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        success_count += 1
                        response_data = response.json()
                        
                        # 检查响应中是否包含重试信息
                        if 'debug' in response_data or 'retry' in str(response_data).lower():
                            retry_detected = True
                        
                    time.sleep(2)  # 间隔2秒
                    
                except Exception as e:
                    logger.warning(f"尝试 {attempt + 1} 失败: {e}")
            
            test_result['success'] = success_count > 0
            test_result['details'] = {
                'attempts': attempts,
                'success_count': success_count,
                'success_rate': success_count / attempts,
                'retry_detected': retry_detected
            }
            
            logger.info(f"重试机制有效性测试完成: {success_count}/{attempts} 成功")
            
        except Exception as e:
            test_result['error'] = str(e)
            logger.error(f"重试机制有效性测试异常: {e}")
        
        test_result['duration'] = time.time() - start_time
        return test_result
    
    def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info("开始运行连接重试机制测试...")
        
        # 运行所有测试
        self.results['tests']['normal_connection'] = self.test_normal_connection()
        self.results['tests']['connection_failure_recovery'] = self.test_connection_failure_recovery()
        self.results['tests']['timeout_handling'] = self.test_timeout_handling()
        self.results['tests']['concurrent_requests'] = self.test_concurrent_requests(3)
        self.results['tests']['retry_mechanism_effectiveness'] = self.test_retry_mechanism_effectiveness()
        
        # 生成测试摘要
        total_tests = len(self.results['tests'])
        passed_tests = sum(1 for test in self.results['tests'].values() if test['success'])
        
        self.results['summary'] = {
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': total_tests - passed_tests,
            'overall_success': passed_tests == total_tests,
            'test_completion_rate': passed_tests / total_tests if total_tests > 0 else 0,
            'recommendations': self._generate_recommendations()
        }
        
        return self.results
    
    def _generate_recommendations(self) -> List[str]:
        """基于测试结果生成建议"""
        recommendations = []
        
        # 检查各项测试结果
        if not self.results['tests']['normal_connection']['success']:
            recommendations.append("❌ 基本连接测试失败 - 请检查n8n服务和webhook配置")
        
        if not self.results['tests']['connection_failure_recovery']['success']:
            recommendations.append("⚠️ 连接失败恢复测试失败 - 重试机制可能未正常工作")
        
        if not self.results['tests']['timeout_handling']['success']:
            recommendations.append("⏰ 超时处理测试失败 - 请检查超时配置")
        
        concurrent_test = self.results['tests']['concurrent_requests']
        if not concurrent_test['success']:
            recommendations.append(f"🔄 并发请求测试失败 - 仅{concurrent_test['details']['successful_requests']}个请求成功")
        
        if not self.results['tests']['retry_mechanism_effectiveness']['success']:
            recommendations.append("🔧 重试机制有效性测试失败 - 重试逻辑需要改进")
        
        # 通用建议
        if len(recommendations) > 0:
            recommendations.extend([
                "📊 建议检查n8n日志以了解详细错误信息",
                "🔍 验证data-diff API服务是否正常运行",
                "⚙️ 检查DataComparison节点的重试配置",
                "🌐 确认网络连接和防火墙设置"
            ])
        else:
            recommendations.append("✅ 所有测试通过，连接重试机制工作正常")
        
        return recommendations

def main():
    """主函数"""
    try:
        # 创建测试实例
        tester = ConnectionRetryTester()
        
        # 运行测试
        results = tester.run_all_tests()
        
        # 保存结果
        with open('connection_retry_test_report.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        # 输出摘要
        logger.info("\n" + "="*50)
        logger.info("连接重试机制测试报告")
        logger.info("="*50)
        
        summary = results['summary']
        logger.info(f"总测试数: {summary['total_tests']}")
        logger.info(f"通过测试: {summary['passed_tests']}")
        logger.info(f"失败测试: {summary['failed_tests']}")
        logger.info(f"测试完成率: {summary['test_completion_rate']:.1%}")
        logger.info(f"整体状态: {'✅ 良好' if summary['overall_success'] else '❌ 需要改进'}")
        
        logger.info("\n建议:")
        for rec in summary['recommendations']:
            logger.info(f"  {rec}")
        
        logger.info(f"\n详细报告已保存至: connection_retry_test_report.json")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
