"""
工具函数模块 - 共享的工具函数
"""

from typing import Dict, Any, Optional
import logging
import urllib.parse

logger = logging.getLogger(__name__)

def parse_connection_string(conn_str: str) -> Dict[str, Any]:
    """从连接字符串解析数据库配置"""
    try:
        # 解析连接字符串
        if '://' in conn_str:
            parsed = urllib.parse.urlparse(conn_str)
            username_password = parsed.netloc.split('@')[0] if '@' in parsed.netloc else ''
            username = username_password.split(':')[0] if ':' in username_password else ''
            password = username_password.split(':')[1] if ':' in username_password else ''
            host_port = parsed.netloc.split('@')[-1] if '@' in parsed.netloc else parsed.netloc
            host = host_port.split(':')[0] if ':' in host_port else host_port
            port = int(host_port.split(':')[1]) if ':' in host_port else None

            # 处理默认端口
            if port is None:
                if parsed.scheme == 'postgresql':
                    port = 5432
                elif parsed.scheme == 'mysql':
                    port = 3306
                elif parsed.scheme == 'clickzetta':
                    port = 8400
                elif parsed.scheme == 'sqlserver':
                    port = 1433
                elif parsed.scheme == 'oracle':
                    port = 1521
                else:
                    port = 0

            # 解析查询参数
            query_params = {}
            if parsed.query:
                for param in parsed.query.split('&'):
                    if '=' in param:
                        key, value = param.split('=', 1)
                        query_params[key] = urllib.parse.unquote(value)

            # 返回解析结果
            result = {
                "type": parsed.scheme,  # 添加type字段，与前端请求一致
                "driver": parsed.scheme,
                "database_type": parsed.scheme,  # 添加 database_type 字段
                "username": username,
                "password": password,
                "host": host,
                "port": port,
                "database": parsed.path.lstrip('/'),
                "query": parsed.query
            }

            # 为ClickZetta添加特殊处理
            if parsed.scheme == 'clickzetta':
                # 解析服务和实例
                # 对于 jnsxwfyr.uat-api.clickzetta.com，分解为:
                # instance = jnsxwfyr, service = uat-api.clickzetta.com
                if '.' in host:
                    host_parts = host.split('.', 1)
                    instance = host_parts[0]
                    service = host_parts[1]
                else:
                    instance = host
                    service = "clickzetta.com"

                result.update({
                    "instance": instance,
                    "service": service,
                    "workspace": parsed.path.lstrip('/'),
                    "virtualcluster": query_params.get("virtualcluster", "default_ap"),  # 使用 virtualcluster 而不是 vcluster
                    "schema": query_params.get("schema", "public")
                })

            return result
        else:
            # 格式不正确，可能是DSN或其他格式
            raise ValueError(f"Unsupported connection string format: {conn_str}")

    except Exception as e:
        logger.error(f"Failed to parse connection string '{conn_str}': {e}")
        raise ValueError(f"Invalid connection string: {conn_str}") from e

# 添加一个方法来将原始比对结果保存到临时存储
def save_result_to_storage(comparison_id: str, result: Dict[str, Any]) -> None:
    """
    将比对结果保存到临时存储
    这里简单实现，生产环境中应该使用Redis或数据库
    """
    import os
    import json

    try:
        os.makedirs('/app/tmp', exist_ok=True)
        file_path = f'/app/tmp/{comparison_id}.json'
        with open(file_path, 'w') as f:
            json.dump(result, f)
        logger.info(f"Saved result for comparison {comparison_id}")
    except Exception as e:
        logger.error(f"Failed to save result for comparison {comparison_id}: {e}")

def get_result_from_storage(comparison_id: str) -> Optional[Dict[str, Any]]:
    """
    从临时存储获取比对结果
    """
    import os
    import json

    try:
        file_path = f'/app/tmp/{comparison_id}.json'
        if not os.path.exists(file_path):
            return None

        with open(file_path, 'r') as f:
            result = json.load(f)
        return result
    except Exception as e:
        logger.error(f"Failed to get result for comparison {comparison_id}: {e}")
        return None
