"""
工具函数模块 - 共享的工具函数
"""

from typing import Dict, Any, Optional
import logging
import urllib.parse
import json
from datetime import datetime
from fastapi import HTTPException, status
from .response_models import ErrorDetail, ErrorCodes
from .json_utils import safe_json_dump, safe_json_dumps

logger = logging.getLogger(__name__)

# 任务状态存储（简单实现，生产环境应使用Redis）
_task_status = {}
_task_progress = {}

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
            safe_json_dump(result, f)
        logger.info(f"Saved result for comparison {comparison_id}")
        
        # 更新任务状态
        if result.get("status") == "failed":
            update_task_status(comparison_id, "failed")
        else:
            update_task_status(comparison_id, "completed")
    except Exception as e:
        logger.error(f"Failed to save result for comparison {comparison_id}: {e}")

def get_result_from_storage(comparison_id: str) -> Optional[Dict[str, Any]]:
    """
    从临时存储获取比对结果，如果不存在则尝试从数据库获取
    """
    import os
    import json

    try:
        # 首先尝试从文件系统获取
        file_path = f'/app/tmp/{comparison_id}.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                result = json.load(f)
            return result
        
        # 如果文件不存在，尝试从数据库获取
        logger.info(f"Result not found in file system for {comparison_id}, checking database...")
        
        import psycopg2
        import psycopg2.extras
        
        # 检查是否在 Docker 容器中运行
        in_docker = os.path.exists('/.dockerenv')
        default_host = 'postgres' if in_docker else 'localhost'
        
        db_host = os.environ.get('DB_HOST', default_host)
        db_port = int(os.environ.get('DB_PORT', '5432'))
        db_user = os.environ.get('DB_USER', 'postgres')
        db_password = os.environ.get('DB_PASSWORD', 'password')
        db_name = os.environ.get('DB_NAME', 'datadiff')
        
        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                dbname=db_name
            )
            
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # 查询比对结果
                cursor.execute("""
                    SELECT 
                        comparison_id,
                        status,
                        source_table,
                        target_table,
                        rows_compared,
                        rows_matched,
                        rows_different,
                        match_rate,
                        execution_time_seconds,
                        created_at,
                        result_json
                    FROM data_diff_results.comparison_summary
                    WHERE comparison_id = %s
                """, (comparison_id,))
                
                row = cursor.fetchone()
                
                if row:
                    # 构建结果对象
                    result = {
                        "comparison_id": row['comparison_id'],
                        "status": row['status'],
                        "timestamp": row['created_at'].isoformat() if row['created_at'] else None,
                        "result": {
                            "summary": {
                                "source_table": row['source_table'],
                                "target_table": row['target_table'],
                                "rows_compared": row['rows_compared'],
                                "rows_matched": row['rows_matched'],
                                "rows_different": row['rows_different'],
                                "match_rate": float(row['match_rate']) if row['match_rate'] else 0.0,
                                "execution_time": float(row['execution_time_seconds']) if row['execution_time_seconds'] else 0.0,
                                "data_quality_score": "Good" if float(row['match_rate'] or 0) > 95 else "Fair" if float(row['match_rate'] or 0) > 80 else "Poor"
                            }
                        }
                    }
                    
                    # 如果有详细的result_json，合并它
                    if row['result_json']:
                        try:
                            stored_result = json.loads(row['result_json'])
                            result['result'].update(stored_result)
                        except:
                            pass
                    
                    # 获取差异样本
                    cursor.execute("""
                        SELECT 
                            key_values,
                            source_values,
                            target_values,
                            difference_type
                        FROM data_diff_results.comparison_differences
                        WHERE comparison_id = %s
                        LIMIT 100
                    """, (comparison_id,))
                    
                    differences = []
                    for diff_row in cursor.fetchall():
                        differences.append({
                            "key": diff_row['key_values'],
                            "source": diff_row['source_values'],
                            "target": diff_row['target_values'],
                            "type": diff_row['difference_type']
                        })
                    
                    if differences:
                        result['result']['sample_differences'] = differences
                    
                    logger.info(f"Successfully retrieved result from database for {comparison_id}")
                    
                    # 将结果保存到文件系统以供后续快速访问
                    try:
                        os.makedirs('/app/tmp', exist_ok=True)
                        with open(file_path, 'w') as f:
                            json.dump(result, f)
                    except:
                        pass  # 忽略文件保存错误
                    
                    return result
                
            conn.close()
            
        except Exception as db_error:
            logger.error(f"Failed to get result from database: {db_error}")
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to get result for comparison {comparison_id}: {e}")
        return None

def update_task_status(comparison_id: str, status: str) -> None:
    """更新任务状态"""
    _task_status[comparison_id] = {
        "status": status,
        "updated_at": datetime.now().isoformat()
    }
    logger.info(f"Updated task {comparison_id} status to {status}")

def get_task_status(comparison_id: str) -> str:
    """获取任务状态，优先从内存，然后从数据库"""
    # 首先检查内存中的状态
    if comparison_id in _task_status:
        return _task_status[comparison_id]["status"]
    
    # 如果内存中没有，尝试从数据库获取
    import os
    import psycopg2
    
    try:
        # 检查是否在 Docker 容器中运行
        in_docker = os.path.exists('/.dockerenv')
        default_host = 'postgres' if in_docker else 'localhost'
        
        db_host = os.environ.get('DB_HOST', default_host)
        db_port = int(os.environ.get('DB_PORT', '5432'))
        db_user = os.environ.get('DB_USER', 'postgres')
        db_password = os.environ.get('DB_PASSWORD', 'password')
        db_name = os.environ.get('DB_NAME', 'datadiff')
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            dbname=db_name
        )
        
        with conn.cursor() as cursor:
            # 查询比对状态
            cursor.execute("""
                SELECT status 
                FROM data_diff_results.comparison_summary 
                WHERE comparison_id = %s
            """, (comparison_id,))
            
            row = cursor.fetchone()
            if row:
                status = row[0]
                # 将状态缓存到内存中
                _task_status[comparison_id] = {
                    "status": status,
                    "timestamp": datetime.now().isoformat()
                }
                conn.close()
                return status
                
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to get task status from database: {e}")
    
    return "not_found"

def update_task_progress(comparison_id: str, progress: int, current_step: str = "", estimated_time: str = "") -> None:
    """更新任务进度"""
    if comparison_id not in _task_progress:
        _task_progress[comparison_id] = {
            "started_at": datetime.now().isoformat()
        }
    
    _task_progress[comparison_id].update({
        "progress": progress,
        "current_step": current_step,
        "estimated_time": estimated_time,
        "updated_at": datetime.now().isoformat()
    })

def get_task_progress(comparison_id: str) -> Dict[str, Any]:
    """获取任务进度"""
    return _task_progress.get(comparison_id, {})

def create_task(comparison_id: str) -> None:
    """创建新任务"""
    update_task_status(comparison_id, "pending")


def create_error_response(
    status_code: int,
    error_code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None
) -> HTTPException:
    """创建标准化的错误响应"""
    error_detail = {
        "code": error_code,
        "message": message
    }
    if details:
        error_detail["details"] = details
    
    return HTTPException(
        status_code=status_code,
        detail=error_detail
    )


def create_success_response(data: Dict[str, Any], message: Optional[str] = None) -> Dict[str, Any]:
    """创建标准化的成功响应"""
    response = {
        "success": True,
        "data": data,
        "timestamp": datetime.utcnow().isoformat()
    }
    if message:
        response["message"] = message
    return response
