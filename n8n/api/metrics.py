"""
Prometheus metrics for data comparison monitoring
"""
from prometheus_client import Counter, Histogram, Gauge, Info
import psutil
import logging

logger = logging.getLogger(__name__)

# Performance Metrics
api_request_duration = Histogram(
    'datadiff_api_request_duration_seconds',
    'API request duration in seconds',
    ['method', 'endpoint', 'status'],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0)
)

api_request_total = Counter(
    'datadiff_api_request_total',
    'Total number of API requests',
    ['method', 'endpoint', 'status']
)

comparison_duration = Histogram(
    'datadiff_comparison_duration_seconds',
    'Comparison execution duration in seconds',
    ['comparison_type', 'algorithm', 'status', 'source_type', 'target_type'],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600)
)

comparison_total = Counter(
    'datadiff_comparison_total',
    'Total number of comparisons',
    ['comparison_type', 'status', 'source_type', 'target_type']
)

db_query_duration = Histogram(
    'datadiff_db_query_duration_seconds',
    'Database query duration in seconds',
    ['database_type', 'operation', 'table_name'],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0)
)

# Business Metrics
comparison_differences = Histogram(
    'datadiff_comparison_differences_total',
    'Number of differences found in comparison',
    ['comparison_type', 'table_name', 'database_type'],
    buckets=(0, 1, 10, 100, 1000, 10000, 100000, 1000000)
)

difference_rate = Gauge(
    'datadiff_difference_rate',
    'Rate of differences found (0-1)',
    ['comparison_type', 'table_name', 'database_type']
)

rows_compared = Histogram(
    'datadiff_rows_compared_total',
    'Total number of rows compared',
    ['comparison_type', 'table_name', 'database_type'],
    buckets=(100, 1000, 10000, 100000, 1000000, 10000000)
)

column_null_rate = Gauge(
    'datadiff_column_null_rate',
    'Null value rate for columns',
    ['table_name', 'column_name', 'database_type']
)

# Resource Metrics
db_connection_active = Gauge(
    'datadiff_db_connections_active',
    'Number of active database connections',
    ['database_type', 'connection_pool']
)

db_connection_idle = Gauge(
    'datadiff_db_connections_idle',
    'Number of idle database connections',
    ['database_type', 'connection_pool']
)

memory_usage_bytes = Gauge(
    'datadiff_memory_usage_bytes',
    'Current memory usage in bytes'
)

cpu_usage_percent = Gauge(
    'datadiff_cpu_usage_percent',
    'Current CPU usage percentage'
)

# Memory optimization metrics
comparison_memory_peak = Gauge(
    'datadiff_comparison_memory_peak_mb',
    'Peak memory usage during comparison in MB',
    ['comparison_id', 'comparison_type']
)

comparison_memory_increase = Gauge(
    'datadiff_comparison_memory_increase_mb',
    'Memory increase during comparison in MB',
    ['comparison_id', 'comparison_type']
)

# Stream processing metrics
streaming_differences_processed = Counter(
    'datadiff_streaming_differences_processed_total',
    'Total number of differences processed in streaming mode',
    ['comparison_id']
)

streaming_memory_saved = Gauge(
    'datadiff_streaming_memory_saved_mb',
    'Estimated memory saved by using streaming processing',
    ['comparison_id']
)

# Data Quality Metrics
data_quality_score = Gauge(
    'datadiff_data_quality_score',
    'Data quality score (0-100)',
    ['table_name', 'check_type']
)

schema_differences = Gauge(
    'datadiff_schema_differences',
    'Number of schema differences found by type',
    ['difference_type']
)

# Usage Metrics
active_users = Gauge(
    'datadiff_active_users_total',
    'Number of active users',
    ['time_period']  # daily, weekly, monthly
)

feature_usage = Counter(
    'datadiff_feature_usage_total',
    'Feature usage counter',
    ['feature_name', 'parameter_name']
)

# System Info
system_info = Info(
    'datadiff_system',
    'System information'
)

# Helper functions
def update_memory_metrics():
    """Update memory usage metrics"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_usage_bytes.set(memory_info.rss)
    except Exception as e:
        logger.error(f"Failed to update memory metrics: {e}")

def update_cpu_metrics():
    """Update CPU usage metrics"""
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_usage_percent.set(cpu_percent)
    except Exception as e:
        logger.error(f"Failed to update CPU metrics: {e}")

def record_comparison_metrics(comparison_result: dict, comparison_config: dict):
    """Record metrics from a comparison result"""
    try:
        comparison_type = comparison_config.get('comparison_type', 'table')
        source_type = comparison_config.get('source_type', 'unknown')
        target_type = comparison_config.get('target_type', 'unknown')
        
        # Extract the actual result data - handle nested structures
        result_data = comparison_result
        if isinstance(comparison_result, dict):
            # Check for nested result structure
            if 'result' in comparison_result and isinstance(comparison_result['result'], dict):
                result_data = comparison_result['result']
            # Also check for summary data
            summary_data = result_data.get('summary', {})
            statistics_data = result_data.get('statistics', {})
        else:
            summary_data = {}
            statistics_data = {}
        
        # Duration - check multiple possible locations
        execution_time = None
        if 'execution_time' in comparison_config:
            execution_time = comparison_config['execution_time']
        elif 'execution_time' in summary_data:
            execution_time = summary_data['execution_time']
        elif 'execution_time' in result_data:
            execution_time = result_data['execution_time']
        
        if execution_time is not None:
            comparison_duration.labels(
                comparison_type=comparison_type,
                algorithm=comparison_config.get('algorithm', 'AUTO'),
                status=comparison_result.get('status', 'completed'),
                source_type=source_type,
                target_type=target_type
            ).observe(execution_time)
        
        # Differences - check multiple possible locations
        total_differences = None
        if 'total_differences' in comparison_config:
            total_differences = comparison_config['total_differences']
        elif 'rows_different' in summary_data:
            total_differences = summary_data['rows_different']
        elif 'total_differences' in summary_data:
            total_differences = summary_data['total_differences']
        elif 'differences' in statistics_data and isinstance(statistics_data['differences'], dict):
            total_differences = statistics_data['differences'].get('total_differences', 0)
        
        if total_differences is not None:
            table_name = comparison_config.get('source_table', 'unknown')
            comparison_differences.labels(
                comparison_type=comparison_type,
                table_name=table_name,
                database_type=source_type
            ).observe(total_differences)
            
            # Difference rate - check multiple possible locations for total rows
            total_rows = None
            if 'total_rows' in comparison_config:
                total_rows = comparison_config['total_rows']
            elif 'total_rows' in summary_data:
                total_rows = summary_data['total_rows']
            elif 'rows_compared' in statistics_data:
                total_rows = statistics_data['rows_compared']
            
            if total_rows and total_rows > 0:
                rate = total_differences / total_rows
                difference_rate.labels(
                    comparison_type=comparison_type,
                    table_name=table_name,
                    database_type=source_type
                ).set(rate)
        
        # Rows compared
        total_rows = None
        if 'total_rows' in comparison_config:
            total_rows = comparison_config['total_rows']
        elif 'total_rows' in summary_data:
            total_rows = summary_data['total_rows']
        elif 'rows_compared' in statistics_data:
            total_rows = statistics_data['rows_compared']
            
        if total_rows is not None:
            rows_compared.labels(
                comparison_type=comparison_type,
                table_name=comparison_config.get('source_table', 'unknown'),
                database_type=source_type
            ).observe(total_rows)
        
        # Log what we recorded for debugging
        logger.info(f"Recorded metrics - type: {comparison_type}, differences: {total_differences}, rows: {total_rows}, time: {execution_time}")
            
    except Exception as e:
        logger.error(f"Failed to record comparison metrics: {e}", exc_info=True)

def record_column_statistics(column_stats: dict, table_name: str, database_type: str):
    """Record column-level statistics"""
    try:
        if not column_stats:
            return
            
        total_quality_score = 0
        column_count = 0
        
        for column_name, stats in column_stats.items():
            # Handle both direct stats and nested stats structures
            if isinstance(stats, dict):
                # Check for null_rate in various formats
                null_rate = None
                if 'null_rate' in stats:
                    # null_rate might already be a percentage (0-100) or a ratio (0-1)
                    null_rate_value = stats['null_rate']
                    if null_rate_value > 1:
                        # Already a percentage
                        null_rate = null_rate_value / 100.0  # Convert to 0-1 range for Prometheus
                    else:
                        null_rate = null_rate_value
                elif 'null_ratio' in stats:
                    null_rate = stats['null_ratio']
                elif 'null_count' in stats and 'total_count' in stats and stats['total_count'] > 0:
                    null_rate = stats['null_count'] / stats['total_count']
                
                if null_rate is not None:
                    column_null_rate.labels(
                        table_name=table_name,
                        column_name=column_name,
                        database_type=database_type
                    ).set(null_rate)
                    
                    # Calculate data quality score based on null rate
                    # Score = (1 - null_rate) * 100
                    quality_score = (1 - null_rate) * 100
                    data_quality_score.labels(
                        table_name=table_name,
                        check_type=f"column_{column_name}"
                    ).set(quality_score)
                    
                    total_quality_score += quality_score
                    column_count += 1
                elif 'quality_score' in stats:
                    # Use provided quality score
                    data_quality_score.labels(
                        table_name=table_name,
                        check_type=f"column_{column_name}"
                    ).set(stats['quality_score'])
                    
                    total_quality_score += stats['quality_score']
                    column_count += 1
        
        # Record table-level average quality score
        if column_count > 0:
            avg_quality_score = total_quality_score / column_count
            data_quality_score.labels(
                table_name=table_name,
                check_type="table_average"
            ).set(avg_quality_score)
                    
        logger.info(f"Recorded column statistics for {len(column_stats)} columns in table {table_name}")
    except Exception as e:
        logger.error(f"Failed to record column statistics: {e}", exc_info=True)

def record_feature_usage(feature_name: str, parameters: dict = None):
    """Record feature usage"""
    try:
        if parameters:
            for param_name, param_value in parameters.items():
                if param_value is not None and param_value != '':
                    feature_usage.labels(
                        feature_name=feature_name,
                        parameter_name=f"{param_name}={param_value}"
                    ).inc()
        else:
            feature_usage.labels(
                feature_name=feature_name,
                parameter_name="default"
            ).inc()
    except Exception as e:
        logger.error(f"Failed to record feature usage: {e}")

# Initialize system info
import sys
system_info.info({
    'version': '1.0.0',
    'python_version': f"{sys.version_info.major}.{sys.version_info.minor}"
})


# 定义总计数指标
table_comparisons_total_gauge = Gauge(
    'datadiff_table_comparisons_total',
    'Total number of table comparisons from database'
)
schema_comparisons_total_gauge = Gauge(
    'datadiff_schema_comparisons_total', 
    'Total number of schema comparisons from database'
)
table_comparisons_completed_gauge = Gauge(
    'datadiff_table_comparisons_completed',
    'Number of completed table comparisons'
)
schema_comparisons_completed_gauge = Gauge(
    'datadiff_schema_comparisons_completed',
    'Number of completed schema comparisons'
)
table_comparisons_failed_gauge = Gauge(
    'datadiff_table_comparisons_failed',
    'Number of failed table comparisons'
)
schema_comparisons_failed_gauge = Gauge(
    'datadiff_schema_comparisons_failed',
    'Number of failed schema comparisons'
)

# 添加表级别的聚合指标
table_total_rows_gauge = Gauge(
    'datadiff_table_total_rows',
    'Total rows compared for each table',
    ['table_name']
)
table_total_differences_gauge = Gauge(
    'datadiff_table_total_differences',
    'Total differences found for each table',
    ['table_name']
)
table_avg_match_rate_gauge = Gauge(
    'datadiff_table_avg_match_rate',
    'Average match rate for each table',
    ['table_name']
)
table_avg_execution_time_gauge = Gauge(
    'datadiff_table_avg_execution_time',
    'Average execution time in seconds for each table',
    ['table_name']
)

# 添加汇总指标
all_tables_total_differences_gauge = Gauge(
    'datadiff_all_tables_total_differences',
    'Total differences found across all tables'
)
all_schemas_total_differences_gauge = Gauge(
    'datadiff_all_schemas_total_differences',
    'Total schema differences found'
)

# 添加平均执行时间指标
table_comparison_avg_time_gauge = Gauge(
    'datadiff_table_comparison_avg_time',
    'Average API execution time for table comparisons in seconds (excludes n8n overhead)'
)
schema_comparison_avg_time_gauge = Gauge(
    'datadiff_schema_comparison_avg_time',
    'Average API execution time for schema comparisons in seconds (excludes n8n overhead)'
)

# 添加总执行时间指标（包含n8n开销）
table_comparison_total_avg_time_gauge = Gauge(
    'datadiff_table_comparison_total_avg_time',
    'Total execution time for table comparisons including n8n workflow overhead (when available)'
)
schema_comparison_total_avg_time_gauge = Gauge(
    'datadiff_schema_comparison_total_avg_time',
    'Total execution time for schema comparisons including n8n workflow overhead (when available)'
)

# Schema级别的详细指标
schema_execution_time_gauge = Gauge(
    'datadiff_schema_execution_time',
    'Execution time for each schema comparison',
    ['source_schema', 'target_schema']
)

async def update_business_metrics_from_db():
    """从数据库读取历史统计信息并更新业务指标"""
    try:
        from prometheus_client import generate_latest
    except ImportError:
        return
        
    try:
        import psycopg2
        import psycopg2.extras
        import os
        
        logger.info("开始从数据库更新业务指标")
        
        # 使用环境变量中的数据库连接或默认值
        # 检查是否在 Docker 容器中运行
        in_docker = os.path.exists('/.dockerenv')
        default_host = 'postgres' if in_docker else 'localhost'
        
        db_host = os.environ.get('DB_HOST', default_host)
        db_port = int(os.environ.get('DB_PORT', '5432'))
        db_user = os.environ.get('DB_USER', 'postgres')
        db_password = os.environ.get('DB_PASSWORD', 'password')
        db_name = os.environ.get('DB_NAME', 'datadiff')
        
        try:
            # 直接使用 psycopg2 连接
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                dbname=db_name
            )
            conn.autocommit = True
            
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # 首先查询总的比对统计
                stats_query = """
                SELECT 
                    COUNT(*) FILTER (WHERE summary_type = 'table') as table_total,
                    COUNT(*) FILTER (WHERE summary_type = 'table' AND status = 'completed') as table_completed,
                    COUNT(*) FILTER (WHERE summary_type = 'table' AND status = 'failed') as table_failed,
                    COUNT(*) FILTER (WHERE summary_type = 'schema') as schema_total,
                    COUNT(*) FILTER (WHERE summary_type = 'schema' AND status = 'completed') as schema_completed,
                    COUNT(*) FILTER (WHERE summary_type = 'schema' AND status = 'failed') as schema_failed
                FROM (
                    SELECT 'table' as summary_type, status FROM data_diff_results.comparison_summary
                    UNION ALL
                    SELECT 'schema' as summary_type, status FROM data_diff_results.schema_comparison_summary
                ) combined_summaries
                """
                
                cursor.execute(stats_query)
                stats = cursor.fetchone()
                
                if stats:
                    table_comparisons_total_gauge.set(stats['table_total'] or 0)
                    table_comparisons_completed_gauge.set(stats['table_completed'] or 0)
                    table_comparisons_failed_gauge.set(stats['table_failed'] or 0)
                    schema_comparisons_total_gauge.set(stats['schema_total'] or 0)
                    schema_comparisons_completed_gauge.set(stats['schema_completed'] or 0)
                    schema_comparisons_failed_gauge.set(stats['schema_failed'] or 0)
                    
                    logger.info(f"更新比对统计: 表比对 {stats['table_total']} 个, Schema比对 {stats['schema_total']} 个")
                
                # 查询表级别的聚合统计
                table_stats_query = """
                SELECT 
                    source_table,
                    COUNT(*) as comparison_count,
                    SUM(rows_compared) as total_rows,
                    SUM(rows_different) as total_differences,
                    AVG(match_rate) as avg_match_rate,
                    AVG(execution_time_seconds) as avg_execution_time
                FROM data_diff_results.comparison_summary
                WHERE status = 'completed'
                GROUP BY source_table
                """
                
                cursor.execute(table_stats_query)
                table_stats = cursor.fetchall()
                
                for stat in table_stats:
                    table_name = stat['source_table']
                    table_total_rows_gauge.labels(table_name=table_name).set(stat['total_rows'] or 0)
                    table_total_differences_gauge.labels(table_name=table_name).set(stat['total_differences'] or 0)
                    table_avg_match_rate_gauge.labels(table_name=table_name).set(float(stat['avg_match_rate'] or 0.0))
                    table_avg_execution_time_gauge.labels(table_name=table_name).set(float(stat['avg_execution_time'] or 0.0))
                    
                logger.info(f"更新表级别统计: {len(table_stats)} 个表")
                
                # 计算所有表的总差异数
                total_table_differences = sum(stat['total_differences'] or 0 for stat in table_stats)
                all_tables_total_differences_gauge.set(total_table_differences)
                
                # 计算表比对的平均执行时间（最近24小时）
                table_avg_time_query = """
                SELECT 
                    AVG(execution_time_seconds) as avg_api_time,
                    AVG(workflow_execution_seconds) as avg_workflow_time,
                    COUNT(*) as count
                FROM data_diff_results.comparison_summary
                WHERE status = 'completed'
                AND created_at > NOW() - INTERVAL '24 hours'
                """
                cursor.execute(table_avg_time_query)
                table_avg_time = cursor.fetchone()
                if table_avg_time and table_avg_time['count'] > 0:
                    # API执行时间
                    if table_avg_time['avg_api_time']:
                        table_comparison_avg_time_gauge.set(float(table_avg_time['avg_api_time']))
                    
                    # 工作流总执行时间（如果有记录）
                    if table_avg_time['avg_workflow_time']:
                        table_comparison_total_avg_time_gauge.set(float(table_avg_time['avg_workflow_time']))
                
                # 查询Schema差异总数（包括表差异和列差异）
                schema_diff_total_query = """
                SELECT 
                    (SELECT COUNT(*) FROM data_diff_results.schema_table_differences) +
                    (SELECT COUNT(*) FROM data_diff_results.schema_column_differences) as total_differences
                """
                cursor.execute(schema_diff_total_query)
                schema_diff_total = cursor.fetchone()
                if schema_diff_total:
                    all_schemas_total_differences_gauge.set(schema_diff_total['total_differences'] or 0)
                
                # 查询Schema比对的平均执行时间和详情
                schema_time_query = """
                SELECT 
                    source_schema,
                    target_schema,
                    AVG(execution_time_seconds) as avg_time,
                    AVG(workflow_execution_seconds) as avg_workflow_time,
                    COUNT(*) as comparison_count
                FROM data_diff_results.schema_comparison_summary
                WHERE status = 'completed'
                GROUP BY source_schema, target_schema
                """
                cursor.execute(schema_time_query)
                schema_times = cursor.fetchall()
                
                total_schema_time = 0
                total_schema_count = 0
                
                for schema_stat in schema_times:
                    # 更新每个schema对的执行时间
                    schema_execution_time_gauge.labels(
                        source_schema=schema_stat['source_schema'],
                        target_schema=schema_stat['target_schema']
                    ).set(float(schema_stat['avg_time'] or 0.0))
                    
                    total_schema_time += float(schema_stat['avg_time'] or 0.0) * (schema_stat['comparison_count'] or 0)
                    total_schema_count += schema_stat['comparison_count'] or 0
                
                # 计算总的Schema比对平均时间（已移到下面的查询中）
                if total_schema_count > 0:
                    api_time = total_schema_time / total_schema_count
                
                # 查询Schema的工作流执行时间（最近24小时）
                schema_workflow_time_query = """
                SELECT 
                    AVG(execution_time_seconds) as avg_api_time,
                    AVG(workflow_execution_seconds) as avg_workflow_time,
                    COUNT(*) as count
                FROM data_diff_results.schema_comparison_summary
                WHERE status = 'completed' 
                AND created_at > NOW() - INTERVAL '24 hours'
                """
                cursor.execute(schema_workflow_time_query)
                schema_workflow_time = cursor.fetchone()
                if schema_workflow_time and schema_workflow_time['count'] > 0:
                    # 设置API平均时间
                    if schema_workflow_time['avg_api_time']:
                        schema_comparison_avg_time_gauge.set(float(schema_workflow_time['avg_api_time']))
                    
                    # 设置工作流平均时间
                    if schema_workflow_time['avg_workflow_time']:
                        schema_comparison_total_avg_time_gauge.set(float(schema_workflow_time['avg_workflow_time']))
                # 查询比对汇总数据
                summary_query = """
                SELECT 
                    comparison_id,
                    source_table,
                    target_table,
                    rows_compared,
                    rows_matched,
                    rows_different,
                    match_rate,
                    execution_time_seconds,
                    created_at
                FROM data_diff_results.comparison_summary
                WHERE created_at > NOW() - INTERVAL '24 hours'
                AND status = 'completed'
                """
            
                cursor.execute(summary_query)
                summaries = cursor.fetchall()
                
                for summary in summaries:
                    table_name = summary['source_table']
                    rows_diff = summary['rows_different'] or 0
                    rows_total = summary['rows_compared'] or 1
                    match_rate = float(summary['match_rate'] or 0.0)
                    exec_time = float(summary['execution_time_seconds'] or 0.0)
                    
                    # 更新差异数量指标
                    comparison_differences.labels(
                        comparison_type='table',
                        table_name=table_name,
                        database_type='postgresql'
                    ).observe(rows_diff)
                    
                    # 更新比对行数指标
                    rows_compared.labels(
                        comparison_type='table',
                        table_name=table_name,
                        database_type='postgresql'
                    ).observe(rows_total)
                    
                    # 更新差异率指标 (1 - match_rate)
                    if rows_total > 0:
                        diff_rate = 1.0 - match_rate
                        difference_rate.labels(
                            comparison_type='table',
                            table_name=table_name,
                            database_type='postgresql'
                        ).set(diff_rate)
                    
                    # 更新比对执行时间
                    comparison_duration.labels(
                        comparison_type='table',
                        algorithm='default',
                        status='completed',
                        source_type='postgresql',
                        target_type='postgresql'
                    ).observe(exec_time)
            
                # 查询列统计数据
                column_stats_query = """
                SELECT DISTINCT
                    cs.column_name,
                    cs.null_count,
                    cs.null_rate,
                    cs.total_count,
                    s.source_table as table_name
                FROM data_diff_results.column_statistics cs
                JOIN data_diff_results.comparison_summary s 
                    ON cs.comparison_id = s.comparison_id
                WHERE cs.created_at > NOW() - INTERVAL '24 hours'
                AND cs.table_side = 'source'
                """
            
                cursor.execute(column_stats_query)
                column_stats = cursor.fetchall()
                
                for stat in column_stats:
                    if stat['total_count'] and stat['total_count'] > 0:
                        # 使用已有的 null_rate 或计算
                        null_rate = stat['null_rate'] if stat['null_rate'] is not None else (stat['null_count'] or 0) / stat['total_count']
                        
                        column_null_rate.labels(
                            table_name=stat['table_name'],
                            column_name=stat['column_name'],
                            database_type='postgresql'
                        ).set(null_rate)
                        
                        # 计算数据质量分数 (简单示例: 1 - null_rate)
                        quality_score = (1 - null_rate) * 100
                        data_quality_score.labels(
                            table_name=stat['table_name'],
                            check_type='null_check'
                        ).set(quality_score)
            
                # 查询 Schema 差异数据
                schema_diff_query = """
                SELECT 
                    difference_type,
                    COUNT(*) as count
                FROM (
                    SELECT difference_type FROM data_diff_results.schema_table_differences
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                    UNION ALL
                    SELECT difference_type FROM data_diff_results.schema_column_differences
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                ) combined_diffs
                GROUP BY difference_type
                """
                
                cursor.execute(schema_diff_query)
                schema_diffs = cursor.fetchall()
                
                for diff in schema_diffs:
                    schema_differences.labels(
                        difference_type=diff['difference_type']
                    ).set(diff['count'])
                        
                logger.info(f"业务指标已从数据库更新: {len(summaries)} 个比对, {len(column_stats)} 个列统计")
                
                # 如果没有列统计数据，基于比对匹配率计算数据质量分数
                if len(column_stats) == 0 and len(summaries) > 0:
                    logger.info("没有找到列统计数据，将基于比对匹配率计算数据质量分数")
                    
                    # 计算每个表的平均数据质量分数（基于匹配率）
                    table_quality_scores = {}
                    for summary in summaries:
                        table_name = summary['source_table']
                        match_rate = float(summary['match_rate'] or 0.0)
                        
                        # 如果 match_rate > 1，说明已经是百分比形式
                        if match_rate > 1:
                            quality_score = match_rate  # 已经是 0-100 的范围
                        else:
                            quality_score = match_rate * 100  # 转换为 0-100 的范围
                        
                        if table_name not in table_quality_scores:
                            table_quality_scores[table_name] = []
                        table_quality_scores[table_name].append(quality_score)
                    
                    # 为每个表设置平均数据质量分数
                    for table_name, scores in table_quality_scores.items():
                        avg_score = sum(scores) / len(scores)
                        data_quality_score.labels(
                            table_name=table_name,
                            check_type='match_rate_based'
                        ).set(avg_score)
                        logger.info(f"设置表 {table_name} 的数据质量分数: {avg_score:.2f}")
                    
                    # 计算所有表的总体平均数据质量分数
                    all_scores = []
                    for scores in table_quality_scores.values():
                        all_scores.extend(scores)
                    
                    if all_scores:
                        overall_avg_score = sum(all_scores) / len(all_scores)
                        data_quality_score.labels(
                            table_name='_all_tables',
                            check_type='overall_average'
                        ).set(overall_avg_score)
                        logger.info(f"设置总体数据质量分数: {overall_avg_score:.2f}")
            
            conn.close()
        except Exception as e:
            logger.error(f"连接数据库失败: {e}")
            raise
                
    except Exception as e:
        logger.error(f"更新业务指标失败: {e}")
        import traceback
        logger.error(traceback.format_exc())

# Initialize memory and CPU metrics at startup
update_memory_metrics()
update_cpu_metrics()