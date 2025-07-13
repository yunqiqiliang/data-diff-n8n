"""
结果物化器
负责将比对结果存储到数据库表中
"""

import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid

logger = logging.getLogger(__name__)


class ResultMaterializer:
    """将比对结果物化到数据库表"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化结果物化器
        
        Args:
            db_config: 数据库连接配置
        """
        self.db_config = db_config
        self.logger = logger
        self.schema_name = "data_diff_results"
        
    def _get_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(
            host=self.db_config.get('host', 'postgres'),
            port=self.db_config.get('port', 5432),
            database=self.db_config.get('database', 'datadiff'),
            user=self.db_config.get('user', 'postgres'),
            password=self.db_config.get('password', 'password')
        )
        
    def create_comparison_task(self, comparison_id: str, config: Dict[str, Any]) -> bool:
        """创建比对任务记录（在 comparison_summary 表中）"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.schema_name}.comparison_summary (
                        comparison_id, source_connection, target_connection,
                        source_table, target_table, key_columns, algorithm,
                        start_time, status, progress, current_step,
                        sampling_config, column_remapping, where_conditions
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    comparison_id,
                    json.dumps(config.get('source_connection', {})),
                    json.dumps(config.get('target_connection', {})),
                    config.get('source_table', ''),
                    config.get('target_table', ''),
                    config.get('key_columns', []),
                    config.get('algorithm', 'AUTO'),
                    datetime.now(),
                    'pending',
                    0,
                    'Task created',
                    json.dumps(config.get('sampling', {})) if config.get('sampling') else None,
                    json.dumps(config.get('column_remapping', {})) if config.get('column_remapping') else None,
                    json.dumps(config.get('where_conditions', {})) if config.get('where_conditions') else None
                ))
                conn.commit()
                self.logger.info(f"Created task record for comparison {comparison_id}")
                return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to create task record: {e}")
            return False
        finally:
            conn.close()
    
    def update_task_status(
        self, 
        comparison_id: str, 
        status: str, 
        progress: Optional[int] = None,
        current_step: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """更新任务状态"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # 构建更新语句
                updates = ["status = %s", "updated_at = NOW()"]
                params = [status]
                
                if progress is not None:
                    updates.append("progress = %s")
                    params.append(progress)
                    
                if current_step is not None:
                    updates.append("current_step = %s")
                    params.append(current_step)
                    
                if error_message is not None:
                    updates.append("error_message = %s")
                    params.append(error_message)
                
                # 处理特殊状态
                if status == 'completed':
                    updates.append("end_time = COALESCE(end_time, NOW())")
                    # execution_time_seconds 将在 materialize_results 中更新
                
                params.append(comparison_id)
                
                cursor.execute(f"""
                    UPDATE {self.schema_name}.comparison_summary 
                    SET {', '.join(updates)}
                    WHERE comparison_id = %s
                """, params)
                
                conn.commit()
                self.logger.info(f"Updated task {comparison_id} status to {status}")
                return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update task status: {e}")
            return False
        finally:
            conn.close()
    
    def ensure_schema_exists(self):
        """确保结果存储的 schema 存在"""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # 创建 schema
                cursor.execute(f"""
                    CREATE SCHEMA IF NOT EXISTS {self.schema_name}
                """)
                
                # 创建比对结果汇总表
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.comparison_summary (
                        id SERIAL PRIMARY KEY,
                        comparison_id UUID UNIQUE NOT NULL,
                        source_connection JSONB NOT NULL,
                        target_connection JSONB NOT NULL,
                        source_table TEXT NOT NULL,
                        target_table TEXT NOT NULL,
                        key_columns TEXT[] NOT NULL,
                        algorithm VARCHAR(50) NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP NOT NULL,
                        execution_time_seconds DECIMAL(10, 3) NOT NULL,
                        rows_compared INTEGER,
                        rows_matched INTEGER,
                        rows_different INTEGER,
                        match_rate DECIMAL(5, 2),
                        sampling_config JSONB,
                        column_remapping JSONB,
                        where_conditions JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建差异详情表
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.difference_details (
                        id SERIAL PRIMARY KEY,
                        comparison_id UUID NOT NULL,
                        row_key JSONB NOT NULL,
                        difference_type VARCHAR(50) NOT NULL,
                        severity VARCHAR(20) NOT NULL,
                        column_name VARCHAR(255),
                        source_value TEXT,
                        target_value TEXT,
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (comparison_id) REFERENCES {self.schema_name}.comparison_summary(comparison_id)
                    )
                """)
                
                # 创建列统计表
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.column_statistics (
                        id SERIAL PRIMARY KEY,
                        comparison_id UUID NOT NULL,
                        table_side VARCHAR(10) NOT NULL CHECK (table_side IN ('source', 'target')),
                        column_name VARCHAR(255) NOT NULL,
                        data_type VARCHAR(100),
                        null_count INTEGER,
                        null_rate DECIMAL(5, 2),
                        total_count INTEGER,
                        unique_count INTEGER,
                        cardinality DECIMAL(10, 6),
                        min_value TEXT,
                        max_value TEXT,
                        avg_value DECIMAL(20, 6),
                        avg_length DECIMAL(10, 2),
                        value_distribution JSONB,
                        percentiles JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (comparison_id) REFERENCES {self.schema_name}.comparison_summary(comparison_id)
                    )
                """)
                
                # 创建时间线分析表
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.timeline_analysis (
                        id SERIAL PRIMARY KEY,
                        comparison_id UUID NOT NULL,
                        time_column VARCHAR(255) NOT NULL,
                        period_type VARCHAR(50) NOT NULL,
                        period_start TIMESTAMP NOT NULL,
                        period_end TIMESTAMP NOT NULL,
                        source_count INTEGER,
                        target_count INTEGER,
                        matched_count INTEGER,
                        difference_count INTEGER,
                        match_rate DECIMAL(5, 2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (comparison_id) REFERENCES {self.schema_name}.comparison_summary(comparison_id)
                    )
                """)
                
                # 创建性能指标表
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.performance_metrics (
                        id SERIAL PRIMARY KEY,
                        comparison_id UUID NOT NULL,
                        metric_name VARCHAR(100) NOT NULL,
                        metric_value DECIMAL(20, 6) NOT NULL,
                        metric_unit VARCHAR(50),
                        metric_context JSONB,
                        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (comparison_id) REFERENCES {self.schema_name}.comparison_summary(comparison_id)
                    )
                """)
                
                # 创建索引
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_summary_comparison_id 
                    ON {self.schema_name}.comparison_summary(comparison_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_summary_start_time 
                    ON {self.schema_name}.comparison_summary(start_time);
                    
                    CREATE INDEX IF NOT EXISTS idx_summary_tables 
                    ON {self.schema_name}.comparison_summary(source_table, target_table);
                    
                    CREATE INDEX IF NOT EXISTS idx_details_comparison_id 
                    ON {self.schema_name}.difference_details(comparison_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_details_type_severity 
                    ON {self.schema_name}.difference_details(difference_type, severity);
                    
                    CREATE INDEX IF NOT EXISTS idx_statistics_comparison_id 
                    ON {self.schema_name}.column_statistics(comparison_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_timeline_comparison_id 
                    ON {self.schema_name}.timeline_analysis(comparison_id);
                    
                    CREATE INDEX IF NOT EXISTS idx_metrics_comparison_id 
                    ON {self.schema_name}.performance_metrics(comparison_id);
                """)
                
                conn.commit()
                self.logger.info(f"Schema {self.schema_name} and tables created successfully")
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to create schema and tables: {e}")
            raise
        finally:
            conn.close()
            
    def materialize_results(self, comparison_id: str, results: Dict[str, Any]) -> bool:
        """
        将比对结果物化到数据库
        
        Args:
            comparison_id: 比对ID
            results: 比对结果字典
            
        Returns:
            是否成功
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 更新比对汇总（记录已在任务创建时插入）
                self._insert_comparison_summary(cursor, comparison_id, results)
                
                # 2. 插入差异详情（如果有）
                if results.get('differences'):
                    self._insert_difference_details(cursor, comparison_id, results['differences'])
                elif results.get('sample_differences'):
                    self._insert_difference_details(cursor, comparison_id, results['sample_differences'])
                
                # 3. 插入列统计（如果有）
                # 支持两种字段名：column_stats 或 column_statistics
                column_stats = results.get('column_stats') or results.get('column_statistics')
                if column_stats:
                    self._insert_column_statistics(cursor, comparison_id, column_stats)
                
                # 4. 插入时间线分析（如果有）
                if results.get('timeline_analysis'):
                    self._insert_timeline_analysis(cursor, comparison_id, results['timeline_analysis'])
                
                # 5. 插入性能指标
                if results.get('performance_metrics'):
                    self._insert_performance_metrics(cursor, comparison_id, results['performance_metrics'])
                
                conn.commit()
                self.logger.info(f"Successfully materialized results for comparison {comparison_id}")
                return True
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to materialize results: {e}")
            return False
        finally:
            conn.close()
            
    def _insert_comparison_summary(self, cursor, comparison_id: str, results: Dict[str, Any]):
        """更新比对汇总信息（任务已经在创建时插入）"""
        summary = results.get('summary', {})
        
        cursor.execute(f"""
            UPDATE {self.schema_name}.comparison_summary 
            SET 
                end_time = %s,
                execution_time_seconds = %s,
                rows_compared = %s,
                rows_matched = %s,
                rows_different = %s,
                match_rate = %s,
                status = 'completed',
                progress = 100,
                current_step = 'Comparison completed',
                updated_at = NOW()
            WHERE comparison_id = %s
        """, (
            results.get('end_time'),
            summary.get('execution_time', 0),
            summary.get('total_rows', 0),
            summary.get('rows_matched', 0),
            summary.get('rows_different', 0),
            summary.get('match_rate', 0),
            comparison_id
        ))
        
    def _insert_difference_details(self, cursor, comparison_id: str, differences: List[Dict[str, Any]]):
        """插入差异详情"""
        for diff in differences[:1000]:  # 限制最多存储1000条差异
            # 处理不同的数据格式
            diff_type = diff.get('type', diff.get('difference_type', 'unknown'))
            
            # 处理 value_different 类型的差异
            if diff_type == 'value_different' and diff.get('differing_columns'):
                # 为每个不同的列创建一条记录
                source_row = diff.get('source_row', {})
                target_row = diff.get('target_row', {})
                
                for col in diff.get('differing_columns', []):
                    cursor.execute(f"""
                        INSERT INTO {self.schema_name}.difference_details (
                            comparison_id, row_key, difference_type, severity,
                            column_name, source_value, target_value, metadata
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        comparison_id,
                        json.dumps(diff.get('key', {})),
                        'value_different',
                        diff.get('severity', 'medium'),  # 默认中等严重程度
                        col,
                        str(source_row.get(col, ''))[:1000],
                        str(target_row.get(col, ''))[:1000],
                        json.dumps({
                            'source_row': source_row,
                            'target_row': target_row,
                            'all_differing_columns': diff.get('differing_columns', [])
                        })
                    ))
            else:
                # 处理其他类型的差异（missing_in_source, missing_in_target）
                cursor.execute(f"""
                    INSERT INTO {self.schema_name}.difference_details (
                        comparison_id, row_key, difference_type, severity,
                        column_name, source_value, target_value, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    comparison_id,
                    json.dumps(diff.get('key', {})),
                    diff_type,
                    diff.get('severity', 'high' if diff_type in ['missing_in_source', 'missing_in_target'] else 'unknown'),
                    diff.get('column'),  # 可能为 None
                    json.dumps(diff.get('source_row', {}))[:1000] if diff.get('source_row') else '',
                    json.dumps(diff.get('target_row', {}))[:1000] if diff.get('target_row') else '',
                    json.dumps(diff.get('metadata', {}))
                ))
            
    def _insert_column_statistics(self, cursor, comparison_id: str, column_stats: Dict[str, Any]):
        """插入列统计信息"""
        # 处理不同的数据格式
        source_stats = column_stats.get('source') or column_stats.get('source_statistics', {})
        target_stats = column_stats.get('target') or column_stats.get('target_statistics', {})
        
        for side, stats in [('source', source_stats), ('target', target_stats)]:
            for col_name, col_stats in stats.items():
                cursor.execute(f"""
                    INSERT INTO {self.schema_name}.column_statistics (
                        comparison_id, table_side, column_name, data_type,
                        null_count, null_rate, total_count, unique_count,
                        cardinality, min_value, max_value, avg_value,
                        avg_length, value_distribution, percentiles
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    comparison_id, side, col_name,
                    col_stats.get('data_type'),
                    col_stats.get('null_count'),
                    col_stats.get('null_rate'),
                    col_stats.get('total_count'),
                    col_stats.get('unique_count'),
                    col_stats.get('cardinality'),
                    str(col_stats.get('min_value'))[:1000] if col_stats.get('min_value') else None,
                    str(col_stats.get('max_value'))[:1000] if col_stats.get('max_value') else None,
                    col_stats.get('avg_value'),
                    col_stats.get('avg_length'),
                    json.dumps(col_stats.get('value_distribution', {})),
                    json.dumps(col_stats.get('percentiles', {}))
                ))
                
    def _insert_timeline_analysis(self, cursor, comparison_id: str, timeline_data: Dict[str, Any]):
        """插入时间线分析数据"""
        self.logger.info(f"Timeline data keys: {list(timeline_data.keys())}")
        self.logger.info(f"Timeline summary: {timeline_data.get('summary')}")
        
        # 处理不同的数据格式
        periods = timeline_data.get('periods') or timeline_data.get('timeline', [])
        
        # 获取时间列名
        time_column = timeline_data.get('time_column')
        if not time_column and timeline_data.get('summary'):
            time_column = timeline_data['summary'].get('time_column')
        
        self.logger.info(f"Processing {len(periods)} timeline periods")
        
        for i, period in enumerate(periods):
            if i < 3:  # 只打印前3个
                self.logger.info(f"Period {i}: {period}")
            cursor.execute(f"""
                INSERT INTO {self.schema_name}.timeline_analysis (
                    comparison_id, time_column, period_type,
                    period_start, period_end, source_count,
                    target_count, matched_count, difference_count, match_rate
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                comparison_id,
                time_column,
                period.get('period_type') or timeline_data.get('period_type', 'day'),
                period.get('period_start') or period.get('window_start'),
                period.get('period_end') or period.get('window_end'),
                period.get('source_count') or period.get('total_rows', 0),
                period.get('target_count') or period.get('total_rows', 0),
                period.get('matched_count') or (period.get('total_rows', 0) - period.get('differences', 0)),
                period.get('difference_count') or period.get('differences', 0),
                period.get('match_rate', 100.0)
            ))
            
    def _insert_performance_metrics(self, cursor, comparison_id: str, metrics: Dict[str, Any]):
        """插入性能指标"""
        for metric_name, metric_value in metrics.items():
            if isinstance(metric_value, dict):
                cursor.execute(f"""
                    INSERT INTO {self.schema_name}.performance_metrics (
                        comparison_id, metric_name, metric_value,
                        metric_unit, metric_context
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    comparison_id,
                    metric_name,
                    metric_value.get('value', 0),
                    metric_value.get('unit'),
                    json.dumps(metric_value.get('context', {}))
                ))
            else:
                cursor.execute(f"""
                    INSERT INTO {self.schema_name}.performance_metrics (
                        comparison_id, metric_name, metric_value
                    ) VALUES (%s, %s, %s)
                """, (
                    comparison_id,
                    metric_name,
                    metric_value
                ))
                
    def get_comparison_history(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        获取比对历史记录
        
        Args:
            limit: 返回记录数限制
            offset: 偏移量
            filters: 过滤条件
            
        Returns:
            比对历史列表
        """
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"""
                    SELECT 
                        comparison_id,
                        source_table,
                        target_table,
                        algorithm,
                        start_time,
                        end_time,
                        execution_time_seconds,
                        rows_compared,
                        match_rate,
                        created_at
                    FROM {self.schema_name}.comparison_summary
                """
                
                where_clauses = []
                params = []
                
                if filters:
                    if filters.get('source_table'):
                        where_clauses.append("source_table = %s")
                        params.append(filters['source_table'])
                    if filters.get('target_table'):
                        where_clauses.append("target_table = %s")
                        params.append(filters['target_table'])
                    if filters.get('start_date'):
                        where_clauses.append("start_time >= %s")
                        params.append(filters['start_date'])
                    if filters.get('end_date'):
                        where_clauses.append("start_time <= %s")
                        params.append(filters['end_date'])
                        
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                    
                query += " ORDER BY start_time DESC LIMIT %s OFFSET %s"
                params.extend([limit, offset])
                
                cursor.execute(query, params)
                return cursor.fetchall()
                
        finally:
            conn.close()
            
    def get_comparison_details(self, comparison_id: str) -> Dict[str, Any]:
        """
        获取比对详细信息
        
        Args:
            comparison_id: 比对ID
            
        Returns:
            比对详细信息
        """
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # 获取汇总信息
                cursor.execute(f"""
                    SELECT * FROM {self.schema_name}.comparison_summary
                    WHERE comparison_id = %s
                """, (comparison_id,))
                summary = cursor.fetchone()
                
                if not summary:
                    return None
                    
                # 获取差异详情（限制数量）
                cursor.execute(f"""
                    SELECT * FROM {self.schema_name}.difference_details
                    WHERE comparison_id = %s
                    LIMIT 100
                """, (comparison_id,))
                differences = cursor.fetchall()
                
                # 获取列统计
                cursor.execute(f"""
                    SELECT * FROM {self.schema_name}.column_statistics
                    WHERE comparison_id = %s
                """, (comparison_id,))
                column_stats = cursor.fetchall()
                
                # 获取性能指标
                cursor.execute(f"""
                    SELECT * FROM {self.schema_name}.performance_metrics
                    WHERE comparison_id = %s
                """, (comparison_id,))
                metrics = cursor.fetchall()
                
                return {
                    'summary': summary,
                    'differences': differences,
                    'column_stats': column_stats,
                    'performance_metrics': metrics
                }
                
        finally:
            conn.close()
            
    async def materialize_schema_comparison(
        self,
        comparison_id: str,
        result: Dict[str, Any],
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        workflow_start_time: Optional[str] = None
    ) -> bool:
        """
        物化 Schema 比对结果到数据库
        
        Args:
            comparison_id: 比对ID
            result: Schema比对结果
            source_config: 源数据库配置
            target_config: 目标数据库配置
            workflow_start_time: 工作流开始时间
            
        Returns:
            是否成功
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # 计算执行时间
                if workflow_start_time:
                    try:
                        start_time = datetime.fromisoformat(workflow_start_time.replace('Z', '+00:00'))
                        execution_time = (datetime.now() - start_time).total_seconds()
                    except:
                        execution_time = None
                else:
                    execution_time = None
                
                # 提取比对结果信息
                summary = result.get('summary', {})
                diff_details = result.get('diff', {})
                
                # 插入主记录到 schema_comparison_summary
                cursor.execute(f"""
                    INSERT INTO {self.schema_name}.schema_comparison_summary (
                        comparison_id,
                        source_connection,
                        target_connection,
                        source_schema,
                        target_schema,
                        table_differences,
                        column_differences,
                        type_differences,
                        total_differences,
                        execution_time_seconds,
                        workflow_execution_seconds,
                        status,
                        start_time,
                        end_time,
                        schemas_identical,
                        table_filters
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    comparison_id,
                    json.dumps(source_config),
                    json.dumps(target_config),
                    source_config.get('schema', 'public'),
                    target_config.get('schema', 'public'),
                    summary.get('table_differences', 0),
                    summary.get('column_differences', 0),
                    summary.get('type_differences', 0),
                    summary.get('total_differences', 0),
                    execution_time,
                    execution_time,  # 暂时使用相同的值
                    'completed',
                    datetime.now() - timedelta(seconds=execution_time) if execution_time else datetime.now(),
                    datetime.now(),
                    summary.get('total_differences', 0) == 0,
                    json.dumps({
                        'source_tables': summary.get('source_tables', 0),
                        'target_tables': summary.get('target_tables', 0)
                    })
                ))
                
                # 插入表差异详情
                table_diffs = diff_details.get('tables', {})
                
                # 只在源表中存在的表
                for table in table_diffs.get('only_in_source', []):
                    cursor.execute(f"""
                        INSERT INTO {self.schema_name}.schema_table_differences (
                            comparison_id,
                            table_name,
                            difference_type,
                            details
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        comparison_id,
                        table,
                        'only_in_source',
                        json.dumps({'source_only': True})
                    ))
                
                # 只在目标表中存在的表
                for table in table_diffs.get('only_in_target', []):
                    cursor.execute(f"""
                        INSERT INTO {self.schema_name}.schema_table_differences (
                            comparison_id,
                            table_name,
                            difference_type,
                            details
                        ) VALUES (%s, %s, %s, %s)
                    """, (
                        comparison_id,
                        table,
                        'only_in_target',
                        json.dumps({'target_only': True})
                    ))
                
                # 插入列差异详情
                for table_name, table_diff in table_diffs.get('tables_with_differences', {}).items():
                    columns_diff = table_diff.get('columns', {})
                    
                    # 只在源表中的列
                    for col in columns_diff.get('only_in_source', []):
                        cursor.execute(f"""
                            INSERT INTO {self.schema_name}.schema_column_differences (
                                comparison_id,
                                table_name,
                                column_name,
                                difference_type,
                                source_type,
                                target_type,
                                details
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            comparison_id,
                            table_name,
                            col,
                            'only_in_source',
                            None,
                            None,
                            json.dumps({'source_only': True})
                        ))
                    
                    # 只在目标表中的列
                    for col in columns_diff.get('only_in_target', []):
                        cursor.execute(f"""
                            INSERT INTO {self.schema_name}.schema_column_differences (
                                comparison_id,
                                table_name,
                                column_name,
                                difference_type,
                                source_type,
                                target_type,
                                details
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            comparison_id,
                            table_name,
                            col,
                            'only_in_target',
                            None,
                            None,
                            json.dumps({'target_only': True})
                        ))
                    
                    # 类型不同的列
                    for col_name, col_diff in columns_diff.get('type_differences', {}).items():
                        cursor.execute(f"""
                            INSERT INTO {self.schema_name}.schema_column_differences (
                                comparison_id,
                                table_name,
                                column_name,
                                difference_type,
                                source_type,
                                target_type,
                                details
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            comparison_id,
                            table_name,
                            col_name,
                            'type_mismatch',
                            col_diff.get('source_type'),
                            col_diff.get('target_type'),
                            json.dumps(col_diff)
                        ))
                
                conn.commit()
                self.logger.info(f"Successfully materialized schema comparison {comparison_id}")
                return True
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to materialize schema comparison: {e}", exc_info=True)
            raise
        finally:
            conn.close()