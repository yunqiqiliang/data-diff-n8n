-- 增量更新脚本：添加 data_diff_results schema
-- 可以在现有数据库上安全执行，支持重复执行

\c datadiff;

-- 开始事务
BEGIN;

-- 创建结果存储的 schema（如果不存在）
CREATE SCHEMA IF NOT EXISTS data_diff_results;

-- 检查并删除已存在的视图（因为视图依赖于表，需要先删除）
DROP VIEW IF EXISTS data_diff_results.recent_comparisons;
DROP VIEW IF EXISTS data_diff_results.active_comparisons;
DROP VIEW IF EXISTS data_diff_results.task_statistics;

-- 比对结果汇总表
CREATE TABLE IF NOT EXISTS data_diff_results.comparison_summary (
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
);

-- 差异详情表
CREATE TABLE IF NOT EXISTS data_diff_results.difference_details (
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
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 列统计表
CREATE TABLE IF NOT EXISTS data_diff_results.column_statistics (
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
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 时间线分析表
CREATE TABLE IF NOT EXISTS data_diff_results.timeline_analysis (
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
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 性能指标表
CREATE TABLE IF NOT EXISTS data_diff_results.performance_metrics (
    id SERIAL PRIMARY KEY,
    comparison_id UUID NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(20, 6) NOT NULL,
    metric_unit VARCHAR(50),
    metric_context JSONB,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 创建索引（使用 IF NOT EXISTS）
CREATE INDEX IF NOT EXISTS idx_summary_comparison_id ON data_diff_results.comparison_summary(comparison_id);
CREATE INDEX IF NOT EXISTS idx_summary_start_time ON data_diff_results.comparison_summary(start_time);
CREATE INDEX IF NOT EXISTS idx_summary_tables ON data_diff_results.comparison_summary(source_table, target_table);
CREATE INDEX IF NOT EXISTS idx_details_comparison_id ON data_diff_results.difference_details(comparison_id);
CREATE INDEX IF NOT EXISTS idx_details_type_severity ON data_diff_results.difference_details(difference_type, severity);
CREATE INDEX IF NOT EXISTS idx_statistics_comparison_id ON data_diff_results.column_statistics(comparison_id);
CREATE INDEX IF NOT EXISTS idx_statistics_column ON data_diff_results.column_statistics(comparison_id, table_side, column_name);
CREATE INDEX IF NOT EXISTS idx_timeline_comparison_id ON data_diff_results.timeline_analysis(comparison_id);
CREATE INDEX IF NOT EXISTS idx_timeline_period ON data_diff_results.timeline_analysis(comparison_id, time_column, period_start);
CREATE INDEX IF NOT EXISTS idx_metrics_comparison_id ON data_diff_results.performance_metrics(comparison_id);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON data_diff_results.performance_metrics(comparison_id, metric_name);

-- 创建视图
CREATE OR REPLACE VIEW data_diff_results.recent_comparisons AS
SELECT 
    cs.comparison_id,
    cs.source_table,
    cs.target_table,
    cs.algorithm,
    cs.start_time,
    cs.execution_time_seconds,
    cs.rows_compared,
    cs.match_rate,
    cs.rows_different,
    CASE 
        WHEN cs.match_rate >= 99.5 THEN 'Excellent'
        WHEN cs.match_rate >= 95.0 THEN 'Good'
        WHEN cs.match_rate >= 90.0 THEN 'Fair'
        ELSE 'Poor'
    END as match_quality,
    cs.created_at
FROM data_diff_results.comparison_summary cs
ORDER BY cs.start_time DESC
LIMIT 100;

-- 授予权限
-- 为 comparison_summary 表添加状态跟踪字段
ALTER TABLE data_diff_results.comparison_summary 
ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'completed',
ADD COLUMN IF NOT EXISTS progress INTEGER DEFAULT 100,
ADD COLUMN IF NOT EXISTS current_step VARCHAR(200),
ADD COLUMN IF NOT EXISTS error_message TEXT,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- 修改表约束，允许部分字段为空（用于运行中的任务）
ALTER TABLE data_diff_results.comparison_summary 
ALTER COLUMN end_time DROP NOT NULL,
ALTER COLUMN execution_time_seconds DROP NOT NULL;

-- 为状态字段创建索引
CREATE INDEX IF NOT EXISTS idx_summary_status ON data_diff_results.comparison_summary(status);
CREATE INDEX IF NOT EXISTS idx_summary_updated_at ON data_diff_results.comparison_summary(updated_at);

GRANT ALL PRIVILEGES ON SCHEMA data_diff_results TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_diff_results TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_diff_results TO postgres;

-- 提交事务
COMMIT;

-- 输出完成信息
DO $$
BEGIN
    RAISE NOTICE 'Successfully created/updated data_diff_results schema and tables';
    RAISE NOTICE 'Tables created: comparison_summary, difference_details, column_statistics, timeline_analysis, performance_metrics';
    RAISE NOTICE 'View created: recent_comparisons';
END $$;