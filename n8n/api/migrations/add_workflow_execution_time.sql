-- 添加工作流执行时间字段到comparison_summary表
ALTER TABLE data_diff_results.comparison_summary
ADD COLUMN IF NOT EXISTS workflow_start_time TIMESTAMP,
ADD COLUMN IF NOT EXISTS workflow_end_time TIMESTAMP,
ADD COLUMN IF NOT EXISTS workflow_execution_seconds NUMERIC(10,3);

-- 添加工作流执行时间字段到schema_comparison_summary表
ALTER TABLE data_diff_results.schema_comparison_summary
ADD COLUMN IF NOT EXISTS workflow_start_time TIMESTAMP,
ADD COLUMN IF NOT EXISTS workflow_end_time TIMESTAMP,
ADD COLUMN IF NOT EXISTS workflow_execution_seconds NUMERIC(10,3);

-- 添加注释
COMMENT ON COLUMN data_diff_results.comparison_summary.workflow_start_time IS 'n8n工作流开始时间';
COMMENT ON COLUMN data_diff_results.comparison_summary.workflow_end_time IS 'n8n工作流结束时间';
COMMENT ON COLUMN data_diff_results.comparison_summary.workflow_execution_seconds IS 'n8n工作流总执行时间(秒)';

COMMENT ON COLUMN data_diff_results.schema_comparison_summary.workflow_start_time IS 'n8n工作流开始时间';
COMMENT ON COLUMN data_diff_results.schema_comparison_summary.workflow_end_time IS 'n8n工作流结束时间';
COMMENT ON COLUMN data_diff_results.schema_comparison_summary.workflow_execution_seconds IS 'n8n工作流总执行时间(秒)';