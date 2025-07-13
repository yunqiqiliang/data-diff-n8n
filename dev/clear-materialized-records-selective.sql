-- =====================================================
-- Selective Clear Materialized Comparison Records
-- =====================================================
-- This script provides options to selectively clear materialized records.
-- You can comment/uncomment sections based on what you want to clear.
-- 
-- Usage:
-- psql -U postgres -d datadiff -f clear-materialized-records-selective.sql
-- =====================================================

\c datadiff

-- Configuration variables (change these as needed)
\set clear_table_comparisons true
\set clear_schema_comparisons true
\set clear_failed_only false
\set days_to_keep 0  -- Set to number of days to keep recent records (0 = clear all)

-- Start transaction
BEGIN;

-- Show current record counts
\echo '=== Current Record Counts ==='
SELECT 
    'Table Comparisons (Total)' as type, 
    COUNT(*) as count 
FROM data_diff_results.comparison_summary
UNION ALL
SELECT 
    'Table Comparisons (Failed)' as type, 
    COUNT(*) as count 
FROM data_diff_results.comparison_summary 
WHERE status = 'failed'
UNION ALL
SELECT 
    'Schema Comparisons (Total)' as type, 
    COUNT(*) as count 
FROM data_diff_results.schema_comparison_summary
UNION ALL
SELECT 
    'Schema Comparisons (Failed)' as type, 
    COUNT(*) as count 
FROM data_diff_results.schema_comparison_summary 
WHERE status = 'failed';

\echo ''

-- Clear schema comparisons if enabled
\if :clear_schema_comparisons
    \echo '=== Clearing Schema Comparison Records ==='
    
    \if :clear_failed_only
        -- Clear only failed schema comparisons
        DELETE FROM data_diff_results.schema_column_differences 
        WHERE comparison_id IN (
            SELECT comparison_id FROM data_diff_results.schema_comparison_summary 
            WHERE status = 'failed'
        );
        
        DELETE FROM data_diff_results.schema_table_differences 
        WHERE comparison_id IN (
            SELECT comparison_id FROM data_diff_results.schema_comparison_summary 
            WHERE status = 'failed'
        );
        
        DELETE FROM data_diff_results.schema_comparison_summary 
        WHERE status = 'failed';
        
        \echo 'Cleared failed schema comparison records only'
    \else
        -- Clear based on age
        \if :days_to_keep
            DELETE FROM data_diff_results.schema_column_differences 
            WHERE comparison_id IN (
                SELECT comparison_id FROM data_diff_results.schema_comparison_summary 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days'
            );
            
            DELETE FROM data_diff_results.schema_table_differences 
            WHERE comparison_id IN (
                SELECT comparison_id FROM data_diff_results.schema_comparison_summary 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days'
            );
            
            DELETE FROM data_diff_results.schema_comparison_summary 
            WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days';
            
            \echo 'Cleared schema comparison records older than :days_to_keep days'
        \else
            -- Clear all
            DELETE FROM data_diff_results.schema_column_differences;
            DELETE FROM data_diff_results.schema_table_differences;
            DELETE FROM data_diff_results.schema_comparison_summary;
            \echo 'Cleared all schema comparison records'
        \endif
    \endif
\endif

-- Clear table comparisons if enabled
\if :clear_table_comparisons
    \echo ''
    \echo '=== Clearing Table Comparison Records ==='
    
    \if :clear_failed_only
        -- Clear only failed table comparisons
        DELETE FROM data_diff_results.performance_metrics 
        WHERE comparison_id IN (
            SELECT comparison_id FROM data_diff_results.comparison_summary 
            WHERE status = 'failed'
        );
        
        DELETE FROM data_diff_results.timeline_analysis 
        WHERE comparison_id IN (
            SELECT comparison_id FROM data_diff_results.comparison_summary 
            WHERE status = 'failed'
        );
        
        DELETE FROM data_diff_results.column_statistics 
        WHERE comparison_id IN (
            SELECT comparison_id FROM data_diff_results.comparison_summary 
            WHERE status = 'failed'
        );
        
        DELETE FROM data_diff_results.difference_details 
        WHERE comparison_id IN (
            SELECT comparison_id FROM data_diff_results.comparison_summary 
            WHERE status = 'failed'
        );
        
        DELETE FROM data_diff_results.comparison_summary 
        WHERE status = 'failed';
        
        \echo 'Cleared failed table comparison records only'
    \else
        -- Clear based on age
        \if :days_to_keep
            DELETE FROM data_diff_results.performance_metrics 
            WHERE comparison_id IN (
                SELECT comparison_id FROM data_diff_results.comparison_summary 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days'
            );
            
            DELETE FROM data_diff_results.timeline_analysis 
            WHERE comparison_id IN (
                SELECT comparison_id FROM data_diff_results.comparison_summary 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days'
            );
            
            DELETE FROM data_diff_results.column_statistics 
            WHERE comparison_id IN (
                SELECT comparison_id FROM data_diff_results.comparison_summary 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days'
            );
            
            DELETE FROM data_diff_results.difference_details 
            WHERE comparison_id IN (
                SELECT comparison_id FROM data_diff_results.comparison_summary 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days'
            );
            
            DELETE FROM data_diff_results.comparison_summary 
            WHERE created_at < CURRENT_TIMESTAMP - INTERVAL ':days_to_keep days';
            
            \echo 'Cleared table comparison records older than :days_to_keep days'
        \else
            -- Clear all
            DELETE FROM data_diff_results.performance_metrics;
            DELETE FROM data_diff_results.timeline_analysis;
            DELETE FROM data_diff_results.column_statistics;
            DELETE FROM data_diff_results.difference_details;
            DELETE FROM data_diff_results.comparison_summary;
            \echo 'Cleared all table comparison records'
        \endif
    \endif
\endif

-- Show record counts after deletion
\echo ''
\echo '=== Record Counts After Clearing ==='
SELECT 
    'Table Comparisons (Total)' as type, 
    COUNT(*) as count 
FROM data_diff_results.comparison_summary
UNION ALL
SELECT 
    'Table Comparisons (Failed)' as type, 
    COUNT(*) as count 
FROM data_diff_results.comparison_summary 
WHERE status = 'failed'
UNION ALL
SELECT 
    'Schema Comparisons (Total)' as type, 
    COUNT(*) as count 
FROM data_diff_results.schema_comparison_summary
UNION ALL
SELECT 
    'Schema Comparisons (Failed)' as type, 
    COUNT(*) as count 
FROM data_diff_results.schema_comparison_summary 
WHERE status = 'failed';

-- Commit transaction
COMMIT;

\echo ''
\echo '=== Operation completed successfully! ===''