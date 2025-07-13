-- =====================================================
-- Clear Materialized Comparison Records
-- =====================================================
-- This script clears all materialized comparison records from the database.
-- Use with caution as this will permanently delete all historical comparison data.
-- 
-- Usage:
-- psql -U postgres -d datadiff -f clear-materialized-records.sql
-- Or in Docker:
-- docker-compose -f docker-compose.dev.yml exec postgres psql -U postgres -d datadiff -f /docker-entrypoint-initdb.d/clear-materialized-records.sql
-- =====================================================

\c datadiff

-- Start transaction
BEGIN;

-- Show current record counts before deletion
\echo '=== Current Record Counts ==='
SELECT 'Table Comparisons' as type, COUNT(*) as count FROM data_diff_results.comparison_summary
UNION ALL
SELECT 'Schema Comparisons' as type, COUNT(*) as count FROM data_diff_results.schema_comparison_summary
UNION ALL
SELECT 'Difference Details' as type, COUNT(*) as count FROM data_diff_results.difference_details
UNION ALL
SELECT 'Column Statistics' as type, COUNT(*) as count FROM data_diff_results.column_statistics
UNION ALL
SELECT 'Timeline Analysis' as type, COUNT(*) as count FROM data_diff_results.timeline_analysis
UNION ALL
SELECT 'Performance Metrics' as type, COUNT(*) as count FROM data_diff_results.performance_metrics;

\echo ''
\echo '=== Clearing Schema Comparison Records ==='

-- Clear schema comparison related tables (child tables first due to foreign keys)
DELETE FROM data_diff_results.schema_column_differences;
\echo 'Cleared schema_column_differences'

DELETE FROM data_diff_results.schema_table_differences;
\echo 'Cleared schema_table_differences'

DELETE FROM data_diff_results.schema_comparison_summary;
\echo 'Cleared schema_comparison_summary'

\echo ''
\echo '=== Clearing Table Comparison Records ==='

-- Clear table comparison related tables (child tables first due to foreign keys)
DELETE FROM data_diff_results.performance_metrics;
\echo 'Cleared performance_metrics'

DELETE FROM data_diff_results.timeline_analysis;
\echo 'Cleared timeline_analysis'

DELETE FROM data_diff_results.column_statistics;
\echo 'Cleared column_statistics'

DELETE FROM data_diff_results.difference_details;
\echo 'Cleared difference_details'

DELETE FROM data_diff_results.comparison_summary;
\echo 'Cleared comparison_summary'

-- Reset sequences to start from 1
ALTER SEQUENCE data_diff_results.comparison_summary_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.difference_details_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.column_statistics_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.timeline_analysis_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.performance_metrics_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.schema_comparison_summary_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.schema_table_differences_id_seq RESTART WITH 1;
ALTER SEQUENCE data_diff_results.schema_column_differences_id_seq RESTART WITH 1;

\echo ''
\echo '=== Reset sequences to start from 1 ==='

-- Show record counts after deletion
\echo ''
\echo '=== Record Counts After Clearing ==='
SELECT 'Table Comparisons' as type, COUNT(*) as count FROM data_diff_results.comparison_summary
UNION ALL
SELECT 'Schema Comparisons' as type, COUNT(*) as count FROM data_diff_results.schema_comparison_summary
UNION ALL
SELECT 'Difference Details' as type, COUNT(*) as count FROM data_diff_results.difference_details
UNION ALL
SELECT 'Column Statistics' as type, COUNT(*) as count FROM data_diff_results.column_statistics
UNION ALL
SELECT 'Timeline Analysis' as type, COUNT(*) as count FROM data_diff_results.timeline_analysis
UNION ALL
SELECT 'Performance Metrics' as type, COUNT(*) as count FROM data_diff_results.performance_metrics;

-- Commit transaction
COMMIT;

\echo ''
\echo '=== All materialized records have been cleared successfully! ==='