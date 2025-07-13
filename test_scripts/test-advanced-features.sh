#!/bin/bash

echo "Testing advanced features with materialization..."

# 测试带列统计和时间线分析的比对
curl -X POST http://localhost:8000/api/v1/compare/tables/nested \
  -H "Content-Type: application/json" \
  -d '{
    "source_config": {
      "database_type": "postgresql",
      "host": "postgres",
      "port": 5432,
      "database": "datadiff",
      "username": "postgres",
      "password": "password"
    },
    "target_config": {
      "database_type": "postgresql",
      "host": "postgres",
      "port": 5432,
      "database": "datadiff",
      "username": "postgres",
      "password": "password"
    },
    "comparison_config": {
      "source_table": "products",
      "target_table": "products_copy",
      "key_columns": ["product_id"],
      "compare_columns": ["product_name", "price", "quantity", "created_at"],
      "materialize_results": true,
      "enable_column_statistics": true,
      "timeline_column": "created_at"
    }
  }' | jq .

echo -e "\n\nWaiting 3 seconds for comparison to complete..."
sleep 3

# 查询物化的结果
echo -e "\n\nQuerying materialized results..."
docker exec data-diff-n8n-postgres-1 psql -U postgres -d datadiff -c "\
SELECT 
    comparison_id,
    status,
    rows_compared,
    rows_matched,
    rows_different,
    match_rate,
    execution_time_seconds
FROM data_diff_results.comparison_summary 
ORDER BY start_time DESC 
LIMIT 1;
"

# 查询列统计
echo -e "\n\nQuerying column statistics..."
docker exec data-diff-n8n-postgres-1 psql -U postgres -d datadiff -c "\
SELECT 
    cs.table_side,
    cs.column_name,
    cs.null_count,
    cs.null_rate,
    cs.total_count
FROM data_diff_results.column_statistics cs
JOIN data_diff_results.comparison_summary cm ON cs.comparison_id = cm.comparison_id
WHERE cm.start_time >= CURRENT_TIMESTAMP - INTERVAL '1 minute'
ORDER BY cs.table_side, cs.column_name
LIMIT 10;
"

# 查询差异详情
echo -e "\n\nQuerying difference details..."
docker exec data-diff-n8n-postgres-1 psql -U postgres -d datadiff -c "\
SELECT 
    dd.difference_type,
    dd.column_name,
    dd.source_value,
    dd.target_value
FROM data_diff_results.difference_details dd
JOIN data_diff_results.comparison_summary cm ON dd.comparison_id = cm.comparison_id
WHERE cm.start_time >= CURRENT_TIMESTAMP - INTERVAL '1 minute'
LIMIT 5;
"