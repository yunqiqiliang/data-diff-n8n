"""
Maintenance routes for data comparison system
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import io
import json
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/maintenance", tags=["maintenance"])

class ClearRecordsRequest(BaseModel):
    record_type: str = "all"  # all, table, schema
    filter: str = "all"  # all, failed, age
    days_to_keep: Optional[int] = None

class CleanupFailedRequest(BaseModel):
    status_to_clean: List[str] = ["failed"]
    stuck_threshold_hours: int = 24

class DbMaintenanceRequest(BaseModel):
    operations: List[str] = ["vacuum", "analyze"]

def get_db_connection():
    """Get database connection"""
    import os
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'postgres'),
        port=int(os.environ.get('DB_PORT', '5432')),
        database=os.environ.get('DB_NAME', 'datadiff'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'password')
    )

@router.post("/clear-records")
async def clear_records(request: ClearRecordsRequest):
    """Clear materialized comparison records"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Track what was cleared
        cleared_counts = {}
        
        # Start transaction
        cur.execute("BEGIN")
        
        # Get counts before clearing
        cur.execute("""
            SELECT 
                'table_comparisons' as type, COUNT(*) as count 
            FROM data_diff_results.comparison_summary
            UNION ALL
            SELECT 
                'schema_comparisons' as type, COUNT(*) as count 
            FROM data_diff_results.schema_comparison_summary
        """)
        before_counts = {row['type']: row['count'] for row in cur.fetchall()}
        
        # Build WHERE clause based on filter
        where_clause = ""
        if request.filter == "failed":
            where_clause = "WHERE status = 'failed'"
        elif request.filter == "age" and request.days_to_keep is not None:
            where_clause = f"WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '{request.days_to_keep} days'"
        
        # Clear schema comparisons if needed
        if request.record_type in ["all", "schema"]:
            # Clear child tables first
            cur.execute(f"""
                DELETE FROM data_diff_results.schema_column_differences 
                WHERE comparison_id IN (
                    SELECT comparison_id FROM data_diff_results.schema_comparison_summary {where_clause}
                )
            """)
            
            cur.execute(f"""
                DELETE FROM data_diff_results.schema_table_differences 
                WHERE comparison_id IN (
                    SELECT comparison_id FROM data_diff_results.schema_comparison_summary {where_clause}
                )
            """)
            
            cur.execute(f"DELETE FROM data_diff_results.schema_comparison_summary {where_clause}")
            cleared_counts['schema_comparisons'] = cur.rowcount
        
        # Clear table comparisons if needed
        if request.record_type in ["all", "table"]:
            # Clear child tables first
            for table in ['performance_metrics', 'timeline_analysis', 'column_statistics', 'difference_details']:
                cur.execute(f"""
                    DELETE FROM data_diff_results.{table}
                    WHERE comparison_id IN (
                        SELECT comparison_id FROM data_diff_results.comparison_summary {where_clause}
                    )
                """)
            
            cur.execute(f"DELETE FROM data_diff_results.comparison_summary {where_clause}")
            cleared_counts['table_comparisons'] = cur.rowcount
        
        # Get counts after clearing
        cur.execute("""
            SELECT 
                'table_comparisons' as type, COUNT(*) as count 
            FROM data_diff_results.comparison_summary
            UNION ALL
            SELECT 
                'schema_comparisons' as type, COUNT(*) as count 
            FROM data_diff_results.schema_comparison_summary
        """)
        after_counts = {row['type']: row['count'] for row in cur.fetchall()}
        
        # Commit transaction
        cur.execute("COMMIT")
        
        return {
            "success": True,
            "message": "Records cleared successfully",
            "before_counts": before_counts,
            "after_counts": after_counts,
            "cleared_counts": cleared_counts,
            "filter_applied": request.filter,
            "days_kept": request.days_to_keep if request.filter == "age" else None
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error clearing records: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.get("/statistics")
async def get_statistics():
    """Get system statistics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        statistics = {}
        
        # Overall counts
        cur.execute("""
            SELECT 
                'table_comparisons_total' as metric, COUNT(*) as value 
            FROM data_diff_results.comparison_summary
            UNION ALL
            SELECT 
                'table_comparisons_completed' as metric, COUNT(*) as value 
            FROM data_diff_results.comparison_summary WHERE status = 'completed'
            UNION ALL
            SELECT 
                'table_comparisons_failed' as metric, COUNT(*) as value 
            FROM data_diff_results.comparison_summary WHERE status = 'failed'
            UNION ALL
            SELECT 
                'schema_comparisons_total' as metric, COUNT(*) as value 
            FROM data_diff_results.schema_comparison_summary
            UNION ALL
            SELECT 
                'schema_comparisons_completed' as metric, COUNT(*) as value 
            FROM data_diff_results.schema_comparison_summary WHERE status = 'completed'
            UNION ALL
            SELECT 
                'schema_comparisons_failed' as metric, COUNT(*) as value 
            FROM data_diff_results.schema_comparison_summary WHERE status = 'failed'
            UNION ALL
            SELECT 
                'table_comparisons_running' as metric, COUNT(*) as value 
            FROM data_diff_results.comparison_summary WHERE status = 'running'
            UNION ALL
            SELECT 
                'schema_comparisons_running' as metric, COUNT(*) as value 
            FROM data_diff_results.schema_comparison_summary WHERE status = 'running'
        """)
        
        for row in cur.fetchall():
            statistics[row['metric']] = row['value']
        
        # Storage usage
        cur.execute("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            WHERE schemaname = 'data_diff_results'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """)
        
        statistics['storage_by_table'] = cur.fetchall()
        
        # Recent activity
        cur.execute("""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as comparisons
            FROM (
                SELECT created_at FROM data_diff_results.comparison_summary
                UNION ALL
                SELECT created_at FROM data_diff_results.schema_comparison_summary
            ) combined
            WHERE created_at > CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        """)
        
        statistics['recent_activity'] = cur.fetchall()
        
        # Average execution times
        cur.execute("""
            SELECT 
                'table_comparisons' as type,
                AVG(execution_time_seconds) as avg_seconds,
                MIN(execution_time_seconds) as min_seconds,
                MAX(execution_time_seconds) as max_seconds
            FROM data_diff_results.comparison_summary
            WHERE status = 'completed' AND execution_time_seconds IS NOT NULL
            UNION ALL
            SELECT 
                'schema_comparisons' as type,
                AVG(execution_time_seconds) as avg_seconds,
                MIN(execution_time_seconds) as min_seconds,
                MAX(execution_time_seconds) as max_seconds
            FROM data_diff_results.schema_comparison_summary
            WHERE status = 'completed' AND execution_time_seconds IS NOT NULL
        """)
        
        statistics['performance_metrics'] = cur.fetchall()
        
        return {
            "success": True,
            "statistics": statistics,
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.post("/cleanup-failed")
async def cleanup_failed(request: CleanupFailedRequest):
    """Clean up failed or stuck comparisons"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cleaned_counts = {}
        
        # Start transaction
        cur.execute("BEGIN")
        
        # Clean failed comparisons
        if "failed" in request.status_to_clean:
            # Get count before cleaning
            cur.execute("""
                SELECT COUNT(*) as count FROM data_diff_results.comparison_summary WHERE status = 'failed'
            """)
            table_failed = cur.fetchone()['count']
            
            cur.execute("""
                SELECT COUNT(*) as count FROM data_diff_results.schema_comparison_summary WHERE status = 'failed'
            """)
            schema_failed = cur.fetchone()['count']
            
            # Clean (using the clear logic)
            for table in ['performance_metrics', 'timeline_analysis', 'column_statistics', 'difference_details']:
                cur.execute(f"""
                    DELETE FROM data_diff_results.{table}
                    WHERE comparison_id IN (
                        SELECT comparison_id FROM data_diff_results.comparison_summary WHERE status = 'failed'
                    )
                """)
            
            cur.execute("DELETE FROM data_diff_results.comparison_summary WHERE status = 'failed'")
            
            # Schema comparisons
            cur.execute("""
                DELETE FROM data_diff_results.schema_column_differences 
                WHERE comparison_id IN (
                    SELECT comparison_id FROM data_diff_results.schema_comparison_summary WHERE status = 'failed'
                )
            """)
            cur.execute("""
                DELETE FROM data_diff_results.schema_table_differences 
                WHERE comparison_id IN (
                    SELECT comparison_id FROM data_diff_results.schema_comparison_summary WHERE status = 'failed'
                )
            """)
            cur.execute("DELETE FROM data_diff_results.schema_comparison_summary WHERE status = 'failed'")
            
            cleaned_counts['failed'] = {
                'table_comparisons': table_failed,
                'schema_comparisons': schema_failed
            }
        
        # Clean stuck comparisons
        stuck_statuses = [s for s in request.status_to_clean if s in ['running', 'pending']]
        if stuck_statuses:
            threshold_time = datetime.utcnow() - timedelta(hours=request.stuck_threshold_hours)
            
            # Update stuck comparisons to failed
            cur.execute("""
                UPDATE data_diff_results.comparison_summary 
                SET status = 'failed', 
                    error_message = 'Task timeout - stuck in ' || status || ' state',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = ANY(%s) 
                AND created_at < %s
            """, (stuck_statuses, threshold_time))
            table_stuck = cur.rowcount
            
            cur.execute("""
                UPDATE data_diff_results.schema_comparison_summary 
                SET status = 'failed',
                    error_message = 'Task timeout - stuck in ' || status || ' state',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = ANY(%s) 
                AND created_at < %s
            """, (stuck_statuses, threshold_time))
            schema_stuck = cur.rowcount
            
            cleaned_counts['stuck'] = {
                'table_comparisons': table_stuck,
                'schema_comparisons': schema_stuck
            }
        
        # Commit transaction
        cur.execute("COMMIT")
        
        return {
            "success": True,
            "message": "Cleanup completed successfully",
            "cleaned_counts": cleaned_counts,
            "threshold_hours": request.stuck_threshold_hours
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error cleaning up failed tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.get("/export-history")
async def export_history(
    format: str = Query("json", enum=["json", "csv"]),
    date_range: str = Query("month", enum=["all", "week", "month", "custom"]),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_details: bool = False
):
    """Export comparison history"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build date filter
        date_filter = ""
        if date_range == "week":
            date_filter = "WHERE created_at > CURRENT_DATE - INTERVAL '7 days'"
        elif date_range == "month":
            date_filter = "WHERE created_at > CURRENT_DATE - INTERVAL '30 days'"
        elif date_range == "custom" and start_date and end_date:
            date_filter = f"WHERE created_at BETWEEN '{start_date}' AND '{end_date}'"
        
        # Get comparison data
        cur.execute(f"""
            SELECT 
                comparison_id,
                'table' as comparison_type,
                source_table,
                target_table,
                status,
                start_time,
                end_time,
                execution_time_seconds,
                total_differences,
                total_rows_compared,
                created_at
            FROM data_diff_results.comparison_summary
            {date_filter}
            UNION ALL
            SELECT 
                comparison_id,
                'schema' as comparison_type,
                source_schema as source_table,
                target_schema as target_table,
                status,
                start_time,
                end_time,
                execution_time_seconds,
                total_differences,
                NULL as total_rows_compared,
                created_at
            FROM data_diff_results.schema_comparison_summary
            {date_filter}
            ORDER BY created_at DESC
        """)
        
        comparisons = cur.fetchall()
        
        if format == "csv":
            # Generate CSV
            output = io.StringIO()
            if comparisons:
                writer = csv.DictWriter(output, fieldnames=comparisons[0].keys())
                writer.writeheader()
                writer.writerows(comparisons)
            
            return output.getvalue()
        else:
            # Return JSON
            return {
                "success": True,
                "count": len(comparisons),
                "date_range": date_range,
                "comparisons": comparisons
            }
            
    except Exception as e:
        logger.error(f"Error exporting history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@router.post("/db-maintenance")
async def perform_db_maintenance(request: DbMaintenanceRequest):
    """Perform database maintenance operations"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Need to commit current transaction before VACUUM
        conn.commit()
        
        # Set isolation level for VACUUM
        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0)
        
        results = []
        
        for operation in request.operations:
            if operation.lower() == "vacuum":
                cur.execute("VACUUM ANALYZE data_diff_results.comparison_summary")
                cur.execute("VACUUM ANALYZE data_diff_results.schema_comparison_summary")
                cur.execute("VACUUM ANALYZE data_diff_results.difference_details")
                cur.execute("VACUUM ANALYZE data_diff_results.column_statistics")
                cur.execute("VACUUM ANALYZE data_diff_results.timeline_analysis")
                results.append({"operation": "vacuum", "status": "completed"})
            elif operation.lower() == "analyze":
                cur.execute("ANALYZE data_diff_results.comparison_summary")
                cur.execute("ANALYZE data_diff_results.schema_comparison_summary")
                cur.execute("ANALYZE data_diff_results.difference_details")
                cur.execute("ANALYZE data_diff_results.column_statistics")
                cur.execute("ANALYZE data_diff_results.timeline_analysis")
                results.append({"operation": "analyze", "status": "completed"})
        
        # Restore isolation level
        conn.set_isolation_level(old_isolation_level)
        
        return {
            "success": True,
            "message": "Database maintenance completed",
            "operations": results,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error performing database maintenance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()