#!/usr/bin/env python3
"""
Simple test to verify metrics recording without database dependency
"""
import requests
import json

API_URL = "http://localhost:8000"

def test_metrics_recording():
    """Test if metrics recording functions are working"""
    # Import and test the metrics recording function directly
    from n8n.api.metrics import record_comparison_metrics, record_column_statistics
    
    # Test data that mimics a comparison result
    test_result = {
        "status": "completed",
        "result": {
            "summary": {
                "total_rows": 1000,
                "rows_matched": 950,
                "rows_different": 50,
                "match_rate": 95.0,
                "execution_time": 2.5
            },
            "statistics": {
                "rows_compared": 1000,
                "differences": {
                    "total_differences": 50
                }
            }
        }
    }
    
    test_config = {
        "comparison_type": "table",
        "source_type": "postgres",
        "target_type": "postgres",
        "source_table": "test_table",
        "algorithm": "hashdiff"
    }
    
    # Record metrics
    print("Recording test metrics...")
    record_comparison_metrics(test_result, test_config)
    
    # Test column statistics
    test_column_stats = {
        "id": {"null_rate": 0.0, "quality_score": 100},
        "name": {"null_rate": 0.05, "quality_score": 95},
        "email": {"null_rate": 0.10, "quality_score": 90}
    }
    
    record_column_statistics(test_column_stats, "test_table", "postgres")
    
    print("✓ Metrics recorded successfully")

def check_metrics_api():
    """Check metrics via API"""
    print("\nChecking metrics endpoint...")
    
    response = requests.get(f"{API_URL}/metrics")
    if response.status_code != 200:
        print(f"✗ Failed to get metrics: {response.status_code}")
        return
    
    metrics_text = response.text
    
    # Check for specific metric values
    checks = [
        ("datadiff_comparison_differences_total", "test_table"),
        ("datadiff_difference_rate", "test_table"),
        ("datadiff_rows_compared_total", "test_table"),
        ("datadiff_column_null_rate", "name")
    ]
    
    print("\nMetric values:")
    for metric_name, label in checks:
        lines = [line for line in metrics_text.split('\n') 
                if line.startswith(metric_name) and label in line and not line.startswith('#')]
        if lines:
            print(f"✓ {metric_name}: {lines[0]}")
        else:
            print(f"✗ {metric_name} with label '{label}' not found")

if __name__ == "__main__":
    print("=== Testing Metrics Recording ===\n")
    
    test_metrics_recording()
    check_metrics_api()
    
    print("\n=== Test Complete ===")