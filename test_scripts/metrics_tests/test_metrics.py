#!/usr/bin/env python3
"""
Test script to verify metrics recording
"""
import requests
import json
import time
import sys

# API base URL
API_URL = "http://localhost:8000"

def test_connection():
    """Test database connection"""
    print("Testing PostgreSQL connection...")
    
    db_config = {
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "username": "postgres",
        "password": "postgres",
        "database": "testdb"
    }
    
    response = requests.post(f"{API_URL}/api/v1/connections/test", json=db_config)
    if response.status_code == 200:
        print("✓ Connection test successful")
    else:
        print(f"✗ Connection test failed: {response.json()}")
        return False
    return True

def create_test_comparison():
    """Create a test table comparison"""
    print("\nCreating test comparison...")
    
    comparison_request = {
        "source_config": {
            "database_type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "postgres",
            "database": "testdb",
            "db_schema": "public"
        },
        "target_config": {
            "database_type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "postgres",
            "database": "testdb",
            "db_schema": "public"
        },
        "comparison_config": {
            "source_table": "test_table1",
            "target_table": "test_table1",
            "key_columns": ["id"],
            "algorithm": "hashdiff",
            "enable_column_statistics": True,
            "enable_classification": True
        }
    }
    
    response = requests.post(
        f"{API_URL}/api/v1/compare/tables/nested", 
        json=comparison_request
    )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Comparison created: {result['comparison_id']}")
        return result['comparison_id']
    else:
        print(f"✗ Failed to create comparison: {response.json()}")
        return None

def wait_for_comparison(comparison_id):
    """Wait for comparison to complete"""
    print(f"\nWaiting for comparison {comparison_id} to complete...")
    
    for i in range(30):  # Wait up to 30 seconds
        response = requests.get(f"{API_URL}/api/v1/compare/results/{comparison_id}")
        if response.status_code == 200:
            result = response.json()
            status = result.get('status')
            
            if status == 'completed':
                print("✓ Comparison completed")
                return True
            elif status == 'failed':
                print(f"✗ Comparison failed: {result}")
                return False
        
        time.sleep(1)
        sys.stdout.write('.')
        sys.stdout.flush()
    
    print("\n✗ Timeout waiting for comparison")
    return False

def check_metrics():
    """Check if metrics are being recorded"""
    print("\nChecking metrics...")
    
    response = requests.get(f"{API_URL}/metrics")
    if response.status_code != 200:
        print(f"✗ Failed to get metrics: {response.status_code}")
        return False
    
    metrics_text = response.text
    
    # Check for business metrics
    metrics_to_check = [
        ("datadiff_comparison_differences_total", "Comparison differences"),
        ("datadiff_difference_rate", "Difference rate"),
        ("datadiff_rows_compared_total", "Rows compared"),
        ("datadiff_comparison_duration_seconds", "Comparison duration"),
        ("datadiff_memory_usage_bytes", "Memory usage"),
        ("datadiff_cpu_usage_percent", "CPU usage")
    ]
    
    found_metrics = []
    missing_metrics = []
    
    for metric_name, description in metrics_to_check:
        if metric_name in metrics_text:
            # Check if metric has actual data (not just definition)
            lines = [line for line in metrics_text.split('\n') if line.startswith(metric_name) and not line.startswith('#')]
            if lines:
                found_metrics.append((metric_name, description))
                print(f"✓ {description} metric found: {lines[0][:100]}...")
            else:
                missing_metrics.append((metric_name, description))
                print(f"⚠ {description} metric defined but no data")
        else:
            missing_metrics.append((metric_name, description))
            print(f"✗ {description} metric not found")
    
    print(f"\nSummary: {len(found_metrics)}/{len(metrics_to_check)} metrics have data")
    
    return len(found_metrics) > len(metrics_to_check) / 2

def main():
    """Main test function"""
    print("=== Testing Data-Diff N8N Metrics ===\n")
    
    # Test connection
    if not test_connection():
        print("\nSkipping comparison test due to connection failure")
        print("Make sure PostgreSQL is running with a test database")
        return
    
    # Create and wait for comparison
    comparison_id = create_test_comparison()
    if comparison_id:
        if wait_for_comparison(comparison_id):
            # Give metrics a moment to be recorded
            time.sleep(2)
    
    # Check metrics
    check_metrics()
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()