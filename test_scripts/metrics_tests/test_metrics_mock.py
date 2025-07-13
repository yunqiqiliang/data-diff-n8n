#!/usr/bin/env python3
"""
Test metrics recording with mock data (no database required)
"""
import requests
import json

API_URL = "http://localhost:8000"

def test_nested_endpoint():
    """Test the nested endpoint which properly records metrics"""
    print("\nTesting nested comparison endpoint...")
    
    request_data = {
        "source_config": {
            "database_type": "postgresql", 
            "host": "mock-host",
            "port": 5432,
            "username": "test",
            "password": "test",
            "database": "test",
            "db_schema": "public"
        },
        "target_config": {
            "database_type": "postgresql",
            "host": "mock-host", 
            "port": 5432,
            "username": "test",
            "password": "test",
            "database": "test",
            "db_schema": "public"
        },
        "comparison_config": {
            "source_table": "test_table",
            "target_table": "test_table",
            "key_columns": ["id"],
            "algorithm": "hashdiff",
            "enable_column_statistics": True,
            "enable_classification": True,
            "sample_size": 1000
        }
    }
    
    response = requests.post(
        f"{API_URL}/api/v1/compare/tables/nested",
        json=request_data
    )
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Nested comparison created: {result['comparison_id']}")
        return result['comparison_id']
    else:
        print(f"✗ Failed to create comparison: {response.text}")
        return None

def check_metrics():
    """Check metrics endpoint"""
    print("\nChecking metrics...")
    
    response = requests.get(f"{API_URL}/metrics")
    if response.status_code != 200:
        print(f"✗ Failed to get metrics: {response.status_code}")
        return
    
    metrics_text = response.text
    
    # Check for specific metrics
    metrics_to_check = [
        ("datadiff_api_request_total", "API requests"),
        ("datadiff_memory_usage_bytes", "Memory usage"),
        ("datadiff_cpu_usage_percent", "CPU usage"),
        ("datadiff_comparison_total", "Comparison count"),
        ("datadiff_comparison_differences_total", "Differences"),
        ("datadiff_difference_rate", "Difference rate"),
        ("datadiff_rows_compared_total", "Rows compared")
    ]
    
    print("\nMetrics Status:")
    print("-" * 60)
    
    for metric_name, description in metrics_to_check:
        # Find non-comment lines
        lines = [line for line in metrics_text.split('\n') 
                if line.startswith(metric_name) and not line.startswith('#')]
        
        if lines:
            print(f"✓ {description}: Found {len(lines)} entries")
            for line in lines[:2]:  # Show first 2
                print(f"  {line}")
        else:
            if f"# HELP {metric_name}" in metrics_text:
                print(f"⚠ {description}: Defined but no data")
            else:
                print(f"✗ {description}: Not found")

def test_prometheus_format():
    """Test if metrics are in valid Prometheus format"""
    print("\n\nValidating Prometheus format...")
    
    response = requests.get(f"{API_URL}/metrics")
    if response.status_code != 200:
        print(f"✗ Failed to get metrics")
        return
    
    lines = response.text.split('\n')
    help_lines = [l for l in lines if l.startswith('# HELP')]
    type_lines = [l for l in lines if l.startswith('# TYPE')]
    metric_lines = [l for l in lines if l and not l.startswith('#')]
    
    print(f"✓ Found {len(help_lines)} HELP lines")
    print(f"✓ Found {len(type_lines)} TYPE lines")
    print(f"✓ Found {len(metric_lines)} metric data lines")
    
    # Show some examples
    print("\nExample metrics:")
    for line in metric_lines[:5]:
        if 'datadiff_' in line:
            print(f"  {line}")

def main():
    """Main test function"""
    print("=== Testing Data-Diff N8N Metrics Recording ===")
    
    # Create a comparison to generate some activity
    comparison_id = test_nested_endpoint()
    
    # Check metrics
    check_metrics()
    
    # Validate format
    test_prometheus_format()
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()