#!/usr/bin/env python3
"""
Test metrics recording through API calls
"""
import requests
import json
import time
import sys

API_URL = "http://localhost:8000"

def create_comparison_with_url_params():
    """Create a comparison using URL parameters (simulating n8n HTTP Request node)"""
    print("\nCreating comparison with URL parameters...")
    
    params = {
        "source_connection": "postgresql://postgres:postgres@localhost:5432/testdb",
        "target_connection": "postgresql://postgres:postgres@localhost:5432/testdb", 
        "source_table": "test_source",
        "target_table": "test_target",
        "primary_key_columns": "id",
        "algorithm": "hashdiff",
        "enable_column_statistics": "true",
        "enable_classification": "true"
    }
    
    response = requests.post(f"{API_URL}/api/v1/compare/tables", params=params)
    
    if response.status_code == 200:
        result = response.json()
        print(f"✓ Comparison created: {result['comparison_id']}")
        return result['comparison_id']
    else:
        print(f"✗ Failed to create comparison: {response.text}")
        return None

def wait_and_get_result(comparison_id):
    """Wait for comparison to complete and get result"""
    print(f"\nWaiting for comparison {comparison_id}...")
    
    for i in range(30):  # Wait up to 30 seconds
        response = requests.get(f"{API_URL}/api/v1/compare/results/{comparison_id}")
        if response.status_code == 200:
            result = response.json()
            status = result.get('status')
            
            if status == 'completed':
                print("✓ Comparison completed successfully")
                print(f"  Result summary: {json.dumps(result.get('result', {}).get('summary', {}), indent=2)}")
                return True
            elif status == 'failed':
                print(f"✗ Comparison failed: {result}")
                return False
        
        time.sleep(1)
        sys.stdout.write('.')
        sys.stdout.flush()
    
    print("\n✗ Timeout waiting for comparison")
    return False

def check_business_metrics():
    """Check if business metrics are recorded"""
    print("\n\nChecking business metrics...")
    
    response = requests.get(f"{API_URL}/metrics")
    if response.status_code != 200:
        print(f"✗ Failed to get metrics: {response.status_code}")
        return
    
    metrics_text = response.text
    
    # Business metrics to check
    business_metrics = [
        ("datadiff_comparison_differences_total", "Number of differences found"),
        ("datadiff_difference_rate", "Difference rate (0-1)"),
        ("datadiff_rows_compared_total", "Total rows compared"),
        ("datadiff_comparison_duration_seconds", "Comparison duration"),
        ("datadiff_column_null_rate", "Column null rates"),
        ("datadiff_data_quality_score", "Data quality scores")
    ]
    
    print("\nBusiness Metrics Status:")
    print("-" * 60)
    
    found_count = 0
    for metric_name, description in business_metrics:
        # Find all non-comment lines for this metric
        metric_lines = [line for line in metrics_text.split('\n') 
                       if line.startswith(metric_name) and not line.startswith('#')]
        
        if metric_lines:
            found_count += 1
            print(f"✓ {description}:")
            for line in metric_lines[:3]:  # Show first 3 instances
                print(f"  {line}")
        else:
            # Check if metric is at least defined
            if f"# HELP {metric_name}" in metrics_text:
                print(f"⚠ {description}: Defined but no data recorded")
            else:
                print(f"✗ {description}: Not found")
    
    # System metrics
    print("\nSystem Metrics Status:")
    print("-" * 60)
    
    system_metrics = [
        ("datadiff_memory_usage_bytes", "Memory usage"),
        ("datadiff_cpu_usage_percent", "CPU usage"),
        ("datadiff_api_request_total", "API request count"),
        ("datadiff_api_request_duration_seconds", "API request duration")
    ]
    
    for metric_name, description in system_metrics:
        metric_lines = [line for line in metrics_text.split('\n') 
                       if line.startswith(metric_name) and not line.startswith('#')]
        
        if metric_lines:
            print(f"✓ {description}: {metric_lines[0]}")
        else:
            print(f"✗ {description}: Not found")
    
    print(f"\n📊 Summary: {found_count}/{len(business_metrics)} business metrics have data")

def main():
    """Main test function"""
    print("=== Testing Data-Diff N8N Business Metrics ===")
    
    # Create comparison
    comparison_id = create_comparison_with_url_params()
    
    if comparison_id:
        # Wait for completion
        if wait_and_get_result(comparison_id):
            # Give metrics a moment to be recorded
            time.sleep(2)
    
    # Check metrics
    check_business_metrics()
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    main()