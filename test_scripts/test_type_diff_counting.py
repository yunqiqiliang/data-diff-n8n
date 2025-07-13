#!/usr/bin/env python3
"""
Test script to verify that type differences are counted correctly in schema comparison.
This tests the fix for the issue where type_differences showed 1 when there were actually 6 type differences.
"""

import requests
import json
import sys

def test_type_diff_counting():
    """Test that type differences are counted correctly"""
    print("🔧 Testing Type Difference Counting in Schema Comparison")
    print("=" * 60)
    
    # Configuration for databases with known type differences
    source_config = {
        "database_type": "postgresql",
        "host": "106.120.41.178",
        "port": 5436,
        "username": "metabase",
        "password": "metasample123",
        "database": "sample",
        "db_schema": "public"
    }
    
    target_config = {
        "database_type": "clickzetta",
        "username": "qiliang",
        "password": "Ql123456!",
        "instance": "jnsxwfyr",
        "service": "uat-api.clickzetta.com",
        "workspace": "quick_start",
        "db_schema": "from_pg",
        "vcluster": "default_ap"
    }
    
    request_data = {
        "source_config": source_config,
        "target_config": target_config
    }
    
    print(f"📤 Sending schema comparison request...")
    print(f"   Source: {source_config['database_type']} - {source_config['db_schema']}")
    print(f"   Target: {target_config['database_type']} - {target_config['db_schema']}")
    
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/compare/schemas/nested",
            json=request_data,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ API request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
        result = response.json()
        print(f"✅ Schema comparison completed successfully")
        
        # Extract the comparison result
        comparison_result = result.get("result", {})
        summary = comparison_result.get("summary", {})
        diff = comparison_result.get("diff", {})
        type_diffs = diff.get("type_diffs", {})
        
        # Count actual type differences
        actual_type_diff_count = 0
        type_diff_details = []
        
        for table, table_type_diffs in type_diffs.items():
            actual_type_diff_count += len(table_type_diffs)
            type_diff_details.append({
                "table": table,
                "count": len(table_type_diffs),
                "differences": table_type_diffs
            })
        
        # Display results
        print(f"\n📊 Type Difference Analysis:")
        print(f"   Summary reports: {summary.get('type_differences', 0)} type differences")
        print(f"   Actual count: {actual_type_diff_count} type differences")
        
        # Show details for each table
        print(f"\n🔍 Type Differences by Table:")
        for detail in type_diff_details:
            print(f"\n   Table: {detail['table']}")
            print(f"   Number of type differences: {detail['count']}")
            for diff in detail['differences'][:5]:  # Show first 5 differences
                print(f"     - {diff['column']}: {diff['source_type']} → {diff['target_type']}")
            if detail['count'] > 5:
                print(f"     ... and {detail['count'] - 5} more")
        
        # Verify the fix
        summary_count = summary.get('type_differences', 0)
        if summary_count == actual_type_diff_count:
            print(f"\n✅ SUCCESS: Type difference counting is correct!")
            print(f"   Summary correctly reports {summary_count} type differences")
            print(f"   This matches the actual count of {actual_type_diff_count}")
            return True
        else:
            print(f"\n❌ FAILURE: Type difference counting mismatch!")
            print(f"   Summary reports: {summary_count}")
            print(f"   Actual count: {actual_type_diff_count}")
            print(f"   Difference: {abs(summary_count - actual_type_diff_count)}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    print("🧪 Type Difference Counting Test")
    print("=" * 60)
    print("This test verifies that type differences are counted correctly")
    print("in the schema comparison summary.")
    print()
    
    success = test_type_diff_counting()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 Test passed! Type differences are being counted correctly.")
    else:
        print("💥 Test failed! Type difference counting needs investigation.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())