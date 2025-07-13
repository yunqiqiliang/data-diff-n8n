#!/usr/bin/env python3
"""
Comprehensive test for schema difference counting.
Tests that table, column, and type differences are all counted correctly.
"""

import requests
import json
import sys

def test_comprehensive_diff_counting():
    """Test comprehensive difference counting in schema comparison"""
    print("🔧 Testing Comprehensive Difference Counting")
    print("=" * 60)
    
    # Configuration for databases with known differences
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
        
        # Count actual differences
        tables_only_in_source = diff.get("tables_only_in_source", [])
        tables_only_in_target = diff.get("tables_only_in_target", [])
        column_diffs = diff.get("column_diffs", {})
        type_diffs = diff.get("type_diffs", {})
        
        # Calculate actual counts
        actual_table_diff_count = len(tables_only_in_source) + len(tables_only_in_target)
        
        actual_column_diff_count = 0
        for table_col_diffs in column_diffs.values():
            actual_column_diff_count += len(table_col_diffs.get("columns_only_in_source", []))
            actual_column_diff_count += len(table_col_diffs.get("columns_only_in_target", []))
        
        actual_type_diff_count = 0
        for table_type_diffs in type_diffs.values():
            actual_type_diff_count += len(table_type_diffs)
        
        actual_total_diff_count = (
            actual_table_diff_count + 
            actual_column_diff_count + 
            actual_type_diff_count
        )
        
        # Display results
        print(f"\n📊 Difference Analysis:")
        print(f"\n1️⃣  Table Differences:")
        print(f"   Summary reports: {summary.get('table_differences', 0)}")
        print(f"   Actual count: {actual_table_diff_count}")
        print(f"   - Tables only in source: {len(tables_only_in_source)}")
        print(f"   - Tables only in target: {len(tables_only_in_target)}")
        
        print(f"\n2️⃣  Column Differences:")
        print(f"   Summary reports: {summary.get('column_differences', 0)}")
        print(f"   Actual count: {actual_column_diff_count}")
        if column_diffs:
            for table, cols in column_diffs.items():
                cols_in_source = len(cols.get("columns_only_in_source", []))
                cols_in_target = len(cols.get("columns_only_in_target", []))
                if cols_in_source or cols_in_target:
                    print(f"   - {table}: {cols_in_source} only in source, {cols_in_target} only in target")
        
        print(f"\n3️⃣  Type Differences:")
        print(f"   Summary reports: {summary.get('type_differences', 0)}")
        print(f"   Actual count: {actual_type_diff_count}")
        if type_diffs:
            for table, types in type_diffs.items():
                print(f"   - {table}: {len(types)} type differences")
        
        print(f"\n4️⃣  Total Differences:")
        print(f"   Summary reports: {summary.get('total_differences', 0)}")
        print(f"   Calculated total: {actual_total_diff_count}")
        
        # Verify all counts match
        all_correct = True
        errors = []
        
        if summary.get('table_differences', 0) != actual_table_diff_count:
            all_correct = False
            errors.append(f"Table differences: expected {actual_table_diff_count}, got {summary.get('table_differences', 0)}")
        
        if summary.get('column_differences', 0) != actual_column_diff_count:
            all_correct = False
            errors.append(f"Column differences: expected {actual_column_diff_count}, got {summary.get('column_differences', 0)}")
        
        if summary.get('type_differences', 0) != actual_type_diff_count:
            all_correct = False
            errors.append(f"Type differences: expected {actual_type_diff_count}, got {summary.get('type_differences', 0)}")
        
        if summary.get('total_differences', 0) != actual_total_diff_count:
            all_correct = False
            errors.append(f"Total differences: expected {actual_total_diff_count}, got {summary.get('total_differences', 0)}")
        
        print(f"\n📋 Verification Results:")
        if all_correct:
            print(f"✅ SUCCESS: All difference counts are correct!")
            print(f"   - Table differences: ✓")
            print(f"   - Column differences: ✓")
            print(f"   - Type differences: ✓")
            print(f"   - Total differences: ✓")
            return True
        else:
            print(f"❌ FAILURE: Some counts are incorrect!")
            for error in errors:
                print(f"   - {error}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    print("🧪 Comprehensive Schema Difference Counting Test")
    print("=" * 60)
    print("This test verifies that all types of differences")
    print("(tables, columns, types) are counted correctly.")
    print()
    
    success = test_comprehensive_diff_counting()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 All tests passed! Difference counting is accurate.")
    else:
        print("💥 Test failed! Some difference counts need investigation.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())