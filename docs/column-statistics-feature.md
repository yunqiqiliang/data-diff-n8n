# Column-Level Statistics Feature

## Overview

The column-level statistics feature provides detailed insights into data quality and distribution during data comparison. It collects and compares statistics for each column, helping identify data quality issues beyond simple row-level differences.

## Features

### Collected Statistics

For each column, the following statistics are collected:

1. **Basic Statistics**
   - Total row count
   - Null count and null rate (percentage)
   - Unique value count and cardinality
   
2. **Type-Specific Statistics**
   - **Numeric columns**: Min, max, average, percentiles (25th, 50th, 75th)
   - **Text columns**: Average length
   - **Temporal columns**: Min and max dates/timestamps
   - **Boolean columns**: Value distribution

3. **Value Distribution**
   - Top N most frequent values with counts
   - Useful for categorical data analysis

### Comparison Analysis

The feature compares statistics between source and target tables:

- **Null Rate Differences**: Identifies significant changes in null rates (>1% difference)
- **Cardinality Changes**: Detects changes in data uniqueness
- **Value Range Differences**: Compares min/max values for numeric and temporal columns
- **Average Value Changes**: Identifies shifts in numeric averages (>5% difference)

### Warnings and Alerts

Automatic warnings are generated for:
- Columns with >10% null rate difference
- Significant changes in cardinality
- Large shifts in average values

## Usage

### Enabling Column Statistics

In n8n Data Comparison node:

1. Add a Data Comparison node to your workflow
2. Configure source and target connections
3. In Advanced Options, enable "Enable Column Statistics"
4. Run the comparison

### Configuration Options

```javascript
{
  "enable_column_statistics": true,  // Enable/disable feature
  "sample_size": 10000,             // Optional: Limit statistics to sample
  "value_distribution_limit": 100    // Max unique values to track
}
```

### Output Format

The comparison result includes a `column_statistics` section:

```json
{
  "column_statistics": {
    "source_statistics": {
      "user_id": {
        "column_name": "user_id",
        "data_type": "INTEGER",
        "null_count": 0,
        "null_rate": 0.0,
        "total_count": 1000,
        "unique_count": 1000,
        "cardinality": 1.0,
        "min_value": "1",
        "max_value": "1000"
      }
    },
    "target_statistics": { ... },
    "comparison": {
      "columns": {
        "email": {
          "differences": {
            "null_rate": {
              "source": 5.0,
              "target": 15.0,
              "difference": 10.0
            }
          }
        }
      },
      "warnings": [
        "Column 'email' has significant null rate difference: 5.0% vs 15.0%"
      ],
      "summary": {
        "total_columns": 10,
        "columns_with_differences": 3
      }
    },
    "report": "Column-Level Statistics Report\n..."
  }
}
```

## Use Cases

### 1. Data Quality Monitoring
- Track null rates over time
- Monitor data completeness
- Identify data degradation

### 2. Migration Validation
- Ensure data types are preserved
- Verify value ranges remain consistent
- Check for unexpected nulls

### 3. Data Profiling
- Understand data distribution
- Identify outliers
- Analyze cardinality for optimization

### 4. Compliance Checking
- Ensure required fields are populated
- Verify data constraints are met
- Monitor sensitive data handling

## Performance Considerations

- Column statistics add overhead to comparison time
- For large tables, consider using sampling
- Statistics queries are optimized per database type
- Results are cached when possible

## Database Support

Column statistics are supported for all databases with optimized queries:
- PostgreSQL: Uses native statistical functions
- MySQL: Standard aggregate functions
- ClickZetta: Optimized for columnar storage
- Snowflake: Leverages metadata when available

## Best Practices

1. **Enable for Initial Analysis**: Use column statistics during initial data analysis
2. **Sample Large Tables**: For tables >1M rows, use sampling
3. **Monitor Key Columns**: Focus on business-critical columns
4. **Set Alerts**: Configure thresholds for automated monitoring
5. **Review Reports**: Regularly review statistics reports for trends

## Limitations

- Value distribution limited to top N values
- Some statistics may not be available for all data types
- Performance impact on very wide tables (>100 columns)
- Complex types (arrays, structs) have limited support

## Future Enhancements

- Histogram generation for numeric columns
- Correlation analysis between columns
- Anomaly detection based on historical statistics
- Custom statistical functions per column type
- Integration with data quality rules engine