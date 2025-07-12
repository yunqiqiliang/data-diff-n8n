# Timeline Analysis Feature

## Overview

The timeline analysis feature provides insights into how data differences evolve over time. It helps identify patterns, trends, and anomalies in temporal data by analyzing differences across configurable time windows.

## Key Features

### 1. Automatic Time Window Detection
- Automatically determines appropriate time bucket sizes based on data range
- Supports various granularities: hourly, daily, weekly, monthly, yearly
- Intelligently adjusts to provide meaningful insights

### 2. Difference Trend Analysis
- Tracks match rates over time periods
- Identifies improving or degrading data quality trends
- Highlights periods with significant issues

### 3. Pattern Detection
- **Improving Quality**: Detects when data quality is getting better over time
- **Degrading Quality**: Alerts when quality is declining
- **Periodic Issues**: Identifies recurring patterns of data problems
- **Data Gaps**: Finds time periods with missing data

### 4. Visual-Ready Output
- Provides data formatted for chart visualization
- Includes both line charts for trends and bar charts for differences
- Annotations for low-quality periods

## Usage

### Configuration in n8n

1. In the Data Comparison node's Advanced Options
2. Expand "Timeline Analysis" section
3. Configure the following:

```javascript
{
  "timelineColumn": "created_at",        // Required: timestamp column
  "timelineStartTime": "2024-01-01",     // Optional: analysis start
  "timelineEndTime": "2024-12-31",       // Optional: analysis end
  "timelineBuckets": 20,                 // Number of time periods
  "timelineMaxDifferences": 10000        // Max differences to analyze
}
```

### Output Structure

```json
{
  "timeline_analysis": {
    "summary": {
      "time_column": "created_at",
      "total_time_periods": 20,
      "average_match_rate": 95.5,
      "time_range": {
        "start": "2024-01-01T00:00:00",
        "end": "2024-12-31T23:59:59"
      }
    },
    "timeline": [
      {
        "window_start": "2024-01-01T00:00:00",
        "window_end": "2024-01-31T23:59:59",
        "window_label": "2024-01",
        "total_rows": 5000,
        "differences": 250,
        "match_rate": 95.0
      }
    ],
    "worst_periods": [
      {
        "period": "2024-03",
        "match_rate": 85.2,
        "differences": 1480
      }
    ],
    "patterns": [
      {
        "type": "degrading_quality",
        "description": "Data quality is degrading over time",
        "confidence": "high"
      }
    ],
    "visualization": {
      "chart_type": "line",
      "x_axis": {
        "label": "Time Period",
        "data": ["2024-01", "2024-02", ...]
      },
      "y_axis": [
        {
          "label": "Match Rate (%)",
          "data": [95.0, 94.2, ...],
          "color": "#27ae60"
        }
      ]
    }
  }
}
```

## Pattern Types

### 1. Quality Trends
- **Improving**: Match rate increasing over recent periods
- **Degrading**: Match rate decreasing over recent periods
- **Stable**: Consistent match rate across periods

### 2. Periodic Patterns
- Detects recurring issues at regular intervals
- Useful for identifying batch processing problems
- Helps plan maintenance windows

### 3. Data Gaps
- Identifies periods with no data
- Highlights potential data pipeline failures
- Useful for SLA monitoring

## Use Cases

### 1. Data Pipeline Monitoring
- Track data quality over time
- Identify degradation before it becomes critical
- Monitor the impact of system changes

### 2. Migration Validation
- Ensure data quality remains consistent during migration
- Identify time periods that need re-processing
- Validate incremental sync processes

### 3. Compliance Reporting
- Generate quality metrics over reporting periods
- Document data integrity trends
- Provide evidence of data governance

### 4. Troubleshooting
- Quickly identify when issues started
- Correlate quality drops with system events
- Focus investigation on problematic periods

## Best Practices

### 1. Column Selection
- Choose columns with good time coverage
- Prefer creation/update timestamps over business dates
- Ensure timezone consistency

### 2. Time Range Configuration
- Start with auto-detection for initial analysis
- Narrow down to specific periods for detailed investigation
- Consider data volume when setting bucket counts

### 3. Performance Optimization
- Limit `timelineMaxDifferences` for large datasets
- Use sampling for initial exploration
- Consider indexing time columns

### 4. Result Interpretation
- Look for sudden drops in match rate
- Pay attention to detected patterns
- Correlate with known system events

## Integration with Other Features

### Works Best With:
- **Column Statistics**: Combine for comprehensive quality view
- **Sampling**: Use sampling for large time ranges
- **Float Tolerance**: Apply tolerance for time-series metrics

### Visualization Options:
- Export to Grafana for real-time dashboards
- Use with Jupyter notebooks for deeper analysis
- Integrate with alerting systems for proactive monitoring

## Limitations

- Requires a timestamp column in the data
- Performance impact on very large datasets
- Pattern detection accuracy depends on data volume
- Time bucket granularity limited by data distribution

## Future Enhancements

- Real-time streaming analysis
- Machine learning-based anomaly detection
- Predictive quality forecasting
- Custom pattern definition support
- Integration with time-series databases