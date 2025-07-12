"""
Timeline analyzer for time-series data comparison.

This module provides functionality to analyze differences over time,
helping identify patterns, trends, and anomalies in temporal data.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import json

from data_diff.abcs.database_types import TemporalType


@dataclass
class TimeWindow:
    """Represents a time window for analysis."""
    start: datetime
    end: datetime
    window_size: timedelta
    
    @property
    def label(self) -> str:
        """Generate a human-readable label for this window."""
        if self.window_size.days >= 365:
            return self.start.strftime("%Y")
        elif self.window_size.days >= 28:
            return self.start.strftime("%Y-%m")
        elif self.window_size.days >= 1:
            return self.start.strftime("%Y-%m-%d")
        elif self.window_size.seconds >= 3600:
            return self.start.strftime("%Y-%m-%d %H:00")
        else:
            return self.start.strftime("%Y-%m-%d %H:%M")


@dataclass
class TimelineBucket:
    """Statistics for a specific time bucket."""
    window: TimeWindow
    total_rows: int = 0
    differences: int = 0
    missing_in_source: int = 0
    missing_in_target: int = 0
    value_differences: int = 0
    match_rate: float = 0.0
    
    def add_difference(self, diff_type: str):
        """Add a difference to this bucket."""
        self.differences += 1
        if diff_type == "missing_in_source":
            self.missing_in_source += 1
        elif diff_type == "missing_in_target":
            self.missing_in_target += 1
        elif diff_type == "value_different":
            self.value_differences += 1
    
    def calculate_match_rate(self):
        """Calculate the match rate for this bucket."""
        if self.total_rows > 0:
            self.match_rate = 1.0 - (self.differences / self.total_rows)
        else:
            self.match_rate = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "window_start": self.window.start.isoformat(),
            "window_end": self.window.end.isoformat(),
            "window_label": self.window.label,
            "total_rows": self.total_rows,
            "differences": self.differences,
            "missing_in_source": self.missing_in_source,
            "missing_in_target": self.missing_in_target,
            "value_differences": self.value_differences,
            "match_rate": round(self.match_rate * 100, 2)
        }


class TimelineAnalyzer:
    """Analyzer for time-series data comparison."""
    
    def __init__(self, time_column: str, dialect):
        """Initialize the timeline analyzer.
        
        Args:
            time_column: Name of the timestamp column to analyze
            dialect: Database dialect for SQL generation
        """
        self.time_column = time_column
        self.dialect = dialect
        self.logger = logging.getLogger(__name__)
        
    def determine_window_size(
        self, 
        start_time: datetime, 
        end_time: datetime,
        target_buckets: int = 20
    ) -> timedelta:
        """Determine appropriate window size based on time range.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            target_buckets: Desired number of buckets
            
        Returns:
            Appropriate window size as timedelta
        """
        time_range = end_time - start_time
        
        # Calculate ideal bucket size
        ideal_seconds = time_range.total_seconds() / target_buckets
        
        # Round to sensible intervals
        if ideal_seconds < 60:  # Less than 1 minute
            return timedelta(minutes=1)
        elif ideal_seconds < 3600:  # Less than 1 hour
            minutes = max(5, int(ideal_seconds / 60 / 5) * 5)  # Round to 5 minutes
            return timedelta(minutes=minutes)
        elif ideal_seconds < 86400:  # Less than 1 day
            hours = max(1, int(ideal_seconds / 3600))
            return timedelta(hours=hours)
        elif ideal_seconds < 604800:  # Less than 1 week
            days = max(1, int(ideal_seconds / 86400))
            return timedelta(days=days)
        elif ideal_seconds < 2592000:  # Less than 30 days
            return timedelta(days=7)  # Weekly
        elif ideal_seconds < 31536000:  # Less than 1 year
            return timedelta(days=30)  # Monthly
        else:
            return timedelta(days=365)  # Yearly
    
    def create_time_buckets(
        self,
        start_time: datetime,
        end_time: datetime,
        window_size: timedelta
    ) -> List[TimeWindow]:
        """Create time buckets for analysis.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            window_size: Size of each time window
            
        Returns:
            List of TimeWindow objects
        """
        buckets = []
        current_time = start_time
        
        while current_time < end_time:
            window_end = min(current_time + window_size, end_time)
            buckets.append(TimeWindow(
                start=current_time,
                end=window_end,
                window_size=window_size
            ))
            current_time = window_end
            
        return buckets
    
    def generate_timeline_sql(
        self,
        table_name: str,
        time_column: str,
        start_time: datetime,
        end_time: datetime,
        window_size: timedelta,
        where_clause: Optional[str] = None
    ) -> str:
        """Generate SQL to get time distribution of data.
        
        Args:
            table_name: Name of the table
            time_column: Name of the timestamp column
            start_time: Start of time range
            end_time: End of time range
            window_size: Size of time windows
            where_clause: Optional WHERE clause
            
        Returns:
            SQL query string
        """
        quoted_col = self.dialect.quote(time_column)
        
        # Determine the date truncation based on window size
        if window_size.days >= 365:
            date_trunc = "YEAR"
        elif window_size.days >= 28:
            date_trunc = "MONTH"
        elif window_size.days >= 1:
            date_trunc = "DAY"
        else:
            date_trunc = "HOUR"
        
        # Build SQL based on database type
        if hasattr(self.dialect, 'date_trunc'):
            # PostgreSQL style
            time_bucket = f"DATE_TRUNC('{date_trunc}', {quoted_col})"
        else:
            # Standard SQL fallback
            if date_trunc == "YEAR":
                time_bucket = f"DATE_FORMAT({quoted_col}, '%Y-01-01')"
            elif date_trunc == "MONTH":
                time_bucket = f"DATE_FORMAT({quoted_col}, '%Y-%m-01')"
            elif date_trunc == "DAY":
                time_bucket = f"DATE({quoted_col})"
            else:
                time_bucket = f"DATE_FORMAT({quoted_col}, '%Y-%m-%d %H:00:00')"
        
        query = f"""
        SELECT 
            {time_bucket} as time_bucket,
            COUNT(*) as row_count,
            MIN({quoted_col}) as min_time,
            MAX({quoted_col}) as max_time
        FROM {table_name}
        WHERE {quoted_col} >= '{start_time.isoformat()}'
          AND {quoted_col} < '{end_time.isoformat()}'
        """
        
        if where_clause:
            query += f" AND {where_clause}"
            
        query += f"""
        GROUP BY {time_bucket}
        ORDER BY {time_bucket}
        """
        
        return query
    
    def analyze_differences(
        self,
        differences: List[Dict[str, Any]],
        time_buckets: List[TimeWindow],
        time_column: str
    ) -> Dict[str, TimelineBucket]:
        """Analyze differences and group by time buckets.
        
        Args:
            differences: List of difference records
            time_buckets: List of time windows
            time_column: Name of timestamp column
            
        Returns:
            Dictionary mapping bucket labels to TimelineBucket objects
        """
        # Initialize buckets
        bucket_map = {}
        for window in time_buckets:
            bucket = TimelineBucket(window=window)
            bucket_map[window.label] = bucket
        
        # Process differences
        for diff in differences:
            # Get timestamp from the difference record
            timestamp = None
            
            # Try to get timestamp from source or target row
            if diff.get("source_row") and time_column in diff["source_row"]:
                timestamp = diff["source_row"][time_column]
            elif diff.get("target_row") and time_column in diff["target_row"]:
                timestamp = diff["target_row"][time_column]
            
            if timestamp:
                # Parse timestamp if it's a string
                if isinstance(timestamp, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except:
                        continue
                
                # Find the appropriate bucket
                for window in time_buckets:
                    if window.start <= timestamp < window.end:
                        bucket = bucket_map[window.label]
                        bucket.add_difference(diff.get("type", "unknown"))
                        break
        
        # Calculate match rates
        for bucket in bucket_map.values():
            bucket.calculate_match_rate()
        
        return bucket_map
    
    def generate_timeline_report(
        self,
        timeline_data: Dict[str, TimelineBucket],
        source_table: str,
        target_table: str
    ) -> Dict[str, Any]:
        """Generate a comprehensive timeline report.
        
        Args:
            timeline_data: Timeline analysis results
            source_table: Source table name
            target_table: Target table name
            
        Returns:
            Timeline report dictionary
        """
        # Sort buckets by time
        sorted_buckets = sorted(
            timeline_data.values(),
            key=lambda b: b.window.start
        )
        
        # Calculate trends
        match_rates = [b.match_rate for b in sorted_buckets]
        avg_match_rate = sum(match_rates) / len(match_rates) if match_rates else 0
        
        # Find periods with most issues
        worst_periods = sorted(
            sorted_buckets,
            key=lambda b: b.match_rate
        )[:5]
        
        # Detect patterns
        patterns = self._detect_patterns(sorted_buckets)
        
        return {
            "summary": {
                "time_column": self.time_column,
                "source_table": source_table,
                "target_table": target_table,
                "total_time_periods": len(sorted_buckets),
                "average_match_rate": round(avg_match_rate * 100, 2),
                "time_range": {
                    "start": sorted_buckets[0].window.start.isoformat() if sorted_buckets else None,
                    "end": sorted_buckets[-1].window.end.isoformat() if sorted_buckets else None
                }
            },
            "timeline": [bucket.to_dict() for bucket in sorted_buckets],
            "worst_periods": [
                {
                    "period": period.window.label,
                    "match_rate": round(period.match_rate * 100, 2),
                    "differences": period.differences
                }
                for period in worst_periods
            ],
            "patterns": patterns,
            "visualization": self._generate_visualization_data(sorted_buckets)
        }
    
    def _detect_patterns(self, buckets: List[TimelineBucket]) -> List[Dict[str, Any]]:
        """Detect patterns in timeline data.
        
        Args:
            buckets: Sorted list of timeline buckets
            
        Returns:
            List of detected patterns
        """
        patterns = []
        
        if not buckets:
            return patterns
        
        # Detect trending (improving or degrading match rate)
        if len(buckets) >= 3:
            recent_rates = [b.match_rate for b in buckets[-3:]]
            older_rates = [b.match_rate for b in buckets[-6:-3]] if len(buckets) >= 6 else [b.match_rate for b in buckets[:3]]
            
            recent_avg = sum(recent_rates) / len(recent_rates)
            older_avg = sum(older_rates) / len(older_rates)
            
            if recent_avg > older_avg + 0.05:
                patterns.append({
                    "type": "improving_quality",
                    "description": "Data quality is improving over time",
                    "confidence": "high" if recent_avg > older_avg + 0.1 else "medium"
                })
            elif recent_avg < older_avg - 0.05:
                patterns.append({
                    "type": "degrading_quality",
                    "description": "Data quality is degrading over time",
                    "confidence": "high" if recent_avg < older_avg - 0.1 else "medium"
                })
        
        # Detect periodic issues
        low_quality_periods = [i for i, b in enumerate(buckets) if b.match_rate < 0.9]
        if len(low_quality_periods) >= 2:
            intervals = [low_quality_periods[i+1] - low_quality_periods[i] for i in range(len(low_quality_periods)-1)]
            if intervals and all(abs(interval - intervals[0]) <= 1 for interval in intervals):
                patterns.append({
                    "type": "periodic_issues",
                    "description": f"Quality issues occur periodically every {intervals[0]} time periods",
                    "confidence": "medium"
                })
        
        # Detect data gaps
        empty_periods = [b for b in buckets if b.total_rows == 0]
        if empty_periods:
            patterns.append({
                "type": "data_gaps",
                "description": f"Found {len(empty_periods)} time periods with no data",
                "periods": [p.window.label for p in empty_periods[:5]]
            })
        
        return patterns
    
    def _generate_visualization_data(self, buckets: List[TimelineBucket]) -> Dict[str, Any]:
        """Generate data suitable for visualization.
        
        Args:
            buckets: Sorted list of timeline buckets
            
        Returns:
            Visualization data dictionary
        """
        return {
            "chart_type": "line",
            "x_axis": {
                "label": "Time Period",
                "data": [b.window.label for b in buckets]
            },
            "y_axis": [
                {
                    "label": "Match Rate (%)",
                    "data": [round(b.match_rate * 100, 2) for b in buckets],
                    "color": "#27ae60"
                },
                {
                    "label": "Differences",
                    "data": [b.differences for b in buckets],
                    "color": "#e74c3c",
                    "type": "bar"
                }
            ],
            "annotations": [
                {
                    "x": b.window.label,
                    "label": f"Low quality: {round(b.match_rate * 100, 1)}%",
                    "color": "#e74c3c"
                }
                for b in buckets if b.match_rate < 0.9
            ]
        }
    
    def generate_timeline_config(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        window_size: Optional[Union[str, timedelta]] = None,
        auto_detect_range: bool = True
    ) -> Dict[str, Any]:
        """Generate configuration for timeline analysis.
        
        Args:
            start_time: Start of analysis range
            end_time: End of analysis range
            window_size: Size of time windows (e.g., "1h", "1d", "1w")
            auto_detect_range: Whether to auto-detect time range from data
            
        Returns:
            Timeline configuration dictionary
        """
        config = {
            "time_column": self.time_column,
            "auto_detect_range": auto_detect_range
        }
        
        if start_time:
            config["start_time"] = start_time.isoformat()
        if end_time:
            config["end_time"] = end_time.isoformat()
            
        if window_size:
            if isinstance(window_size, str):
                # Parse string like "1h", "1d", "1w"
                config["window_size"] = window_size
            else:
                config["window_size_seconds"] = int(window_size.total_seconds())
        
        return config