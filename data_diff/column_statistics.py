"""
Column-level statistics collector for data comparison.

This module provides functionality to collect detailed statistics for each column
during data comparison, including:
- Null value counts and percentages
- Unique value counts
- Data type distribution
- Min/max values for numeric columns
- Average length for string columns
- Value frequency distribution
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import json

from data_diff.abcs.database_types import (
    ColType, NumericType, TemporalType, Text, Boolean, JSON
)


@dataclass
class ColumnStatistics:
    """Container for column-level statistics."""
    column_name: str
    data_type: str
    null_count: int = 0
    total_count: int = 0
    unique_count: Optional[int] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    avg_value: Optional[float] = None
    avg_length: Optional[float] = None
    value_distribution: Dict[str, int] = field(default_factory=dict)
    percentiles: Dict[int, Any] = field(default_factory=dict)
    
    @property
    def null_rate(self) -> float:
        """Calculate null percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.null_count / self.total_count) * 100
    
    @property
    def cardinality(self) -> float:
        """Calculate cardinality (unique values / total values)."""
        if self.total_count == 0 or self.unique_count is None:
            return 0.0
        return self.unique_count / self.total_count
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "null_count": self.null_count,
            "null_rate": round(self.null_rate, 2),
            "total_count": self.total_count,
            "unique_count": self.unique_count,
            "cardinality": round(self.cardinality, 4) if self.unique_count else None,
            "min_value": str(self.min_value) if self.min_value is not None else None,
            "max_value": str(self.max_value) if self.max_value is not None else None,
            "avg_value": round(self.avg_value, 2) if self.avg_value is not None else None,
            "avg_length": round(self.avg_length, 2) if self.avg_length is not None else None,
            "value_distribution": self.value_distribution if len(self.value_distribution) <= 20 else {
                "top_20_values": dict(sorted(
                    self.value_distribution.items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:20]),
                "total_unique_values": len(self.value_distribution)
            },
            "percentiles": self.percentiles
        }


class ColumnStatisticsCollector:
    """Collector for column-level statistics during data comparison."""
    
    def __init__(self, dialect):
        """Initialize the statistics collector.
        
        Args:
            dialect: Database dialect for SQL generation
        """
        self.dialect = dialect
        self.logger = logging.getLogger(__name__)
        
    def generate_statistics_sql(
        self, 
        table_name: str, 
        columns: List[Tuple[str, ColType]],
        sample_size: Optional[int] = None,
        where_clause: Optional[str] = None
    ) -> str:
        """Generate SQL to collect column statistics.
        
        Args:
            table_name: Name of the table
            columns: List of (column_name, column_type) tuples
            sample_size: Optional sample size limit
            where_clause: Optional WHERE clause
            
        Returns:
            SQL query string
        """
        stats_parts = []
        
        for col_name, col_type in columns:
            quoted_col = self.dialect.quote(col_name)
            # Create safe alias by removing quotes and special characters
            safe_alias = col_name.replace('"', '').replace('`', '').replace('[', '').replace(']', '')
            
            # Basic stats for all columns
            # Note: aliases must be quoted as a whole, not have quotes in the middle
            stats_parts.extend([
                f"COUNT(*) AS {self.dialect.quote(f'{safe_alias}_total')}",
                f"COUNT({quoted_col}) AS {self.dialect.quote(f'{safe_alias}_non_null')}",
                f"COUNT(DISTINCT {quoted_col}) AS {self.dialect.quote(f'{safe_alias}_unique')}"
            ])
            
            # Type-specific statistics
            if isinstance(col_type, NumericType):
                stats_parts.extend([
                    f"MIN({quoted_col}) AS {self.dialect.quote(f'{safe_alias}_min')}",
                    f"MAX({quoted_col}) AS {self.dialect.quote(f'{safe_alias}_max')}",
                    f"AVG(CAST({quoted_col} AS FLOAT)) AS {self.dialect.quote(f'{safe_alias}_avg')}"
                ])
                
                # Add percentiles for numeric columns
                if hasattr(self.dialect, 'percentile'):
                    for p in [25, 50, 75]:
                        stats_parts.append(
                            f"{self.dialect.percentile(quoted_col, p/100)} AS {self.dialect.quote(f'{safe_alias}_p{p}')}"
                        )
                        
            elif isinstance(col_type, TemporalType):
                stats_parts.extend([
                    f"MIN({quoted_col}) AS {self.dialect.quote(f'{safe_alias}_min')}",
                    f"MAX({quoted_col}) AS {self.dialect.quote(f'{safe_alias}_max')}"
                ])
                
            elif isinstance(col_type, Text):
                # Average length for text columns
                if hasattr(self.dialect, 'length_func'):
                    stats_parts.append(
                        f"AVG({self.dialect.length_func}({quoted_col})) AS {self.dialect.quote(f'{safe_alias}_avg_length')}"
                    )
                else:
                    stats_parts.append(
                        f"AVG(LENGTH({quoted_col})) AS {self.dialect.quote(f'{safe_alias}_avg_length')}"
                    )
        
        # Build the query
        query = f"SELECT {', '.join(stats_parts)} FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
            
        if sample_size and hasattr(self.dialect, 'limit_clause'):
            query = f"{query} {self.dialect.limit_clause(sample_size)}"
            
        return query
    
    def generate_value_distribution_sql(
        self,
        table_name: str,
        column_name: str,
        col_type: ColType,
        limit: int = 100,
        where_clause: Optional[str] = None
    ) -> str:
        """Generate SQL to get value distribution for a column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            col_type: Column type
            limit: Maximum number of distinct values to return
            where_clause: Optional WHERE clause
            
        Returns:
            SQL query string
        """
        quoted_col = self.dialect.quote(column_name)
        
        # For boolean columns, always get full distribution
        if isinstance(col_type, Boolean):
            limit = 10
            
        query = f"""
        SELECT {quoted_col} AS value, COUNT(*) AS count
        FROM {table_name}
        """
        
        if where_clause:
            query += f" WHERE {where_clause}"
            
        query += f"""
        GROUP BY {quoted_col}
        ORDER BY COUNT(*) DESC
        """
        
        if hasattr(self.dialect, 'limit_clause'):
            query += f" {self.dialect.limit_clause(limit)}"
        else:
            query += f" LIMIT {limit}"
            
        return query
    
    def parse_statistics_result(
        self,
        result: Dict[str, Any],
        columns: List[Tuple[str, ColType]]
    ) -> Dict[str, ColumnStatistics]:
        """Parse statistics query result into ColumnStatistics objects.
        
        Args:
            result: Query result dictionary
            columns: List of (column_name, column_type) tuples
            
        Returns:
            Dictionary mapping column names to ColumnStatistics
        """
        stats_dict = {}
        
        for col_name, col_type in columns:
            # Use same safe alias logic as in generate_statistics_sql
            safe_alias = col_name.replace('"', '').replace('`', '').replace('[', '').replace(']', '')
            
            stats = ColumnStatistics(
                column_name=col_name,
                data_type=str(col_type),
                total_count=result.get(f"{safe_alias}_total", 0),
                null_count=result.get(f"{safe_alias}_total", 0) - result.get(f"{safe_alias}_non_null", 0),
                unique_count=result.get(f"{safe_alias}_unique")
            )
            
            # Type-specific stats
            if isinstance(col_type, NumericType):
                stats.min_value = result.get(f"{safe_alias}_min")
                stats.max_value = result.get(f"{safe_alias}_max")
                stats.avg_value = result.get(f"{safe_alias}_avg")
                
                # Percentiles
                for p in [25, 50, 75]:
                    if f"{safe_alias}_p{p}" in result:
                        stats.percentiles[p] = result[f"{safe_alias}_p{p}"]
                        
            elif isinstance(col_type, TemporalType):
                stats.min_value = result.get(f"{safe_alias}_min")
                stats.max_value = result.get(f"{safe_alias}_max")
                
            elif isinstance(col_type, Text):
                stats.avg_length = result.get(f"{safe_alias}_avg_length")
            
            stats_dict[col_name] = stats
            
        return stats_dict
    
    def compare_column_statistics(
        self,
        source_stats: Dict[str, ColumnStatistics],
        target_stats: Dict[str, ColumnStatistics]
    ) -> Dict[str, Any]:
        """Compare statistics between source and target columns.
        
        Args:
            source_stats: Source table column statistics
            target_stats: Target table column statistics
            
        Returns:
            Comparison results with differences and warnings
        """
        comparison = {
            "columns": {},
            "warnings": [],
            "summary": {
                "total_columns": len(source_stats),
                "columns_with_differences": 0,
                "missing_columns": [],
                "extra_columns": []
            }
        }
        
        all_columns = set(source_stats.keys()) | set(target_stats.keys())
        
        for col in all_columns:
            if col not in source_stats:
                comparison["summary"]["extra_columns"].append(col)
                continue
            if col not in target_stats:
                comparison["summary"]["missing_columns"].append(col)
                continue
                
            source = source_stats[col]
            target = target_stats[col]
            
            col_comparison = {
                "source": source.to_dict(),
                "target": target.to_dict(),
                "differences": {}
            }
            
            # Compare null rates
            null_rate_diff = abs(source.null_rate - target.null_rate)
            if null_rate_diff > 1.0:  # More than 1% difference
                col_comparison["differences"]["null_rate"] = {
                    "source": round(source.null_rate, 2),
                    "target": round(target.null_rate, 2),
                    "difference": round(null_rate_diff, 2)
                }
                
                if null_rate_diff > 10.0:
                    comparison["warnings"].append(
                        f"Column '{col}' has significant null rate difference: "
                        f"{source.null_rate:.1f}% vs {target.null_rate:.1f}%"
                    )
            
            # Compare cardinality
            if source.unique_count and target.unique_count:
                cardinality_diff = abs(source.cardinality - target.cardinality)
                if cardinality_diff > 0.1:  # More than 10% difference
                    col_comparison["differences"]["cardinality"] = {
                        "source": round(source.cardinality, 4),
                        "target": round(target.cardinality, 4),
                        "difference": round(cardinality_diff, 4)
                    }
            
            # Compare value ranges for numeric/temporal columns
            if source.min_value is not None and target.min_value is not None:
                if source.min_value != target.min_value:
                    col_comparison["differences"]["min_value"] = {
                        "source": str(source.min_value),
                        "target": str(target.min_value)
                    }
                    
                if source.max_value != target.max_value:
                    col_comparison["differences"]["max_value"] = {
                        "source": str(source.max_value),
                        "target": str(target.max_value)
                    }
            
            # Compare average values for numeric columns
            if source.avg_value is not None and target.avg_value is not None:
                avg_diff_pct = abs(source.avg_value - target.avg_value) / max(abs(source.avg_value), 0.0001) * 100
                if avg_diff_pct > 5.0:  # More than 5% difference
                    col_comparison["differences"]["avg_value"] = {
                        "source": round(source.avg_value, 2),
                        "target": round(target.avg_value, 2),
                        "difference_pct": round(avg_diff_pct, 2)
                    }
            
            if col_comparison["differences"]:
                comparison["summary"]["columns_with_differences"] += 1
                
            comparison["columns"][col] = col_comparison
        
        return comparison
    
    def generate_statistics_report(
        self,
        source_stats: Dict[str, ColumnStatistics],
        target_stats: Dict[str, ColumnStatistics],
        comparison: Dict[str, Any]
    ) -> str:
        """Generate a human-readable statistics report.
        
        Args:
            source_stats: Source table statistics
            target_stats: Target table statistics
            comparison: Comparison results
            
        Returns:
            Formatted report string
        """
        report = ["Column-Level Statistics Report", "=" * 50, ""]
        
        # Summary
        summary = comparison["summary"]
        report.append(f"Total Columns: {summary['total_columns']}")
        report.append(f"Columns with Differences: {summary['columns_with_differences']}")
        
        if summary["missing_columns"]:
            report.append(f"Missing in Target: {', '.join(summary['missing_columns'])}")
        if summary["extra_columns"]:
            report.append(f"Extra in Target: {', '.join(summary['extra_columns'])}")
            
        report.append("")
        
        # Warnings
        if comparison["warnings"]:
            report.append("⚠️  Warnings:")
            for warning in comparison["warnings"]:
                report.append(f"  - {warning}")
            report.append("")
        
        # Detailed column statistics
        report.append("Column Details:")
        report.append("-" * 50)
        
        for col_name, col_data in comparison["columns"].items():
            if col_data.get("differences"):
                report.append(f"\n📊 {col_name}:")
                
                source = col_data["source"]
                target = col_data["target"]
                
                report.append(f"  Type: {source['data_type']}")
                report.append(f"  Null Rate: {source['null_rate']}% → {target['null_rate']}%")
                
                if source.get("unique_count"):
                    report.append(
                        f"  Unique Values: {source['unique_count']:,} → {target['unique_count']:,}"
                    )
                
                for diff_type, diff_data in col_data["differences"].items():
                    if diff_type == "null_rate":
                        report.append(
                            f"  ⚠️  Null rate difference: {diff_data['difference']}%"
                        )
                    elif diff_type == "avg_value":
                        report.append(
                            f"  ⚠️  Average value difference: {diff_data['difference_pct']}%"
                        )
        
        return "\n".join(report)