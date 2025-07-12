"""
Difference Classifier for Data-Diff N8N.

This module provides functionality to classify differences found during data comparison
and assess their severity based on various criteria.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional, Any, Tuple
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class DifferenceType(Enum):
    """Types of differences that can be found during comparison."""
    MISSING_IN_SOURCE = "missing_in_source"
    MISSING_IN_TARGET = "missing_in_target"
    VALUE_MISMATCH = "value_mismatch"
    TYPE_MISMATCH = "type_mismatch"
    NULL_MISMATCH = "null_mismatch"
    PRECISION_DIFFERENCE = "precision_difference"
    CASE_DIFFERENCE = "case_difference"
    WHITESPACE_DIFFERENCE = "whitespace_difference"
    DATE_FORMAT_DIFFERENCE = "date_format_difference"
    ENCODING_DIFFERENCE = "encoding_difference"


class SeverityLevel(Enum):
    """Severity levels for differences."""
    CRITICAL = "critical"      # Data loss or corruption
    HIGH = "high"              # Significant data issues
    MEDIUM = "medium"          # Notable differences
    LOW = "low"                # Minor differences
    INFO = "info"              # Informational only


@dataclass
class ClassifiedDifference:
    """Container for a classified difference."""
    difference_type: DifferenceType
    severity: SeverityLevel
    column_name: Optional[str]
    source_value: Any
    target_value: Any
    key_values: Dict[str, Any]
    details: str
    suggested_action: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "type": self.difference_type.value,
            "severity": self.severity.value,
            "column": self.column_name,
            "source_value": str(self.source_value) if self.source_value is not None else None,
            "target_value": str(self.target_value) if self.target_value is not None else None,
            "key_values": self.key_values,
            "details": self.details,
            "suggested_action": self.suggested_action
        }


class DifferenceClassifier:
    """Classifier for data differences."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the classifier.
        
        Args:
            config: Configuration dictionary with optional settings
        """
        self.config = config or {}
        self.case_sensitive = self.config.get('case_sensitive', True)
        self.treat_null_as_critical = self.config.get('treat_null_as_critical', False)
        self.numeric_tolerance = self.config.get('numeric_tolerance', 0.0)
        
    def classify_differences(
        self, 
        differences: List[Dict[str, Any]], 
        column_types: Optional[Dict[str, str]] = None
    ) -> List[ClassifiedDifference]:
        """
        Classify a list of differences.
        
        Args:
            differences: List of difference dictionaries
            column_types: Optional mapping of column names to their data types
            
        Returns:
            List of classified differences
        """
        classified = []
        
        for diff in differences:
            classified_diff = self._classify_single_difference(diff, column_types)
            if classified_diff:
                classified.append(classified_diff)
                
        return classified
    
    def _classify_single_difference(
        self, 
        diff: Dict[str, Any], 
        column_types: Optional[Dict[str, str]] = None
    ) -> Optional[ClassifiedDifference]:
        """Classify a single difference."""
        # Extract difference information
        if diff.get('is_duplicate'):
            # Handle duplicate key scenario
            return self._classify_duplicate(diff)
            
        # Handle regular value differences
        column_diffs = diff.get('columns', {})
        key_values = {k: v for k, v in diff.items() if k not in ['columns', 'is_duplicate']}
        
        # Classify each column difference
        for column, values in column_diffs.items():
            source_value = values.get('source')
            target_value = values.get('target')
            column_type = column_types.get(column) if column_types else None
            
            return self._classify_column_difference(
                column, source_value, target_value, key_values, column_type
            )
            
        return None
    
    def _classify_duplicate(self, diff: Dict[str, Any]) -> ClassifiedDifference:
        """Classify duplicate key differences."""
        key_values = {k: v for k, v in diff.items() if k not in ['is_duplicate', 'source_count', 'target_count']}
        source_count = diff.get('source_count', 0)
        target_count = diff.get('target_count', 0)
        
        # Determine which side has more duplicates
        if source_count > target_count:
            diff_type = DifferenceType.MISSING_IN_TARGET
            details = f"Key appears {source_count} times in source but only {target_count} times in target"
        else:
            diff_type = DifferenceType.MISSING_IN_SOURCE
            details = f"Key appears {target_count} times in target but only {source_count} times in source"
            
        return ClassifiedDifference(
            difference_type=diff_type,
            severity=SeverityLevel.HIGH,  # Duplicate keys are usually serious
            column_name=None,
            source_value=source_count,
            target_value=target_count,
            key_values=key_values,
            details=details,
            suggested_action="Investigate duplicate key values and ensure key uniqueness"
        )
    
    def _classify_column_difference(
        self,
        column: str,
        source_value: Any,
        target_value: Any,
        key_values: Dict[str, Any],
        column_type: Optional[str] = None
    ) -> ClassifiedDifference:
        """Classify a difference in a specific column."""
        # Check for null mismatches
        if (source_value is None) != (target_value is None):
            return self._classify_null_mismatch(
                column, source_value, target_value, key_values
            )
            
        # Both are null - no difference
        if source_value is None and target_value is None:
            return None
            
        # Check for type mismatches
        if type(source_value) != type(target_value):
            return self._classify_type_mismatch(
                column, source_value, target_value, key_values, column_type
            )
            
        # Check for value differences based on type
        if isinstance(source_value, (int, float)):
            return self._classify_numeric_difference(
                column, source_value, target_value, key_values, column_type
            )
        elif isinstance(source_value, str):
            return self._classify_string_difference(
                column, source_value, target_value, key_values, column_type
            )
        else:
            # Generic value mismatch
            return self._classify_value_mismatch(
                column, source_value, target_value, key_values, column_type
            )
    
    def _classify_null_mismatch(
        self, column: str, source_value: Any, target_value: Any, key_values: Dict[str, Any]
    ) -> ClassifiedDifference:
        """Classify null/not-null mismatches."""
        if source_value is None:
            details = f"Column '{column}' is NULL in source but has value in target"
            suggested_action = "Check if NULL values are expected or if data is missing in source"
        else:
            details = f"Column '{column}' has value in source but is NULL in target"
            suggested_action = "Check if NULL values are expected or if data is missing in target"
            
        severity = SeverityLevel.CRITICAL if self.treat_null_as_critical else SeverityLevel.HIGH
        
        return ClassifiedDifference(
            difference_type=DifferenceType.NULL_MISMATCH,
            severity=severity,
            column_name=column,
            source_value=source_value,
            target_value=target_value,
            key_values=key_values,
            details=details,
            suggested_action=suggested_action
        )
    
    def _classify_type_mismatch(
        self, column: str, source_value: Any, target_value: Any, 
        key_values: Dict[str, Any], column_type: Optional[str]
    ) -> ClassifiedDifference:
        """Classify type mismatches."""
        details = (f"Type mismatch in column '{column}': "
                  f"source type is {type(source_value).__name__}, "
                  f"target type is {type(target_value).__name__}")
        
        return ClassifiedDifference(
            difference_type=DifferenceType.TYPE_MISMATCH,
            severity=SeverityLevel.HIGH,
            column_name=column,
            source_value=source_value,
            target_value=target_value,
            key_values=key_values,
            details=details,
            suggested_action="Ensure consistent data types across systems"
        )
    
    def _classify_numeric_difference(
        self, column: str, source_value: float, target_value: float,
        key_values: Dict[str, Any], column_type: Optional[str]
    ) -> ClassifiedDifference:
        """Classify numeric differences."""
        abs_diff = abs(source_value - target_value)
        
        # Check if within tolerance
        if self.numeric_tolerance > 0 and abs_diff <= self.numeric_tolerance:
            return ClassifiedDifference(
                difference_type=DifferenceType.PRECISION_DIFFERENCE,
                severity=SeverityLevel.INFO,
                column_name=column,
                source_value=source_value,
                target_value=target_value,
                key_values=key_values,
                details=f"Numeric difference within tolerance ({abs_diff:.6f})",
                suggested_action="No action needed - difference within acceptable tolerance"
            )
        
        # Determine severity based on magnitude
        rel_diff = abs_diff / max(abs(source_value), abs(target_value)) if max(abs(source_value), abs(target_value)) > 0 else abs_diff
        
        if rel_diff > 0.1:  # More than 10% difference
            severity = SeverityLevel.HIGH
        elif rel_diff > 0.01:  # More than 1% difference
            severity = SeverityLevel.MEDIUM
        else:
            severity = SeverityLevel.LOW
            
        return ClassifiedDifference(
            difference_type=DifferenceType.VALUE_MISMATCH,
            severity=severity,
            column_name=column,
            source_value=source_value,
            target_value=target_value,
            key_values=key_values,
            details=f"Numeric difference: {abs_diff:.6f} ({rel_diff*100:.2f}% relative difference)",
            suggested_action="Investigate calculation or rounding differences"
        )
    
    def _classify_string_difference(
        self, column: str, source_value: str, target_value: str,
        key_values: Dict[str, Any], column_type: Optional[str]
    ) -> ClassifiedDifference:
        """Classify string differences."""
        # Check for case differences
        if source_value.lower() == target_value.lower():
            return ClassifiedDifference(
                difference_type=DifferenceType.CASE_DIFFERENCE,
                severity=SeverityLevel.LOW if not self.case_sensitive else SeverityLevel.MEDIUM,
                column_name=column,
                source_value=source_value,
                target_value=target_value,
                key_values=key_values,
                details="Values differ only in case",
                suggested_action="Standardize text case if case-insensitive comparison is desired"
            )
            
        # Check for whitespace differences
        if source_value.strip() == target_value.strip():
            return ClassifiedDifference(
                difference_type=DifferenceType.WHITESPACE_DIFFERENCE,
                severity=SeverityLevel.LOW,
                column_name=column,
                source_value=source_value,
                target_value=target_value,
                key_values=key_values,
                details="Values differ only in leading/trailing whitespace",
                suggested_action="Trim whitespace if not significant"
            )
            
        # Check if it's a date/time column
        if column_type and 'date' in column_type.lower() or 'time' in column_type.lower():
            return ClassifiedDifference(
                difference_type=DifferenceType.DATE_FORMAT_DIFFERENCE,
                severity=SeverityLevel.MEDIUM,
                column_name=column,
                source_value=source_value,
                target_value=target_value,
                key_values=key_values,
                details="Date/time values differ - possibly different formats",
                suggested_action="Standardize date/time formats across systems"
            )
            
        # General string mismatch
        return ClassifiedDifference(
            difference_type=DifferenceType.VALUE_MISMATCH,
            severity=SeverityLevel.MEDIUM,
            column_name=column,
            source_value=source_value,
            target_value=target_value,
            key_values=key_values,
            details="String values differ",
            suggested_action="Verify data accuracy and consistency"
        )
    
    def _classify_value_mismatch(
        self, column: str, source_value: Any, target_value: Any,
        key_values: Dict[str, Any], column_type: Optional[str]
    ) -> ClassifiedDifference:
        """Generic value mismatch classification."""
        return ClassifiedDifference(
            difference_type=DifferenceType.VALUE_MISMATCH,
            severity=SeverityLevel.MEDIUM,
            column_name=column,
            source_value=source_value,
            target_value=target_value,
            key_values=key_values,
            details=f"Values differ in column '{column}'",
            suggested_action="Investigate data discrepancy"
        )
    
    def generate_summary(self, classified_differences: List[ClassifiedDifference]) -> Dict[str, Any]:
        """
        Generate a summary of classified differences.
        
        Args:
            classified_differences: List of classified differences
            
        Returns:
            Summary dictionary with statistics
        """
        summary = {
            "total_differences": len(classified_differences),
            "by_type": defaultdict(int),
            "by_severity": defaultdict(int),
            "by_column": defaultdict(lambda: defaultdict(int)),
            "critical_issues": [],
            "recommendations": []
        }
        
        for diff in classified_differences:
            summary["by_type"][diff.difference_type.value] += 1
            summary["by_severity"][diff.severity.value] += 1
            
            if diff.column_name:
                summary["by_column"][diff.column_name][diff.difference_type.value] += 1
                
            if diff.severity == SeverityLevel.CRITICAL:
                summary["critical_issues"].append({
                    "column": diff.column_name,
                    "type": diff.difference_type.value,
                    "details": diff.details
                })
        
        # Generate recommendations based on patterns
        summary["recommendations"] = self._generate_recommendations(summary)
        
        # Convert defaultdicts to regular dicts for JSON serialization
        summary["by_type"] = dict(summary["by_type"])
        summary["by_severity"] = dict(summary["by_severity"])
        summary["by_column"] = {k: dict(v) for k, v in summary["by_column"].items()}
        
        return summary
    
    def _generate_recommendations(self, summary: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on the summary."""
        recommendations = []
        
        # Check for critical issues
        if summary["critical_issues"]:
            recommendations.append(
                f"⚠️ Found {len(summary['critical_issues'])} critical issues that require immediate attention"
            )
        
        # Check for null mismatches
        null_count = summary["by_type"].get(DifferenceType.NULL_MISMATCH.value, 0)
        if null_count > 0:
            recommendations.append(
                f"🔍 Found {null_count} NULL value mismatches - review data completeness"
            )
        
        # Check for type mismatches
        type_count = summary["by_type"].get(DifferenceType.TYPE_MISMATCH.value, 0)
        if type_count > 0:
            recommendations.append(
                f"🔧 Found {type_count} type mismatches - ensure consistent data types"
            )
        
        # Check for case/whitespace issues
        case_count = summary["by_type"].get(DifferenceType.CASE_DIFFERENCE.value, 0)
        ws_count = summary["by_type"].get(DifferenceType.WHITESPACE_DIFFERENCE.value, 0)
        if case_count + ws_count > 0:
            recommendations.append(
                f"📝 Found {case_count + ws_count} case/whitespace differences - consider data normalization"
            )
        
        # Add general recommendation if many differences
        total = summary["total_differences"]
        if total > 100:
            recommendations.append(
                f"📊 Large number of differences ({total}) - consider reviewing data pipeline"
            )
            
        return recommendations