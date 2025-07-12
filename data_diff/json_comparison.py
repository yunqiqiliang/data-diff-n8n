"""
JSON comparison functionality for data-diff
Handles intelligent comparison of JSON columns across databases
"""

from typing import Optional, Tuple, Any
import logging

from data_diff.abcs.database_types import ColType, JSON
from data_diff.databases.base import BaseDialect

logger = logging.getLogger(__name__)


class JSONComparisonMode:
    """JSON comparison modes"""
    EXACT = "exact"              # Exact string match
    NORMALIZED = "normalized"    # Normalize whitespace and formatting
    SEMANTIC = "semantic"        # Semantic comparison (order-insensitive)
    KEYS_ONLY = "keys_only"      # Compare only top-level keys
    
    ALL_MODES = [EXACT, NORMALIZED, SEMANTIC, KEYS_ONLY]


class JSONComparisonManager:
    """
    Manages JSON column comparison with different modes
    """
    
    def __init__(self, mode: str = JSONComparisonMode.NORMALIZED):
        if mode not in JSONComparisonMode.ALL_MODES:
            raise ValueError(f"Invalid JSON comparison mode: {mode}. Must be one of {JSONComparisonMode.ALL_MODES}")
        self.mode = mode
        logger.info(f"JSONComparisonManager initialized with mode: {mode}")
    
    def should_handle_column(self, coltype: ColType) -> bool:
        """Check if this column should be handled by JSON comparison"""
        return isinstance(coltype, JSON)
    
    def generate_comparison_sql(self, col1: str, col2: str, coltype: ColType, dialect: BaseDialect) -> str:
        """
        Generate SQL for JSON comparison based on the selected mode
        
        Args:
            col1: First column expression
            col2: Second column expression  
            coltype: Column type (should be JSON)
            dialect: Database dialect
            
        Returns:
            SQL expression that returns boolean indicating if values are different
        """
        if not self.should_handle_column(coltype):
            raise ValueError(f"Column type {coltype} is not a JSON type")
        
        # For databases that don't support JSON, fall back to string comparison
        if not hasattr(dialect, 'normalize_json'):
            logger.warning(f"{dialect.name} does not support JSON normalization, using string comparison")
            return dialect.is_distinct_from(col1, col2)
        
        if self.mode == JSONComparisonMode.EXACT:
            # Exact string comparison
            return dialect.is_distinct_from(col1, col2)
        
        elif self.mode == JSONComparisonMode.NORMALIZED:
            # Normalize JSON before comparison
            norm1 = dialect.normalize_json(col1, coltype)
            norm2 = dialect.normalize_json(col2, coltype)
            return dialect.is_distinct_from(norm1, norm2)
        
        elif self.mode == JSONComparisonMode.SEMANTIC:
            # Semantic comparison (database-specific)
            return self._generate_semantic_comparison(col1, col2, dialect)
        
        elif self.mode == JSONComparisonMode.KEYS_ONLY:
            # Compare only top-level keys
            return self._generate_keys_comparison(col1, col2, dialect)
        
        else:
            raise ValueError(f"Unexpected JSON comparison mode: {self.mode}")
    
    def _generate_semantic_comparison(self, col1: str, col2: str, dialect: BaseDialect) -> str:
        """
        Generate semantic JSON comparison SQL
        
        This is database-specific and may not be supported by all databases
        """
        db_name = dialect.name.lower()
        
        if db_name == "postgresql":
            # PostgreSQL supports JSON equality operator
            return f"({col1}::jsonb IS DISTINCT FROM {col2}::jsonb)"
        
        elif db_name == "mysql":
            # MySQL 5.7+ has JSON_EQUALS (hypothetical, need to verify)
            # For now, fall back to normalized comparison
            logger.info(f"Semantic JSON comparison not fully supported in {dialect.name}, using normalized comparison")
            norm1 = dialect.normalize_json(col1, JSON())
            norm2 = dialect.normalize_json(col2, JSON())
            return dialect.is_distinct_from(norm1, norm2)
        
        elif db_name == "bigquery":
            # BigQuery can parse JSON and compare
            # TO_JSON_STRING normalizes the JSON
            return f"(TO_JSON_STRING({col1}) IS DISTINCT FROM TO_JSON_STRING({col2}))"
        
        elif db_name == "snowflake":
            # Snowflake VARIANT type comparison
            return f"({col1} != {col2} OR ({col1} IS NULL) != ({col2} IS NULL))"
        
        else:
            # Fall back to normalized comparison for other databases
            logger.info(f"Semantic JSON comparison not supported in {dialect.name}, using normalized comparison")
            norm1 = dialect.normalize_json(col1, JSON())
            norm2 = dialect.normalize_json(col2, JSON())
            return dialect.is_distinct_from(norm1, norm2)
    
    def _generate_keys_comparison(self, col1: str, col2: str, dialect: BaseDialect) -> str:
        """
        Generate SQL to compare only top-level JSON keys
        
        This is useful when you only care about schema differences, not values
        """
        db_name = dialect.name.lower()
        
        if db_name == "postgresql":
            # Use json_object_keys() function
            keys1 = f"ARRAY(SELECT json_object_keys({col1}::json) ORDER BY 1)"
            keys2 = f"ARRAY(SELECT json_object_keys({col2}::json) ORDER BY 1)"
            return f"({keys1} IS DISTINCT FROM {keys2})"
        
        elif db_name == "mysql":
            # Use JSON_KEYS() function
            return f"(JSON_KEYS({col1}) IS DISTINCT FROM JSON_KEYS({col2}))"
        
        elif db_name == "bigquery":
            # BigQuery doesn't have a direct JSON_KEYS function
            # Fall back to full comparison
            logger.info(f"Keys-only JSON comparison not supported in {dialect.name}, using normalized comparison")
            return self._generate_semantic_comparison(col1, col2, dialect)
        
        elif db_name == "snowflake":
            # Snowflake OBJECT_KEYS function
            return f"(ARRAY_SORT(OBJECT_KEYS({col1})) != ARRAY_SORT(OBJECT_KEYS({col2})) OR ({col1} IS NULL) != ({col2} IS NULL))"
        
        else:
            # Fall back to normalized comparison
            logger.info(f"Keys-only JSON comparison not supported in {dialect.name}, using normalized comparison")
            norm1 = dialect.normalize_json(col1, JSON())
            norm2 = dialect.normalize_json(col2, JSON())
            return dialect.is_distinct_from(norm1, norm2)
    
    def get_mode_description(self) -> str:
        """Get a description of the current comparison mode"""
        descriptions = {
            JSONComparisonMode.EXACT: "Exact string match - JSON must be identical including whitespace",
            JSONComparisonMode.NORMALIZED: "Normalized comparison - ignores whitespace and formatting differences",
            JSONComparisonMode.SEMANTIC: "Semantic comparison - order-insensitive, handles nested structures",
            JSONComparisonMode.KEYS_ONLY: "Keys-only comparison - compares structure without values"
        }
        return descriptions.get(self.mode, "Unknown mode")
    
    def validate_database_support(self, db1_name: str, db2_name: str) -> Tuple[bool, Optional[str]]:
        """
        Check if the databases support the requested JSON comparison mode
        
        Returns:
            Tuple of (is_supported, warning_message)
        """
        warnings = []
        
        # Databases with limited JSON support
        limited_json_dbs = ['clickzetta', 'oracle', 'databricks', 'duckdb', 'vertica']
        
        db1_lower = db1_name.lower()
        db2_lower = db2_name.lower()
        
        if any(db in db1_lower for db in limited_json_dbs) or any(db in db2_lower for db in limited_json_dbs):
            warnings.append(f"One or both databases may have limited JSON support. JSON will be compared as strings.")
        
        if self.mode == JSONComparisonMode.SEMANTIC:
            # Check specific database support for semantic comparison
            semantic_supported = ['postgresql', 'mysql', 'bigquery', 'snowflake']
            if not (any(db in db1_lower for db in semantic_supported) and 
                   any(db in db2_lower for db in semantic_supported)):
                warnings.append(f"Semantic JSON comparison may fall back to normalized comparison for {db1_name} and {db2_name}")
        
        if self.mode == JSONComparisonMode.KEYS_ONLY:
            # Check specific database support for keys comparison
            keys_supported = ['postgresql', 'mysql', 'snowflake']
            if not (any(db in db1_lower for db in keys_supported) and 
                   any(db in db2_lower for db in keys_supported)):
                warnings.append(f"Keys-only JSON comparison may fall back to normalized comparison for {db1_name} and {db2_name}")
        
        warning_msg = " ".join(warnings) if warnings else None
        return True, warning_msg