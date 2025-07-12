"""
Column remapping functionality for data-diff
Allows comparing tables with different column names
"""

from typing import Dict, List, Tuple, Optional, Union
import logging

logger = logging.getLogger(__name__)


class ColumnRemapper:
    """
    Manages column name mappings between two tables
    
    Supports:
    - Simple 1:1 column mappings
    - Case-insensitive matching option
    - Automatic mapping suggestions based on similarity
    """
    
    def __init__(self, mappings: Optional[Dict[str, str]] = None, case_sensitive: bool = True):
        """
        Initialize column remapper
        
        Args:
            mappings: Dictionary mapping source column names to target column names
                     e.g., {"user_id": "customer_id", "created_at": "creation_date"}
            case_sensitive: Whether column name matching should be case-sensitive
        """
        self.mappings = mappings or {}
        self.case_sensitive = case_sensitive
        self.reverse_mappings = {v: k for k, v in self.mappings.items()}
        
        if not case_sensitive:
            # Create lowercase mappings for case-insensitive matching
            self._lower_mappings = {k.lower(): v for k, v in self.mappings.items()}
            self._lower_reverse_mappings = {v.lower(): k for v, k in self.reverse_mappings.items()}
        
        logger.info(f"ColumnRemapper initialized with {len(self.mappings)} mappings, case_sensitive={case_sensitive}")
    
    def map_column(self, column: str, reverse: bool = False) -> str:
        """
        Map a column name according to the configured mappings
        
        Args:
            column: The column name to map
            reverse: If True, map from target to source instead of source to target
            
        Returns:
            The mapped column name, or the original name if no mapping exists
        """
        if reverse:
            mappings = self.reverse_mappings
            lower_mappings = getattr(self, '_lower_reverse_mappings', {})
        else:
            mappings = self.mappings
            lower_mappings = getattr(self, '_lower_mappings', {})
        
        # Try exact match first
        if column in mappings:
            return mappings[column]
        
        # Try case-insensitive match if enabled
        if not self.case_sensitive and column.lower() in lower_mappings:
            return lower_mappings[column.lower()]
        
        # No mapping found, return original
        return column
    
    def map_columns(self, columns: List[str], reverse: bool = False) -> List[str]:
        """
        Map a list of column names
        
        Args:
            columns: List of column names to map
            reverse: If True, map from target to source
            
        Returns:
            List of mapped column names
        """
        return [self.map_column(col, reverse) for col in columns]
    
    def get_mapped_pairs(self, source_columns: List[str], target_columns: List[str]) -> List[Tuple[str, str]]:
        """
        Get pairs of columns that should be compared based on mappings
        
        Args:
            source_columns: Columns from the source table
            target_columns: Columns from the target table
            
        Returns:
            List of (source_column, target_column) pairs to compare
        """
        pairs = []
        source_set = set(source_columns)
        target_set = set(target_columns)
        
        if not self.case_sensitive:
            source_lower = {col.lower(): col for col in source_columns}
            target_lower = {col.lower(): col for col in target_columns}
        
        # Process explicit mappings
        for src_col in source_columns:
            mapped_col = self.map_column(src_col)
            
            # Check if mapped column exists in target
            if mapped_col in target_set:
                pairs.append((src_col, mapped_col))
            elif not self.case_sensitive:
                # Try case-insensitive match
                if mapped_col.lower() in target_lower:
                    pairs.append((src_col, target_lower[mapped_col.lower()]))
        
        # Add unmapped columns that match exactly (or case-insensitively)
        mapped_sources = {pair[0] for pair in pairs}
        mapped_targets = {pair[1] for pair in pairs}
        
        for src_col in source_columns:
            if src_col in mapped_sources:
                continue
                
            if src_col in target_set:
                # Exact match
                if src_col not in mapped_targets:
                    pairs.append((src_col, src_col))
            elif not self.case_sensitive and src_col.lower() in target_lower:
                # Case-insensitive match
                target_col = target_lower[src_col.lower()]
                if target_col not in mapped_targets:
                    pairs.append((src_col, target_col))
        
        return pairs
    
    def validate_mappings(self, source_columns: List[str], target_columns: List[str]) -> Tuple[bool, List[str]]:
        """
        Validate that all mappings refer to existing columns
        
        Args:
            source_columns: Columns from the source table
            target_columns: Columns from the target table
            
        Returns:
            Tuple of (is_valid, list_of_warnings)
        """
        warnings = []
        source_set = set(source_columns)
        target_set = set(target_columns)
        
        if not self.case_sensitive:
            source_lower = {col.lower() for col in source_columns}
            target_lower = {col.lower() for col in target_columns}
        
        for src, tgt in self.mappings.items():
            # Check source column exists
            if src not in source_set:
                if self.case_sensitive or src.lower() not in source_lower:
                    warnings.append(f"Source column '{src}' in mapping not found in source table")
            
            # Check target column exists
            if tgt not in target_set:
                if self.case_sensitive or tgt.lower() not in target_lower:
                    warnings.append(f"Target column '{tgt}' in mapping not found in target table")
        
        is_valid = len(warnings) == 0
        return is_valid, warnings
    
    @staticmethod
    def suggest_mappings(source_columns: List[str], target_columns: List[str], 
                        similarity_threshold: float = 0.8) -> Dict[str, str]:
        """
        Suggest column mappings based on name similarity
        
        Args:
            source_columns: Columns from the source table
            target_columns: Columns from the target table
            similarity_threshold: Minimum similarity score (0-1) to suggest a mapping
            
        Returns:
            Dictionary of suggested mappings
        """
        from difflib import SequenceMatcher
        
        suggestions = {}
        used_targets = set()
        
        for src_col in source_columns:
            best_match = None
            best_score = 0
            
            for tgt_col in target_columns:
                if tgt_col in used_targets:
                    continue
                    
                # Calculate similarity
                score = SequenceMatcher(None, src_col.lower(), tgt_col.lower()).ratio()
                
                if score > best_score and score >= similarity_threshold:
                    best_score = score
                    best_match = tgt_col
            
            if best_match:
                suggestions[src_col] = best_match
                used_targets.add(best_match)
                logger.info(f"Suggested mapping: '{src_col}' -> '{best_match}' (similarity: {best_score:.2f})")
        
        return suggestions
    
    @staticmethod
    def from_string(mapping_str: str) -> 'ColumnRemapper':
        """
        Create a ColumnRemapper from a string representation
        
        Args:
            mapping_str: String like "user_id:customer_id,created_at:creation_date"
            
        Returns:
            ColumnRemapper instance
        """
        mappings = {}
        if mapping_str:
            for pair in mapping_str.split(','):
                pair = pair.strip()
                if ':' in pair:
                    src, tgt = pair.split(':', 1)
                    mappings[src.strip()] = tgt.strip()
        
        return ColumnRemapper(mappings)
    
    def to_string(self) -> str:
        """
        Convert mappings to string representation
        
        Returns:
            String like "user_id:customer_id,created_at:creation_date"
        """
        return ','.join(f"{k}:{v}" for k, v in self.mappings.items())