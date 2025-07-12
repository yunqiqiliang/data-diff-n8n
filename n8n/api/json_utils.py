"""
JSON utilities for handling special data types in API responses.
"""

import json
import datetime
from decimal import Decimal
from typing import Any

# Try to import numpy, but make it optional
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


class EnhancedJSONEncoder(json.JSONEncoder):
    """Enhanced JSON encoder that handles special data types."""
    
    def default(self, obj: Any) -> Any:
        """Convert special types to JSON-serializable formats."""
        
        # Handle Decimal
        if isinstance(obj, Decimal):
            # Convert to float for JSON serialization
            # Note: This may lose precision for very large decimals
            return float(obj)
        
        # Handle datetime types
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        
        elif isinstance(obj, datetime.time):
            return obj.isoformat()
        
        elif isinstance(obj, datetime.timedelta):
            return str(obj)
        
        # Handle numpy types (often returned by data analysis libraries)
        elif HAS_NUMPY and hasattr(obj, 'dtype'):
            # NumPy scalar
            if hasattr(obj, 'item'):
                return obj.item()
            # NumPy array
            elif hasattr(obj, 'tolist'):
                return obj.tolist()
        
        # Handle bytes
        elif isinstance(obj, bytes):
            try:
                # Try to decode as UTF-8
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                # If that fails, return base64 encoded
                import base64
                return base64.b64encode(obj).decode('ascii')
        
        # Handle sets
        elif isinstance(obj, set):
            return list(obj)
        
        # Handle any object with a to_dict method
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        
        # Handle any object with __dict__
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        
        # Default to string representation for unknown types
        else:
            try:
                return str(obj)
            except:
                # If even string conversion fails, use the parent's default
                return super().default(obj)


def safe_json_dumps(obj: Any, **kwargs) -> str:
    """
    Safely serialize an object to JSON string.
    
    Args:
        obj: Object to serialize
        **kwargs: Additional arguments to pass to json.dumps
        
    Returns:
        JSON string
    """
    kwargs.setdefault('cls', EnhancedJSONEncoder)
    kwargs.setdefault('ensure_ascii', False)  # Support Unicode characters
    return json.dumps(obj, **kwargs)


def safe_json_dump(obj: Any, fp, **kwargs) -> None:
    """
    Safely serialize an object to a JSON file.
    
    Args:
        obj: Object to serialize
        fp: File pointer to write to
        **kwargs: Additional arguments to pass to json.dump
    """
    kwargs.setdefault('cls', EnhancedJSONEncoder)
    kwargs.setdefault('ensure_ascii', False)  # Support Unicode characters
    json.dump(obj, fp, **kwargs)