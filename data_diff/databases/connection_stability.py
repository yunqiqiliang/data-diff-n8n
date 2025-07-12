"""
Connection stability utilities for all database types.

This module provides common utilities to improve connection stability
across different database implementations.
"""

import logging
import time
from typing import Any, Callable, Optional, TypeVar, Union
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')


def retry_on_connection_error(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry a function on connection errors.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_error = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    # Check if this is a connection-related error
                    if any(keyword in error_msg for keyword in [
                        'connection', 'closed', 'lost', 'gone away', 
                        'broken pipe', 'reset', 'refused', 'timeout'
                    ]):
                        last_error = e
                        if attempt < max_retries:
                            logger.warning(
                                f"Connection error in {func.__name__} (attempt {attempt + 1}/{max_retries + 1}): {e}"
                            )
                            time.sleep(current_delay)
                            current_delay *= backoff
                        else:
                            logger.error(f"All retry attempts failed for {func.__name__}")
                    else:
                        # Not a connection error, raise immediately
                        raise
            
            # All retries exhausted
            if last_error:
                raise last_error
            raise Exception(f"Unexpected error in {func.__name__}")
        
        return wrapper
    return decorator


class ConnectionValidator:
    """Base class for database-specific connection validators."""
    
    def is_connection_valid(self, conn: Any) -> bool:
        """
        Check if a connection is still valid.
        
        Args:
            conn: Database connection object
            
        Returns:
            True if connection is valid, False otherwise
        """
        raise NotImplementedError
    
    def validate_or_reconnect(self, conn: Any, create_func: Callable[[], Any]) -> Any:
        """
        Validate connection and reconnect if necessary.
        
        Args:
            conn: Current connection object
            create_func: Function to create a new connection
            
        Returns:
            Valid connection object
        """
        if not self.is_connection_valid(conn):
            logger.info("Connection invalid, creating new connection")
            return create_func()
        return conn


class PostgreSQLValidator(ConnectionValidator):
    """Connection validator for PostgreSQL."""
    
    def is_connection_valid(self, conn: Any) -> bool:
        try:
            # Check if connection has 'closed' attribute
            if hasattr(conn, 'closed') and conn.closed:
                return False
            
            # Try a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception:
            return False


class MySQLValidator(ConnectionValidator):
    """Connection validator for MySQL."""
    
    def is_connection_valid(self, conn: Any) -> bool:
        try:
            # Check if connection has is_connected method
            if hasattr(conn, 'is_connected'):
                return conn.is_connected()
            
            # Fallback: try a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False


class ClickZettaValidator(ConnectionValidator):
    """Connection validator for ClickZetta."""
    
    def is_connection_valid(self, conn: Any) -> bool:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False


class OracleValidator(ConnectionValidator):
    """Connection validator for Oracle."""
    
    def is_connection_valid(self, conn: Any) -> bool:
        try:
            # Oracle specific ping
            if hasattr(conn, 'ping'):
                conn.ping()
                return True
            
            # Fallback: try a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False


class SnowflakeValidator(ConnectionValidator):
    """Connection validator for Snowflake."""
    
    def is_connection_valid(self, conn: Any) -> bool:
        try:
            # Check if connection is closed
            if hasattr(conn, 'is_closed') and conn.is_closed():
                return False
            
            # Try a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False


# Map database types to their validators
VALIDATORS = {
    'postgresql': PostgreSQLValidator(),
    'mysql': MySQLValidator(),
    'clickzetta': ClickZettaValidator(),
    'oracle': OracleValidator(),
    'snowflake': SnowflakeValidator(),
}


def get_validator(db_type: str) -> Optional[ConnectionValidator]:
    """Get the appropriate validator for a database type."""
    return VALIDATORS.get(db_type.lower())


def ensure_stable_connection(db_type: str, conn: Any, create_func: Callable[[], Any]) -> Any:
    """
    Ensure a stable connection for the given database type.
    
    Args:
        db_type: Type of database
        conn: Current connection object
        create_func: Function to create a new connection
        
    Returns:
        Valid connection object
    """
    validator = get_validator(db_type)
    if validator:
        return validator.validate_or_reconnect(conn, create_func)
    
    # No validator available, return existing connection
    logger.debug(f"No validator available for database type: {db_type}")
    return conn


def with_connection_retry(db_type: str):
    """
    Decorator to add connection retry logic to database methods.
    
    Args:
        db_type: Type of database
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            # Try to get connection and validate it
            if hasattr(self, 'thread_local') and hasattr(self.thread_local, 'conn'):
                validator = get_validator(db_type)
                if validator and not validator.is_connection_valid(self.thread_local.conn):
                    logger.info(f"Invalid {db_type} connection detected, recreating...")
                    try:
                        self.thread_local.conn = self.create_connection()
                    except Exception as e:
                        logger.error(f"Failed to recreate {db_type} connection: {e}")
                        raise
            
            # Execute the function with retry logic
            return retry_on_connection_error()(func)(self, *args, **kwargs)
        
        return wrapper
    return decorator