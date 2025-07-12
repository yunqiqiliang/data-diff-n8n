"""
PostgreSQL connection stability patch.

This module provides additional connection stability improvements for PostgreSQL.
"""

import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class PostgreSQLConnectionPool:
    """Simple connection pool for PostgreSQL to reuse connections."""
    
    def __init__(self, create_conn_func, max_size=5):
        self.create_conn_func = create_conn_func
        self.max_size = max_size
        self.pool = []
        self.in_use = set()
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        conn = None
        try:
            # Try to get a connection from the pool
            while self.pool:
                candidate = self.pool.pop()
                # Check if connection is still valid
                if self._is_connection_valid(candidate):
                    conn = candidate
                    break
                else:
                    # Close invalid connection
                    try:
                        candidate.close()
                    except:
                        pass
            
            # Create new connection if needed
            if conn is None:
                logger.debug("Creating new PostgreSQL connection")
                conn = self.create_conn_func()
            
            self.in_use.add(conn)
            yield conn
            
        finally:
            if conn:
                self.in_use.discard(conn)
                # Return to pool if pool is not full and connection is valid
                if len(self.pool) < self.max_size and self._is_connection_valid(conn):
                    self.pool.append(conn)
                else:
                    # Close connection
                    try:
                        conn.close()
                    except:
                        pass
    
    def _is_connection_valid(self, conn):
        """Check if a connection is still valid."""
        try:
            if hasattr(conn, 'closed') and conn.closed:
                return False
            # Try a simple query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except:
            return False
    
    def close_all(self):
        """Close all connections in the pool."""
        for conn in self.pool:
            try:
                conn.close()
            except:
                pass
        self.pool.clear()


def create_stable_connection(conn_func, retries=3):
    """Create a connection with retry logic."""
    last_error = None
    for i in range(retries):
        try:
            conn = conn_func()
            # Test the connection
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return conn
        except Exception as e:
            last_error = e
            logger.warning(f"Connection attempt {i+1} failed: {e}")
            if i < retries - 1:
                import time
                time.sleep(1 * (i + 1))  # Exponential backoff
    
    raise Exception(f"Failed to create connection after {retries} attempts: {last_error}")


def apply_postgresql_patches():
    """Apply stability patches to PostgreSQL driver."""
    try:
        import psycopg2
        
        # Monkey patch to add connection validation
        original_connect = psycopg2.connect
        
        def patched_connect(*args, **kwargs):
            # Ensure keepalive settings
            kwargs.setdefault('keepalives', 1)
            kwargs.setdefault('keepalives_idle', 30)
            kwargs.setdefault('keepalives_interval', 10)
            kwargs.setdefault('keepalives_count', 5)
            
            return original_connect(*args, **kwargs)
        
        psycopg2.connect = patched_connect
        logger.info("PostgreSQL connection patches applied")
        
    except ImportError:
        logger.debug("psycopg2 not available, skipping patches")