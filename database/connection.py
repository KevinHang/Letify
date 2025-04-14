"""
Database connection management.
"""

import logging
import psycopg
from pgvector.psycopg import register_vector

logger = logging.getLogger(__name__)


def get_connection(connection_string: str):
    """Create and return a database connection."""
    try:
        conn = psycopg.connect(connection_string)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def close_connection(conn):
    """Close a database connection."""
    if conn:
        try:
            conn.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")