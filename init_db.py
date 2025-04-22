"""
Script to initialize all database tables in the correct order.
"""

import logging
import sys
from database.migrations import initialize_db, initialize_telegram_db
from config import DB_CONNECTION_STRING

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("db_init")

def main():
    """Initialize database tables in the correct order."""
    try:
        logger.info("Initializing property database tables...")
        initialize_db(DB_CONNECTION_STRING)
        logger.info("Property database tables initialized successfully")
        
        logger.info("Initializing Telegram database tables...")
        initialize_telegram_db(DB_CONNECTION_STRING)
        logger.info("Telegram database tables initialized successfully")
        
        return 0
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())