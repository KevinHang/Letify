"""
Script to initialize all database tables in the correct order.
"""

import logging
import sys
import time
import psycopg
from database.migrations import initialize_db, initialize_telegram_db
from config import DB_CONNECTION_STRING

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("db_init")

def wait_for_extensions():
    """Wait for all required extensions to be available."""
    required_extensions = ["postgis", "fuzzystrmatch", "vector"]
    max_attempts = 10
    attempt = 0
    
    while attempt < max_attempts:
        attempt += 1
        missing_extensions = []
        
        try:
            conn = psycopg.connect(DB_CONNECTION_STRING)
            with conn.cursor() as cur:
                for ext in required_extensions:
                    try:
                        cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
                        conn.commit()
                    except psycopg.errors.UndefinedFile as e:
                        missing_extensions.append(ext)
                        conn.rollback()
            conn.close()
            
            if not missing_extensions:
                logger.info("All required extensions are available")
                return True
            
            logger.warning(f"Missing extensions: {', '.join(missing_extensions)}. Attempt {attempt}/{max_attempts}")
            
            # Special handling for vector extension
            if "vector" in missing_extensions:
                logger.warning("The pgvector extension might not be fully installed. Continuing without it.")
                if "vector" in required_extensions:
                    required_extensions.remove("vector")
                
        except Exception as e:
            logger.error(f"Error checking extensions: {e}")
        
        time.sleep(5)
    
    # If we get here, we couldn't enable all extensions, but we'll still try to continue
    logger.warning("Could not enable all extensions, but will attempt to continue with initialization")
    return False

def main():
    """Initialize database tables in the correct order."""
    try:
        # First, wait for extensions to be ready
        wait_for_extensions()
        
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