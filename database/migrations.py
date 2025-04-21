"""
Database initialization and migrations.
"""

from .connection import get_connection
from utils.logging_config import get_scraper_logger

# Use a child logger of the telegram logger
logger = get_scraper_logger("migrations_db")


def initialize_db(connection_string: str):
    """Create tables and indexes if they don't exist."""
    conn = get_connection(connection_string)
    
    try:
        with conn.cursor() as cur:
            # Enable vector extension for similarity search
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            # Enable postgis for geospatial queries
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            # Enable levenshtein for text similarity
            cur.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
            
            # Create properties table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                id SERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                source_id TEXT,
                property_hash TEXT UNIQUE,
                url TEXT,
                title TEXT,
                address TEXT,
                postal_code TEXT,
                city TEXT,
                neighborhood TEXT,
                price TEXT,
                price_numeric INTEGER,
                price_period TEXT,
                service_costs FLOAT,
                description TEXT,
                property_type TEXT,
                offering_type TEXT,
                living_area INTEGER,
                plot_area INTEGER,
                volume INTEGER,
                rooms INTEGER,
                bedrooms INTEGER,
                bathrooms INTEGER,
                floors INTEGER,
                balcony BOOLEAN,
                garden BOOLEAN,
                parking BOOLEAN,
                construction_year INTEGER,
                energy_label TEXT,
                interior TEXT,
                coordinates JSONB,
                location GEOGRAPHY(POINT),
                date_listed TEXT,
                date_available TEXT,
                date_scraped TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                images JSONB,
                features JSONB,
                description_embedding vector(384),
                UNIQUE (source, source_id)
            )
            """)
            
            # Create scan history table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS scan_history (
                id SERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                city TEXT NOT NULL,
                scan_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                url TEXT,
                new_listings_count INTEGER DEFAULT 0,
                total_listings_count INTEGER DEFAULT 0,
                scan_duration_seconds FLOAT,
                UNIQUE (source, city)
            )
            """)
            
            # Create duplicate detection table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS duplicate_properties (
                id SERIAL PRIMARY KEY,
                property_hash TEXT NOT NULL,
                source_1 TEXT NOT NULL,
                source_id_1 TEXT NOT NULL,
                source_2 TEXT NOT NULL,
                source_id_2 TEXT NOT NULL,
                similarity_score FLOAT,
                date_detected TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE (source_1, source_id_1, source_2, source_id_2)
            )
            """)
            
            # Create query_urls table for specific URLs to scrape
            cur.execute("""
            CREATE TABLE IF NOT EXISTS query_urls (
                id SERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                queryurl TEXT NOT NULL,
                method TEXT NOT NULL DEFAULT 'GET',
                enabled BOOLEAN DEFAULT false NOT NULL,
                last_scan_time TIMESTAMP WITH TIME ZONE,
                description TEXT,
                UNIQUE (source, queryurl)
            )
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_source ON properties(source)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_city ON properties(city)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_postal_code ON properties(postal_code)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_price_numeric ON properties(price_numeric)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_bedrooms ON properties(bedrooms)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_living_area ON properties(living_area)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_offering_type ON properties(offering_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_property_type ON properties(property_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_date_scraped ON properties(date_scraped)")
            
            # Create index for query_urls
            cur.execute("CREATE INDEX IF NOT EXISTS idx_query_urls_source ON query_urls(source)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_query_urls_enabled ON query_urls(enabled)")
            
            # Create spatial index for location
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_location ON properties USING GIST(location)")
            
            # Create index on property_hash for deduplication
            cur.execute("CREATE INDEX IF NOT EXISTS idx_properties_property_hash ON properties(property_hash)")
            
            conn.commit()
            logger.info("Database initialized successfully")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        conn.close()

def initialize_telegram_db(connection_string: str):
    """Create Telegram-related tables and indexes if they don't exist."""
    conn = get_connection(connection_string)
    
    try:
        with conn.cursor() as cur:
            # Create telegram_users table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS telegram_users (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                is_admin BOOLEAN DEFAULT FALSE,
                notification_enabled BOOLEAN DEFAULT TRUE,
                date_joined TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_active TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            """)
            
            # Create user_preferences table with cities as TEXT[]
            cur.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES telegram_users(user_id) ON DELETE CASCADE,
                cities TEXT[],  -- Changed from city TEXT to cities TEXT[]
                min_price INTEGER,
                max_price INTEGER,
                min_rooms INTEGER,
                max_rooms INTEGER,
                property_type TEXT,
                min_area INTEGER,
                max_area INTEGER,
                neighborhood TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE (user_id)
            )
            """)
            
            # Create notification_history table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS notification_history (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES telegram_users(user_id) ON DELETE CASCADE,
                property_id INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
                sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                was_read BOOLEAN DEFAULT FALSE,
                user_reaction TEXT,
                UNIQUE (user_id, property_id)
            )
            """)
            
            # Create notification_queue table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS notification_queue (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES telegram_users(user_id) ON DELETE CASCADE,
                property_id INTEGER NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                last_attempt TIMESTAMP WITH TIME ZONE,
                UNIQUE (user_id, property_id)
            )
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON telegram_users(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_users_is_active ON telegram_users(is_active)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_preferences_user_id ON user_preferences(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notification_history_user_id ON notification_history(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notification_history_property_id ON notification_history(property_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notification_queue_user_id ON notification_queue(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notification_queue_status ON notification_queue(status)")
            
            conn.commit()
            logger.info("Telegram database tables initialized successfully")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error initializing Telegram database: {e}")
        raise
    finally:
        conn.close()