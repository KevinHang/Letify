"""
Telegram user database operations.
"""

from typing import List, Dict, Any, Optional, Tuple

import psycopg
from psycopg.rows import dict_row
from utils.logging_config import get_scraper_logger

# Use a child logger of the telegram logger
logger = get_scraper_logger("telegram_db")

class TelegramDatabase:
    """Database handler for Telegram users and notifications"""
    
    def __init__(self, connection_string: str):
        """Initialize database connection"""
        self.connection_string = connection_string
        self.conn = psycopg.connect(connection_string)
    
    def register_user(self, user_id: int, username: Optional[str] = None, 
                     first_name: Optional[str] = None, last_name: Optional[str] = None, 
                     is_admin: bool = False) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO telegram_users 
                    (user_id, username, first_name, last_name, is_admin, is_active, last_active) 
                VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (user_id) DO UPDATE 
                SET 
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    is_active = TRUE,
                    last_active = NOW()
                """, (user_id, username, first_name, last_name, is_admin))
                self.conn.commit()
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error registering user: {e}")
            return False
    
    def update_user_activity(self, user_id: int) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE telegram_users
                SET last_active = NOW()
                WHERE user_id = %s
                """, (user_id,))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error updating user activity: {e}")
            return False
    
    def toggle_user_active(self, user_id: int, is_active: bool) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE telegram_users
                SET is_active = %s
                WHERE user_id = %s
                """, (is_active, user_id))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error toggling user active status: {e}")
            return False
    
    def set_admin_status(self, user_id: int, is_admin: bool) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE telegram_users
                SET is_admin = %s
                WHERE user_id = %s
                """, (is_admin, user_id))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error setting admin status: {e}")
            return False
    
    def toggle_notifications(self, user_id: int, enabled: bool) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE telegram_users
                SET notification_enabled = %s
                WHERE user_id = %s
                """, (enabled, user_id))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error toggling notifications: {e}")
            return False
    
    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                SELECT * FROM telegram_users
                WHERE user_id = %s
                """, (user_id,))
                return cur.fetchone()
        except Exception as e:
            logger.error(f"Error getting user: {e}")
            return None
    
    def get_active_users(self) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                SELECT * FROM telegram_users
                WHERE is_active = TRUE AND notification_enabled = TRUE
                """)
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting active users: {e}")
            return []
    
    def get_admin_users(self) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                SELECT * FROM telegram_users
                WHERE is_admin = TRUE
                """)
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting admin users: {e}")
            return []
    
    def set_user_preferences(self, user_id: int, preferences: Dict[str, Any]) -> bool:
        """
        Set a user's property preferences, ensuring cities are stored as uppercase.
        """
        cities = [city.strip().upper() for city in preferences.get('cities', []) if city.strip()] if preferences.get('cities') else None
        min_price = preferences.get('min_price')
        max_price = preferences.get('max_price')
        min_rooms = preferences.get('min_rooms')
        max_rooms = preferences.get('max_rooms')
        property_type = preferences.get('property_type')
        min_area = preferences.get('min_area')
        max_area = preferences.get('max_area')
        neighborhood = preferences.get('neighborhood')
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO user_preferences 
                    (user_id, cities, min_price, max_price, min_rooms, max_rooms, 
                     property_type, min_area, max_area, neighborhood, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE 
                SET 
                    cities = EXCLUDED.cities,
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price,
                    min_rooms = EXCLUDED.min_rooms,
                    max_rooms = EXCLUDED.max_rooms,
                    property_type = EXCLUDED.property_type,
                    min_area = EXCLUDED.min_area,
                    max_area = EXCLUDED.max_area,
                    neighborhood = EXCLUDED.neighborhood,
                    updated_at = NOW()
                RETURNING id
                """, (
                    user_id, cities, min_price, max_price, min_rooms, max_rooms,
                    property_type, min_area, max_area, neighborhood
                ))
                result = cur.fetchone()
                self.conn.commit()
                logger.info(f"Preferences saved for user_id {user_id}, query ID: {result[0] if result else 'None'}")
                return result is not None
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error setting user preferences for user_id {user_id}: {e}")
            return False
    
    def get_user_preferences(self, user_id: int) -> Optional[Dict[str, Any]]:
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                SELECT * FROM user_preferences
                WHERE user_id = %s
                """, (user_id,))
                return cur.fetchone()
        except Exception as e:
            logger.error(f"Error getting user preferences: {e}")
            return None
    
    def add_to_notification_queue(self, user_id: int, property_id: int) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO notification_queue 
                    (user_id, property_id, created_at, status) 
                VALUES (%s, %s, NOW(), 'pending')
                ON CONFLICT (user_id, property_id) DO NOTHING
                """, (user_id, property_id))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error adding to notification queue: {e}")
            return False
    
    def get_pending_notifications(self, limit: int = 100) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute("""
                SELECT nq.id AS notification_id, nq.user_id, nq.property_id, nq.status, 
                    nq.created_at, nq.attempts, nq.last_attempt, p.*
                FROM notification_queue nq
                JOIN properties p ON nq.property_id = p.id
                JOIN telegram_users tu ON nq.user_id = tu.user_id
                WHERE nq.status = 'pending' 
                AND tu.is_active = TRUE 
                AND tu.notification_enabled = TRUE
                ORDER BY nq.created_at ASC
                LIMIT %s
                """, (limit,))
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting pending notifications: {e}")
            return []
    
    def update_notification_status(self, notification_id: int, status: str, attempts: int = None) -> bool:
        try:
            with self.conn.cursor() as cur:
                if attempts is not None:
                    cur.execute("""
                    UPDATE notification_queue
                    SET status = %s, attempts = %s, last_attempt = NOW()
                    WHERE id = %s
                    """, (status, attempts, notification_id))
                else:
                    cur.execute("""
                    UPDATE notification_queue
                    SET status = %s, last_attempt = NOW()
                    WHERE id = %s
                    """, (status, notification_id))
                rowcount = cur.rowcount
                self.conn.commit()
                if rowcount == 0:
                    logger.warning(f"No rows updated for notification_id {notification_id} when setting status to {status}")
                return rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error updating notification status for notification_id {notification_id}: {e}")
            return False
        
    def record_notification_sent(self, user_id: int, property_id: int) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO notification_history 
                    (user_id, property_id, sent_at) 
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id, property_id) DO UPDATE 
                SET sent_at = NOW()
                """, (user_id, property_id))
                self.conn.commit()
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error recording notification: {e}")
            return False
    
    def update_notification_reaction(self, user_id: int, property_id: int, reaction: str) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE notification_history
                SET user_reaction = %s, was_read = TRUE
                WHERE user_id = %s AND property_id = %s
                """, (reaction, user_id, property_id))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error updating notification reaction: {e}")
            return False
    
    def clean_old_notifications(self, days: int = 30) -> int:
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                DELETE FROM notification_queue
                WHERE status IN ('sent', 'failed')
                AND created_at < NOW() - INTERVAL '%s days'
                """, (days,))
                count = cur.rowcount
                self.conn.commit()
                return count
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error cleaning old notifications: {e}")
            return 0
    
    def add_matched_properties_to_queue(self, property_id: int) -> int:
        """
        Add a property to the notification queue for all matching users, handling multiple cities.
        """
        try:
            with self.conn.cursor() as cur:
                # Get property details
                cur.execute("""
                SELECT * FROM properties
                WHERE id = %s
                """, (property_id,))
                property_row = cur.fetchone()
                
                if not property_row:
                    logger.warning(f"No property found for property_id {property_id}")
                    return 0
                
                # Match with user preferences, checking if property city is in user's cities array
                cur.execute("""
                INSERT INTO notification_queue (user_id, property_id, created_at, status)
                SELECT tu.user_id, %s, NOW(), 'pending'
                FROM telegram_users tu
                JOIN user_preferences up ON tu.user_id = up.user_id
                WHERE tu.is_active = TRUE AND up.cities IS NOT NULL
                AND tu.notification_enabled = TRUE
                AND (up.cities IS NULL OR %s = ANY(up.cities))
                AND (up.min_price IS NULL OR %s >= up.min_price)
                AND (up.max_price IS NULL OR %s <= up.max_price OR up.max_price = 0)
                AND (up.min_rooms IS NULL OR %s >= up.min_rooms)
                AND (up.max_rooms IS NULL OR %s <= up.max_rooms OR up.max_rooms = 0)
                AND (up.property_type IS NULL OR %s = up.property_type)
                AND (up.min_area IS NULL OR %s >= up.min_area)
                AND (up.max_area IS NULL OR %s <= up.max_area OR up.max_area = 0)
                AND (up.neighborhood IS NULL OR %s ILIKE CONCAT('%%', up.neighborhood, '%%'))
                ON CONFLICT (user_id, property_id) DO NOTHING
                """, (
                    property_id, 
                    property_row[8].upper(),  # city
                    property_row[11],         # price_numeric
                    property_row[11],         # price_numeric
                    property_row[20],         # rooms (corrected to match schema)
                    property_row[20],         # rooms (corrected to match schema)
                    property_row[15],         # property_type (e.g., "room", "apartment")
                    property_row[17],         # living_area (corrected to match schema)
                    property_row[17],         # living_area (corrected to match schema)
                    property_row[9]           # neighborhood
                ))
                
                count = cur.rowcount
                self.conn.commit()
                logger.debug(f"Queued property_id {property_id} for {count} users")
                return count
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error adding matched properties to queue for property_id {property_id}: {e}, params: {locals().get('property_row')}")
            return 0