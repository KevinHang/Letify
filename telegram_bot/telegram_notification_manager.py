import asyncio
import time
from datetime import datetime
from typing import Dict, Any
import random

import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, CopyTextButton
from utils.utils import construct_full_address

from config import (
    MAX_NOTIFICATIONS_PER_USER_PER_DAY,
    NOTIFICATION_BATCH_SIZE,
    NOTIFICATION_RETRY_ATTEMPTS
)
from database.property_db import PropertyDatabase
from database.telegram_db import TelegramDatabase
from utils.formatting import format_listing_message
from utils.logging_config import get_telegram_logger

# Use a child logger of the telegram logger
logger = get_telegram_logger("notification_manager")

class TelegramNotificationManager:
    """Manager for sending property notifications to Telegram users"""
    
    def __init__(self, bot_token: str, db_connection_string: str):
        """Initialize the notification manager"""
        self.bot_token = bot_token
        self.db_connection_string = db_connection_string
        self.bot = telegram.Bot(token=bot_token)
        
        # Initialize databases
        self.property_db = PropertyDatabase(db_connection_string)
        self.telegram_db = TelegramDatabase(db_connection_string)
        
        # Track statistics
        self.stats = {
            "total_sent": 0,
            "total_failed": 0,
            "users_notified": 0,
            "properties_notified": 0,
            "last_run": None
        }
    
    async def process_new_listing(self, property_id: int) -> int:
        """
        Process a new property listing by adding it to the notification queue
        for matching users.
        
        Args:
            property_id: The ID of the new property listing
            
        Returns:
            Number of users who will receive notifications for this property
        """
        try:
            # Add property to notification queue for matching users
            matched_users = self.telegram_db.add_matched_properties_to_queue(property_id)
            
            logger.info(f"Added property ID {property_id} to notification queue for {matched_users} users")
            return matched_users
        
        except Exception as e:
            logger.error(f"Error processing new listing {property_id}: {e}")
            return 0
    
    async def send_notification(self, user_id: int, property_data: Dict[str, Any]) -> bool:
        try:
            # Format property message
            message_text = format_listing_message(property_data)
            
            # Get user's reaction text
            user = self.telegram_db.get_user(user_id)
            reaction_text = user.get('reaction_text', 'No reaction text set') if user else 'No reaction text set'
            address = property_data.get('address', 'Unknown address') if property_data else 'Unknown address'
            location = construct_full_address(property_data=property_data, include_neighborhood=False)
            maps_url = f"https://www.google.com/maps/search/?api=1&query={location}"
            formatted_reaction = reaction_text.replace('{ADDRESS}', address) if reaction_text else 'No reaction text set'
            
            # Create reaction keyboard with View Details and Copy Reaction Text
            keyboard = [
                [
                    InlineKeyboardButton("✉️ Letter", copy_text=CopyTextButton(text=formatted_reaction)),
                    InlineKeyboardButton("📍 Maps", url=maps_url)
                ],
                [
                    InlineKeyboardButton("🔍 View Details", url=property_data.get('url', 'https://example.com'))
                ]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Send message with retries
            for attempt in range(NOTIFICATION_RETRY_ATTEMPTS):
                try:
                    # Add image if available
                    images_json = property_data.get('images')
                    image_url = None
                    if images_json:
                        try:
                            if isinstance(images_json, str):
                                import json
                                images = json.loads(images_json)
                                if images and len(images) > 0:
                                    image_url = images[0]
                            elif isinstance(images_json, list) and len(images_json) > 0:
                                image_url = images_json[0]
                            
                            if image_url:
                                # Send photo with caption and keyboard
                                await self.bot.send_photo(
                                    chat_id=user_id,
                                    photo=image_url,
                                    caption=message_text[:1024],  # Telegram limit
                                    reply_markup=reply_markup,
                                    parse_mode=telegram.constants.ParseMode.HTML
                                )
                                return True
                        except Exception as img_error:
                            logger.error(f"Error sending property image for property {property_data['id']}: {img_error}, falling back to text message")
                    
                    # Send text message with keyboard
                    await self.bot.send_message(
                        chat_id=user_id,
                        text=message_text,
                        reply_markup=reply_markup,
                        parse_mode=telegram.constants.ParseMode.HTML
                    )
                    return True
                    
                except telegram.error.BadRequest as e:
                    logger.error(f"Bad request error sending notification to {user_id} for property {property_data['id']}: {e}")
                    return False
                    
                except telegram.error.Unauthorized as e:
                    logger.error(f"User {user_id} has blocked the bot: {e}")
                    # Deactivate the user
                    self.telegram_db.toggle_user_active(user_id, False)
                    return False
                    
                except Exception as e:
                    logger.error(f"Error sending notification to {user_id} for property {property_data['id']} (attempt {attempt+1}): {e}")
                    # Wait before retrying
                    if attempt < NOTIFICATION_RETRY_ATTEMPTS - 1:
                        await asyncio.sleep(1)
            
            return False
            
        except Exception as e:
            logger.error(f"Unhandled error in send_notification for user {user_id}, property {property_data.get('id')}: {e}, property_data: {property_data}")
            return False
    
    async def process_notification_queue(self, batch_size: int = NOTIFICATION_BATCH_SIZE) -> Dict[str, int]:
        """
        Process pending notifications in the queue.
        
        Args:
            batch_size: Number of notifications to process in one batch
            
        Returns:
            Dictionary with statistics about the processing
        """
        stats = {
            "notifications_processed": 0,
            "notifications_sent": 0,
            "notifications_failed": 0,
            "users_notified": set(),
            "properties_notified": set()
        }
        
        try:
            # Get pending notifications
            notifications = self.telegram_db.get_pending_notifications(batch_size)
            
            if not notifications:
                logger.debug("No pending notifications to process")
                return stats
            
            logger.info(f"Processing {len(notifications)} pending notifications")
            
            # Track users who have received notifications today
            user_notification_counts = {}
            
            # Process notifications
            for notification in notifications:
                stats["notifications_processed"] += 1
                
                user_id = notification['user_id']
                property_id = notification['property_id']
                notification_id = notification['notification_id']
                
                # Check if user has reached the daily limit
                if user_id in user_notification_counts:
                    if user_notification_counts[user_id] >= MAX_NOTIFICATIONS_PER_USER_PER_DAY:
                        logger.info(f"User {user_id} has reached the daily notification limit")
                        # Update status to 'rate_limited'
                        self.telegram_db.update_notification_status(notification_id, 'rate_limited')
                        continue
                else:
                    # Count existing notifications sent today
                    with self.property_db.conn.cursor() as cur:
                        cur.execute("""
                        SELECT COUNT(*) FROM notification_history
                        WHERE user_id = %s AND sent_at > NOW() - INTERVAL '24 hours'
                        """, (user_id,))
                        count = cur.fetchone()[0] or 0
                        user_notification_counts[user_id] = count
                
                # Check if still below limit
                if user_notification_counts[user_id] >= MAX_NOTIFICATIONS_PER_USER_PER_DAY:
                    logger.info(f"User {user_id} has reached the daily notification limit")
                    # Update status to 'rate_limited'
                    self.telegram_db.update_notification_status(notification_id, 'rate_limited')
                    continue
                
                # Update notification status to 'processing'
                attempts = notification.get('attempts', 0) + 1
                self.telegram_db.update_notification_status(notification_id, 'processing', attempts)
                
                # Send notification
                success = await self.send_notification(user_id, notification)
                
                if success:
                    # Update notification status to 'sent'
                    if not self.telegram_db.update_notification_status(notification_id, 'sent'):
                        logger.error(f"Failed to update notification status to 'sent' for notification_id {notification_id}")
                    
                    # Record notification in history
                    self.telegram_db.record_notification_sent(user_id, property_id)
                    
                    # Update statistics
                    stats["notifications_sent"] += 1
                    stats["users_notified"].add(user_id)
                    stats["properties_notified"].add(property_id)
                    
                    # Increment user notification count
                    user_notification_counts[user_id] = user_notification_counts.get(user_id, 0) + 1
                    
                    logger.debug(f"Notification sent to user {user_id} for property {property_id}")
                else:
                    # Update notification status to 'failed'
                    self.telegram_db.update_notification_status(notification_id, 'failed', attempts)
                    
                    stats["notifications_failed"] += 1
                    logger.error(f"Failed to send notification to user {user_id} for property {property_id}")
                
                # Add a small delay between notifications to avoid rate limiting
                await asyncio.sleep(0.1)
            
            # Convert sets to counts for the return value
            stats["users_notified"] = len(stats["users_notified"])
            stats["properties_notified"] = len(stats["properties_notified"])
            
            # Update global stats
            self.stats["total_sent"] += stats["notifications_sent"]
            self.stats["total_failed"] += stats["notifications_failed"]
            self.stats["users_notified"] += stats["users_notified"]
            self.stats["properties_notified"] += stats["properties_notified"]
            self.stats["last_run"] = datetime.now()
            
            logger.info(f"Processed {stats['notifications_processed']} notifications: {stats['notifications_sent']} sent, {stats['notifications_failed']} failed")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error processing notification queue: {e}")
            return stats
    
    def get_stats(self) -> Dict[str, Any]:
        """Get notification statistics."""
        return self.stats
    
    async def run_once(self):
        """Process the notification queue once."""
        try:
            start_time = time.time()
            stats = await self.process_notification_queue()
            duration = time.time() - start_time
            
            logger.debug(f"Notification run completed in {duration:.2f}s: "
                        f"{stats['notifications_sent']} sent, {stats['notifications_failed']} failed")
            
            # Clean up old notifications
            if random.random() < 0.1:  # 10% chance to run cleanup
                cleaned = self.telegram_db.clean_old_notifications(30)
                if cleaned > 0:
                    logger.info(f"Cleaned up {cleaned} old notifications")
            
            return stats
        
        except Exception as e:
            logger.error(f"Error in notification run: {e}")
            return {"error": str(e)}
    
    async def run_continuously(self, interval: int = 30, stop_event=None):
        """
        Process the notification queue continuously with the specified interval.
        
        Args:
            interval: Seconds to wait between processing runs
            stop_event: Optional asyncio.Event to signal stopping
        """
        if stop_event is None:
            stop_event = asyncio.Event()
        
        logger.info(f"Starting continuous notification processing (interval: {interval}s)")
        
        try:
            while not stop_event.is_set():
                await self.run_once()
                
                logger.debug(f"Waiting {interval} seconds until next notification run...")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass  # This is expected when the timeout occurs
        
        except asyncio.CancelledError:
            logger.info("Notification task cancelled")
        except Exception as e:
            logger.error(f"Error in continuous notification processor: {e}")
            raise