"""
Integration between Dutch Real Estate Scraper and Telegram notification system.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Set

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_ADMIN_USER_IDS,
    DB_CONNECTION_STRING,
    NOTIFICATION_INTERVAL,
)
from database.migrations import initialize_telegram_db
from database.telegram_db import TelegramDatabase
from telegram_bot.telegram_bot import TelegramRealEstateBot
from telegram_bot.telegram_notification_manager import TelegramNotificationManager

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class TelegramIntegration:
    """
    Integration class that connects the real estate scraper with the Telegram notification system.
    """
    
    def __init__(self, connection_string: str, bot_token: str, admin_ids: List[int] = None):
        """
        Initialize the Telegram integration.
        
        Args:
            connection_string: Database connection string
            bot_token: Telegram bot token
            admin_ids: List of Telegram user IDs that have admin privileges
        """
        self.connection_string = connection_string
        self.bot_token = bot_token
        self.admin_ids = admin_ids or []
        
        # Initialize Telegram database tables
        initialize_telegram_db(connection_string)
        
        # Initialize components
        self.telegram_db = TelegramDatabase(connection_string)
        self.bot = TelegramRealEstateBot(bot_token, admin_ids)
        self.notification_manager = TelegramNotificationManager(bot_token, connection_string)
        
        # Track processed properties to avoid duplicates
        self.processed_properties: Set[int] = set()
        
        logger.info("Telegram integration initialized")
    
    async def process_new_listings(self, property_ids: List[int]) -> int:
        """
        Process a list of new property listings by adding them to the notification queue.
        
        Args:
            property_ids: List of property IDs
            
        Returns:
            Number of notifications created
        """
        total_notifications = 0
        
        for property_id in property_ids:
            # Skip if already processed
            if property_id in self.processed_properties:
                continue
            
            # Process new listing
            notifications = await self.notification_manager.process_new_listing(property_id)
            total_notifications += notifications
            
            # Add to processed set
            self.processed_properties.add(property_id)
            
            # Log
            if notifications > 0:
                logger.info(f"Added property ID {property_id} to notification queue for {notifications} users")
        
        # Limit the size of the processed set
        if len(self.processed_properties) > 10000:
            self.processed_properties = set(list(self.processed_properties)[-5000:])
        
        return total_notifications
    
    async def start(self):
        """Start the Telegram bot and notification manager."""
        # Start the bot
        bot_task = asyncio.create_task(self.bot.run())
        
        # Start the notification manager
        notification_task = asyncio.create_task(
            self.notification_manager.run_continuously(NOTIFICATION_INTERVAL)
        )
        
        # Log
        logger.info("Telegram integration started")
        
        return bot_task, notification_task
    
    async def notify_admins(self, message: str):
        """
        Send a notification to all admin users.
        
        Args:
            message: Message to send
        """
        admin_users = self.telegram_db.get_admin_users()
        
        for admin in admin_users:
            try:
                await self.bot.application.bot.send_message(
                    chat_id=admin['user_id'],
                    text=message
                )
                logger.info(f"Notification sent to admin {admin['user_id']}")
            except Exception as e:
                logger.error(f"Error sending notification to admin {admin['user_id']}: {e}")
    
    async def stop(self):
        """Stop the Telegram bot and notification manager."""
        await self.bot.application.stop()
        logger.info("Telegram integration stopped")


# Default integration instance for use in main application
telegram_integration = None

async def init_telegram(telegram_token=None, admin_user_ids=None):
    """Initialize the Telegram integration."""
    global telegram_integration
    token = telegram_token or TELEGRAM_BOT_TOKEN
    admin_ids = admin_user_ids or TELEGRAM_ADMIN_USER_IDS
    
    telegram_integration = TelegramIntegration(
        DB_CONNECTION_STRING,
        token,
        admin_ids
    )
    return telegram_integration

async def get_telegram_integration() -> Optional[TelegramIntegration]:
    """Get the Telegram integration instance."""
    global telegram_integration
    if telegram_integration is None:
        telegram_integration = await init_telegram()
    return telegram_integration