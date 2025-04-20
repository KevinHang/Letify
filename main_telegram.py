"""
Main entry point for the Telegram bot and notification system.
Responsible for running the Telegram bot and processing notification queue.
"""

import asyncio
import logging
import sys
from typing import List

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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("telegram.log")
    ]
)
logger = logging.getLogger(__name__)

# Suppress httpx and telegram logs to avoid /getUpdates spam
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

class TelegramIntegration:
    """Integration class for Telegram bot and notification manager."""
    
    def __init__(self, connection_string: str, bot_token: str, admin_ids: List[int] = None):
        """
        Initialize the Telegram integration.
        
        Args:
            connection_string: Database connection string
            bot_token: Telegram bot token
            admin_ids: List of Telegram user IDs with admin privileges
        """
        self.connection_string = connection_string
        self.bot_token = bot_token
        self.admin_ids = admin_ids or []
        
        initialize_telegram_db(connection_string)
        self.telegram_db = TelegramDatabase(connection_string)
        self.bot = TelegramRealEstateBot(bot_token, admin_ids)
        self.notification_manager = TelegramNotificationManager(bot_token, connection_string)
        
        logger.info("Telegram integration initialized")
    
    async def start(self):
        """Start the Telegram bot and notification manager."""
        bot_task = asyncio.create_task(self.bot.run())
        notification_task = asyncio.create_task(
            self.notification_manager.run_continuously(NOTIFICATION_INTERVAL)
        )
        logger.info("Telegram integration started")
        return bot_task, notification_task
    
    async def notify_admins(self, message: str):
        """Send a notification to all admin users."""
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

async def main():
    """Main entry point for the Telegram bot and notification system."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set, exiting")
        sys.exit(1)
    
    integration = TelegramIntegration(
        DB_CONNECTION_STRING,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_ADMIN_USER_IDS
    )
    
    stop_event = asyncio.Event()
    try:
        import signal
        loop = asyncio.get_running_loop()
        for signal_name in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, signal_name),
                lambda: asyncio.create_task(stop_event.set())
            )
    except (NotImplementedError, ImportError):
        pass
    
    try:
        logger.info("Starting Telegram bot and notification manager...")
        bot_task, notification_task = await integration.start()
        await integration.notify_admins("🤖 Telegram bot started")
        
        await stop_event.wait()
    except Exception as e:
        logger.error(f"Error in Telegram main loop: {e}")
    finally:
        logger.info("Shutting down Telegram integration...")
        await integration.stop()
        await asyncio.gather(bot_task, notification_task, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())