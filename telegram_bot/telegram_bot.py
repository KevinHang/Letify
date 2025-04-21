"""
Telegram Bot for Dutch Real Estate Scraper - Refactored Implementation with Direct Command Preferences
"""

import asyncio
from typing import List
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes
)

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_USER_IDS, DB_CONNECTION_STRING
from database.property_db import PropertyDatabase
from database.telegram_db import TelegramDatabase
from utils.formatting import format_currency
from utils.logging_config import get_telegram_logger

# Use a child logger of the telegram logger
logger = get_telegram_logger("bot")

# Initialize databases
property_db = PropertyDatabase(DB_CONNECTION_STRING)
telegram_db = TelegramDatabase(DB_CONNECTION_STRING)

# Only conversation state we need is for admin commands
ADMIN_COMMAND = 0

# Property types
PROPERTY_TYPES = ["apartment", "house", "room", "studio", "any"]

class TelegramRealEstateBot:
    """Telegram bot for Dutch Real Estate Scraper"""
    
    def __init__(self, token: str, admin_ids: List[int] = None):
        """Initialize the bot with token and admin user IDs"""
        self.token = token
        self.admin_ids = admin_ids or []
        if not token:
            raise ValueError("Telegram Bot Token is empty or not set properly")
        
        # Create the application
        self.application = Application.builder().token(token).build()
        
        # Set up command handlers
        self.setup_handlers()
    
    def setup_handlers(self):
        """Set up command and message handlers"""
        logger.info("Setting up bot handlers")
        
        # Basic command handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("subscribe", self.subscribe_command))
        self.application.add_handler(CommandHandler("unsubscribe", self.unsubscribe_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("debug", self.debug_command))
        self.application.add_handler(CommandHandler("preferences", self.preferences_command))
        
        # Admin command handlers
        self.application.add_handler(CommandHandler("admin", self.admin_command))
        self.application.add_handler(CommandHandler("broadcast", self.broadcast_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        
        # Simplified preference setting commands
        self.application.add_handler(CommandHandler("cities", self.set_cities_command))
        self.application.add_handler(CommandHandler("minprice", self.set_min_price_command))
        self.application.add_handler(CommandHandler("maxprice", self.set_max_price_command))
        self.application.add_handler(CommandHandler("minrooms", self.set_min_rooms_command))
        self.application.add_handler(CommandHandler("maxrooms", self.set_max_rooms_command))
        self.application.add_handler(CommandHandler("minarea", self.set_min_area_command))
        self.application.add_handler(CommandHandler("maxarea", self.set_max_area_command))
        self.application.add_handler(CommandHandler("type", self.set_property_type_command))
        
        # Create a separate conversation handler for admin commands
        admin_conv_handler = ConversationHandler(
            entry_points=[CommandHandler("admin", self.admin_command)],
            states={
                ADMIN_COMMAND: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_admin_command)
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_admin)],
            name="admin_conversation",
            persistent=False,
        )
        self.application.add_handler(admin_conv_handler)
        
        # Handle property reactions
        self.application.add_handler(CallbackQueryHandler(self.property_reaction_handler))
        
        # Handle regular text messages
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Error handler
        self.application.add_error_handler(self.error_handler)
        
        logger.info("Handlers set up successfully")

    # ===== Base Commands =====
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /start command."""
        user = update.effective_user
        user_id = user.id
        is_admin = user_id in self.admin_ids
        logger.info(f"User {user_id} started the bot")
        
        # Register user in the database
        telegram_db.register_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            is_admin=is_admin
        )
        
        # Welcome message
        welcome_text = (
            f"👋 Hello {user.first_name}! Welcome to the Letify Bot.\n\n"
            f"I can notify you about new property listings that match your preferences.\n\n"
            f"Use /preferences to set your search criteria\n"
            f"Use /subscribe to start receiving notifications\n"
            f"Use /unsubscribe to stop receiving notifications\n"
            f"Use /status to check your current settings\n"
            f"Use /help to see all available commands"
        )
        
        # Quick reply keyboard
        keyboard = [
            [KeyboardButton("⚙️ Set Preferences"), KeyboardButton("📊 My Status")],
            [KeyboardButton("✅ Subscribe"), KeyboardButton("❌ Unsubscribe")],
            [KeyboardButton("❓ Help")]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /help command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        help_text = (
            "📋 Available commands:\n\n"
            "/start - Start the bot and see welcome message\n"
            "/help - Show this help message\n"
            "/preferences - View and set your property search preferences\n"
            "/subscribe - Start receiving notifications\n"
            "/unsubscribe - Stop receiving notifications\n"
            "/status - Check your current settings\n\n"
        )
        
        # Add admin commands if user is an admin
        user = telegram_db.get_user(user_id)
        if user and user.get('is_admin'):
            help_text += (
                "👑 Admin commands:\n\n"
                "/admin - Access admin functions\n"
                "/broadcast - Send a message to all users\n"
                "/stats - Show bot statistics\n"
            )
            
        await update.message.reply_text(help_text)

    async def subscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /subscribe command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        success = telegram_db.toggle_notifications(user_id, True)
        if success:
            await update.message.reply_text("✅ You've successfully subscribed to property notifications!")
        else:
            await update.message.reply_text("❌ Something went wrong. Please try again later.")

    async def unsubscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /unsubscribe command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        success = telegram_db.toggle_notifications(user_id, False)
        if success:
            await update.message.reply_text("✅ You've successfully unsubscribed from property notifications.")
        else:
            await update.message.reply_text("❌ Something went wrong. Please try again later.")

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /status command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        preferences = telegram_db.get_user_preferences(user_id)
        
        if not user:
            await update.message.reply_text("❌ User not found in database. Please use /start to register.")
            return
            
        status_text = "📊 Your current settings:\n\n"
        status_text += f"👤 User: {user.get('first_name', '')}\n"
        status_text += f"🔔 Notifications: {'Enabled' if user.get('notification_enabled') else 'Disabled'}\n"
        status_text += f"👑 Admin: {'Yes' if user.get('is_admin') else 'No'}\n\n"
        
        if preferences:
            status_text += "🏠 Property preferences:\n"
            if preferences.get('cities'):
                status_text += f"📍 Cities: {', '.join(preferences.get('cities'))}\n"
            if preferences.get('neighborhood'):
                status_text += f"🏙️ Neighborhood: {preferences.get('neighborhood')}\n"
            if preferences.get('property_type'):
                status_text += f"🏢 Property type: {preferences.get('property_type')}\n"
            if preferences.get('min_price') is not None:
                status_text += f"💰 Min price: {format_currency(preferences.get('min_price'))}\n"
            if preferences.get('max_price') is not None:
                status_text += f"💰 Max price: {'No limit' if preferences.get('max_price') == 0 else format_currency(preferences.get('max_price'))}\n"
            if preferences.get('min_rooms') is not None:
                status_text += f"🚪 Min rooms: {preferences.get('min_rooms')}\n"
            if preferences.get('max_rooms') is not None:
                status_text += f"🚪 Max rooms: {'No limit' if preferences.get('max_rooms') == 0 else preferences.get('max_rooms')}\n"
            if preferences.get('min_area') is not None:
                status_text += f"📏 Min area: {preferences.get('min_area')} m²\n"
            if preferences.get('max_area') is not None:
                status_text += f"📏 Max area: {'No limit' if preferences.get('max_area') == 0 else preferences.get('max_area')} m²\n"
            status_text += f"\nLast updated: {preferences.get('updated_at').strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            status_text += "🏠 No property preferences set. Use /preferences to set them."
            
        await update.message.reply_text(status_text)

    async def debug_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Debug command to inspect bot state."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        debug_text = (
            f"🛠 Debug Info\n\n"
            f"User ID: {user_id}\n"
            f"Bot Active: {self.application.running}\n"
        )
        
        await update.message.reply_text(debug_text)

    # ===== Admin Commands =====
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle the /admin command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return ConversationHandler.END
            
        admin_text = (
            "👑 Admin Menu\n\n"
            "Available commands:\n"
            "/broadcast - Send message to all users\n"
            "/stats - Show bot statistics\n\n"
            "Or use one of these functions:\n"
            "- makeadmin [user_id] - Make a user an admin\n"
            "- removeadmin [user_id] - Remove admin status\n"
            "- listusers - List all active users\n"
            "- listadmins - List all admin users\n"
            "- cleanqueue - Clean old notifications\n\n"
            "Type /cancel to exit admin mode."
        )
        
        await update.message.reply_text(admin_text)
        return ADMIN_COMMAND

    async def cancel_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Cancel the admin conversation."""
        await update.message.reply_text("Admin mode exited.")
        return ConversationHandler.END

    async def process_admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Process admin commands."""
        user_id = update.effective_user.id
        user = telegram_db.get_user(user_id)
        
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return ConversationHandler.END
            
        command_text = update.message.text.strip()
        command_parts = command_text.split()
        
        if not command_parts:
            await update.message.reply_text("❌ Invalid command. Use /admin to see available commands.")
            return ConversationHandler.END
            
        command = command_parts[0].lower()
        
        if command == "makeadmin" and len(command_parts) > 1:
            try:
                target_user_id = int(command_parts[1])
                success = telegram_db.set_admin_status(target_user_id, True)
                await update.message.reply_text(f"✅ User {target_user_id} is now an admin." if success else f"❌ Failed to make user {target_user_id} an admin.")
            except ValueError:
                await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")
                
        elif command == "removeadmin" and len(command_parts) > 1:
            try:
                target_user_id = int(command_parts[1])
                success = telegram_db.set_admin_status(target_user_id, False)
                await update.message.reply_text(f"✅ Admin status removed from user {target_user_id}." if success else f"❌ Failed to remove admin status from user {target_user_id}.")
            except ValueError:
                await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")
                
        elif command == "listusers":
            users = telegram_db.get_active_users()
            if users:
                user_text = "👥 Active users:\n\n"
                for i, u in enumerate(users, 1):
                    user_text += f"{i}. ID: {u['user_id']}, Name: {u['first_name']} {u['last_name'] or ''}"
                    if u['username']:
                        user_text += f" (@{u['username']})"
                    user_text += f" - Notifications: {'Enabled' if u['notification_enabled'] else 'Disabled'}\n"
                await update.message.reply_text(user_text)
            else:
                await update.message.reply_text("❌ No active users found.")
                
        elif command == "listadmins":
            admins = telegram_db.get_admin_users()
            if admins:
                admin_text = "👑 Admin users:\n\n"
                for i, a in enumerate(admins, 1):
                    admin_text += f"{i}. ID: {a['user_id']}, Name: {a['first_name']} {a['last_name'] or ''}"
                    if a['username']:
                        admin_text += f" (@{a['username']})"
                    admin_text += f" - Active: {'Yes' if a['is_active'] else 'No'}\n"
                await update.message.reply_text(admin_text)
            else:
                await update.message.reply_text("❌ No admin users found.")
                
        elif command == "cleanqueue":
            count = telegram_db.clean_old_notifications()
            await update.message.reply_text(f"✅ Cleaned {count} old notifications from the queue.")
            
        else:
            await update.message.reply_text("❌ Unknown command. Use /admin to see available commands.")
            
        # Stay in admin mode
        return ADMIN_COMMAND

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /broadcast command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
            
        if not context.args or not ' '.join(context.args).strip():
            await update.message.reply_text(
                "📢 Please provide a message to broadcast.\n"
                "Example: /broadcast Hello everyone! This is an announcement."
            )
            return
            
        broadcast_message = ' '.join(context.args)
        active_users = telegram_db.get_active_users()
        
        if not active_users:
            await update.message.reply_text("❌ No active users to broadcast to.")
            return
            
        confirm_text = (
            f"📢 You are about to broadcast the following message to {len(active_users)} users:\n\n"
            f"{broadcast_message}\n\n"
            f"Are you sure you want to proceed?"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("Yes", callback_data=f"broadcast_yes_{user_id}"),
                InlineKeyboardButton("No", callback_data=f"broadcast_no_{user_id}")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        context.user_data['broadcast_message'] = broadcast_message
        
        await update.message.reply_text(confirm_text, reply_markup=reply_markup)

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /stats command."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
            
        try:
            with property_db.conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM telegram_users")
                total_users = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM telegram_users WHERE is_active = TRUE")
                active_users = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM telegram_users WHERE is_active = TRUE AND notification_enabled = TRUE")
                subscribed_users = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM properties")
                total_properties = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM properties WHERE date_scraped > NOW() - INTERVAL '24 hours'")
                new_properties_24h = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM properties WHERE date_scraped > NOW() - INTERVAL '7 days'")
                new_properties_7d = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM notification_queue WHERE status = 'pending'")
                pending_notifications = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM notification_history WHERE sent_at > NOW() - INTERVAL '24 hours'")
                sent_notifications_24h = cur.fetchone()[0]
                
                stats_text = (
                    "📊 Bot Statistics\n\n"
                    f"👥 Users:\n"
                    f"  • Total users: {total_users}\n"
                    f"  • Active users: {active_users}\n"
                    f"  • Subscribed users: {subscribed_users}\n\n"
                    f"🏠 Properties:\n"
                    f"  • Total properties: {total_properties}\n"
                    f"  • New in last 24 hours: {new_properties_24h}\n"
                    f"  • New in last 7 days: {new_properties_7d}\n\n"
                    f"🔔 Notifications:\n"
                    f"  • Pending notifications: {pending_notifications}\n"
                    f"  • Sent in last 24 hours: {sent_notifications_24h}\n\n"
                    f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                
                await update.message.reply_text(stats_text)
                
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            await update.message.reply_text("❌ Error getting statistics. Please try again later.")

    # ===== Preferences Commands =====
    
    async def preferences_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /preferences command - shows a list of available preference commands."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        logger.info(f"Preferences command called by user_id: {user_id}")
        
        # Get current preferences to show their values
        preferences = telegram_db.get_user_preferences(user_id)
        
        # Format current values for display
        cities = ', '.join(preferences.get('cities', [])) if preferences and preferences.get('cities') else "Not set"
        min_price = format_currency(preferences.get('min_price')) if preferences and preferences.get('min_price') is not None else "Not set"
        max_price = format_currency(preferences.get('max_price')) if preferences and preferences.get('max_price') is not None else "Not set"
        
        if preferences and preferences.get('max_price') == 0:
            max_price = "No limit"
            
        min_rooms = str(preferences.get('min_rooms')) if preferences and preferences.get('min_rooms') is not None else "Not set"
        max_rooms = str(preferences.get('max_rooms')) if preferences and preferences.get('max_rooms') is not None else "Not set"
        
        if preferences and preferences.get('max_rooms') == 0:
            max_rooms = "No limit"
            
        min_area = f"{preferences.get('min_area')} m²" if preferences and preferences.get('min_area') is not None else "Not set"
        max_area = f"{preferences.get('max_area')} m²" if preferences and preferences.get('max_area') is not None else "Not set"
        
        if preferences and preferences.get('max_area') == 0:
            max_area = "No limit"
            
        property_type = preferences.get('property_type') if preferences and preferences.get('property_type') else "Not set"
        
        # Create the preferences menu text with simplified commands
        preferences_text = (
            "⚙️ Property Preferences\n\n"
            f"📍 Cities: {cities}\n"
            f"💰 Min price: {min_price}\n"
            f"💰 Max price: {max_price}\n"
            f"🚪 Min rooms: {min_rooms}\n"
            f"🚪 Max rooms: {max_rooms}\n"
            f"📏 Min area: {min_area}\n"
            f"📏 Max area: {max_area}\n"
            f"🏢 Property type: {property_type}\n\n"
            "Use these commands to update preferences:\n\n"
            "/cities Amsterdam, Rotterdam - Set your cities of interest\n"
            "/minprice 1000 - Set the minimum price\n"
            "/maxprice 2000 - Set the maximum price (use 0 for no limit)\n"
            "/minrooms 2 - Set the minimum number of rooms\n"
            "/maxrooms 4 - Set the maximum number of rooms (use 0 for no limit)\n"
            "/minarea 50 - Set the minimum area in m²\n"
            "/maxarea 100 - Set the maximum area in m² (use 0 for no limit)\n"
            "/type apartment - Set the property type (options: apartment, house, room, studio, any)\n"
        )
        
        await update.message.reply_text(preferences_text)

    async def set_cities_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set cities preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "📍 Please provide cities separated by commas.\n"
                "Example: /cities Amsterdam, Rotterdam"
            )
            return
            
        # Parse cities from arguments
        cities_input = ' '.join(context.args)
        cities = [city.strip().upper() for city in cities_input.split(',') if city.strip()]
        
        if not cities:
            await update.message.reply_text("❌ Invalid input. Please enter valid cities (e.g., Amsterdam, Rotterdam).")
            return
        
        # Get existing preferences
        preferences = telegram_db.get_user_preferences(user_id)
        if not preferences:
            preferences = {}
        
        # Update cities in preferences
        preferences['cities'] = cities
        
        # Save back to database
        success = telegram_db.set_user_preferences(user_id, preferences)
        
        if success:
            await update.message.reply_text(f"✅ Cities set to: {', '.join(cities)}")
        else:
            await update.message.reply_text("❌ Error saving preferences. Please try again.")

    async def set_min_price_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set minimum price preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "💰 Please provide a minimum price in EUR.\n"
                "Example: /minprice 1000"
            )
            return
        
        # Parse price from arguments
        try:
            min_price = int(''.join(context.args).replace('.', '').replace(',', ''))
            
            if min_price < 0:
                await update.message.reply_text("❌ Price cannot be negative. Please enter a valid price.")
                return
            
            # Get existing preferences
            preferences = telegram_db.get_user_preferences(user_id)
            if not preferences:
                preferences = {}
            
            # Update min price in preferences
            preferences['min_price'] = min_price
            
            # Save back to database
            success = telegram_db.set_user_preferences(user_id, preferences)
            
            if success:
                await update.message.reply_text(f"✅ Minimum price set to: {format_currency(min_price)}")
            else:
                await update.message.reply_text("❌ Error saving preferences. Please try again.")
                
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid price (numbers only).")

    async def set_max_price_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set maximum price preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "💰 Please provide a maximum price in EUR, or 0 for no limit.\n"
                "Example: /maxprice 2000"
            )
            return
        
        # Parse price from arguments
        try:
            max_price = int(''.join(context.args).replace('.', '').replace(',', ''))
            
            if max_price < 0:
                await update.message.reply_text("❌ Price cannot be negative. Please enter a valid price.")
                return
            
            # Get existing preferences
            preferences = telegram_db.get_user_preferences(user_id)
            if not preferences:
                preferences = {}
            
            # Update max price in preferences
            preferences['max_price'] = max_price
            
            # Save back to database
            success = telegram_db.set_user_preferences(user_id, preferences)
            
            if success:
                display_value = "No limit" if max_price == 0 else format_currency(max_price)
                await update.message.reply_text(f"✅ Maximum price set to: {display_value}")
            else:
                await update.message.reply_text("❌ Error saving preferences. Please try again.")
        except ValueError:
                    await update.message.reply_text("❌ Invalid input. Please enter a valid price (numbers only).")

    async def set_min_rooms_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set minimum rooms preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "🚪 Please provide a minimum number of rooms.\n"
                "Example: /minrooms 2"
            )
            return
        
        # Parse rooms from arguments
        try:
            min_rooms = int(context.args[0].strip())
            
            if min_rooms < 0:
                await update.message.reply_text("❌ Number of rooms cannot be negative. Please enter a valid number.")
                return
            
            # Get existing preferences
            preferences = telegram_db.get_user_preferences(user_id)
            if not preferences:
                preferences = {}
            
            # Update min rooms in preferences
            preferences['min_rooms'] = min_rooms
            
            # Save back to database
            success = telegram_db.set_user_preferences(user_id, preferences)
            
            if success:
                await update.message.reply_text(f"✅ Minimum rooms set to: {min_rooms}")
            else:
                await update.message.reply_text("❌ Error saving preferences. Please try again.")
                
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid number of rooms.")

    async def set_max_rooms_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set maximum rooms preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "🚪 Please provide a maximum number of rooms, or 0 for no limit.\n"
                "Example: /maxrooms 4"
            )
            return
        
        # Parse rooms from arguments
        try:
            max_rooms = int(context.args[0].strip())
            
            if max_rooms < 0:
                await update.message.reply_text("❌ Number of rooms cannot be negative. Please enter a valid number.")
                return
            
            # Get existing preferences
            preferences = telegram_db.get_user_preferences(user_id)
            if not preferences:
                preferences = {}
            
            # Update max rooms in preferences
            preferences['max_rooms'] = max_rooms
            
            # Save back to database
            success = telegram_db.set_user_preferences(user_id, preferences)
            
            if success:
                display_value = "No limit" if max_rooms == 0 else str(max_rooms)
                await update.message.reply_text(f"✅ Maximum rooms set to: {display_value}")
            else:
                await update.message.reply_text("❌ Error saving preferences. Please try again.")
                
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid number of rooms.")

    async def set_min_area_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set minimum area preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "📏 Please provide a minimum area in m².\n"
                "Example: /minarea 50"
            )
            return
        
        # Parse area from arguments
        try:
            min_area = int(''.join(context.args).replace('.', '').replace(',', ''))
            
            if min_area < 0:
                await update.message.reply_text("❌ Area cannot be negative. Please enter a valid area.")
                return
            
            # Get existing preferences
            preferences = telegram_db.get_user_preferences(user_id)
            if not preferences:
                preferences = {}
            
            # Update min area in preferences
            preferences['min_area'] = min_area
            
            # Save back to database
            success = telegram_db.set_user_preferences(user_id, preferences)
            
            if success:
                await update.message.reply_text(f"✅ Minimum area set to: {min_area} m²")
            else:
                await update.message.reply_text("❌ Error saving preferences. Please try again.")
                
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid area (numbers only).")

    async def set_max_area_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set maximum area preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            await update.message.reply_text(
                "📏 Please provide a maximum area in m², or 0 for no limit.\n"
                "Example: /maxarea 100"
            )
            return
        
        # Parse area from arguments
        try:
            max_area = int(''.join(context.args).replace('.', '').replace(',', ''))
            
            if max_area < 0:
                await update.message.reply_text("❌ Area cannot be negative. Please enter a valid area.")
                return
            
            # Get existing preferences
            preferences = telegram_db.get_user_preferences(user_id)
            if not preferences:
                preferences = {}
            
            # Update max area in preferences
            preferences['max_area'] = max_area
            
            # Save back to database
            success = telegram_db.set_user_preferences(user_id, preferences)
            
            if success:
                display_value = "No limit" if max_area == 0 else f"{max_area} m²"
                await update.message.reply_text(f"✅ Maximum area set to: {display_value}")
            else:
                await update.message.reply_text("❌ Error saving preferences. Please try again.")
                
        except ValueError:
            await update.message.reply_text("❌ Invalid input. Please enter a valid area (numbers only).")

    async def set_property_type_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Command to set property type preference directly."""
        user_id = update.effective_user.id
        
        # Get the command arguments
        if not context.args:
            property_types_text = ", ".join(PROPERTY_TYPES)
            await update.message.reply_text(
                f"🏢 Please provide a property type. Available options: {property_types_text}\n"
                "Example: /type apartment"
            )
            return
        
        # Parse property type from arguments
        property_type = context.args[0].strip().lower()
        
        # Check if valid property type
        if property_type not in PROPERTY_TYPES:
            property_types_text = ", ".join(PROPERTY_TYPES)
            await update.message.reply_text(
                f"❌ Invalid property type. Available options: {property_types_text}"
            )
            return
        
        # Get existing preferences
        preferences = telegram_db.get_user_preferences(user_id)
        if not preferences:
            preferences = {}
        
        # Update property type in preferences
        preferences['property_type'] = None if property_type == "any" else property_type
        
        # Save back to database
        success = telegram_db.set_user_preferences(user_id, preferences)
        
        if success:
            display_value = "Any" if property_type == "any" else property_type.capitalize()
            await update.message.reply_text(f"✅ Property type set to: {display_value}")
        else:
            await update.message.reply_text("❌ Error saving preferences. Please try again.")

    # ===== Property Reactions =====

    async def property_reaction_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle reactions to property notifications."""
        query = update.callback_query
        await query.answer()
        
        if not query.data.startswith(('like_', 'dislike_', 'save_', 'broadcast_')):
            return
            
        user_id = query.from_user.id
        telegram_db.update_user_activity(user_id)
        
        parts = query.data.split('_')
        action = parts[0]
        
        if action == "broadcast":
            await self.handle_broadcast_confirmation(query, context, parts)
            return
            
        property_id = int(parts[1])
        telegram_db.update_notification_reaction(user_id, property_id, action)
        
        keyboard = [
            [
                InlineKeyboardButton(
                    f"{'👍 Liked' if action == 'like' else '👎 Disliked' if action == 'dislike' else '🔖 Saved'}",
                    callback_data=f"{action}d_{property_id}"
                ),
                InlineKeyboardButton("🔍 View Details", url=f"YOUR_WEBSITE_URL/property/{property_id}")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_reply_markup(reply_markup=reply_markup)

    async def handle_broadcast_confirmation(self, query, context, parts):
        """Handle broadcast confirmation buttons."""
        if len(parts) < 3:
            await query.edit_message_text("❌ Invalid broadcast confirmation.")
            return
            
        admin_action = parts[1]
        admin_id = int(parts[2])
        user_id = query.from_user.id
        
        if user_id != admin_id:
            await query.edit_message_text("❌ Only the admin who initiated the broadcast can confirm it.")
            return
            
        if admin_action == "yes":
            active_users = telegram_db.get_active_users()
            broadcast_message = context.user_data.get('broadcast_message', '')
            
            if not broadcast_message:
                await query.edit_message_text("❌ Broadcast message not found.")
                return
                
            success_count = 0
            for user in active_users:
                try:
                    await context.bot.send_message(
                        chat_id=user['user_id'],
                        text=f"📢 Broadcast message from administrator:\n\n{broadcast_message}"
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"Error sending broadcast to user {user['user_id']}: {e}")
                    
            await query.edit_message_text(f"✅ Broadcast sent to {success_count} of {len(active_users)} users.")
        else:
            await query.edit_message_text("❌ Broadcast cancelled.")
            
        if 'broadcast_message' in context.user_data:
            del context.user_data['broadcast_message']

    # ===== General Message Handler =====

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle regular text messages that are not commands."""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        message_text = update.message.text.lower()
        
        # Handle custom keyboard button presses
        if message_text == "⚙️ set preferences":
            return await self.preferences_command(update, context)
        elif message_text == "📊 my status":
            return await self.status_command(update, context)
        elif message_text == "✅ subscribe":
            return await self.subscribe_command(update, context)
        elif message_text == "❌ unsubscribe":
            return await self.unsubscribe_command(update, context)
        elif message_text == "❓ help":
            return await self.help_command(update, context)
            
        # Default response for unrecognized messages - use safe send
        await self.safe_send_message(update, context, "I didn't understand that. Use /help to see available commands.")

    # ===== Error Handler =====

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle errors in the dispatcher."""
        logger.error(f"Exception while handling an update: {context.error}")
        
        # First, log the complete error with traceback
        logger.error("Error details:", exc_info=context.error)
        
        # Safe way to extract user_id
        user_id = None
        if update and hasattr(update, 'effective_user') and update.effective_user:
            user_id = update.effective_user.id
        
        # Notify admins
        error_text = f"⚠️ Error: {context.error}"
        
        if user_id:
            error_text += f"\nUser ID: {user_id}"
        
        if hasattr(context, 'chat_data') and context.chat_data:
            error_text += f"\nChat data: {str(context.chat_data)[:100]}..."
        
        if hasattr(context, 'user_data') and context.user_data:
            error_text += f"\nUser data: {str(context.user_data)[:100]}..."
        
        admin_users = telegram_db.get_admin_users()
        for admin in admin_users:
            try:
                await context.bot.send_message(chat_id=admin['user_id'], text=error_text)
            except Exception as e:
                logger.error(f"Error sending error notification to admin {admin['user_id']}: {e}")
        
        # Safely notify the user
        try:
            if update and hasattr(update, 'effective_chat') and update.effective_chat:
                chat_id = update.effective_chat.id
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="Sorry, something went wrong. Please try again later."
                )
        except Exception as e:
            logger.error(f"Error sending error message to user: {e}")

    # ===== Bot Runner =====

    async def run(self):
        """Start the bot."""
        try:
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling(
                poll_interval=1.0,
                timeout=10,
                drop_pending_updates=True
            )
            logger.info("Bot started successfully!")
            
            # Keep the bot running
            while True:
                await asyncio.sleep(3600)
                
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            raise
            
        finally:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            logger.info("Bot stopped successfully.")

    async def safe_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        """Safely send a message, falling back to different methods if one fails."""
        chat_id = None
        
        try:
            # First try: Use reply_text on the message if available
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(text)
                return
                
            # Second try: Use callback_query.message.reply_text if this is a callback
            if hasattr(update, 'callback_query') and update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(text)
                return
                
            # Third try: Use effective_message.reply_text
            if hasattr(update, 'effective_message') and update.effective_message:
                await update.effective_message.reply_text(text)
                return
                
            # Fourth try: Use effective_chat.id with context.bot.send_message
            if hasattr(update, 'effective_chat') and update.effective_chat:
                chat_id = update.effective_chat.id
                await context.bot.send_message(chat_id=chat_id, text=text)
                return
                
            # Fifth try: If we can get user_id, try to send a direct message
            if hasattr(update, 'effective_user') and update.effective_user:
                user_id = update.effective_user.id
                await context.bot.send_message(chat_id=user_id, text=text)
                return
                
            # Final fallback: Log that we couldn't send the message
            logger.error(f"Could not send message: {text[:50]}...")
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            
            # Last resort fallback if we have a chat_id
            if chat_id:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=text)
                except Exception as final_e:
                    logger.error(f"Final fallback failed: {final_e}")