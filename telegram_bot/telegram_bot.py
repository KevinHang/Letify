import asyncio
from typing import List
from datetime import datetime, timezone, timedelta
import uuid

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)

from config import DB_CONNECTION_STRING, ALL_CITIES
from database.property_db import PropertyDatabase
from database.telegram_db import TelegramDatabase
from utils.utils import suggest_city, get_source_status_summary
from utils.formatting import format_currency
from utils.logging_config import get_telegram_logger

logger = get_telegram_logger("bot")

# Initialize databases
property_db = PropertyDatabase(DB_CONNECTION_STRING)
telegram_db = TelegramDatabase(DB_CONNECTION_STRING)

# Menu states (for callback data and input context)
MENU_STATES = {
    'main': 'main',
    'preferences': 'prefs',
    'cities': 'cities',
    'price': 'price',
    'rooms': 'rooms',
    'area': 'area',
    'type': 'type',
    'status': 'status',
    'help': 'help',
    'subscription': 'subs',
    'faq': 'faq',
}

# Property types
PROPERTY_TYPES = ["apartment", "house", "room", "studio", "any"]
UPDATING_CONTENT = "🤖 Updating content..."

class TelegramRealEstateBot:
    """Telegram bot for Dutch Real Estate Scraper with stateless menu system"""
    
    def __init__(self, token: str, admin_ids: List[int] = None):
        """Initialize the bot with token and admin user IDs"""
        self.token = token
        self.admin_ids = admin_ids or []
        if not token:
            raise ValueError("Telegram Bot Token is empty or not set properly")
        
        self.application = Application.builder().token(token).build()
        self.setup_handlers()
        logger.info("Loaded TelegramRealEstateBot v6 with reaction text support (2025-05-05)")

    def setup_handlers(self):
        """Set up command and message handlers"""
        logger.info("Setting up bot handlers")
        
        # Basic command handlers
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("menu", self.menu_command))
        self.application.add_handler(CommandHandler("cancel", self.cancel_command))
        self.application.add_handler(CommandHandler("debug", self.debug_command))
        
        # Admin command handlers
        self.application.add_handler(CommandHandler("admin", self.admin_command))
        self.application.add_handler(CommandHandler("makeadmin", self.makeadmin_command))
        self.application.add_handler(CommandHandler("removeadmin", self.removeadmin_command))
        self.application.add_handler(CommandHandler("listusers", self.listusers_command))
        self.application.add_handler(CommandHandler("listadmins", self.listadmins_command))
        self.application.add_handler(CommandHandler("cleanqueue", self.cleanqueue_command))
        self.application.add_handler(CommandHandler("broadcast", self.broadcast_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        
        # Menu interaction handlers
        self.application.add_handler(CallbackQueryHandler(self.handle_menu_callback, pattern="^menu:"))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        
        # Handle property reactions
        self.application.add_handler(CallbackQueryHandler(self.property_reaction_handler))
        
        # Error handler
        self.application.add_error_handler(self.error_handler)
        
        logger.info("Handlers set up successfully")

    # ===== Menu System =====
    
    async def menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Open a new main navigation menu"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        # Create a shortened menu ID (first 8 chars of UUID)
        full_menu_id = str(uuid.uuid4())
        menu_id = full_menu_id[:8]
        context.user_data['latest_menu_id'] = menu_id
        context.user_data['current_state'] = MENU_STATES['main']
        logger.debug(f"Opening new menu for user {user_id}: {menu_id}")
        
        await self.show_menu(update, context, MENU_STATES['main'], menu_id)

    async def show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, state: str, menu_id: str) -> None:
        """Display a menu based on the current state"""
        # Edge case where current state is same as new state (e.g. handle quick double-tap bug, still happends for cities, price, etc)
        if context.user_data.get('current_state', '') == state and state != 'main' and state != 'cities' and state != 'price' and state != 'rooms' and state != 'area' and state != 'type':
            return

        user_id = update.effective_user.id
        menu_text, keyboard = self.build_menu(state, menu_id, user_id)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = None
        disable_preview = False if "Frequently Asked Questions" in menu_text else True
        
        if update.callback_query:
            try:
                message = await update.callback_query.edit_message_text(menu_text, reply_markup=reply_markup, parse_mode="HTML", disable_web_page_preview=disable_preview)
            except Exception as e:
                logger.error(f"Error editing menu message for user {user_id} at state {state}: {e}")
                message = await update.callback_query.message.reply_text(menu_text, reply_markup=reply_markup, parse_mode="HTML", disable_web_page_preview=disable_preview)
        else:
            message = await update.message.reply_text(menu_text, reply_markup=reply_markup, parse_mode="HTML", disable_web_page_preview=disable_preview)
        
        # Store the message ID for future edits
        context.user_data['current_menu_message_id'] = message.message_id
        context.user_data['current_menu_chat_id'] = message.chat_id
        context.user_data['current_state'] = state
        context.user_data['latest_menu_id'] = menu_id

    def build_menu(self, state: str, menu_id: str, user_id: int) -> tuple[str, List[List[InlineKeyboardButton]]]:
        """Build menu text and keyboard based on state"""
        logger.debug(f"Building menu for user {user_id}, state: {state}")
        if state == MENU_STATES['main']:
            menu_text = (
                "🏡 Thanks for using Letify Bot!\n\n"
                "🏠 <b>Rental Preferences:</b> Set preferences to find your ideal home\n"
                "🔔 <b>Notifications:</b> Manage notifications\n"
                "📊 <b>Status:</b> Live system status\n"
                "❓ <b>Help:</b> Show available commands\n"
                "📚 <b>FAQ:</b> Learn more about Letify Bot\n"
                "❎ <b>Close Menu:</b> Close the current menu\n\n"
                'Official website: <a href="https://letify.nl">Letify.nl</a>\n'
                'Star the project on <a href="https://github.com/KevinHang/Letify">GitHub</a> to show your support ⭐️'
            )
            keyboard = [
                [InlineKeyboardButton("🏠 Rental Preferences", callback_data=f"menu:{MENU_STATES['preferences']}:{menu_id}")],
                [InlineKeyboardButton("🔔 Notifications", callback_data=f"menu:{MENU_STATES['subscription']}:{menu_id}"),
                 InlineKeyboardButton("📊 Status", callback_data=f"menu:{MENU_STATES['status']}:{menu_id}")],
                [InlineKeyboardButton("❓ Help", callback_data=f"menu:{MENU_STATES['help']}:{menu_id}"),
                 InlineKeyboardButton("📚 FAQ", callback_data=f"menu:{MENU_STATES['faq']}:{menu_id}")],
                [InlineKeyboardButton("❎ Close Menu", callback_data=f"menu:done:{menu_id}")]
            ]
            return menu_text, keyboard
        
        elif state == MENU_STATES['preferences']:
            preferences = telegram_db.get_user_preferences(user_id) or {}
            cities = ', '.join([city.title() for city in (preferences.get('cities', []))]) if preferences.get('cities') else "Not set"
            min_price = format_currency(preferences.get('min_price')) if preferences.get('min_price') is not None else "Not set"
            max_price = "No limit" if preferences.get('max_price') == 0 else format_currency(preferences.get('max_price')) if preferences.get('max_price') is not None else "Not set"
            min_rooms = str(preferences.get('min_rooms')) if preferences.get('min_rooms') is not None else "Not set"
            max_rooms = "No limit" if preferences.get('max_rooms') == 0 else str(preferences.get('max_rooms')) if preferences.get('max_rooms') is not None else "Not set"
            min_area = f"{preferences.get('min_area')} m²" if preferences.get('min_area') is not None else "Not set"
            max_area = "No limit" if preferences.get('max_area') == 0 else f"{preferences.get('max_area')} m²" if preferences.get('max_area') is not None else "Not set"
            property_type = ', '.join([pref.capitalize() for pref in (preferences.get('property_type', []))]) if preferences.get('property_type') else "Not set"
            last_update = preferences.get('updated_at').strftime('%Y-%m-%d %H:%M:%S') if preferences.get('updated_at') else "Never updated"

            menu_text = (
                "⚙️ Preferences Menu\n\n"
                f"📍 Cities: {cities}\n"
                f"💰 Price Range: {min_price} - {max_price}\n"
                f"🚪 Rooms: {min_rooms} - {max_rooms}\n"
                f"📏 Area: {min_area} - {max_area}\n"
                f"🏢 Property Types: {property_type}\n\n"
                f"Last updated: {last_update}\n\n"
                "Select an option to modify:"
            )
            keyboard = [
                [InlineKeyboardButton("📍 Cities", callback_data=f"menu:{MENU_STATES['cities']}:{menu_id}")],
                [InlineKeyboardButton("💰 Price Range", callback_data=f"menu:{MENU_STATES['price']}:{menu_id}")],
                [InlineKeyboardButton("🚪 Rooms", callback_data=f"menu:{MENU_STATES['rooms']}:{menu_id}")],
                [InlineKeyboardButton("📏 Area", callback_data=f"menu:{MENU_STATES['area']}:{menu_id}")],
                [InlineKeyboardButton("🏢 Property Types", callback_data=f"menu:{MENU_STATES['type']}:{menu_id}")],
                [InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]
            ]
            return menu_text, keyboard
        
        elif state == MENU_STATES['cities']:
            preferences = telegram_db.get_user_preferences(user_id) or {}
            cities = preferences.get('cities', []) or []
            cities_text = ', '.join([city.title() for city in cities]) if cities else "No cities selected"
            
            menu_text = (
                "📍 Cities Menu\n\n"
                f"Current cities: {cities_text}\n\n"
                "<b>Enter a city name to add, or use buttons to remove existing cities</b>\n"
            )
            keyboard = []
            for city in cities:
                callback_data = f"menu:city_rm:{city}:{menu_id}"
                if len(callback_data.encode('utf-8')) > 64:
                    logger.warning(f"Callback data too long for city {city}: {callback_data}")
                    continue
                keyboard.append([InlineKeyboardButton(f"Remove {city.title()}", callback_data=callback_data)])
            keyboard.append([InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['preferences']}:{menu_id}")])
            return menu_text, keyboard
        
        elif state == MENU_STATES['price']:
            preferences = telegram_db.get_user_preferences(user_id) or {}
            min_price = format_currency(preferences.get('min_price')) if preferences.get('min_price') is not None else "Not set"
            max_price = "No limit" if preferences.get('max_price') == 0 else format_currency(preferences.get('max_price')) if preferences.get('max_price') is not None else "Not set"
            
            menu_text = (
                "💰 Price Range Menu\n\n"
                f"Current minimum: {min_price}\n"
                f"Current maximum: {max_price}\n\n"
                "Enter a number to set minimum or maximum price (in EUR).\n"
                "Format: 'min 1000' or 'max 2000' (use 0 for no maximum)"
            )
            keyboard = [[InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['preferences']}:{menu_id}")]]
            return menu_text, keyboard
        
        elif state == MENU_STATES['rooms']:
            preferences = telegram_db.get_user_preferences(user_id) or {}
            min_rooms = str(preferences.get('min_rooms')) if preferences.get('min_rooms') is not None else "Not set"
            max_rooms = "No limit" if preferences.get('max_rooms') == 0 else str(preferences.get('max_rooms')) if preferences.get('max_rooms') is not None else "Not set"
            
            menu_text = (
                "🚪 Rooms Menu\n\n"
                f"Current minimum: {min_rooms}\n"
                f"Current maximum: {max_rooms}\n\n"
                "Enter a number to set minimum or maximum rooms.\n"
                "Format: 'min 2' or 'max 4' (use 0 for no maximum)"
            )
            keyboard = [[InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['preferences']}:{menu_id}")]]
            return menu_text, keyboard
        
        elif state == MENU_STATES['area']:
            preferences = telegram_db.get_user_preferences(user_id) or {}
            min_area = f"{preferences.get('min_area')} m²" if preferences.get('min_area') is not None else "Not set"
            max_area = "No limit" if preferences.get('max_area') == 0 else f"{preferences.get('max_area')} m²" if preferences.get('max_area') is not None else "Not set"
            
            menu_text = (
                "📏 Area Menu\n\n"
                f"Current minimum: {min_area}\n"
                f"Current maximum: {max_area}\n\n"
                "Enter a number to set minimum or maximum area (in m²).\n"
                "Format: 'min 50' or 'max 100' (use 0 for no maximum)"
            )
            keyboard = [[InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['preferences']}:{menu_id}")]]
            return menu_text, keyboard
        
        elif state == MENU_STATES['type']:
            preferences = telegram_db.get_user_preferences(user_id) or {}
            types = list(set(preferences.get('property_type', []) or []))  # Ensure no duplicates
            logger.debug(f"Building Property Types menu for user {user_id}, types: {types}")
            
            menu_text = (
                "🏢 Property Types\n\n"
                "Select or deselect property types."
            )
            keyboard = []
            for type_ in PROPERTY_TYPES:
                callback_data = f"menu:type_toggle:{type_}:{menu_id}"
                if len(callback_data.encode('utf-8')) > 64:
                    logger.warning(f"Callback data too long for type {type_}: {callback_data}")
                    continue
                button_text = f"✅ {type_.capitalize()}" if type_.upper() in types else type_.capitalize()
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                logger.debug(f"Built button for type {type_}: {button_text}")
            keyboard.append([InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['preferences']}:{menu_id}")])
            return menu_text, keyboard
        
        elif state == MENU_STATES['subscription']:
            user = telegram_db.get_user(user_id)
            status = "Enabled ✅ " if user and user.get('notification_enabled') and user.get('is_active') else "Disabled ❌"
            menu_text = (
                "🔔 Subscription Menu\n\n"
                f"Receive notifications: {status}\n\n"
                "Select an option:"
            )
            keyboard = [
                [InlineKeyboardButton("✅ Subscribe", callback_data=f"menu:sub:{menu_id}")],
                [InlineKeyboardButton("❌ Unsubscribe", callback_data=f"menu:unsub:{menu_id}")],
                [InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]
            ]
            return menu_text, keyboard
        
        elif state == MENU_STATES['status']:
            sources = telegram_db.get_distinct_sources_by_city()
            latest_per_source = telegram_db.get_latest_3_properties_per_source()

            menu_text = "📊 System Status\n\n"
            
            if sources and latest_per_source:
                status_summaries = get_source_status_summary(sources, latest_per_source)
                menu_text += f"{status_summaries}\n\n"

                menu_text += (
                    "<b>Scraper Status Explanation:</b>\n"
                    "🟢: Operational\n"
                    "🔴: No listings scraped → scraper is broken\n\n"
                    "<b>Formatter Status Explanation:</b>\n"
                    "🟢: Operational\n"
                    "🔴: Critical fields missing → no message can be built"
                )
            else:
                menu_text += "⚠️ Something went wrong while fetching system status."
            
            keyboard = [[InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]]
            return menu_text, keyboard
        
        elif state == MENU_STATES['help']:
            user = telegram_db.get_user(user_id)
            menu_text = (
                "📋 Available commands:\n\n"
                "/start - Start the bot and see welcome message\n"
                "/menu - Open the main navigation menu\n\n"
            )
            if user and user.get('is_admin'):
                menu_text += (
                    "👑 Admin commands:\n\n"
                    "/admin - Show admin command help\n"
                    "/makeadmin 'user_id' - Make a user an admin\n"
                    "/removeadmin 'user_id' - Remove admin status\n"
                    "/listusers - List all active users\n"
                    "/listadmins - List all admin users\n"
                    "/cleanqueue - Clean old notifications\n"
                    "/broadcast 'message' - Send a message to all users\n"
                    "/debug - Show debug information\n"
                    "/stats - Show bot statistics\n"
                )
            keyboard = [[InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]]
            return menu_text, keyboard
        
        elif state == MENU_STATES['faq']:
            menu_text = (
                "📚 Frequently Asked Questions\n\n"
                "<b>How does the rental finding work?</b>\n"
                "Letify Bot scans trusted Dutch rental websites every 5 minutes. Set at least one city and enable notifications to receive listings. Price, area, and room preferences are optional. Listings matching your criteria are sent with key details so you can act quickly.\n\n"
                "<b>Why is Letify Bot free?</b>\n"
                "As a solo developer, I believe everyone deserves fair housing opportunities without financial barriers. Unlike paid services with high fees, Letify Bot focuses on helping people find homes, not profiting from their search.\n\n"
                "<b>How does Letify Bot differ from competitors?</b>\n"
                "Many services exploit the urgency of house-hunting in the Netherlands' competitive market. Letify Bot doesn't hide essential features behind paywalls or sell your data. We're transparent, user-focused, and deliver timely rental listings.\n\n"
                "<b>What inspired Letify Bot?</b>\n"
                "Letify Bot was inspired by some other community efforts but is coded completely from scratch. This new implementation fixes several shortcomings of existing solutions, offering improved reliability, better matching algorithms, and enhanced user experience while maintaining simplicity and accessibility.\n\n"
                "<b>What data does Letify Bot store?</b>\n"
                "Letify Bot only stores your preference choices (cities, price range, etc.) which are necessary to match you with relevant listings, including your reaction text. No personal data, search history, or usage patterns are collected or stored. Your privacy is a priority!\n\n"
                "<b>Why am I not seeing many listings?</b>\n"
                "This could be due to limited properties matching your preferences in the competitive Dutch market. Try broadening your price range, area, or room requirements. Remember, at least one city must be set and notifications enabled.\n\n"
                "<b>How can I share feedback?</b>\n"
                "I welcome all suggestions and questions! Contact me directly at @wifbeliever on Telegram. Your input helps improve Letify Bot for everyone.\n\n"
                "<b>When will Letify Bot be open source?</b>\n"
                "The project was officially open-sourced on November 1st, 2025. You can check it out on <a href='https://github.com/KevinHang/Letify'>GitHub</a>. Contributions are welcome!\n\n"
            )
            keyboard = [[InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]]
            return menu_text, keyboard
        
        return "Unknown menu state.", [[]]

    async def handle_menu_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle menu callback queries"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        
        parts = query.data.split(':')
        if len(parts) < 3:
            await query.edit_message_text("❌ Invalid callback data.")
            return

        user = telegram_db.get_user(user_id)
        
        if user is None:
            await self.register_user_action(update)
        
        last_active = telegram_db.get_user_last_active(user_id)

        if last_active:
            current_time = datetime.now(timezone.utc)
            time_difference = current_time - last_active
            
            # Check if last active time is more than 8 hours ago, close menu
            if time_difference > timedelta(hours=8):
                await query.edit_message_text(
                    "⚠️ Your menu was opened more than 8 hours ago. Please use /menu to open a new menu."
                )
                message = await context.bot.send_message(chat_id=update.effective_user.id, text=UPDATING_CONTENT, disable_notification=True)
                await context.bot.delete_message(message.chat_id, message.message_id)
                telegram_db.update_user_activity(user_id)
                return
            # Check if last active time is more than 5 minutes ago, refresh context
            elif time_difference > timedelta(minutes=5):
                message = await context.bot.send_message(chat_id=update.effective_user.id, text=UPDATING_CONTENT, disable_notification=True)
                await context.bot.delete_message(message.chat_id, message.message_id)
        
        action = parts[1]
        
        # Handle actions with extra parameters (city_rm, type_toggle)
        if action in ['city_rm', 'type_toggle']:
            if len(parts) != 4:
                await query.edit_message_text("❌ Invalid callback data for action.")
                return
            item, menu_id = parts[2], parts[3]
        else:
            menu_id = parts[2]
        
        # Validate menu ID
        latest_menu_id = context.user_data.get('latest_menu_id')
        if menu_id != latest_menu_id:
            logger.debug(f"Callback for menu {menu_id} is outdated for user {user_id}")
            await query.edit_message_text("⚠️ This menu is outdated. Use /menu to open a new one.")
            message = await context.bot.send_message(chat_id=update.effective_user.id, text=UPDATING_CONTENT, disable_notification=True)
            await context.bot.delete_message(message.chat_id, message.message_id)
            telegram_db.update_user_activity(user_id)
            return
        
        telegram_db.update_user_activity(user_id)
        
        if action == 'done':
            await query.edit_message_text("✅ Menu closed. Use /menu to open a new one.")
            context.user_data.pop('latest_menu_id', None)
            context.user_data.pop('current_state', None)
            context.user_data.pop('current_menu_message_id', None)
            context.user_data.pop('current_menu_chat_id', None)
            logger.debug(f"Closed menu for user {user_id}: {menu_id}")
            return
        
        if action in MENU_STATES.values():
            await self.show_menu(update, context, action, menu_id)
            return
        
        # Handle specific actions
        preferences = telegram_db.get_user_preferences(user_id) or {}
        
        if action == 'city_rm':
            preferences['cities'] = [c for c in preferences.get('cities', []) if c != item]
            telegram_db.set_user_preferences(user_id, preferences)
            confirmation = await query.message.reply_text(
                f"✅ City <b>{item.title()}</b> removed.\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                parse_mode="HTML"
            )
            asyncio.create_task(self.delete_message_later(confirmation.chat_id, confirmation.message_id))
            await self.show_menu(update, context, MENU_STATES['cities'], menu_id)
        
        elif action == 'type_toggle':
            types = list(set(t.lower() for t in preferences.get('property_type', []) or []))  # Normalize to lowercase
            old_types = types.copy()  # Store for comparison
            item = item.lower()  # Normalize item
            logger.debug(f"Type toggle for user {user_id}: item={item}, current_types={types}")
            
            # Toggle logic
            if item in types:
                types.remove(item)
                logger.debug(f"Deselected {item}, new_types={types}")
            else:
                if item == 'any':
                    types = ['any']
                    logger.debug(f"Selected 'any', cleared others, new_types={types}")
                else:
                    types = [t for t in types if t != 'any']
                    types.append(item)
                    logger.debug(f"Selected {item}, removed 'any', new_types={types}")
            
            # Skip if no change
            if sorted(types) == sorted(old_types):
                logger.debug(f"No change in types for user {user_id}: {types}, skipping update")
                return
            
            # Update preferences and menu
            preferences['property_type'] = list(set(types))
            telegram_db.set_user_preferences(user_id, preferences)
            logger.debug(f"Updated preferences for user {user_id}: property_type={types}")
            await self.show_menu(update, context, MENU_STATES['type'], menu_id)
        
        elif action == 'sub':
            user = telegram_db.get_user(user_id)
            if user and user.get('notification_enabled') and user.get('is_active'):
                logger.debug(f"User {user_id} already subscribed, skipping update")
                return
            success = telegram_db.toggle_notifications(user_id, True)
            if success and not user.get('is_active'):
                telegram_db.toggle_user_active(user_id, True)
            menu_text = (
                "🔔 Subscription Menu\n\n" +
                ("Receive notifications: Enabled ✅" if success
                 else "❌ Something went wrong. Please try again later.") +
                "\n\nSelect an option:"
            )
            keyboard = [
                [InlineKeyboardButton("✅ Subscribe", callback_data=f"menu:sub:{menu_id}")],
                [InlineKeyboardButton("❌ Unsubscribe", callback_data=f"menu:unsub:{menu_id}")],
                [InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]
            ]
            await query.edit_message_text(menu_text, reply_markup=InlineKeyboardMarkup(keyboard))
        
        elif action == 'unsub':
            user = telegram_db.get_user(user_id)
            if user and (not user.get('notification_enabled') or not user.get('is_active')):
                logger.debug(f"User {user_id} already unsubscribed, skipping update")
                return
            success = telegram_db.toggle_notifications(user_id, False)
            menu_text = (
                "🔔 Subscription Menu\n\n" +
                ("Receive notifications: Disabled ❌" if success
                 else "❌ Something went wrong. Please try again later.") +
                "\n\nSelect an option:"
            )
            keyboard = [
                [InlineKeyboardButton("✅ Subscribe", callback_data=f"menu:sub:{menu_id}")],
                [InlineKeyboardButton("❌ Unsubscribe", callback_data=f"menu:unsub:{menu_id}")],
                [InlineKeyboardButton("↩ Return", callback_data=f"menu:{MENU_STATES['main']}:{menu_id}")]
            ]
            await query.edit_message_text(menu_text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle text messages for menu inputs or general messages"""
        user_id = update.effective_user.id
        user = telegram_db.get_user(user_id)
        
        if user is None:
            await self.register_user_action(update)

        telegram_db.update_user_activity(user_id)
        
        message_text = update.message.text.lower().strip()
        
        current_state = context.user_data.get('current_state')
        menu_id = context.user_data.get('latest_menu_id')
        message_id = context.user_data.get('current_menu_message_id')
        chat_id = context.user_data.get('current_menu_chat_id')
        
        if not current_state or not menu_id or not message_id or not chat_id:
            message = await update.message.reply_text(
                "No active menu. Use /menu to open one.",
                parse_mode="HTML"
            )
            return
        
        preferences = telegram_db.get_user_preferences(user_id) or {}
        
        # Store input message details for deletion
        input_chat_id = update.message.chat_id
        input_message_id = update.message.message_id
        
        if current_state == MENU_STATES['cities']:
            city_input = update.message.text.strip().upper()
            cities = preferences.get('cities', []) or []
            
            if city_input not in ALL_CITIES:
                suggestion = suggest_city(city_input)
                error_message = (
                    f'❌ City <b>{city_input.title()}</b> does not exist! Do you mean <b>{suggestion[0].title()}</b>?'
                    if suggestion else f"❌ City '{city_input.title()}' does not exist!"
                )
                error_message += "\n\n<em>This message will be auto-deleted in 10 seconds ⏳</em>"
                message = await update.message.reply_text(error_message, parse_mode="HTML")
                asyncio.create_task(self.delete_message_later(message.chat_id, message.message_id, 15))
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete city input message for user {user_id}: {e}")
                return
            
            if city_input in cities:
                logger.debug(f"City {city_input} already in preferences for user {user_id}, skipping menu update")
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete city input message for user {user_id}: {e}")
                return
            
            cities.append(city_input)
            preferences['cities'] = cities
            telegram_db.set_user_preferences(user_id, preferences)
            
            # Send confirmation message
            confirmation = await update.message.reply_text(
                f"✅ City <b>{city_input.title()}</b> added.\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                parse_mode="HTML"
            )
            asyncio.create_task(self.delete_message_later(confirmation.chat_id, confirmation.message_id))
            
            # Update the existing menu
            menu_text, keyboard = self.build_menu(MENU_STATES['cities'], menu_id, user_id)
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=menu_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error editing cities menu for user {user_id}: {e}")
                # Send a new message and update stored IDs
                new_message = await context.bot.send_message(
                    chat_id=chat_id,
                    text=menu_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                context.user_data['current_menu_message_id'] = new_message.message_id
                context.user_data['current_menu_chat_id'] = new_message.chat_id
            
            # Delete the user's input message
            try:
                await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
            except Exception as e:
                logger.warning(f"Failed to delete city input message for user {user_id}: {e}")
        
        elif current_state == MENU_STATES['price']:
            try:
                parts = message_text.split()
                if len(parts) != 2 or parts[0] not in ['min', 'max']:
                    raise ValueError("Invalid format")
                
                value = int(parts[1].replace('.', '').replace(',', ''))
                if value < 0:
                    raise ValueError("Price cannot be negative")
                
                # Check if the value is already set
                if parts[0] == 'min' and preferences.get('min_price') == value:
                    logger.debug(f"Min price {value} already set for user {user_id}, skipping menu update")
                    try:
                        await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete price input message for user {user_id}: {e}")
                    return
                if parts[0] == 'max' and preferences.get('max_price') == value:
                    logger.debug(f"Max price {value} already set for user {user_id}, skipping menu update")
                    try:
                        await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete price input message for user {user_id}: {e}")
                    return
                
                if parts[0] == 'min':
                    preferences['min_price'] = value
                else:
                    preferences['max_price'] = value
                
                telegram_db.set_user_preferences(user_id, preferences)

                if parts[0] == 'max' and value == 0:
                    set_value = 'no limit'
                else:
                    set_value = format_currency(value)
                
                # Send confirmation message
                confirmation = await update.message.reply_text(
                    f"✅ {'Minimum' if parts[0] == 'min' else 'Maximum'} price set to {set_value}.\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                    parse_mode="HTML"
                )
                asyncio.create_task(self.delete_message_later(confirmation.chat_id, confirmation.message_id))
                
                menu_text, keyboard = self.build_menu(MENU_STATES['price'], menu_id, user_id)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=menu_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except Exception as e:
                    logger.error(f"Error editing price menu for user {user_id}: {e}")
                    new_message = await context.bot.send_message(
                        chat_id=chat_id,
                        text=menu_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    context.user_data['current_menu_message_id'] = new_message.message_id
                    context.user_data['current_menu_chat_id'] = new_message.chat_id
                
                # Delete the user's input message
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete price input message for user {user_id}: {e}")
            
            except ValueError:
                message = await update.message.reply_text(
                    "❌ Invalid input. Use format: 'min 1000' or 'max 2000'\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                    parse_mode="HTML"
                )
                asyncio.create_task(self.delete_message_later(message.chat_id, message.message_id))
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete price input message for user {user_id}: {e}")
        
        elif current_state == MENU_STATES['rooms']:
            try:
                parts = message_text.split()
                if len(parts) != 2 or parts[0] not in ['min', 'max']:
                    raise ValueError("Invalid format")
                
                value = int(parts[1])
                if value < 0:
                    raise ValueError("Rooms cannot be negative")
                
                # Check if the value is already set
                if parts[0] == 'min' and preferences.get('min_rooms') == value:
                    logger.debug(f"Min rooms {value} already set for user {user_id}, skipping menu update")
                    try:
                        await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete rooms input message for user {user_id}: {e}")
                    return
                if parts[0] == 'max' and preferences.get('max_rooms') == value:
                    logger.debug(f"Max rooms {value} already set for user {user_id}, skipping menu update")
                    try:
                        await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete rooms input message for user {user_id}: {e}")
                    return
                
                if parts[0] == 'min':
                    preferences['min_rooms'] = value
                else:
                    preferences['max_rooms'] = value
                
                telegram_db.set_user_preferences(user_id, preferences)

                if parts[0] == 'max' and value == 0:
                    set_value = 'no limit'
                else:
                    set_value = value
                
                # Send confirmation message
                confirmation = await update.message.reply_text(
                    f"✅ {'Minimum' if parts[0] == 'min' else 'Maximum'} rooms set to {set_value}.\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                    parse_mode="HTML"
                )
                asyncio.create_task(self.delete_message_later(confirmation.chat_id, confirmation.message_id))
                
                menu_text, keyboard = self.build_menu(MENU_STATES['rooms'], menu_id, user_id)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=menu_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except Exception as e:
                    logger.error(f"Error editing rooms menu for user {user_id}: {e}")
                    new_message = await context.bot.send_message(
                        chat_id=chat_id,
                        text=menu_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    context.user_data['current_menu_message_id'] = new_message.message_id
                    context.user_data['current_menu_chat_id'] = new_message.chat_id
                
                # Delete the user's input message
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete rooms input message for user {user_id}: {e}")
            
            except ValueError:
                message = await update.message.reply_text(
                    "❌ Invalid input. Use format: 'min 2' or 'max 4'\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                    parse_mode="HTML"
                )
                asyncio.create_task(self.delete_message_later(message.chat_id, message.message_id))
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete rooms input message for user {user_id}: {e}")
        
        elif current_state == MENU_STATES['area']:
            try:
                parts = message_text.split()
                if len(parts) != 2 or parts[0] not in ['min', 'max']:
                    raise ValueError("Invalid format")
                
                value = int(parts[1])
                if value < 0:
                    raise ValueError("Area cannot be negative")
                
                # Check if the value is already set
                if parts[0] == 'min' and preferences.get('min_area') == value:
                    logger.debug(f"Min area {value} already set for user {user_id}, skipping menu update")
                    try:
                        await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete area input message for user {user_id}: {e}")
                    return
                if parts[0] == 'max' and preferences.get('max_area') == value:
                    logger.debug(f"Max area {value} already set for user {user_id}, skipping menu update")
                    try:
                        await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                    except Exception as e:
                        logger.warning(f"Failed to delete area input message for user {user_id}: {e}")
                    return
                
                if parts[0] == 'min':
                    preferences['min_area'] = value
                else:
                    preferences['max_area'] = value
                
                telegram_db.set_user_preferences(user_id, preferences)

                if parts[0] == 'max' and value == 0:
                    set_value = 'no limit'
                else:
                    set_value = f"{value}  m²"
                
                # Send confirmation message
                confirmation = await update.message.reply_text(
                    f"✅ {'Minimum' if parts[0] == 'min' else 'Maximum'} area set to {set_value}.\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                    parse_mode="HTML"
                )
                asyncio.create_task(self.delete_message_later(confirmation.chat_id, confirmation.message_id))
                
                menu_text, keyboard = self.build_menu(MENU_STATES['area'], menu_id, user_id)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=menu_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except Exception as e:
                    logger.error(f"Error editing area menu for user {user_id}: {e}")
                    new_message = await context.bot.send_message(
                        chat_id=chat_id,
                        text=menu_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    context.user_data['current_menu_message_id'] = new_message.message_id
                    context.user_data['current_menu_chat_id'] = new_message.chat_id
                
                # Delete the user's input message
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete area input message for user {user_id}: {e}")
            
            except ValueError:
                message = await update.message.reply_text(
                    "❌ Invalid input. Use format: 'min 50' or 'max 100'\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                    parse_mode="HTML"
                )
                asyncio.create_task(self.delete_message_later(message.chat_id, message.message_id))
                try:
                    await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
                except Exception as e:
                    logger.warning(f"Failed to delete area input message for user {user_id}: {e}")
        
        elif current_state == MENU_STATES['type']:
            # Ignore text input for property types; use buttons instead
            message = await update.message.reply_text(
                "Please use the buttons to select property types.\n\n<em>This message will be auto-deleted in 5 seconds ⏳</em>",
                parse_mode="HTML"
            )
            asyncio.create_task(self.delete_message_later(message.chat_id, message.message_id))
            try:
                await context.bot.delete_message(chat_id=input_chat_id, message_id=input_message_id)
            except Exception as e:
                logger.warning(f"Failed to delete type input message for user {user_id}: {e}")
        
        else:
            message = await update.message.reply_text(
                "Please use the menu buttons or /menu to open a new one.",
                parse_mode="HTML"
            )

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Cancel the current menu"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        if 'latest_menu_id' in context.user_data:
            logger.debug(f"Closing menu for user {user_id}: {context.user_data['latest_menu_id']}")
            context.user_data.pop('latest_menu_id', None)
            context.user_data.pop('current_state', None)
            context.user_data.pop('current_menu_message_id', None)
            context.user_data.pop('current_menu_chat_id', None)
        
        await update.message.reply_text("✅ Menu closed. Use /menu to open a new one.")

    async def delete_message_later(self, chat_id: int, message_id: int, delay_seconds: int = 5):
        """
        Deletes a message after a specified delay.
        
        Args:
            chat_id (int): The chat ID where the message is located
            message_id (int): The ID of the message to delete
            delay_seconds (int, optional): Delay in seconds before deletion. Defaults to 5.
        """
        try:
            await asyncio.sleep(delay_seconds)
            await self.application.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.debug(f"Successfully deleted message {message_id} in chat {chat_id} after {delay_seconds}s")
        except Exception as e:
            logger.error(f"Error deleting message {message_id} in chat {chat_id}: {e}")

    # ===== Base Commands =====
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /start command"""
        user = update.effective_user
        user_id = user.id
        user_info = telegram_db.get_user(user_id)

        if user_info is None:
            await self.register_user_action(update)
        
        welcome_text = (
            f"👋 Hello {user.first_name}! Welcome to the Letify Bot.\n\n"
            f"I can notify you about new property listings that match your preferences.\n\n"
            f"Set at least one city in the preferences to start receiving notifications. Other preferences such as price and rooms are optional.\n\n"
            # f"You can set a custom reaction text in the menu to quickly copy a message for each listing.\n\n"
            f"Use /menu to access all features and settings.\n"
        )
        
        await update.message.reply_text(welcome_text)

    async def debug_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Debug command to inspect bot state"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)

        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
        
        debug_text = (
            f"🛠 Debug Info\n\n"
            f"User ID: {user_id}\n"
            f"Bot Active: {self.application.running}\n"
            f"Latest Menu ID: {context.user_data.get('latest_menu_id', 'None')}\n"
            f"Current State: {context.user_data.get('current_state', 'None')}\n"
            f"Current Menu Message ID: {context.user_data.get('current_menu_message_id', 'None')}\n"
            f"Current Menu Chat ID: {context.user_data.get('current_menu_chat_id', 'None')}\n"
        )
        
        await update.message.reply_text(debug_text)

    # ===== Admin Commands =====
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /admin command"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
            
        admin_text = (
            "👑 Admin Commands\n\n"
            "Available commands:\n"
            "/makeadmin <user_id> - Make a user an admin\n"
            "/removeadmin <user_id> - Remove admin status\n"
            "/listusers - List all active users\n"
            "/listadmins - List all admin users\n"
            "/cleanqueue - Clean old notifications\n"
            "/broadcast <message> - Send a message to all users\n"
            "/stats - Show bot statistics\n"
            "/debug - See bot debug information\n"
            # "/cancel - Cancel current operation\n"
        )
        
        await update.message.reply_text(admin_text)

    async def makeadmin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /makeadmin command"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
        
        if not context.args:
            await update.message.reply_text("❌ Please provide a user ID. Usage: /makeadmin <user_id>")
            return
        
        try:
            target_user_id = int(context.args[0])
            success = telegram_db.set_admin_status(target_user_id, True)
            await update.message.reply_text(
                f"✅ User {target_user_id} is now an admin." if success
                else f"❌ Failed to make user {target_user_id} an admin."
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")

    async def removeadmin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /removeadmin command"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
        
        if not context.args:
            await update.message.reply_text("❌ Please provide a user ID. Usage: /removeadmin <user_id>")
            return
        
        try:
            target_user_id = int(context.args[0])
            success = telegram_db.set_admin_status(target_user_id, False)
            await update.message.reply_text(
                f"✅ Admin status removed from user {target_user_id}." if success
                else f"❌ Failed to remove admin status from user {target_user_id}."
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID. Please provide a numeric ID.")

    async def listusers_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /listusers command"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
        
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

    async def listadmins_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /listadmins command"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
        
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

    async def cleanqueue_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /cleanqueue command"""
        user_id = update.effective_user.id
        telegram_db.update_user_activity(user_id)
        
        user = telegram_db.get_user(user_id)
        if not user or not user.get('is_admin'):
            await update.message.reply_text("❌ You do not have permission to use admin commands.")
            return
        
        count = telegram_db.clean_old_notifications()
        await update.message.reply_text(f"✅ Cleaned {count} old notifications from the queue.")

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle the /broadcast command"""
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
        """Handle the /stats command"""
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

    # ===== Property Reactions =====

    async def property_reaction_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle reactions to property notifications"""
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
        """Handle broadcast confirmation buttons"""
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
            else:
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

    # ===== Error Handler =====

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle errors in the dispatcher"""
        logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
        
        user_id = None
        if update and hasattr(update, 'effective_user') and update.effective_user:
            user_id = update.effective_user.id
        
        error_text = f"⚠️ Error: {context.error}"
        if user_id:
            error_text += f"\nUser ID: {user_id}"
        if hasattr(context, 'chat_data') and context.chat_data:
            error_text += f"\nChat data: {str(context.chat_data)}"
        if hasattr(context, 'user_data') and context.user_data:
            error_text += f"\nUser data: {str(context.user_data)}"

        logger.error(f"Exception while handling an update (extended): {error_text}")
        
        admin_users = telegram_db.get_admin_users()
        for admin in admin_users:
            try:
                await context.bot.send_message(chat_id=admin['user_id'], text=error_text)
            except Exception as e:
                logger.error(f"Error sending error notification to admin {admin['user_id']}: {e}")
        
        try:
            if update and hasattr(update, 'effective_chat') and update.effective_chat:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="Sorry, something went wrong. Please try again later."
                )
        except Exception as e:
            logger.error(f"Error sending error message to user: {e}")

    # ===== Bot Runner =====

    async def run(self):
        """Start the bot"""
        try:
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling(
                poll_interval=1.0,
                timeout=10,
                drop_pending_updates=True
            )
            logger.info("Bot started successfully!")
            
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

    async def register_user_action(self, update: Update) -> None:
        user = update.effective_user
        user_id = user.id
        is_admin = user_id in self.admin_ids
        
        default_reaction_text = (
            "Interested in {ADDRESS}, please contact me!"
        )
        
        telegram_db.register_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            is_admin=is_admin,
            reaction_text=default_reaction_text
        )

    async def safe_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
        """Safely send a message, falling back to different methods if one fails"""
        chat_id = None
        
        try:
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(text)
                return
            if hasattr(update, 'callback_query') and update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text(text)
                return
            if hasattr(update, 'effective_message') and update.effective_message:
                await update.effective_message.reply_text(text)
                return
            if hasattr(update, 'effective_chat') and update.effective_chat:
                chat_id = update.effective_chat.id
                await context.bot.send_message(chat_id=chat_id, text=text)
                return
            if hasattr(update, 'effective_user') and update.effective_user:
                await context.bot.send_message(chat_id=update.effective_user.id, text=text)
                return
            logger.error(f"Could not send message: {text[:50]}...")
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            if chat_id:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=text)
                except Exception as final_e:
                    logger.error(f"Final fallback failed: {final_e}")
