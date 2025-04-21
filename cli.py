"""
Command-line interface for the Dutch Real Estate Scraper.
Handles scraping configuration and Telegram user management without running the Telegram bot.
"""

import os
import sys
import argparse
import asyncio
import logging
from datetime import datetime
from utils.logging_config import configure_cli_logging

from config import (
    DB_CONNECTION_STRING,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_SOURCES,
    DEFAULT_CITIES,
    MAX_RESULTS_PER_SCAN,
    MAX_CONCURRENT_REQUESTS,
    USE_PROXIES,
    PROXY_LIST,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_ADMIN_USER_IDS
)
from main import RealEstateScraper
from scrapers.factory import RealEstateScraperFactory
from utils.proxy_manager import ProxyManager
from database.property_db import PropertyDatabase
from database.telegram_db import TelegramDatabase
from database.migrations import initialize_telegram_db
import telegram

# Set up logging
logger = configure_cli_logging()

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Dutch Real Estate Scraper CLI")
    
    # Scan mode group
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--city-scan", action="store_true",
                            help="City scan mode (default when cities are specified)")
    mode_group.add_argument("--query-scan", action="store_true",
                            help="Query URL scan mode")
    mode_group.add_argument("--combined-scan", action="store_true",
                            help="Combined mode: scan both cities and query URLs")
    
    parser.add_argument("--sources", type=str, default=",".join(DEFAULT_SOURCES),
                        help=f"Comma-separated list of sources (default: {','.join(DEFAULT_SOURCES)})")
    parser.add_argument("--cities", type=str, default=",".join(DEFAULT_CITIES),
                        help=f"Comma-separated list of cities (default: {','.join(DEFAULT_CITIES)})")
    parser.add_argument("--interval", type=int, default=DEFAULT_SCAN_INTERVAL,
                        help=f"Scraping interval in seconds (default: {DEFAULT_SCAN_INTERVAL})")
    parser.add_argument("--db", type=str, default=DB_CONNECTION_STRING,
                        help="PostgreSQL connection string")
    parser.add_argument("--max-results", type=int, default=MAX_RESULTS_PER_SCAN,
                        help=f"Maximum results per scan (default: {MAX_RESULTS_PER_SCAN})")
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT_REQUESTS,
                        help=f"Maximum concurrent requests (default: {MAX_CONCURRENT_REQUESTS})")
    parser.add_argument("--once", action="store_true",
                        help="Run only one scan cycle")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    parser.add_argument("--list-sources", action="store_true",
                        help="List available sources and exit")

    # Query URL options
    url_group = parser.add_argument_group("Query URL Options")
    url_group.add_argument("--add-query-url", type=str,
                          help="Add query URL in format source:url")
    url_group.add_argument("--query-method", type=str, choices=["GET", "POST"], default="GET",
                          help="HTTP method for query URL (GET or POST)")
    url_group.add_argument("--disable", action="store_true",
                          help="Add query URL in disabled state")
    url_group.add_argument("--add-query-description", type=str,
                          help="Description for the query URL")
    url_group.add_argument("--list-query-urls", action="store_true",
                          help="List all query URLs")
    url_group.add_argument("--toggle-query-url", type=int, metavar="ID",
                          help="Toggle enabled status of query URL by ID")
    url_group.add_argument("--delete-query-url", type=int, metavar="ID",
                          help="Delete query URL by ID")
    
    # Proxy options
    proxy_group = parser.add_argument_group("Proxy Options")
    proxy_group.add_argument("--use-proxies", action="store_true", default=USE_PROXIES,
                            help="Enable proxy usage")
    proxy_group.add_argument("--no-proxies", action="store_true",
                            help="Disable proxy usage")
    proxy_group.add_argument("--proxy-list", type=str,
                            help="Comma-separated list of proxy URLs")
    proxy_group.add_argument("--proxy-rotation", type=str, choices=["round_robin", "random", "fallback"],
                            help="Proxy rotation strategy")
    proxy_group.add_argument("--proxy-stats", action="store_true",
                            help="Display proxy stats before exiting")
    
    # Telegram options
    telegram_group = parser.add_argument_group("Telegram Options")
    telegram_group.add_argument("--init-telegram-db", action="store_true",
                               help="Initialize Telegram database tables and exit")
    telegram_group.add_argument("--list-telegram-users", action="store_true",
                               help="List all registered Telegram users and exit")
    telegram_group.add_argument("--make-admin", type=int, metavar="USER_ID",
                               help="Make Telegram user an admin by user ID")
    telegram_group.add_argument("--revoke-admin", type=int, metavar="USER_ID",
                               help="Revoke admin status from Telegram user by user ID")
    telegram_group.add_argument("--send-broadcast", type=str,
                               help="Send broadcast message to all active Telegram users")
    telegram_group.add_argument("--telegram-token", type=str, default=TELEGRAM_BOT_TOKEN,
                               help="Telegram bot token")
    telegram_group.add_argument("--telegram-admin", type=str,
                               help="Comma-separated list of Telegram admin user IDs")
    
    return parser.parse_args()

async def send_telegram_message(bot, user_id, message):
    """Send a message to a Telegram user."""
    try:
        await bot.send_message(chat_id=user_id, text=message)
        return True
    except Exception as e:
        logger.error(f"Failed to send message to user {user_id}: {e}")
        return False

async def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Set up logging initially with default level
    logger = configure_cli_logging()

    # Adjust log level if debug flag is set
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    logger.info(f"Starting Dutch Real Estate Scraper CLI at {datetime.now()}")
    
    # Initialize databases
    db = PropertyDatabase(args.db)
    telegram_db = TelegramDatabase(args.db)
    
    # Handle Telegram database initialization
    if args.init_telegram_db:
        logger.info("Initializing Telegram database tables...")
        initialize_telegram_db(args.db)
        logger.info("Telegram database tables initialized successfully")
        return 0
    
    # Handle Telegram user management
    if args.list_telegram_users:
        users = telegram_db.get_active_users()
        admins = telegram_db.get_admin_users()
        print("\nRegistered Telegram Users:")
        if not users:
            print("  No users found")
        else:
            print(f"  {'ID':<12} {'Username':<15} {'Name':<20} {'Active':<8} {'Notifications':<13} {'Admin':<8} {'Last Activity':<20}")
            print(f"  {'-'*12} {'-'*15} {'-'*20} {'-'*8} {'-'*13} {'-'*8} {'-'*20}")
            for user in users:
                username = f"@{user['username']}" if user['username'] else "-"
                name = f"{user['first_name']} {user['last_name'] or ''}".strip()
                is_active = "Yes" if user['is_active'] else "No"
                notifications = "Enabled" if user['notification_enabled'] else "Disabled"
                is_admin = "Yes" if user['user_id'] in [a['user_id'] for a in admins] else "No"
                last_active = user['last_active'].strftime('%Y-%m-%d %H:%M:%S') if user['last_active'] else 'Never'
                print(f"  {user['user_id']:<12} {username:<15} {name[:20]:<20} {is_active:<8} {notifications:<13} {is_admin:<8} {last_active:<20}")
        return 0
    
    if args.make_admin is not None:
        success = telegram_db.set_admin_status(args.make_admin, True)
        logger.info(f"User {args.make_admin} is now an admin" if success else f"Failed to make user {args.make_admin} an admin")
        return 0
    
    if args.revoke_admin is not None:
        success = telegram_db.set_admin_status(args.revoke_admin, False)
        logger.info(f"Admin status revoked from user {args.revoke_admin}" if success else f"Failed to revoke admin status from user {args.revoke_admin}")
        return 0
    
    if args.send_broadcast is not None:
        if not args.telegram_token:
            logger.error("Telegram bot token not provided")
            return 1
        bot = telegram.Bot(token=args.telegram_token)
        admin_ids = [int(id.strip()) for id in args.telegram_admin.split(",") if id.strip().isdigit()] if args.telegram_admin else TELEGRAM_ADMIN_USER_IDS
        # Notify admins
        for admin_id in admin_ids:
            await send_telegram_message(bot, admin_id, f"CLI broadcast message: {args.send_broadcast}")
        logger.info("Broadcast message sent to admin users")
        # Confirm broadcast to all users
        confirm = input("Do you want to send this message to ALL active users? (y/N): ")
        if confirm.lower() == 'y':
            active_users = telegram_db.get_active_users()
            success_count = 0
            for user in active_users:
                if await send_telegram_message(bot, user['user_id'], f"📢 Broadcast message from administrator:\n\n{args.send_broadcast}"):
                    success_count += 1
            logger.info(f"Broadcast message sent to {success_count} of {len(active_users)} users")
        return 0
    
    # Handle query URL management
    if args.list_query_urls:
        print("\nQuery URLs in the database:")
        query_urls = db.get_enabled_query_urls()
        if not query_urls:
            print("  No query URLs found")
        else:
            print(f"  {'ID':<4} {'Source':<10} {'Method':<6} {'Enabled':<10} {'Last Scan':<20} {'URL':<50} {'Description':<30}")
            print(f"  {'-'*4} {'-'*10} {'-'*6} {'-'*10} {'-'*20} {'-'*50} {'-'*30}")
            for url in query_urls:
                enabled_str = "Yes" if url['enabled'] else "No"
                last_scan = url.get('last_scan_time', 'Never').strftime('%Y-%m-%d %H:%M:%S') if url.get('last_scan_time') else 'Never'
                url_display = url['queryurl'][:47] + "..." if len(url['queryurl']) > 50 else url['queryurl']
                desc = url.get('description', '')[:27] + "..." if url.get('description', '') and len(url.get('description', '')) > 30 else url.get('description', '')
                method = url.get('method', 'GET')
                print(f"  {url['id']:<4} {url['source']:<10} {method:<6} {enabled_str:<10} {last_scan:<20} {url_display:<50} {desc:<30}")
        return 0
    
    if args.add_query_url:
        parts = args.add_query_url.split(":", 1)
        if len(parts) != 2:
            logger.error("Invalid format for --add-query-url. Use 'source:url'")
            return 1
        source, url = parts
        description = args.add_query_description
        method = args.query_method
        enabled = not args.disable
        query_id = db.add_query_url(source, url, method, enabled, description)
        status = "enabled" if enabled else "disabled"
        logger.info(f"Added query URL with ID {query_id}: {source}:{url} (Method: {method}, Status: {status})" if query_id > 0 else f"Failed to add query URL: {source}:{url}")
        return 0
    
    if args.toggle_query_url is not None:
        query_urls = db.get_enabled_query_urls()
        found = False
        new_status = False
        for url in query_urls:
            if url['id'] == args.toggle_query_url:
                found = True
                new_status = not url['enabled']
                break
        if not found:
            logger.error(f"Query URL with ID {args.toggle_query_url} not found")
            return 1
        success = db.toggle_query_url(args.toggle_query_url, new_status)
        logger.info(f"Query URL with ID {args.toggle_query_url} {'enabled' if new_status else 'disabled'}" if success else f"Failed to toggle query URL with ID {args.toggle_query_url}")
        return 0
    
    if args.delete_query_url is not None:
        success = db.delete_query_url(args.delete_query_url)
        logger.info(f"Deleted query URL with ID {args.delete_query_url}" if success else f"Failed to delete query URL with ID {args.delete_query_url}")
        return 0
    
    # List available sources
    if args.list_sources:
        available_scrapers = RealEstateScraperFactory.get_available_scrapers()
        print("\nAvailable sources:")
        for source, config in available_scrapers.items():
            print(f"  - {source}: {config['base_url']}")
        return 0
    
    # Parse sources and cities
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    cities = [s.strip() for s in args.cities.split(",") if s.strip()]
    if not sources:
        logger.error("No sources specified")
        return 1
    
    # Determine scan mode
    use_query_urls = args.query_scan or args.combined_scan
    use_city_scan = args.city_scan or args.combined_scan
    if not (args.query_scan or args.city_scan or args.combined_scan):
        if cities:
            use_city_scan = True
            use_query_urls = False
        else:
            logger.error("No cities specified and no scan mode selected. Please specify cities or use --query-scan.")
            return 1
    
    if use_city_scan and not cities:
        logger.error("City-based scanning selected but no cities specified")
        return 1
    
    if use_query_urls:
        query_urls = db.get_enabled_query_urls(sources)
        if not query_urls and not use_city_scan:
            logger.error("Query URL scanning selected but no enabled query URLs found and no city-based scanning")
            return 1
    
    # Handle proxy options
    use_proxies = args.use_proxies
    if args.no_proxies:
        use_proxies = False
    proxy_list = [p.strip() for p in args.proxy_list.split(",") if p.strip()] if args.proxy_list else None
    
    # Log configuration
    logger.info(f"Starting scraper for sources: {sources}")
    logger.info(f"Scan mode: {'Query URLs' if use_query_urls and not use_city_scan else 'Cities' if use_city_scan and not use_query_urls else 'Combined'}")
    if use_city_scan:
        logger.info(f"Cities to scan: {cities}")
    if use_query_urls:
        query_urls = db.get_enabled_query_urls(sources)
        logger.info(f"Found {len(query_urls)} enabled query URLs to scan" if query_urls else "No enabled query URLs found")
    if use_proxies:
        proxy_count = len(proxy_list) if proxy_list else len(PROXY_LIST)
        logger.info(f"Proxy usage enabled with {proxy_count} proxies")
    
    # Create scraper instance
    scraper = RealEstateScraper(
        sources=sources,
        cities=cities,
        db_connection_string=args.db,
        interval=args.interval,
        max_results_per_scan=args.max_results,
        max_concurrent_requests=args.max_concurrent,
        use_proxies=use_proxies,
        skip_cities=not use_city_scan,
        skip_query_urls=not use_query_urls
    )
    
    # Set proxy rotation strategy
    if args.proxy_rotation and use_proxies:
        scraper.proxy_manager.rotation_strategy = args.proxy_rotation
    
    # Set proxy list
    if proxy_list and use_proxies:
        scraper.proxy_manager = ProxyManager(
            enabled=True,
            proxy_list=proxy_list,
            rotation_strategy=scraper.proxy_manager.rotation_strategy
        )
        scraper.http_client.use_proxies = True
    
    # Run the scraper
    try:
        if args.once:
            logger.info("Running a single scan cycle")
            new_count, total_count = await scraper.run_one_scan()
            logger.info(f"Scan completed: {new_count} new listings from {total_count} total")
        else:
            logger.info(f"Running continuous scanning with {args.interval}s interval")
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
            await scraper.run_continuous(stop_event)
    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        return 1
    finally:
        if args.proxy_stats and use_proxies:
            stats = scraper.proxy_manager.get_proxy_stats()
            print("\nProxy Statistics:")
            print(f"  Total proxies: {stats['total_proxies']}")
            print(f"  Healthy proxies: {stats['healthy_proxies']}")
            print(f"  Rotation strategy: {stats['rotation_strategy']}")
            if args.debug:
                print("\nDetailed proxy health:")
                for proxy, health in stats['proxy_health'].items():
                    print(f"  {proxy}:")
                    print(f"    Successes: {health['successes']}")
                    print(f"    Failures: {health['failures']}")
                    print(f"    Healthy: {health['healthy']}")
                    print(f"    Avg response time: {health['avg_response_time']:.2f}s")
    
    return 0

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    exit_code = asyncio.run(main())
    sys.exit(exit_code)