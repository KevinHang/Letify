#!/usr/bin/env python
"""
Command-line interface for the Dutch Real Estate Scraper.
"""

import os
import sys
import argparse
import asyncio
import logging
from datetime import datetime

from config import (
    DB_CONNECTION_STRING,
    DEFAULT_SCAN_INTERVAL, 
    DEFAULT_SOURCES, 
    DEFAULT_CITIES,
    MAX_RESULTS_PER_SCAN,
    MAX_CONCURRENT_REQUESTS,
    USE_PROXIES,
    PROXY_LIST
)
from main import RealEstateScraper
from scrapers.factory import RealEstateScraperFactory
from utils.proxy_manager import ProxyManager
from database.property_db import PropertyDatabase


def configure_logging(log_level=logging.INFO):
    """Configure logging for the application."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler("realestate_scraper.log"),
            logging.StreamHandler()
        ]
    )


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Dutch Real Estate Scraper")
    
    # Create a mutually exclusive group for scan mode
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--city-scan", action="store_true", 
                          help="City scan mode (default when cities are specified)")
    mode_group.add_argument("--query-scan", action="store_true",
                          help="Query URL scan mode (looks for enabled query URLs in database)")
    mode_group.add_argument("--combined-scan", action="store_true",
                          help="Combined mode: scan both cities and query URLs")
    
    parser.add_argument("--sources", type=str, default=",".join(DEFAULT_SOURCES),
                        help=f"Comma-separated list of sources to scrape (default: {','.join(DEFAULT_SOURCES)})")
    
    parser.add_argument("--cities", type=str, default=",".join(DEFAULT_CITIES),
                        help=f"Comma-separated list of cities to scrape (default: {','.join(DEFAULT_CITIES)})")
    
    parser.add_argument("--interval", type=int, default=DEFAULT_SCAN_INTERVAL,
                        help=f"Scraping interval in seconds (default: {DEFAULT_SCAN_INTERVAL})")
    
    parser.add_argument("--db", type=str, default=DB_CONNECTION_STRING,
                        help="PostgreSQL connection string")
    
    parser.add_argument("--max-results", type=int, default=MAX_RESULTS_PER_SCAN,
                        help=f"Maximum number of results to process per scan (default: {MAX_RESULTS_PER_SCAN})")
    
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT_REQUESTS,
                        help=f"Maximum number of concurrent requests (default: {MAX_CONCURRENT_REQUESTS})")
    
    parser.add_argument("--once", action="store_true",
                        help="Run only one scan cycle")
    
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    
    parser.add_argument("--list-sources", action="store_true",
                        help="List available sources and exit")

    # Query URL related arguments
    url_group = parser.add_argument_group("Query URL Options")
    
    url_group.add_argument("--add-query-url", type=str,
                          help="Add a query URL in format source:url (e.g., 'funda:https://www.funda.nl/...')")
    
    url_group.add_argument("--query-method", type=str, choices=["GET", "POST"], default="GET",
                          help="HTTP method for the query URL (GET or POST)")
    
    url_group.add_argument("--disable", action="store_true",
                          help="Add the query URL in disabled state (default is enabled)")
    
    url_group.add_argument("--add-query-description", type=str,
                          help="Description for the query URL being added")
    
    url_group.add_argument("--list-query-urls", action="store_true",
                          help="List all query URLs in the database")
    
    url_group.add_argument("--toggle-query-url", type=int, metavar="ID",
                          help="Toggle the enabled status of a query URL by ID")
    
    url_group.add_argument("--delete-query-url", type=int, metavar="ID",
                          help="Delete a query URL by ID")
    
    # Proxy related arguments
    proxy_group = parser.add_argument_group("Proxy Options")
    
    proxy_group.add_argument("--use-proxies", action="store_true", default=USE_PROXIES,
                            help="Enable proxy usage for HTTP requests")
    
    proxy_group.add_argument("--no-proxies", action="store_true",
                            help="Disable proxy usage for HTTP requests")
    
    proxy_group.add_argument("--proxy-list", type=str,
                            help="Comma-separated list of proxy URLs to use")
    
    proxy_group.add_argument("--proxy-rotation", type=str, choices=["round_robin", "random", "fallback"],
                            help="Proxy rotation strategy")
    
    proxy_group.add_argument("--proxy-stats", action="store_true",
                            help="Display proxy stats before exiting")
    
    return parser.parse_args()


async def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    configure_logging(log_level)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting Dutch Real Estate Scraper at {datetime.now()}")
    
    # Initialize the database
    db = PropertyDatabase(args.db)
    
    # Handle query URL management commands
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
        return
    
    if args.add_query_url:
        parts = args.add_query_url.split(":", 1)
        if len(parts) != 2:
            logger.error("Invalid format for --add-query-url. Use 'source:url'")
            return
            
        source, url = parts
        description = args.add_query_description
        method = args.query_method
        enabled = not args.disable  # Default is enabled unless --disable is used
        
        query_id = db.add_query_url(source, url, method, enabled, description)
        
        if query_id > 0:
            status = "enabled" if enabled else "disabled"
            logger.info(f"Added query URL with ID {query_id}: {source}:{url} (Method: {method}, Status: {status})")
        else:
            logger.error(f"Failed to add query URL: {source}:{url}")
        return
    
    if args.toggle_query_url is not None:
        # Fetch current status first
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
            return
            
        success = db.toggle_query_url(args.toggle_query_url, new_status)
        if success:
            logger.info(f"Query URL with ID {args.toggle_query_url} {'enabled' if new_status else 'disabled'}")
        else:
            logger.error(f"Failed to toggle query URL with ID {args.toggle_query_url}")
        return
    
    if args.delete_query_url is not None:
        success = db.delete_query_url(args.delete_query_url)
        if success:
            logger.info(f"Deleted query URL with ID {args.delete_query_url}")
        else:
            logger.error(f"Failed to delete query URL with ID {args.delete_query_url}")
        return
    
    # List available sources and exit if requested
    if args.list_sources:
        available_scrapers = RealEstateScraperFactory.get_available_scrapers()
        print("\nAvailable sources:")
        for source, config in available_scrapers.items():
            print(f"  - {source}: {config['base_url']}")
        return
    
    # Parse sources and cities
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    cities = [s.strip() for s in args.cities.split(",") if s.strip()]
    
    if not sources:
        logger.error("No sources specified")
        return
    
    # Determine scan mode
    use_query_urls = args.query_scan or args.combined_scan
    use_city_scan = args.city_scan or args.combined_scan
    
    # If no mode is explicitly specified, determine based on input
    if not (args.query_scan or args.city_scan or args.combined_scan):
        if cities:
            use_city_scan = True  # Default to city scan if cities are provided
            use_query_urls = False
        else:
            logger.error("No cities specified and no scan mode selected. Please specify cities or use --query-scan.")
            return
    
    # Error if city scan is needed but no cities specified
    if use_city_scan and not cities:
        logger.error("City-based scanning selected but no cities specified")
        return
    
    # Check if query URLs are available if needed
    if use_query_urls:
        query_urls = db.get_enabled_query_urls(sources)
        if not query_urls:
            logger.warning("Query URL scanning selected but no enabled query URLs found in database")
            if not use_city_scan:
                logger.error("No query URLs and no city-based scanning - nothing to do")
                return
    
    # Handle proxy options
    use_proxies = args.use_proxies
    if args.no_proxies:
        use_proxies = False
    
    proxy_list = None
    if args.proxy_list:
        proxy_list = [p.strip() for p in args.proxy_list.split(",") if p.strip()]
        if proxy_list:
            use_proxies = True
    
    # Log configuration
    logger.info(f"Starting scraper for sources: {sources}")
    logger.info(f"Scan mode: {'Query URLs' if use_query_urls and not use_city_scan else 'Cities' if use_city_scan and not use_query_urls else 'Combined'}")
    
    if use_city_scan:
        logger.info(f"Cities to scan: {cities}")
    
    if use_query_urls:
        query_urls = db.get_enabled_query_urls(sources)
        if query_urls:
            logger.info(f"Found {len(query_urls)} enabled query URLs to scan")
    
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
        skip_cities=not use_city_scan,  # Skip cities if not using city scan
        skip_query_urls=not use_query_urls  # Skip query URLs if not using query scan
    )
    
    # Set proxy rotation strategy if specified
    if args.proxy_rotation and use_proxies:
        scraper.proxy_manager.rotation_strategy = args.proxy_rotation
    
    # Set proxy list if specified
    if proxy_list and use_proxies:
        # Reset the proxy manager with the new list
        scraper.proxy_manager = ProxyManager(
            enabled=True,
            proxy_list=proxy_list,
            rotation_strategy=scraper.proxy_manager.rotation_strategy
        )
        # Update the HTTP client with the new proxy manager
        scraper.http_client.use_proxies = True
    
    # Run the scraper
    try:
        if args.once:
            logger.info("Running a single scan cycle")
            await scraper.run_one_scan()
        else:
            logger.info(f"Running continuous scanning with {args.interval}s interval")
            stop_event = asyncio.Event()
            try:
                await scraper.run_continuous(stop_event)
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt detected. Stopping scraper...")
                stop_event.set()
    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        return 1
    
    # Display proxy stats if requested
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
    # Windows-specific event loop policy
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Run the main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)