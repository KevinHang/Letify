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
    
    if not cities:
        logger.error("No cities specified")
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
    
    logger.info(f"Starting scraper for sources: {sources}, cities: {cities}")
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
        use_proxies=use_proxies
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