"""
Main entry point for the Dutch Real Estate Scraper.
Responsible for scraping real estate listings and queuing notifications in the database.
"""

import asyncio
import argparse
import time
from typing import List, Dict, Any, Tuple
from datetime import datetime, timezone

from config import (
    DB_CONNECTION_STRING,
    SITE_CONFIGS, 
    DEFAULT_SOURCES,
    DEFAULT_CITIES,
    DEFAULT_SCAN_INTERVAL,
    MAX_RESULTS_PER_SCAN,
    MAX_CONCURRENT_REQUESTS,
    USE_PROXIES,
    STOP_AFTER_NO_RESULT,
)
from database.migrations import initialize_db
from database.property_db import PropertyDatabase
from database.telegram_db import TelegramDatabase
from scrapers.factory import RealEstateScraperFactory
from utils.http import EnhancedHttpClient
from utils.proxy_manager import ProxyManager
from utils.logging_config import configure_scraper_logging

# Set up logging
logger = configure_scraper_logging()

class RealEstateScraper:
    """Main scraper class orchestrating the scraping process"""
    
    def __init__(self, 
                 sources: List[str], 
                 cities: List[str], 
                 db_connection_string: str = DB_CONNECTION_STRING,
                 interval: int = DEFAULT_SCAN_INTERVAL,
                 max_results_per_scan: int = MAX_RESULTS_PER_SCAN,
                 max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
                 use_proxies: bool = USE_PROXIES,
                 stop_after_no_result: bool = STOP_AFTER_NO_RESULT,
                 skip_cities: bool = False,
                 skip_query_urls: bool = False):
        """
        Initialize the scraper with sources and cities to scan.
        
        Args:
            sources: List of source names (e.g., ["funda", "pararius"])
            cities: List of cities to scan (e.g., ["amsterdam", "rotterdam"])
            db_connection_string: PostgreSQL connection string
            interval: Default scan interval in seconds
            max_results_per_scan: Maximum number of listings to process per scan
            max_concurrent_requests: Maximum number of concurrent HTTP requests
            use_proxies: Whether to use proxies for HTTP requests
            skip_cities: Whether to skip city-based scanning
            skip_query_urls: Whether to skip query URL scanning
            stop_after_no_result: Stop scanning next pages if current page has no result
        """
        self.sources = sources
        self.cities = cities
        self.interval = interval
        self.max_results_per_scan = max_results_per_scan
        self.skip_cities = skip_cities
        self.skip_query_urls = skip_query_urls
        self.stop_after_no_result = stop_after_no_result
        
        # Initialize database
        initialize_db(db_connection_string)
        self.db = PropertyDatabase(db_connection_string)
        self.telegram_db = TelegramDatabase(db_connection_string)
        
        # Initialize HTTP client and proxy manager
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.proxy_manager = ProxyManager(enabled=use_proxies)
        self.http_client = EnhancedHttpClient(semaphore=self.semaphore, use_proxies=use_proxies)
        
        # Initialize scrapers for each source
        self.scrapers = {}
        for source in sources:
            try:
                self.scrapers[source] = RealEstateScraperFactory.create_scraper(source)
            except ValueError as e:
                logger.error(f"{e}")

    async def scan_query_url(self, query_url: Dict[str, Any]) -> Tuple[int, int, str]:
        """
        Scan a specific URL for listings and queue notifications for matching users.
        
        Args:
            query_url: Dictionary containing query URL information
            
        Returns:
            Tuple of (new listings count, total listings count, final_url)
        """
        source = query_url['source']
        url = query_url['queryurl']
        query_id = query_url['id']
        
        logger.info(f"Scanning specific {source} URL (ID={query_id}): {url}")
        scraper = self.scrapers.get(source)
        if not scraper:
            logger.error(f"No scraper available for {source}")
            return 0, 0, ""
        
        start_time = time.time()
        new_listings = 0
        total_listings = 0
        final_url = ""
        new_listing_ids = []
        
        try:
            proxy = await self.proxy_manager.get_proxy() if self.proxy_manager.enabled else None
            try:
                response = await self.http_client.get(url)
                if proxy and response.status_code == 200:
                    await self.proxy_manager.report_success(proxy, time.time() - start_time)
                final_url = str(response.url)
            except Exception as e:
                if proxy:
                    await self.proxy_manager.report_failure(proxy, e)
                raise
            
            listings = await scraper.parse_search_page(response.text)
            total_listings = len(listings)
            logger.info(f"Found {total_listings} listings for {source} from specific URL")
            
            for listing in listings:
                try:
                    if not listing.city:
                        for city in self.cities:
                            if city.lower() in url.lower():
                                listing.city = city
                                break
                        if not listing.city:
                            listing.city = "unknown"
                    
                    is_new = self.db.save_listing(listing)
                    if is_new:
                        new_listings += 1
                        property_id = self.db.get_property_id_by_source_id(listing.source, listing.source_id)
                        if property_id:
                            new_listing_ids.append(property_id)
                            notification_count = self.telegram_db.add_matched_properties_to_queue(property_id)
                            logger.info(f"Added new listing: {listing.title} ({listing.source} - {listing.price}), "
                                       f"queued for {notification_count} users")
                except Exception as e:
                    logger.error(f"Error processing listing: {e}")
            
            duration = time.time() - start_time
            self.db.update_scan_history(source, f"query_url_{query_id}", url, new_listings, total_listings, duration)
            self.db.update_query_url_scan_time(query_id)
            
            logger.info(f"Completed scan of query URL (ID={query_id}): {new_listings} new, {total_listings} total in {duration:.2f}s")
            return new_listings, total_listings, final_url
            
        except Exception as e:
            logger.error(f"Error scanning query URL (ID={query_id}): {e}")
            return 0, 0, ""
    
    async def scan_source_city(self, source: str, city: str, days: int = 1) -> Tuple[int, int]:
        """
        Scan a specific source and city for new listings and queue notifications.
        
        Args:
            source: Source name (e.g., "funda")
            city: City name (e.g., "amsterdam")
            days: Number of days to look back for new listings
            
        Returns:
            Tuple of (new listings count, total listings count)
        """
        logger.info(f"Scanning {source} for {city}...")
        scraper = self.scrapers.get(source)
        if not scraper:
            logger.error(f"No scraper available for {source}")
            return 0, 0
        
        search_url = await scraper.build_search_url(city, days)
        start_time = time.time()
        new_listings = 0
        total_listings = 0
        new_listing_ids = []
        
        try:
            proxy = await self.proxy_manager.get_proxy() if self.proxy_manager.enabled else None
            try:
                response = await self.http_client.get(search_url)
                if proxy and response.status_code == 200:
                    await self.proxy_manager.report_success(proxy, time.time() - start_time)
            except Exception as e:
                if proxy:
                    await self.proxy_manager.report_failure(proxy, e)
                raise
            
            listings = await scraper.parse_search_page(response.text)
            total_listings = len(listings)
            logger.info(f"Found {total_listings} listings for {source} in {city}")
            
            for listing in listings:
                try:
                    if not listing.city:
                        listing.city = city
                    
                    is_new = self.db.save_listing(listing)
                    if is_new:
                        new_listings += 1
                        property_id = self.db.get_property_id_by_source_id(listing.source, listing.source_id)
                        if property_id:
                            new_listing_ids.append(property_id)
                            notification_count = self.telegram_db.add_matched_properties_to_queue(property_id)
                            logger.info(f"Added new listing: {listing.title} ({listing.source} - {listing.price}), "
                                       f"queued for {notification_count} users")
                except Exception as e:
                    logger.error(f"Error processing listing: {e}")
            
            duration = time.time() - start_time
            self.db.update_scan_history(source, city, search_url, new_listings, total_listings, duration)
            
            logger.info(f"Completed scan of {source} for {city}: {new_listings} new, {total_listings} total in {duration:.2f}s")
            return new_listings, total_listings
            
        except Exception as e:
            logger.error(f"Error scanning {source} for {city}: {e}")
            return 0, 0
    
    async def run_one_scan(self):
        """Run a single scan based on configured scan modes."""
        total_new = 0
        total_processed = 0
        first_scan_by_source = {}
        sources_to_skip = set()
        
        if self.proxy_manager.enabled:
            stats = self.proxy_manager.get_proxy_stats()
            logger.info(f"Using proxies: {stats['healthy_proxies']} healthy out of {stats['total_proxies']} total")
        
        if not self.skip_query_urls:
            query_urls = self.db.get_enabled_query_urls(self.sources)
            if query_urls:
                logger.info(f"Found {len(query_urls)} enabled query URLs to scan")
                query_urls_by_source = {}
                for url in query_urls:
                    source = url['source']
                    if source not in query_urls_by_source:
                        query_urls_by_source[source] = []
                    query_urls_by_source[source].append(url)
                
                for source in self.sources:
                    first_scan_by_source[source] = True
                
                for source in self.sources:
                    if source not in query_urls_by_source:
                        continue
                    
                    for query_url in query_urls_by_source[source]:
                        original_url = query_url['queryurl']
                        if source in sources_to_skip:
                            logger.info(f"Skipping query URL (ID={query_url['id']}) - source {source} marked for skipping")
                            continue
                        
                        last_scan_time = query_url.get('last_scan_time')
                        source_min_interval = SITE_CONFIGS.get(source, {}).get("min_interval", self.interval)
                        should_scan = True
                        if last_scan_time is not None:
                            if last_scan_time.tzinfo is None:
                                last_scan_time = last_scan_time.replace(tzinfo=timezone.utc)
                            current_time = datetime.now(timezone.utc)
                            time_since_last_scan = (current_time - last_scan_time).total_seconds()
                            if time_since_last_scan < source_min_interval:
                                logger.info(f"Skipping query URL (ID={query_url['id']}) - not due yet")
                                should_scan = False
                        
                        if should_scan:
                            try:
                                new_count, total_count, final_url = await self.scan_query_url(query_url)
                                total_new += new_count
                                total_processed += total_count
                                
                                if first_scan_by_source[source] and new_count == 0 and total_count != 0:
                                    logger.info(f"First scan of {source} query URL (ID={query_url['id']}) resulted in no new listings. Skipping new pages of this source.")
                                    sources_to_skip.add(source)
                                    continue
                                elif first_scan_by_source[source] and total_count == 0:
                                    logger.info(f"First scan of {source} query URL (ID={query_url['id']}) failed with no result. HTML structure may have changed!")
                                    sources_to_skip.add(source)
                                    continue

                                if not first_scan_by_source[source] and new_count == 0 and total_count != 0:
                                    logger.info(f"Scan {query_url['id']} of {source} query URL (ID={query_url['id']}) resulted in no new listings. Skipping new pages of this source.")
                                    sources_to_skip.add(source)
                                    continue

                                if self.stop_after_no_result:
                                    if source.lower() == "pararius" and original_url != final_url:
                                        logger.info(f"Pagination ended for {source}: URL {original_url} redirected to {final_url}")
                                        sources_to_skip.add(source)
                                        continue
                                    elif total_count == 0:
                                        if not first_scan_by_source[source]:
                                            logger.info(f"Scan of {source} query URL (ID={query_url['id']}) succeeded with no result. End of pagination reached.")
                                        sources_to_skip.add(source)
                                        continue

                                first_scan_by_source[source] = False
                            except Exception as e:
                                logger.error(f"Error in scan of query URL (ID={query_url['id']}): {e}")
            else:
                logger.info("No enabled query URLs found in database")
        else:
            logger.info("Query URL scanning skipped")
        
        if not self.skip_cities:
            for source in self.sources:
                for city in self.cities:
                    try:
                        last_scan_time = self.db.get_last_scan_time(source, city)
                        source_min_interval = SITE_CONFIGS.get(source, {}).get("min_interval", self.interval)
                        should_scan = True
                        if last_scan_time is not None:
                            if last_scan_time.tzinfo is None:
                                last_scan_time = last_scan_time.replace(tzinfo=timezone.utc)
                            current_time = datetime.now(timezone.utc)
                            time_since_last_scan = (current_time - last_scan_time).total_seconds()
                            if time_since_last_scan < source_min_interval:
                                logger.info(f"Skipping {source} for {city} - not due yet")
                                should_scan = False
                        
                        if should_scan:
                            new_count, total_count = await self.scan_source_city(source, city)
                            total_new += new_count
                            total_processed += total_count
                    except Exception as e:
                        logger.error(f"Error in scan of {source} for {city}: {e}")
        else:
            logger.info("City-based scanning skipped")
        
        logger.info(f"Scan complete: {total_new} new listings from {total_processed} total processed")
        await self.detect_duplicates()
        
        if self.proxy_manager.enabled and self.proxy_manager.healthy_count < (self.proxy_manager.proxy_count / 2):
            logger.info("More than half of proxies are unhealthy. Attempting to fetch new proxies...")
            success = await self.proxy_manager.fetch_new_proxies()
            if not success:
                logger.info("Failed to fetch new proxies. Resetting all proxies instead.")
                await self.proxy_manager.reset_all_proxies()
        
        return total_new, total_processed
    
    async def detect_duplicates(self):
        """Detect and record potential duplicate listings across sources."""
        logger.info("Detecting potential duplicate listings...")
        try:
            duplicates = self.db.find_potential_duplicates()
            logger.info(f"Found {len(duplicates)} potential duplicate pairs")
            for dup in duplicates:
                try:
                    price_ratio = min(dup["price_1"] or 0, dup["price_2"] or 0) / max(dup["price_1"] or 1, dup["price_2"] or 1) if dup["price_1"] and dup["price_2"] else 0.5
                    area_ratio = min(dup["area_1"] or 0, dup["area_2"] or 0) / max(dup["area_1"] or 1, dup["area_2"] or 1) if dup["area_1"] and dup["area_2"] else 0.5
                    distance_factor = 1.0 if not dup.get("distance_meters") else max(0.0, 1.0 - (dup["distance_meters"] / 100))
                    similarity_score = (price_ratio * 0.4) + (area_ratio * 0.4) + (distance_factor * 0.2)
                    self.db.record_duplicate_pair(
                        dup["source_1"], dup["source_id_1"],
                        dup["source_2"], dup["source_id_2"],
                        dup["property_hash"], similarity_score
                    )
                except Exception as e:
                    logger.error(f"Error processing duplicate: {dup.get('property_hash')}: {e}")
        except Exception as e:
            logger.error(f"Error in duplicate detection: {e}")
    
    async def run_continuous(self, stop_event=None):
        """Run the scraper continuously with the configured interval."""
        if stop_event is None:
            stop_event = asyncio.Event()
        try:
            while not stop_event.is_set():
                logger.info("Starting new scan cycle...")
                await self.run_one_scan()
                logger.info(f"Waiting {self.interval} seconds until next scan...")
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            logger.info("Scraper task cancelled")
        except Exception as e:
            logger.error(f"Error in continuous scraper: {e}")
            raise

async def main():
    """Main entry point for the scraper."""
    parser = argparse.ArgumentParser(description="Dutch Real Estate Scraper")
    parser.add_argument("--sources", nargs="+", default=DEFAULT_SOURCES, help="List of sources to scrape")
    parser.add_argument("--cities", nargs="+", default=DEFAULT_CITIES, help="List of cities to scrape")
    parser.add_argument("--interval", type=int, default=DEFAULT_SCAN_INTERVAL, help="Scan interval in seconds")
    parser.add_argument("--max-results", type=int, default=MAX_RESULTS_PER_SCAN, help="Maximum results per scan")
    parser.add_argument("--max-concurrent", type=int, default=MAX_CONCURRENT_REQUESTS, help="Maximum concurrent requests")
    parser.add_argument("--use-proxies", action="store_true", default=USE_PROXIES, help="Use proxies for HTTP requests")
    parser.add_argument("--skip-cities", action="store_true", help="Skip city-based scanning")
    parser.add_argument("--skip-query-urls", action="store_true", help="Skip query URL scanning")
    parser.add_argument("--once", action="store_true", help="Run only once, then exit")
    
    args = parser.parse_args()
    
    scraper = RealEstateScraper(
        sources=args.sources,
        cities=args.cities,
        db_connection_string=DB_CONNECTION_STRING,
        interval=args.interval,
        max_results_per_scan=args.max_results,
        max_concurrent_requests=args.max_concurrent,
        use_proxies=args.use_proxies,
        skip_cities=args.skip_cities,
        skip_query_urls=args.skip_query_urls
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
        if args.once:
            logger.info("Running one-time scan...")
            new_count, processed_count = await scraper.run_one_scan()
            logger.info(f"Scan completed: {new_count} new listings from {processed_count} total")
        else:
            logger.info("Starting continuous scanning...")
            await scraper.run_continuous(stop_event)
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        logger.info("Shutting down scraper...")
        stop_event.set()

if __name__ == "__main__":
    asyncio.run(main())