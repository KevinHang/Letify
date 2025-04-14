"""
Main entry point for the Dutch Real Estate Scraper.
"""

import asyncio
import logging
import time
import random
from typing import List, Dict, Any, Tuple, Optional

from config import (
    DB_CONNECTION_STRING,
    SITE_CONFIGS, 
    DEFAULT_SCAN_INTERVAL,
    MAX_RESULTS_PER_SCAN,
    MAX_CONCURRENT_REQUESTS,
    USE_PROXIES
)
from database.migrations import initialize_db
from database.property_db import PropertyDatabase
from scrapers.factory import RealEstateScraperFactory
from utils.http import EnhancedHttpClient
from utils.proxy_manager import ProxyManager


logger = logging.getLogger(__name__)


class RealEstateScraper:
    """Main scraper class orchestrating the scraping process"""
    
    def __init__(self, 
                 sources: List[str], 
                 cities: List[str], 
                 db_connection_string: str = DB_CONNECTION_STRING,
                 interval: int = DEFAULT_SCAN_INTERVAL,
                 max_results_per_scan: int = MAX_RESULTS_PER_SCAN,
                 max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
                 use_proxies: bool = USE_PROXIES):
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
        """
        self.sources = sources
        self.cities = cities
        self.interval = interval
        self.max_results_per_scan = max_results_per_scan
        
        # Initialize database
        initialize_db(db_connection_string)
        self.db = PropertyDatabase(db_connection_string)
        
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
    
    async def scan_source_city(self, source: str, city: str, days: int = 1) -> Tuple[int, int]:
        """
        Scan a specific source and city for new listings.
        
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
        
        # Build search URL for this source and city
        search_url = await scraper.build_search_url(city, days)
        
        start_time = time.time()
        new_listings = 0
        total_listings = 0
        
        try:
            # Get a proxy from the proxy manager if enabled
            proxy = await self.proxy_manager.get_proxy() if self.proxy_manager.enabled else None
            
            try:
                # Fetch search page
                if proxy:
                    response = await self.http_client.get(search_url)
                    if response.status_code == 200:
                        await self.proxy_manager.report_success(proxy, time.time() - start_time)
                else:
                    response = await self.http_client.get(search_url)
            except Exception as e:
                if proxy:
                    await self.proxy_manager.report_failure(proxy, e)
                raise
            
            # Parse search page to get listing URLs
            listing_urls = await scraper.parse_search_page(response.text)

            print("xxx", response.text)
            
            # Limit the number of listings to process
            listing_urls = listing_urls[:self.max_results_per_scan]
            total_listings = len(listing_urls)
            
            logger.info(f"Found {total_listings} listings for {source} in {city}")
            
            # Process each listing URL
            for listing_url in listing_urls:
                try:
                    # Get a new proxy for each listing if enabled
                    proxy = await self.proxy_manager.get_proxy() if self.proxy_manager.enabled else None
                    request_start = time.time()
                    
                    try:
                        # Fetch listing detail page
                        if proxy:
                            detail_response = await self.http_client.get(listing_url)
                            if detail_response.status_code == 200:
                                await self.proxy_manager.report_success(proxy, time.time() - request_start)
                        else:
                            detail_response = await self.http_client.get(listing_url)
                    except Exception as e:
                        if proxy:
                            await self.proxy_manager.report_failure(proxy, e)
                        raise
                    
                    # Parse listing detail page
                    listing = await scraper.parse_listing_page(detail_response.text, listing_url)
                    
                    # Set city if not already set in the listing
                    if not listing.city:
                        listing.city = city
                    
                    # Save listing to database
                    is_new = self.db.save_listing(listing)
                    if is_new:
                        new_listings += 1
                        logger.info(f"Added new listing: {listing.title} ({listing.source} - {listing.price})")
                    
                    # Random delay between requests to avoid detection
                    await asyncio.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.error(f"Error processing listing {listing_url}: {e}")
            
            # Update scan history
            duration = time.time() - start_time
            self.db.update_scan_history(source, city, search_url, new_listings, total_listings, duration)
            
            logger.info(f"Completed scan of {source} for {city}: {new_listings} new, {total_listings} total in {duration:.2f}s")
            return new_listings, total_listings
            
        except Exception as e:
            logger.error(f"Error scanning {source} for {city}: {e}")
            return 0, 0
    
    async def run_one_scan(self):
        """Run a single scan of all sources and cities."""
        total_new = 0
        total_processed = 0
        
        # Log proxy status if enabled
        if self.proxy_manager.enabled:
            stats = self.proxy_manager.get_proxy_stats()
            logger.info(f"Using proxies: {stats['healthy_proxies']} healthy out of {stats['total_proxies']} total")
        
        for source in self.sources:
            for city in self.cities:
                try:
                    # Check if it's time to scan this source/city again
                    last_scan_time = self.db.get_last_scan_time(source, city)
                    source_min_interval = SITE_CONFIGS.get(source, {}).get("min_interval", self.interval)
                    
                    if (last_scan_time is None or 
                        (time.time() - last_scan_time.timestamp()) >= source_min_interval):
                        
                        # Scan this source and city
                        new_count, total_count = await self.scan_source_city(source, city)
                        total_new += new_count
                        total_processed += total_count
                    else:
                        logger.info(f"Skipping {source} for {city} - not due yet")
                
                except Exception as e:
                    logger.error(f"Error in scan of {source} for {city}: {e}")
        
        logger.info(f"Scan complete: {total_new} new listings from {total_processed} total processed")
        
        # Find and record potential duplicates
        await self.detect_duplicates()
        
        # Refresh proxies if needed and enabled
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
            # Find potential duplicates
            duplicates = self.db.find_potential_duplicates()
            logger.info(f"Found {len(duplicates)} potential duplicate pairs")
            
            # Record each duplicate pair
            for dup in duplicates:
                try:
                    # Calculate a simple similarity score based on price and area
                    price_ratio = min(dup["price_1"] or 0, dup["price_2"] or 0) / max(dup["price_1"] or 1, dup["price_2"] or 1) if dup["price_1"] and dup["price_2"] else 0.5
                    area_ratio = min(dup["area_1"] or 0, dup["area_2"] or 0) / max(dup["area_1"] or 1, dup["area_2"] or 1) if dup["area_1"] and dup["area_2"] else 0.5
                    
                    # Distance factor (1.0 if close, lower if further apart)
                    distance_factor = 1.0
                    if dup.get("distance_meters"):
                        distance_factor = max(0.0, 1.0 - (dup["distance_meters"] / 100))
                    
                    # Combine factors for an overall similarity score
                    similarity_score = (price_ratio * 0.4) + (area_ratio * 0.4) + (distance_factor * 0.2)
                    
                    # Record the duplicate pair
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
                    pass  # This is expected when the timeout occurs
        
        except asyncio.CancelledError:
            logger.info("Scraper task cancelled")
        except Exception as e:
            logger.error(f"Error in continuous scraper: {e}")
            raise