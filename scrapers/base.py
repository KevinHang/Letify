"""
Base scraper strategy for real estate websites.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any

from models.property import PropertyListing


class BaseScraperStrategy(ABC):
    """Base class for site-specific scraper strategies"""
    
    def __init__(self, site_name: str, config: Dict[str, Any]):
        self.site_name = site_name
        self.config = config
        self.base_url = config["base_url"]
        self.search_url_template = config["search_url_template"]
        self.listing_selector = config["listing_selector"]
    
    @abstractmethod
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for the given parameters"""
        pass
    
    @abstractmethod
    async def parse_search_page(self, html: str) -> List[str]:
        """Parse the search results page and extract listing URLs"""
        pass
    
    @abstractmethod
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """Parse a listing detail page and extract property information"""
        pass