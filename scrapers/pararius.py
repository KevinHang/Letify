"""
Pararius.com scraper implementation that extracts data from search page only.
"""

import re
import uuid
import hashlib
from typing import List
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from models.property import PropertyListing, PropertyType, InteriorType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the telegram logger
logger = get_scraper_logger("pararius_scraper")


class ParariusScraper(BaseScraperStrategy):
    """Scraper strategy for Pararius.com that extracts data from search results only"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for Pararius"""
        if days > 0:
            # If days is specified, add since filter
            since_value = 1  # 1 day
            if days >= 30:
                since_value = 30  # 1 month
            elif days >= 14:
                since_value = 10  # 2 weeks
            elif days >= 7:
                since_value = 5   # 1 week
            elif days >= 3:
                since_value = 3   # 3 days
            
            base_url = self.search_url_template.format(city=city.lower())
            return f"{base_url}?filters[since]={since_value}"
        else:
            return self.search_url_template.format(city=city.lower())
    
    def _generate_property_hash(self, listing: PropertyListing) -> str:
        """
        Generate a unique hash for the property based on available information.
        This is a more robust implementation that works even with partial data.
        """
        # Collect all available identifiers
        identifiers = []
        
        # Use URL or ID as primary identifier - these should be unique per listing
        if listing.url:
            identifiers.append(listing.url)
        if listing.source_id:
            identifiers.append(listing.source_id)
            
        # Add other identifying information if available
        if listing.title:
            identifiers.append(listing.title)
        if listing.address:
            identifiers.append(listing.address)
        if listing.postal_code:
            identifiers.append(listing.postal_code)
        if listing.city:
            identifiers.append(listing.city)
        if listing.living_area:
            identifiers.append(f"area:{listing.living_area}")
        if listing.price_numeric:
            identifiers.append(f"price:{listing.price_numeric}")
        if listing.rooms:
            identifiers.append(f"rooms:{listing.rooms}")
            
        # Ensure we have at least something unique
        if not identifiers:
            identifiers.append(str(uuid.uuid4()))
            
        # Create hash input
        hash_input = "|".join([str(x) for x in identifiers if x])
        
        # Generate hash
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    async def parse_search_page(self, html: str) -> List[PropertyListing]:
        """
        Parse the Pararius search results page and extract listings directly
        
        Returns a list of PropertyListing objects instead of URLs
        """
        parser = HTMLParser(html)
        listings = []
        
        # Extract data from each listing card
        for listing_item in parser.css(".listing-search-item"):
            try:
                # Skip ads (which might be in search results)
                if listing_item.parent and "search-list__item--listing" not in listing_item.parent.attributes.get("class", ""):
                    continue
                
                # Create a new property listing
                listing = PropertyListing(source="pararius")
                
                # Extract URL and source ID
                link = listing_item.css_first(".listing-search-item__link--title")
                if link and link.attributes.get("href"):
                    href = link.attributes.get("href")
                    full_url = urljoin(self.base_url, href)
                    listing.url = full_url
                    
                    # Extract source ID from URL
                    url_match = re.search(r'/([a-f0-9]{8})/', full_url)
                    if url_match:
                        listing.source_id = url_match.group(1)
                    else:
                        # Generate a unique ID if none found in URL
                        listing.source_id = str(uuid.uuid4())
                
                # Extract title and determine property type
                title_elem = listing_item.css_first(".listing-search-item__link--title")
                if title_elem:
                    title_text = title_elem.text().strip()
                    listing.title = title_text
                    
                    # If title starts with "Flat", it's an apartment
                    if title_text.startswith("Flat "):
                        listing.property_type = PropertyType.APARTMENT
                        # Extract address from title (remove "Flat " prefix)
                        listing.address = title_text[5:]
                    elif title_text.startswith("House "):
                        listing.property_type = PropertyType.HOUSE
                        # Extract address from title (remove "House " prefix)
                        listing.address = title_text[6:]
                    elif title_text.startswith("Room "):
                        listing.property_type = PropertyType.ROOM
                        # Extract address from title (remove "Room " prefix)
                        listing.address = title_text[5:]
                    elif title_text.startswith("Studio "):
                        listing.property_type = PropertyType.STUDIO
                        # Extract address from title (remove "Studio " prefix)
                        listing.address = title_text[7:]
                
                # Extract sub-title with postal code, city, and neighborhood
                subtitle_elem = listing_item.css_first(".listing-search-item__sub-title")
                if subtitle_elem:
                    subtitle_text = subtitle_elem.text().strip()
                    
                    # Pattern: "1017 AS Amsterdam (De Weteringschans)"
                    subtitle_match = re.match(r'(\d{4}\s*[A-Z]{2})\s+([^(]+)(?:\s*\(([^)]+)\))?', subtitle_text)
                    if subtitle_match:
                        listing.postal_code = subtitle_match.group(1).strip()
                        listing.city = subtitle_match.group(2).strip().upper()
                        if len(subtitle_match.groups()) > 2 and subtitle_match.group(3):
                            listing.neighborhood = subtitle_match.group(3).strip()
                
                # Extract price
                price_elem = listing_item.css_first(".listing-search-item__price")
                if price_elem:
                    price_text = price_elem.text().strip()
                    listing.price = price_text
                    
                    # Extract numeric price 
                    price_match = re.search(r'€\s*([\d\.,]+)', price_text)
                    if price_match:
                        price_str = price_match.group(1)
                        # Remove all thousands separators (dots/commas)
                        price_str = price_str.replace(".", "").replace(",", "")
                        try:
                            listing.price_numeric = int(price_str)
                        except ValueError:
                            pass
                    
                    # Extract price period
                    if "per month" in price_text or "per maand" in price_text:
                        listing.price_period = "month"
                    elif "per week" in price_text or "per week" in price_text:
                        listing.price_period = "week"
                
                # Extract featured image
                img_elem = listing_item.css_first(".picture__image")
                if img_elem and img_elem.attributes.get("src"):
                    src = img_elem.attributes.get("src")
                    # Skip placeholder images
                    if not src.startswith("data:") and not "svg" in src:
                        listing.images = [src]
                
                # Extract property features
                features_elems = listing_item.css(".illustrated-features__item")
                for feature in features_elems:
                    feature_text = feature.text().strip().lower()
                    
                    # Check if this is a surface area feature by class
                    if feature.attributes.get("class") and "illustrated-features__item--surface-area" in feature.attributes.get("class"):
                        # Extract just the number from the text
                        area_match = re.search(r'(\d+)', feature_text)
                        if area_match:
                            listing.living_area = int(area_match.group(1))
                    
                    # Extract number of rooms
                    room_match = re.search(r'(\d+)\s+room', feature_text)
                    if room_match:
                        listing.rooms = int(room_match.group(1))
                        # Estimate bedrooms if not specified
                        if listing.rooms > 1:
                            listing.bedrooms = listing.rooms - 1
                    
                    # Extract interior type
                    if "shell" in feature_text:
                        listing.interior = InteriorType.SHELL
                    elif "upholstered" in feature_text:
                        listing.interior = InteriorType.UPHOLSTERED
                    elif "furnished" in feature_text:
                        listing.interior = InteriorType.FURNISHED
                
                # Determine property type from URL if not already set
                if not listing.property_type and listing.url:
                    if "apartment-for-rent" in listing.url:
                        listing.property_type = PropertyType.APARTMENT
                    elif "house-for-rent" in listing.url:
                        listing.property_type = PropertyType.HOUSE
                    elif "studio-for-rent" in listing.url:
                        listing.property_type = PropertyType.STUDIO
                    elif "room-for-rent" in listing.url:
                        listing.property_type = PropertyType.ROOM
                
                # Generate custom property hash
                listing.property_hash = self._generate_property_hash(listing)
                
                # Add the completed listing to the results
                listings.append(listing)
                
            except Exception as e:
                # Log error and continue with next listing
                logger.error(f"Error extracting listing from search page: {e}")
                continue
        
        return listings
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """
        This method is included for compatibility but should not be called
        since we extract all information from the search page
        """
        # Create a minimal listing with just the URL and source
        listing = PropertyListing(source="pararius", url=url)
        
        # Extract source ID from URL
        url_match = re.search(r'/([a-f0-9]{8})/', url)
        if url_match:
            listing.source_id = url_match.group(1)
        else:
            listing.source_id = str(uuid.uuid4())
        
        # Generate a custom property hash
        listing.property_hash = self._generate_property_hash(listing)
        
        return listing