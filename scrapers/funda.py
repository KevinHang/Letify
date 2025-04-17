"""
Funda.nl scraper implementation with search-page-only extraction.
"""

import re
import uuid
import hashlib
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

from selectolax.parser import HTMLParser, Node

from models.property import PropertyListing, PropertyType, InteriorType
from scrapers.base import BaseScraperStrategy


logger = logging.getLogger(__name__)


class FundaScraper(BaseScraperStrategy):
    """Scraper strategy for Funda.nl that extracts data from search results only"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for Funda"""
        return self.search_url_template.format(city=city.lower(), days=days)
    
    def _generate_property_hash(self, listing: PropertyListing) -> str:
        """
        Generate a unique hash for the property based on available information.
        This is a robust implementation that works even with partial data.
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
        if listing.bedrooms:
            identifiers.append(f"bedrooms:{listing.bedrooms}")
            
        # Ensure we have at least something unique
        if not identifiers:
            identifiers.append(str(uuid.uuid4()))
            
        # Create hash input
        hash_input = "|".join([str(x) for x in identifiers if x])
        
        # Generate hash
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    async def parse_search_page(self, html: str) -> List[PropertyListing]:
        """
        Parse the Funda search results page and extract listings directly
        
        Returns a list of PropertyListing objects with data extracted only from search page
        """
        parser = HTMLParser(html)
        listings = []
        
        # Find all listing cards/items in the search results
        # Try different selectors to account for potential HTML structure changes
        listing_elements = parser.css(".flex.flex-col.sm\\:flex-row")
        
        if not listing_elements:
            # Fallback to alternative selector
            listing_elements = parser.css("div.border-b.pb-3")
            
        if not listing_elements:
            logger.warning("No listing elements found in the HTML. Check if the page structure has changed.")
            return []
            
        logger.info(f"Found {len(listing_elements)} listing elements to process")
        
        for card in listing_elements:
            try:
                # Create a new property listing
                listing = PropertyListing(source="funda")
                
                # Extract listing URL and source ID
                link = card.css_first("h2 a") or card.css_first("a[data-testid='listingDetailsAddress']")
                if not link:
                    continue
                    
                href = link.attributes.get("href")
                if not href or "/detail/" not in href:
                    continue
                    
                full_url = urljoin(self.base_url, href)
                listing.url = full_url
                
                # Try to extract source ID from URL
                url_match = re.search(r'/(\d+)/', full_url)
                if url_match:
                    listing.source_id = url_match.group(1)
                else:
                    # Generate a unique ID if none found in URL
                    listing.source_id = str(uuid.uuid4())
                
                # Extract address
                address_elem = card.css_first("h2 a .flex.font-semibold span.truncate") or card.css_first("h2 a span.truncate")
                if address_elem:
                    address_text = address_elem.text().strip()
                    if address_text:
                        listing.address = address_text
                        listing.title = address_text
                
                # Extract postal code and city
                postal_city_elem = card.css_first("h2 a div.truncate.text-neutral-80") or card.css_first("div.truncate.text-neutral-80")
                if postal_city_elem:
                    postal_city_text = postal_city_elem.text().strip()
                    
                    # Pattern: "1017DX Amsterdam" or similar
                    postal_city_match = re.match(r'(\d{4}[A-Z]{2})\s+(.+)', postal_city_text)
                    if postal_city_match:
                        listing.postal_code = postal_city_match.group(1)
                        listing.city = postal_city_match.group(2)
                    else:
                        # Try to extract city only
                        listing.city = postal_city_text
                
                # Extract price
                # First case is if there is only monthly price, second case is if it contains buying price and monthly rent
                price_elem = card.css_first("div.font-semibold.mt-2.mb-0 div") or card.css_first("div.font-semibold.mt-2 div:nth-child(2)")
                if price_elem:
                    price_text = price_elem.text().strip()
                    if price_elem.parent and "line-through" in price_elem.parent.attributes.get("class", ""):
                        # Property is under option or sold
                        status_elem = card.css_first("span.mb-1.mr-1.inline-block.rounded.px-2.py-0\\.5.text-xs.font-semibold.bg-red-70")
                        if status_elem:
                            listing.status = status_elem.text().strip()
                    
                    listing.price = price_text
                    
                    # Extract numeric price
                    price_match = re.search(r'€\s*([\d\.,]+)', price_text)
                    if price_match:
                        price_str = price_match.group(1).replace(".", "").replace(",", ".")
                        try:
                            listing.price_numeric = int(float(price_str))
                        except ValueError:
                            pass
                    
                    # Extract price period
                    if "/maand" in price_text or "/mnd" in price_text:
                        listing.price_period = "month"
                    elif "/week" in price_text:
                        listing.price_period = "week"
                
                # Extract property features (living area, bedrooms, energy label, etc.)
                features_list = card.css_first("ul.flex.h-8.flex-wrap.gap-4.overflow-hidden.truncate.py-1")
                if features_list:
                    for feature in features_list.css("li"):
                        feature_text = feature.text().strip()
                        
                        # Extract living area
                        area_match = re.search(r'(\d+)\s*m²', feature_text)
                        if area_match:
                            try:
                                listing.living_area = int(area_match.group(1))
                            except ValueError:
                                pass
                        
                        # Extract rooms/bedrooms
                        if not "m²" in feature_text:
                            rooms_match = re.search(r'(\d+)', feature_text)
                            if rooms_match:
                                try:
                                    rooms = int(rooms_match.group(1))
                                    # Room icon usually represents bedrooms
                                    if "bed" in feature.html or "slaap" in feature.html:
                                        listing.bedrooms = rooms
                                    else:
                                        listing.rooms = rooms
                                except ValueError:
                                    pass
                        
                        # Extract energy label
                        energy_label_match = re.search(r'([A-G][\+\-]*)$', feature_text)
                        if energy_label_match:
                            listing.energy_label = energy_label_match.group(1)
                
                # Extract featured image
                img_elem = card.css_first("img")
                if img_elem:
                    src = None
                    # Check for srcset attribute first
                    srcset = img_elem.attributes.get("srcset")
                    if srcset:
                        # Get the highest resolution image from srcset
                        src_matches = re.findall(r'(https://[^\s]+)', srcset)
                        if src_matches:
                            src = src_matches[-1].split(' ')[0]
                    
                    # Fallback to src attribute
                    if not src:
                        src = img_elem.attributes.get("src")
                    
                    if src and not src.startswith("data:") and not "svg" in src:
                        listing.images = [src]
                
                # Extract property type from URL
                if "appartement" in listing.url:
                    listing.property_type = PropertyType.APARTMENT
                elif "huis" in listing.url:
                    listing.property_type = PropertyType.HOUSE
                elif "studio" in listing.url:
                    listing.property_type = PropertyType.STUDIO
                elif "kamer" in listing.url:
                    listing.property_type = PropertyType.ROOM
                
                # Extract realtor
                realtor_elem = card.css_first("a.truncate.text-secondary-70") or card.css_first("a.truncate.text-secondary-70.hover\\:text-secondary-70-darken-1")
                if realtor_elem:
                    listing.realtor = realtor_elem.text().strip()
                
                # Check if the listing is new
                new_tag = card.css_first("span.mb-1.mr-1.inline-block.rounded.px-2.py-0\\.5.text-xs.font-semibold.bg-primary-50")
                if new_tag and "nieuw" in new_tag.text().lower():
                    listing.is_new = True
                
                # Generate custom property hash
                listing.property_hash = self._generate_property_hash(listing)
                
                # Add the completed listing to the results
                listings.append(listing)
                
            except Exception as e:
                # Log error and continue with next listing
                logger.error(f"Error extracting listing from Funda search page: {e}")
                continue
        
        return listings
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """
        This method is included for compatibility but should not be called
        since we extract all information from the search page
        """
        # Create a minimal listing with just the URL and source
        listing = PropertyListing(source="funda", url=url)
        
        # Extract source ID from URL
        url_match = re.search(r'/(\d+)/', url)
        if url_match:
            listing.source_id = url_match.group(1)
        else:
            listing.source_id = str(uuid.uuid4())
        
        # Generate a custom property hash
        listing.property_hash = self._generate_property_hash(listing)
        
        return listing