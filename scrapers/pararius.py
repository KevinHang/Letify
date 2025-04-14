"""
Pararius.com scraper implementation with fixed selectors.
"""

import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

from selectolax.parser import HTMLParser, Node

from models.property import PropertyListing, PropertyType, InteriorType
from scrapers.base import BaseScraperStrategy


class ParariusScraper(BaseScraperStrategy):
    """Scraper strategy for Pararius.com"""
    
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
    
    async def parse_search_page(self, html: str) -> List[str]:
        """Parse the Pararius search results page for listing URLs"""
        parser = HTMLParser(html)
        listing_urls = []
        
        # Extract listing URLs from search page
        for listing_item in parser.css(".listing-search-item"):
            link = listing_item.css_first(".listing-search-item__link--title")
            if link and link.attributes.get("href"):
                href = link.attributes.get("href")
                full_url = urljoin(self.base_url, href)
                listing_urls.append(full_url)
        
        return listing_urls
    
    def _find_definition_term_value(self, parser: HTMLParser, term_text: str) -> Optional[str]:
        """
        Find a definition term (dt) containing the given text and return the value (dd)
        This replaces the :-soup-contains() selector which is not supported in selectolax
        """
        # Find all dt elements
        dt_elements = parser.css("dt")
        
        for dt in dt_elements:
            # Check if the text contains our search term
            if dt.text() and term_text.lower() in dt.text().lower():
                # Get the next dd element (sibling)
                dd = dt.next
                # Skip non-element nodes
                while dd and dd.tag != "dd":
                    dd = dd.next
                
                if dd and dd.tag == "dd":
                    return dd.text().strip()
        
        return None
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """Parse a Pararius listing detail page"""
        parser = HTMLParser(html)
        listing = PropertyListing(source="pararius", url=url)
        
        # Extract source ID from URL
        url_match = re.search(r'/([a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12})/', url)
        if url_match:
            listing.source_id = url_match.group(1)
        
        # Extract title
        title_elem = parser.css_first("h1.listing__title")
        if title_elem:
            listing.title = title_elem.text().strip()
        
        # Extract address and location info
        address_elem = parser.css_first(".listing__address")
        if address_elem:
            listing.address = address_elem.text().strip()
        
        location_elem = parser.css_first(".listing__sub-title")
        if location_elem:
            loc_text = location_elem.text().strip()
            postal_city_match = re.match(r'(\d{4}\s*[A-Z]{2})\s+(.+?)\s*(?:\((.+)\))?$', loc_text)
            if postal_city_match:
                listing.postal_code = postal_city_match.group(1)
                listing.city = postal_city_match.group(2)
                if postal_city_match.group(3):
                    listing.neighborhood = postal_city_match.group(3)
        
        # Extract price
        price_elem = parser.css_first(".listing-detail-summary__price")
        if price_elem:
            price_text = price_elem.text().strip()
            listing.price = price_text
            
            # Extract numeric price
            price_match = re.search(r'€\s*([\d\.,]+)', price_text)
            if price_match:
                price_str = price_match.group(1).replace(".", "").replace(",", ".")
                try:
                    listing.price_numeric = float(price_str)
                except ValueError:
                    pass
            
            # Extract price period
            if "per month" in price_text or "per maand" in price_text:
                listing.price_period = "month"
            elif "per week" in price_text or "per week" in price_text:
                listing.price_period = "week"
        
        # Extract description
        description_elem = parser.css_first(".listing-detail-description__text")
        if description_elem:
            listing.description = description_elem.text().strip()
        
        # Extract property characteristics
        for feature in parser.css(".illustrated-features__item"):
            feature_class = feature.attributes.get("class", "")
            feature_text = feature.text().strip()
            
            # Extract living area
            if "surface-area" in feature_class:
                area_match = re.search(r'(\d+)\s*m²', feature_text)
                if area_match:
                    listing.living_area = int(area_match.group(1))
            
            # Extract number of rooms/bedrooms
            elif "number-of-rooms" in feature_class:
                rooms_match = re.search(r'(\d+)\s+room', feature_text)
                if rooms_match:
                    listing.rooms = int(rooms_match.group(1))
                
                # Try to extract bedrooms specifically
                bedrooms_match = re.search(r'(\d+)\s+bedroom', feature_text)
                if bedrooms_match:
                    listing.bedrooms = int(bedrooms_match.group(1))
                elif listing.rooms and listing.rooms > 1:
                    # If specific bedrooms not mentioned, estimate based on total rooms
                    listing.bedrooms = listing.rooms - 1
            
            # Extract interior type
            elif "interior" in feature_class:
                if "shell" in feature_text.lower():
                    listing.interior = InteriorType.SHELL
                elif "upholstered" in feature_text.lower():
                    listing.interior = InteriorType.UPHOLSTERED
                elif "furnished" in feature_text.lower():
                    listing.interior = InteriorType.FURNISHED
        
        # Extract property type 
        property_type_elem = parser.css_first(".listing-detail-summary__type")
        if property_type_elem:
            type_text = property_type_elem.text().strip().lower()
            if "apartment" in type_text or "appartement" in type_text:
                listing.property_type = PropertyType.APARTMENT
            elif "house" in type_text or "huis" in type_text:
                listing.property_type = PropertyType.HOUSE
            elif "studio" in type_text:
                listing.property_type = PropertyType.STUDIO
            elif "room" in type_text or "kamer" in type_text:
                listing.property_type = PropertyType.ROOM
        
        # Extract images
        for img in parser.css(".listing-detail-media__pictures img"):
            src = img.attributes.get('src') or img.attributes.get('data-src')
            if src and src not in listing.images:
                listing.images.append(src)
        
        # Extract construction year using the helper method instead of unsupported selector
        construction_year_text = self._find_definition_term_value(parser, "Construction year")
        if construction_year_text:
            year_match = re.search(r'\d{4}', construction_year_text)
            if year_match:
                listing.construction_year = int(year_match.group())
        
        # Extract availability date
        available_date_text = self._find_definition_term_value(parser, "Available from")
        if available_date_text:
            listing.date_available = available_date_text
        
        # Extract service costs
        service_costs_text = self._find_definition_term_value(parser, "Service costs")
        if service_costs_text:
            costs_match = re.search(r'€\s*([\d\.,]+)', service_costs_text)
            if costs_match:
                costs_str = costs_match.group(1).replace(".", "").replace(",", ".")
                try:
                    listing.service_costs = float(costs_str)
                except ValueError:
                    pass
        
        return listing