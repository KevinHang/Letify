"""
123Wonen API scraper implementation.
Extracts rental properties from 123wonen.nl website.
"""

import re
import uuid
import hashlib
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from models.property import PropertyListing, PropertyType, InteriorType, OfferingType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the main scraper logger
logger = get_scraper_logger("wonen123_scraper")


class Wonen123Scraper(BaseScraperStrategy):
    """Scraper strategy for 123wonen.nl that extracts rental properties"""
    
    async def build_search_url(self, city: str = None, page: int = 1, **kwargs) -> str:
        """Build a search URL for 123wonen.nl"""
        # Base URL for search pages
        base_url = "https://www.123wonen.nl/huurwoningen"
        
        # Handle pagination
        if page > 1:
            url = f"{base_url}/page/{page}/sort/newest"
        else:
            url = f"{base_url}/sort/newest"
        
        # Add city parameter if provided
        if city:
            # Convert city name to lowercase and replace spaces with plus signs
            city_param = city.lower().replace(' ', '+')
            url = f"{url}?location={city_param}"
        
        return url
    
    def _generate_property_hash(self, listing: PropertyListing) -> str:
        """
        Generate a unique hash for the property based on available information.
        """
        # Collect all available identifiers
        identifiers = []
        
        # Use URL or ID as primary identifier
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
            
        # Ensure we have at least something unique
        if not identifiers:
            identifiers.append(str(uuid.uuid4()))
            
        # Create hash input
        hash_input = "|".join([str(x) for x in identifiers if x])
        
        # Generate hash
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _map_property_type(self, category: str) -> PropertyType:
        """
        Map 123wonen.nl property types to our PropertyType enum
        
        Args:
            category: Property category from website
            
        Returns:
            PropertyType enum value
        """
        category = category.lower() if category else ""
        
        # Mapping of Dutch property types to our enum values
        property_type_mapping = {
            "appartement": PropertyType.APARTMENT,
            "studio": PropertyType.STUDIO,
            "eengezinswoning": PropertyType.HOUSE,
            "bungalow": PropertyType.HOUSE,
            "2-onder-1-kap": PropertyType.HOUSE,
            "hoekwoning": PropertyType.HOUSE,
            "halfvrijstaande woonboerderij": PropertyType.HOUSE,
            "bovenwoning": PropertyType.APARTMENT,
            "vrijstaand geschakeld": PropertyType.HOUSE,
            "villa": PropertyType.HOUSE,
            "vrijstaand": PropertyType.HOUSE,
            "benedenwoning": PropertyType.APARTMENT,
            "tussenwoning": PropertyType.HOUSE,
            "maisonnette": PropertyType.APARTMENT,
            "woonboerderij": PropertyType.HOUSE,
            "herenhuis": PropertyType.HOUSE,
            "kamer": PropertyType.ROOM,
            "recreatiewoning": PropertyType.HOUSE,
            "half-vrijstaand": PropertyType.HOUSE,
            "flat (galerij/portiek)": PropertyType.APARTMENT,
            "loods": None,  # Skip this type
            "nieuwbouw": PropertyType.APARTMENT,  # Default to apartment, will be refined later
            "penthouse": PropertyType.APARTMENT,
            "berging/opslag": None,  # Skip this type
            "landhuis": PropertyType.HOUSE,
            "woonboot": PropertyType.HOUSE,
            "parkeerplaats": None  # Skip parking
        }
        
        # Return mapped type or default to APARTMENT if not found
        return property_type_mapping.get(category, PropertyType.APARTMENT)
    
    def _map_interior_type(self, interior: str) -> Optional[InteriorType]:
        """
        Map 123wonen.nl interior types to our InteriorType enum
        
        Args:
            interior: Interior type from website
            
        Returns:
            InteriorType enum value or None
        """
        interior = interior.lower() if interior else ""
        
        # Mapping of Dutch interior types to our enum values
        interior_mapping = {
            "gemeubileerd": InteriorType.FURNISHED,
            "gemeubileerd mogelijk": InteriorType.FURNISHED,
            "gestoffeerd": InteriorType.UPHOLSTERED,
            "kaal": InteriorType.SHELL,
            "onbekend": None
        }
        
        return interior_mapping.get(interior, None)
    
    def _parse_date_available(self, date_str: str) -> Optional[str]:
        """
        Parse availability date string from 123wonen.nl
        
        Args:
            date_str: Date string from website (e.g., "Vanaf 01-06-2025" or "Per Direct")
            
        Returns:
            Standardized date string (YYYY-MM-DD) or None
        """
        if not date_str:
            return None
            
        # Remove "Vanaf " prefix if present
        date_str = date_str.replace("Vanaf ", "")
        
        # Handle "Per Direct" case
        if date_str.lower() == "per direct":
            return datetime.now().strftime('%Y-%m-%d')
            
        try:
            # Parse Dutch date format (DD-MM-YYYY)
            date_obj = datetime.strptime(date_str, '%d-%m-%Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            logger.error(f"Could not parse date: {date_str}")
            return None
    
    def _parse_price(self, price_text: str) -> (Optional[int], Optional[str]):
        """
        Parse price string from 123wonen.nl
        
        Args:
            price_text: Price string from website (e.g., "€1.112,-p/mnd")
            
        Returns:
            Tuple of (numeric price, price period)
        """
        if not price_text:
            return None, None
            
        # Extract the numeric part of the price
        price_match = re.search(r'€?\s*([0-9.,]+)', price_text)
        if not price_match:
            return None, None
            
        # Convert price string to integer
        price_str = price_match.group(1).replace('.', '').replace(',', '')
        try:
            price = int(price_str)
        except ValueError:
            logger.error(f"Could not parse price: {price_text}")
            return None, None
            
        # Determine price period
        if "p/mnd" in price_text or "per month" in price_text:
            period = "month"
        elif "p/wk" in price_text or "per week" in price_text:
            period = "week"
        else:
            period = "month"  # Default to month
            
        return price, period
    
    def _extract_area(self, area_text: str) -> Optional[int]:
        """
        Extract area in square meters from text
        
        Args:
            area_text: Area text (e.g., "52 m²")
            
        Returns:
            Area as integer or None
        """
        if not area_text:
            return None
            
        area_match = re.search(r'(\d+)\s*m\s*[²&sup2;]', area_text)
        if area_match:
            try:
                return int(area_match.group(1))
            except ValueError:
                pass
                
        return None
    
    def _extract_energy_label(self, label_text: str) -> Optional[str]:
        """
        Extract energy label from text
        
        Args:
            label_text: Label text from website
            
        Returns:
            Energy label as string or None
        """
        if not label_text:
            return None
            
        # Extract label (A, B, C, etc.)
        label_match = re.search(r'[A-G][+]*', label_text)
        if label_match:
            return label_match.group(0)
            
        return None
    
    def _parse_listing_from_html(self, listing_element, base_url: str) -> Optional[PropertyListing]:
        """
        Parse a single listing from the HTML element
        
        Args:
            listing_element: BeautifulSoup element representing a property listing
            base_url: Base URL for constructing absolute URLs
            
        Returns:
            PropertyListing object or None if parsing fails
        """
        try:
            # Create a new property listing
            listing = PropertyListing(source="123wonen")
            
            # Extract URL
            detail_link = listing_element.select_one('a[href*="/huur/"]')
            if detail_link and 'href' in detail_link.attrs:
                listing.url = urljoin(base_url, detail_link['href'])
                
                # Extract source_id from URL
                source_id_match = re.search(r'/huur/.*-(\d+)-\d+', listing.url)
                if source_id_match:
                    listing.source_id = source_id_match.group(1)
            
            # Extract title/slogan
            slogan_element = listing_element.select_one('.pand-slogan span')
            if slogan_element:
                listing.title = slogan_element.text.strip()
            
            # Extract city and address
            address_element = listing_element.select_one('.pand-title')
            if address_element:
                address_text = address_element.text.strip()
                city_match = re.match(r'([^,]+),\s+(.+)', address_text)
                if city_match:
                    listing.city = city_match.group(1).strip().upper()
                    listing.address = city_match.group(2).strip()
            
            # Extract price
            price_element = listing_element.select_one('.pand-price')
            if price_element:
                price_text = price_element.text.strip()
                price_numeric, price_period = self._parse_price(price_text)
                listing.price_numeric = price_numeric
                listing.price_period = price_period
                listing.price = f"€ {price_numeric} per {price_period}" if price_numeric else None
            
            # Extract specifications
            specs_element = listing_element.select_one('.pand-specs')
            if specs_element:
                spec_items = specs_element.select('li')
                for item in spec_items:
                    # Each spec item has two spans: name and value
                    spans = item.select('span')
                    if len(spans) >= 2:
                        spec_name = spans[0].text.strip().lower()
                        spec_value = spans[1].text.strip()
                        
                        if spec_name == "type":
                            listing.property_type = self._map_property_type(spec_value)
                        elif spec_name == "interieur":
                            listing.interior = self._map_interior_type(spec_value)
                        elif spec_name == "woonoppervlakte":
                            listing.living_area = self._extract_area(spec_value)
                        elif spec_name == "slaapkamers":
                            try:
                                bedrooms_match = re.search(r'(\d+)', spec_value)
                                if bedrooms_match:
                                    # listing.bedrooms = int(bedrooms_match.group(1))
                                    listing.rooms = int(bedrooms_match.group(1))
                            except (ValueError, AttributeError):
                                pass
                        elif spec_name == "beschikbaarheid":
                            listing.date_available = self._parse_date_available(spec_value)
                        elif spec_name == "energielabel":
                            listing.energy_label = self._extract_energy_label(spec_value)
                        # disable for now as its in dutch and idk best approach to translate
                        # elif spec_name == "max. huurperiode":
                        #     listing.availability_period = spec_value
                        elif spec_name == "aangeboden sinds":
                            listing.date_listed = self._parse_date_available(spec_value)
            
            # Extract images
            image_elements = listing_element.select('.pand-image')
            listing.images = []
            for img_elem in image_elements:
                if 'data-src' in img_elem.attrs:
                    img_url = img_elem['data-src']
                    if img_url:
                        listing.images.append(urljoin(base_url, img_url))
            
            # Set offering type (always rental)
            listing.offering_type = OfferingType.RENTAL
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
            
        except Exception as e:
            logger.error(f"Error parsing listing from HTML: {str(e)}")
            return None
    
    async def parse_search_page(self, response_text: str) -> List[PropertyListing]:
        """
        Parse the search page to extract listings
        
        Args:
            response_text: HTML content of the search page
            
        Returns:
            List of PropertyListing objects
        """
        base_url = "https://www.123wonen.nl"
        listings = []
        
        try:
            # Parse HTML
            soup = BeautifulSoup(response_text, 'html.parser')
            
            # Find all property listings
            listing_elements = soup.select('.pandlist-container')
            
            for element in listing_elements:
                listing = self._parse_listing_from_html(element, base_url)
                if listing:
                    # Skip parkings and non-residential properties
                    if listing.property_type:
                        listings.append(listing)
            
            logger.info(f"Successfully extracted {len(listings)} listings from 123wonen.nl search page")
            
        except Exception as e:
            logger.error(f"Error parsing 123wonen.nl search page: {str(e)}")
        
        return listings
    
    async def parse_listing_page(self, response_text: str, url: str) -> PropertyListing:
        """
        Parse the individual listing page to extract detailed information
        
        Args:
            response_text: HTML content of the listing page
            url: URL of the listing page
            
        Returns:
            PropertyListing object with detailed information
        """
        try:
            # Parse HTML
            soup = BeautifulSoup(response_text, 'html.parser')
            
            # Create a basic listing with source and URL
            listing = PropertyListing(source="123wonen", url=url)
            
            # Extract ID from URL
            id_match = re.search(r'/huur/.*-(\d+)-\d+', url)
            if id_match:
                listing.source_id = id_match.group(1)
            
            # Extract title
            title_element = soup.select_one('h1')
            if title_element:
                listing.title = title_element.text.strip()
            
            # Extract address and city
            address_element = soup.select_one('.property-address')
            if address_element:
                address_text = address_element.text.strip()
                city_match = re.match(r'([^,]+),\s+(.+)', address_text)
                if city_match:
                    listing.city = city_match.group(1).strip().upper()
                    listing.address = city_match.group(2).strip()
            
            # Extract price
            price_element = soup.select_one('.property-price')
            if price_element:
                price_text = price_element.text.strip()
                price_numeric, price_period = self._parse_price(price_text)
                listing.price_numeric = price_numeric
                listing.price_period = price_period
                listing.price = f"€ {price_numeric} per {price_period}" if price_numeric else None
            
            # Extract specifications from detail page
            specs_table = soup.select_one('.property-specs table')
            if specs_table:
                rows = specs_table.select('tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) >= 2:
                        spec_name = cells[0].text.strip().lower()
                        spec_value = cells[1].text.strip()
                        
                        if spec_name == "type":
                            listing.property_type = self._map_property_type(spec_value)
                        elif spec_name == "interieur":
                            listing.interior = self._map_interior_type(spec_value)
                        elif spec_name == "woonoppervlakte":
                            listing.living_area = self._extract_area(spec_value)
                        elif spec_name == "slaapkamers":
                            try:
                                bedrooms_match = re.search(r'(\d+)', spec_value)
                                if bedrooms_match:
                                    listing.bedrooms = int(bedrooms_match.group(1))
                            except (ValueError, AttributeError):
                                pass
                        elif spec_name == "beschikbaarheid":
                            listing.date_available = self._parse_date_available(spec_value)
                        elif spec_name == "energielabel":
                            listing.energy_label = self._extract_energy_label(spec_value)
                        # elif spec_name == "max. huurperiode":
                        #     listing.availability_period = spec_value
                        elif spec_name == "kamers":
                            try:
                                rooms_match = re.search(r'(\d+)', spec_value)
                                if rooms_match:
                                    listing.rooms = int(rooms_match.group(1))
                            except (ValueError, AttributeError):
                                pass
                        elif spec_name == "servicekosten":
                            try:
                                service_costs = float(re.search(r'\d+', spec_value.replace('.', '').replace(',', '.')).group(0))
                                listing.service_costs = service_costs
                            except (ValueError, AttributeError):
                                pass
                        elif spec_name == "balkon" or spec_name == "balcony":
                            listing.balcony = "ja" in spec_value.lower() or "yes" in spec_value.lower()
                        elif spec_name == "tuin" or spec_name == "garden":
                            listing.garden = "ja" in spec_value.lower() or "yes" in spec_value.lower()
                        elif spec_name == "parkeergelegenheid" or spec_name == "parking":
                            listing.parking = "ja" in spec_value.lower() or "yes" in spec_value.lower()
                        elif spec_name == "bouwjaar" or spec_name == "construction year":
                            try:
                                listing.construction_year = int(re.search(r'\d{4}', spec_value).group(0))
                            except (ValueError, AttributeError):
                                pass
            
            # Extract detailed description
            description_element = soup.select_one('.property-description')
            if description_element:
                listing.description = description_element.text.strip()
            
            # Extract images
            image_elements = soup.select('.property-images img')
            listing.images = []
            for img_elem in image_elements:
                if 'data-src' in img_elem.attrs:
                    img_url = img_elem['data-src']
                elif 'src' in img_elem.attrs:
                    img_url = img_elem['src']
                else:
                    continue
                    
                if img_url:
                    # Ensure absolute URL
                    if not img_url.startswith('http'):
                        img_url = urljoin("https://www.123wonen.nl", img_url)
                    listing.images.append(img_url)
            
            # Set offering type (always rental)
            listing.offering_type = OfferingType.RENTAL
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
            
        except Exception as e:
            logger.error(f"Error parsing listing page: {str(e)}")
            
            # Create a basic listing with source and URL if parsing fails
            listing = PropertyListing(source="123wonen", url=url)
            
            # Extract ID from URL
            id_match = re.search(r'/huur/.*-(\d+)-\d+', url)
            if id_match:
                listing.source_id = id_match.group(1)
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing