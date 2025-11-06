"""
Huurwoningen.nl scraper implementation with HTML parsing.
Extracts rental properties with particular focus on "Nieuw" labeled listings.
"""

import re
import uuid
import hashlib
from typing import List, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from models.property import PropertyListing, PropertyType, InteriorType, OfferingType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the main scraper logger
logger = get_scraper_logger("huurwoningen_scraper")


class HuurwoningenScraper(BaseScraperStrategy):
    """Scraper strategy for Huurwoningen.nl that extracts rental properties"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for Huurwoningen.nl"""
        # Format: https://www.huurwoningen.nl/huren/[city]/
        if city:
            city_slug = city.lower().replace(' ', '-')
            return f"https://www.huurwoningen.nl/huren/{city_slug}/"
        else:
            return "https://www.huurwoningen.nl/aanbod-huurwoningen/"
    
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
    
    def _map_property_type(self, type_text: str) -> PropertyType:
        """
        Map Huurwoningen property types to our PropertyType enum
        
        Args:
            type_text: Huurwoningen's property type text
            
        Returns:
            PropertyType enum value
        """
        type_text = type_text.lower() if type_text else ""
        
        if "kamer" in type_text:
            return PropertyType.ROOM
        elif "appartement" in type_text:
            return PropertyType.APARTMENT
        elif "studio" in type_text:
            return PropertyType.STUDIO
        elif "woning" in type_text or "huis" in type_text:
            return PropertyType.HOUSE
        else:
            # Default to apartment if unknown
            return PropertyType.APARTMENT
    
    def _map_interior_type(self, interior_text: str) -> Optional[InteriorType]:
        """
        Map Huurwoningen interior types to our InteriorType enum
        
        Args:
            interior_text: Huurwoningen's interior type text
            
        Returns:
            InteriorType enum value
        """
        if not interior_text:
            return None
            
        interior_text = interior_text.lower()
        if "gemeubileerd" in interior_text:
            return InteriorType.FURNISHED
        elif "gestoffeerd" in interior_text:
            return InteriorType.UPHOLSTERED
        elif "kaal" in interior_text:
            return InteriorType.SHELL
        else:
            # Return None if unknown
            return None
    
    def _extract_price(self, price_text: str) -> Optional[int]:
        """
        Extract numeric price from price text
        
        Args:
            price_text: The price text (e.g., "€ 2.500 per maand")
            
        Returns:
            Price as integer
        """
        if not price_text:
            return None
            
        matches = re.search(r'€\s*([\d.,]+)', price_text)
        if matches:
            price_str = matches.group(1).replace('.', '').replace(',', '.')
            try:
                return int(float(price_str))
            except ValueError:
                logger.error(f"Could not parse price: {price_text}")
        return None
    
    def _extract_size(self, size_text: str) -> Optional[int]:
        """
        Extract numeric size in m² from size text
        
        Args:
            size_text: The size text (e.g., "175 m²")
            
        Returns:
            Size in square meters as integer
        """
        if not size_text:
            return None
            
        matches = re.search(r'(\d+)\s*m²', size_text)
        if matches:
            try:
                return int(matches.group(1))
            except ValueError:
                logger.error(f"Could not parse size: {size_text}")
        return None
    
    def _extract_rooms(self, rooms_text: str) -> Optional[int]:
        """
        Extract number of rooms from rooms text
        
        Args:
            rooms_text: The rooms text (e.g., "5 kamers")
            
        Returns:
            Number of rooms as integer
        """
        if not rooms_text:
            return None
            
        matches = re.search(r'(\d+)\s*kamer', rooms_text)
        if matches:
            try:
                return int(matches.group(1))
            except ValueError:
                logger.error(f"Could not parse rooms: {rooms_text}")
        return None
    
    def _extract_source_id(self, url: str) -> str:
        """
        Extract source ID from the URL
        
        Args:
            url: The property URL
            
        Returns:
            Source ID as string
        """
        if not url:
            return str(uuid.uuid4())
            
        matches = re.search(r'/(\d+)/', url)
        if matches:
            return matches.group(1)
        return str(uuid.uuid4())  # Generate a random ID if not found
    
    def _extract_city_and_district(self, location_text: str) -> tuple:
        """
        Extract city and district from location text
        
        Args:
            location_text: The location text (e.g., "1791 TL Den Burg (Den Burg)")
            
        Returns:
            Tuple of (city, postal_code, district)
        """
        city = None
        postal_code = None
        district = None
        
        if not location_text:
            return postal_code, city, district
        
        # Try to extract postal code
        postal_match = re.search(r'(\d{4}\s*[A-Z]{2})', location_text)
        if postal_match:
            postal_code = postal_match.group(1)
        
        # Extract city and district
        # Format is typically: "POSTAL_CODE CITY (DISTRICT)"
        district_match = re.search(r'\((.*?)\)', location_text)
        if district_match:
            district = district_match.group(1).strip()
            
        # Remove postal code and district to get city
        city_text = location_text
        if postal_code:
            city_text = city_text.replace(postal_code, "")
        if district:
            city_text = city_text.replace(f"({district})", "")
        
        # Clean up and extract city
        city_text = city_text.strip()
        if city_text:
            city = city_text
            
        return postal_code, city, district
    
    def _add_feature(self, listing: PropertyListing, name: str, value: str) -> None:
        """
        Add a feature to the listing's feature list
        
        Args:
            listing: PropertyListing object
            name: Feature name
            value: Feature value
        """
        # Initialize features as an empty list if it doesn't exist
        if not hasattr(listing, 'features') or listing.features is None:
            listing.features = []
            
        # Add the feature as a dictionary
        listing.features.append({name: value})
    
    async def parse_search_page(self, html: str) -> List[PropertyListing]:
        """
        Parse the Huurwoningen search results page and extract listings
        
        Args:
            html: HTML content of the search page
            
        Returns:
            List of PropertyListing objects
        """
        listings = []
        soup = BeautifulSoup(html, 'html.parser')
        first_is_new = True
        
        try:
            # Find all listing sections
            listing_sections = soup.select('section.listing-search-item')
            
            if not listing_sections:
                logger.warning("No listing sections found in Huurwoningen search results.")
                return []
            
            for section in listing_sections:
                try:
                    # Create a new property listing
                    listing = PropertyListing(source="huurwoningen")
                    # Initialize features as an empty list
                    listing.features = []
                    
                    # Extract URL
                    link_element = section.select_one('.listing-search-item__link--title')
                    if link_element and 'href' in link_element.attrs:
                        relative_url = link_element['href']
                        listing.url = urljoin("https://www.huurwoningen.nl", relative_url)
                        listing.source_id = self._extract_source_id(relative_url)
                    else:
                        logger.warning("Could not find URL in listing section")
                        continue
                    
                    # Check for "Nieuw" label - this is what we're specifically looking for
                    is_new = False
                    label_element = section.select_one('.listing-label')
                    if label_element and "Nieuw" in label_element.text:
                        is_new = True
                        self._add_feature(listing, "is_new", "Yes")
                    
                    # Check if the listing is exclusive
                    exclusive_element = section.select_one('.listing-search-item__exclusivity-mark')
                    if exclusive_element:
                        self._add_feature(listing, "exclusive_listing", "Yes")
                    
                    # Check if it's a top listing (Topwoning)
                    is_top_listing = False
                    top_listing_element = section.select_one('.listing-label--featured')
                    if top_listing_element and "Topwoning" in top_listing_element.text:
                        is_top_listing = True
                    
                    # Skip this listing if it's exclusive or a top listing
                    if is_top_listing:
                        logger.info(f"Skipping listing because its is a top listing: {is_top_listing}")
                        continue
                        
                    # If it's not a new listing, we're not interested
                    if not is_new:
                        # Only log is not new once, then silencly skip
                        if first_is_new:
                            logger.info("Skipping listing because it's not new")
                            first_is_new = False
                        continue
                    
                    # Extract title
                    title_element = section.select_one('.listing-search-item__title')
                    if title_element:
                        listing.title = title_element.text.strip()
                        # Set address to title (they are identical in this case)
                        listing.address = listing.title
                    
                    # Extract location info (postal code, city, district)
                    location_element = section.select_one('.listing-search-item__sub-title')
                    if location_element:
                        location_text = location_element.text.strip()
                        postal_code, city, district = self._extract_city_and_district(location_text)
                        listing.postal_code = postal_code
                        listing.city = city.upper()
                        if district:
                            # Store district in neighborhood field instead of features
                            listing.neighborhood = district
                    
                    # Extract price
                    price_element = section.select_one('.listing-search-item__price')
                    if price_element:
                        price_text = price_element.text.strip()
                        if price_text == "Prijs op aanvraag":
                            logger.info(f"Skipping listing because it's price is: {price_text}")
                            continue
                        listing.price = re.split(r' per maand', price_text, flags=re.IGNORECASE)[0] if "per maand" in price_text else price_text
                        listing.price_numeric = self._extract_price(price_text)
                        listing.price_period = "month"
                    
                    # Extract features
                    features_element = section.select_one('.illustrated-features')
                    if features_element:
                        # Extract size
                        size_element = features_element.select_one('.illustrated-features__item--surface-area')
                        if size_element:
                            size_text = size_element.text.strip()
                            listing.living_area = self._extract_size(size_text)
                        
                        # Extract number of rooms
                        rooms_element = features_element.select_one('.illustrated-features__item--number-of-rooms')
                        if rooms_element:
                            rooms_text = rooms_element.text.strip()
                            listing.rooms = self._extract_rooms(rooms_text)
                        
                        # Extract interior type
                        interior_element = features_element.select_one('.illustrated-features__item--interior')
                        if interior_element:
                            interior_text = interior_element.text.strip()
                            listing.interior = self._map_interior_type(interior_text)
                        
                        # Extract construction period
                        construction_element = features_element.select_one('.illustrated-features__item--construction-period')
                        if construction_element:
                            construction_text = construction_element.text.strip()
                            # Store directly in construction_year field instead of features
                            try:
                                listing.construction_year = int(construction_text)
                            except ValueError:
                                logger.warning(f"Could not parse construction year as int: {construction_text}")
                                # Store as string if we can't parse as int
                                listing.construction_year = construction_text
                    
                    # Extract image URL
                    img_element = section.select_one('.picture__image')
                    if img_element and 'src' in img_element.attrs:
                        # Filter out placeholder images
                        if 'data:image' not in img_element['src']:
                            listing.images = [img_element['src']]
                    
                    # Set property type based on URL that we scrape
                    listing.property_type = self._map_property_type(self.config.get("type"))
                    
                    # Set offering type (always rental on Huurwoningen)
                    listing.offering_type = OfferingType.RENTAL
                    
                    # Generate property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    logger.error(f"Error extracting listing from Huurwoningen section: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {len(listings)} listings from Huurwoningen search results")
            
        except Exception as e:
            logger.error(f"Error parsing Huurwoningen search results: {str(e)}")
        
        return listings
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """
        Parse the individual listing page to extract detailed information
        
        Args:
            html: HTML content of the listing page
            url: URL of the listing page
            
        Returns:
            PropertyListing object with detailed information
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Create a basic listing with source and URL
        listing = PropertyListing(source="huurwoningen", url=url)
        listing.source_id = self._extract_source_id(url)
        # Initialize features as an empty list
        listing.features = []
        
        try:
            # Extract title
            title_elem = soup.select_one('h1.listing-detail__title, .listing-detail-summary__title')
            if title_elem:
                listing.title = title_elem.text.strip()
                # Set address to title (they are identical in this case)
                listing.address = listing.title
            
            # Extract address and location info
            address_elem = soup.select_one('.listing-detail-summary__location')
            if address_elem:
                location_text = address_elem.text.strip()
                postal_code, city, district = self._extract_city_and_district(location_text)
                listing.postal_code = postal_code
                listing.city = city
                if district:
                    # Store district in neighborhood field instead of features
                    listing.neighborhood = district
            
            # Extract price
            price_elem = soup.select_one('.listing-detail-summary__price')
            if price_elem:
                price_text = price_elem.text.strip()
                listing.price = re.split(r' per maand', price_text, flags=re.IGNORECASE)[0] if "per maand" in price_text else price_text
                listing.price_numeric = self._extract_price(price_text)
                listing.price_period = "month"
            
            # Extract description
            description_elem = soup.select_one('.listing-detail__description-content')
            if description_elem:
                listing.description = description_elem.text.strip()
            
            # Extract features
            features_section = soup.select_one('.listing-features')
            if features_section:
                # Extract all feature items
                feature_items = features_section.select('.listing-features__item')
                for item in feature_items:
                    label_elem = item.select_one('.listing-features__label')
                    value_elem = item.select_one('.listing-features__value')
                    
                    if not label_elem or not value_elem:
                        continue
                    
                    label = label_elem.text.strip().lower()
                    value = value_elem.text.strip()
                    
                    if "oppervlakte" in label:
                        listing.living_area = self._extract_size(value)
                    elif "kamers" in label:
                        listing.rooms = self._extract_rooms(value)
                    elif "interieur" in label:
                        listing.interior = self._map_interior_type(value)
                    elif "soort" in label or "type" in label:
                        listing.property_type = self._map_property_type(value)
                    elif "bouwjaar" in label:
                        # Store directly in construction_year field
                        try:
                            listing.construction_year = int(value)
                        except ValueError:
                            listing.construction_year = value
                    elif "beschikbaar" in label:
                        listing.date_available = value
                    elif "energielabel" in label:
                        self._add_feature(listing, "energy_label", value)
                    elif "verwarming" in label:
                        self._add_feature(listing, "heating", value)
                    elif "balkon" in label or "terras" in label:
                        self._add_feature(listing, "balcony_terrace", value)
                    elif "tuin" in label:
                        self._add_feature(listing, "garden", value)
                    elif "parkeer" in label:
                        self._add_feature(listing, "parking", value)
                    else:
                        # Add any other features with converted key names
                        feature_key = label.replace(" ", "_").replace("-", "_")
                        self._add_feature(listing, feature_key, value)
            
            # Extract images
            image_elems = soup.select('.listing-detail__gallery-item-image, .listing-media__image')
            listing.images = []
            for img in image_elems:
                if 'data-src' in img.attrs:
                    listing.images.append(img['data-src'])
                elif 'src' in img.attrs and 'data:image' not in img['src']:
                    listing.images.append(img['src'])
            
            # Set offering type (always rental on Huurwoningen)
            listing.offering_type = OfferingType.RENTAL
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
        except Exception as e:
            logger.error(f"Error parsing Huurwoningen listing page: {str(e)}")
        
        return listing