"""
Kamernet.nl scraper implementation with HTML parsing.
Modified to store features as an array of dictionaries.
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
logger = get_scraper_logger("kamernet_scraper")


class KamernetScraper(BaseScraperStrategy):
    """Scraper strategy for Kamernet.nl that extracts rental properties"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for Kamernet"""
        # Format: https://kamernet.nl/huren/[type]-[city]
        # If no city is specified, use a default nationwide search
        if city:
            city_slug = city.lower().replace(' ', '-')
            return self.search_url_template.format(city=city_slug, days=days)
        else:
            return "https://kamernet.nl/huren/huurwoningen-nederland"
    
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
        Map Kamernet property types to our PropertyType enum
        
        Args:
            type_text: Kamernet's property type text
            
        Returns:
            PropertyType enum value
        """
        type_text = type_text.lower()
        if "kamer" in type_text:
            return PropertyType.ROOM
        elif "appartement" in type_text:
            return PropertyType.APARTMENT
        elif "studio" in type_text:
            return PropertyType.STUDIO
        elif "woning" in type_text or "huis" in type_text:
            return PropertyType.HOUSE
        else:
            # Default to room if unknown
            return PropertyType.ROOM
    
    def _map_interior_type(self, interior_text: str) -> Optional[InteriorType]:
        """
        Map Kamernet interior types to our InteriorType enum
        
        Args:
            interior_text: Kamernet's interior type text
            
        Returns:
            InteriorType enum value
        """
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
    
    def _parse_date_period(self, date_text: str) -> tuple:
        """
        Parse availability text to extract date_available and availability_period
        
        Args:
            date_text: The availability text from Kamernet
            
        Returns:
            Tuple of (date_available, availability_period)
        """
        date_available = None
        availability_period = None
        
        if not date_text:
            return date_available, availability_period
        
        # Clean the text
        date_text = date_text.strip()
        
        # Check if it's a starting date ("Vanaf X")
        if date_text.startswith("Vanaf"):
            date_available = date_text.replace("Vanaf", "").strip()
        # Check if it's a period (e.g., "1 Jul 2025 - 30 Jun 2027")
        elif " - " in date_text:
            availability_period = date_text
        else:
            # If it's just a single date without "Vanaf"
            date_available = date_text
            
        return date_available, availability_period
    
    def _extract_price(self, price_text: str) -> Optional[int]:
        """
        Extract numeric price from price text
        
        Args:
            price_text: The price text (e.g., "€ 800")
            
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
    
    def _utilities_included(self, period_text: str) -> bool:
        """
        Check if utilities are included in the rent
        
        Args:
            period_text: The price period text (e.g., "/maand incl.")
            
        Returns:
            True if utilities are included
        """
        if not period_text:
            return False
            
        return "incl" in period_text.lower()
    
    def _extract_size(self, size_text: str) -> Optional[int]:
        """
        Extract numeric size in m² from size text
        
        Args:
            size_text: The size text (e.g., "16 m²")
            
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
            
        matches = re.search(r'\/([a-z]+-\d+)$', url)
        if matches:
            return matches.group(1)
        return str(uuid.uuid4())  # Generate a random ID if not found
    
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
        Parse the Kamernet search results page and extract listings
        
        Args:
            html: HTML content of the search page
            
        Returns:
            List of PropertyListing objects
        """
        listings = []
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            # Find all listing cards - using the exact class structure from the HTML
            # Target <a> tags that are listing cards using partial class matching and structure
            listing_cards = soup.find_all('a', class_=lambda x: x and any(
                cls.endswith('mui-style-i2963i') 
                for cls in x.split()
            ))
            if not listing_cards:
                logger.warning(f"No listing cards found in Kamernet search results. Using alternative selector.")
                # Try with a more permissive selector
                listing_cards = soup.select('.ListingCard_root__e9Z81')
                
                if not listing_cards:
                    logger.error("Still could not find listing cards with alternative selector")
                    return []
            
            for card in listing_cards:
                try:
                    # Create a new property listing
                    listing = PropertyListing(source="kamernet")
                    # Initialize features as an empty list
                    listing.features = []
                    
                    # Extract URL directly from the card, which is the <a> element itself
                    if card.name == 'a' and 'href' in card.attrs:
                        relative_url = card['href']
                        listing.url = urljoin("https://kamernet.nl", relative_url)
                        listing.source_id = self._extract_source_id(relative_url)
                    else:
                        # If the card is not an <a> element, try to find one inside
                        link_element = card.find('a', href=True)
                        if link_element:
                            relative_url = link_element['href']
                            listing.url = urljoin("https://kamernet.nl", relative_url)
                            listing.source_id = self._extract_source_id(relative_url)
                        else:
                            logger.warning("Could not find URL in listing card")
                            continue
                    
                    # Extract address and city
                    address_elem = card.select_one('span.MuiTypography-root.MuiTypography-subtitle1.CommonStyles_whiteSpaceNoWrap__wYjK1.mui-style-qn273e')
                    city_elem = card.select_one('span.MuiTypography-root.MuiTypography-subtitle1.MuiTypography-noWrap.mui-style-1ejqop2')
                    
                    if address_elem:
                        listing.address = address_elem.text.replace(',', '').strip()
                    
                    if city_elem:
                        listing.city = city_elem.text.strip().upper()
                    
                    # Create title from address and city
                    if listing.address and listing.city:
                        listing.title = f"{listing.address}, {listing.city}"
                    
                    # Extract property type
                    property_type_elems = card.select('p.MuiTypography-root.MuiTypography-body2.MuiTypography-noWrap.mui-style-1i83cky')
                    if property_type_elems:
                        for elem in property_type_elems:
                            text = elem.text.strip()
                            if "kamer" in text.lower() or "appartement" in text.lower() or "studio" in text.lower():
                                listing.property_type = self._map_property_type(text)
                                break
                    
                    # Extract size
                    size_elems = card.select('p.MuiTypography-root.MuiTypography-body2.CommonStyles_whiteSpaceNoWrap__wYjK1.mui-style-1fsfdy1')
                    for elem in size_elems:
                        text = elem.text.strip()
                        if 'm²' in text:
                            listing.living_area = self._extract_size(text)
                            break
                    
                    # Extract interior type
                    interior_elems = card.select('p.MuiTypography-root.MuiTypography-body2.mui-style-1fsfdy1')
                    for elem in interior_elems:
                        text = elem.text.strip().lower()
                        if text in ['gemeubileerd', 'gestoffeerd', 'kaal']:
                            listing.interior = self._map_interior_type(text)
                            break
                    
                    # Extract availability
                    date_elems = card.select('p.MuiTypography-root.MuiTypography-body2.mui-style-1fsfdy1')
                    for elem in date_elems:
                        text = elem.text.strip()
                        if ('Vanaf' in text or ' - ' in text or 
                            any(month in text for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])):
                            date_available, availability_period = self._parse_date_period(text)
                            if date_available:
                                listing.date_available = date_available
                            if availability_period:
                                listing.availability_period = availability_period
                            break
                    
                    # Extract price and utilities included
                    price_elem = card.select_one('span.MuiTypography-root.MuiTypography-h5.mui-style-1pios4g')
                    if price_elem:
                        price_text = price_elem.text.strip()
                        listing.price = price_text
                        listing.price_numeric = self._extract_price(price_text)
                        listing.price_period = "month"
                        
                        # Check if utilities are included in a nearby element
                        utilities_elem = price_elem.find_next_sibling('p')
                        if utilities_elem and "incl" in utilities_elem.text.lower():
                            self._add_feature(listing, "utilities_included", "Yes")
                    
                    # Extract image
                    img_elem = card.select_one('img.MuiCardMedia-img')
                    if img_elem and 'src' in img_elem.attrs:
                        listing.images = [img_elem['src']]
                    
                    # Extract special labels
                    chip_labels = card.select('span.MuiChip-label')
                    for label in chip_labels:
                        label_text = label.text.strip()
                        
                        if label_text == "Nieuw":
                            self._add_feature(listing, "new_listing", "Yes")
                        elif label_text == "Top ad":
                            self._add_feature(listing, "featured", "Yes")
                        elif label_text == "Gratis reageren":
                            self._add_feature(listing, "free_response", "Yes")
                    
                    # Extract additional info that might be in the description
                    student_house_elem = card.select_one('.MuiTypography-root.MuiTypography-body2.MuiTypography-noWrap.CommonStyles_textEllipsis__Z5sTe.mui-style-1i83cky')
                    if student_house_elem and "studentenhuis" in student_house_elem.text.lower():
                        self._add_feature(listing, "student_housing", "Yes")
                    
                    # Set offering type (always rental on Kamernet)
                    listing.offering_type = OfferingType.RENTAL
                    
                    # Generate property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    logger.error(f"Error extracting listing from Kamernet card: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {len(listings)} listings from Kamernet search results")
            
        except Exception as e:
            logger.error(f"Error parsing Kamernet search results: {str(e)}")
        
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
        listing = PropertyListing(source="kamernet", url=url)
        listing.source_id = self._extract_source_id(url)
        # Initialize features as an empty list
        listing.features = []
        
        try:
            # Extract title
            title_elem = soup.select_one('h1')
            if title_elem:
                listing.title = title_elem.text.strip()
            
            # Extract description - look for main content area
            description_elem = soup.select_one('.PropertyDescription_description__root__GDaYe, .property-description')
            if description_elem:
                listing.description = description_elem.text.strip()
            
            # Extract address and city from title or dedicated field
            address_elem = soup.select_one('.PropertyDetails_address__MMwv4, .property-address')
            if address_elem:
                address_parts = address_elem.text.split(',')
                if len(address_parts) >= 2:
                    listing.address = address_parts[0].strip()
                    listing.city = address_parts[-1].strip()
            # If no dedicated address field, try to parse from title
            elif listing.title and ',' in listing.title:
                address_parts = listing.title.split(',')
                if len(address_parts) >= 2:
                    listing.address = address_parts[0].strip()
                    listing.city = address_parts[-1].strip()
            
            # Extract price
            price_elem = soup.select_one('.PropertyDetails_price__Lf45C, .property-price')
            if price_elem:
                price_text = price_elem.text.strip()
                listing.price = price_text
                listing.price_numeric = self._extract_price(price_text)
                listing.price_period = "month"
                
                # Check if utilities are included
                if "incl" in price_text.lower():
                    self._add_feature(listing, "utilities_included", "Yes")
            
            # Extract property details
            details_elems = soup.select('.PropertyDetails_propertySpec__l32TP, .property-details')
            for elem in details_elems:
                label = elem.select_one('.PropertyDetails_label__2oRnP, .property-label')
                value = elem.select_one('.PropertyDetails_value__dCfvw, .property-value')
                
                if not label or not value:
                    continue
                
                label_text = label.text.strip().lower()
                value_text = value.text.strip()
                
                if "type" in label_text:
                    listing.property_type = self._map_property_type(value_text)
                elif "oppervlakte" in label_text or "size" in label_text or "area" in label_text:
                    listing.living_area = self._extract_size(value_text)
                elif "beschikbaar" in label_text or "available" in label_text or "date" in label_text:
                    date_available, availability_period = self._parse_date_period(value_text)
                    listing.date_available = date_available
                    listing.availability_period = availability_period
                elif "interieur" in label_text or "interior" in label_text or "furnished" in label_text:
                    listing.interior = self._map_interior_type(value_text)
            
            # Extract images
            image_elems = soup.select('.PropertyCarousel_slide__PfSRt img, .property-image img')
            if image_elems:
                listing.images = [img['src'] for img in image_elems if 'src' in img.attrs]
            
            # Extract features and amenities
            amenities_elems = soup.select('.PropertyAmenities_item__RUz32, .property-features li')
            if amenities_elems:
                for elem in amenities_elems:
                    feature_text = elem.text.strip()
                    # Convert to English and normalize
                    feature_key = feature_text.lower().replace(' ', '_')
                    self._add_feature(listing, feature_key, feature_text)
            
            # Set offering type (always rental on Kamernet)
            listing.offering_type = OfferingType.RENTAL
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
        except Exception as e:
            logger.error(f"Error parsing Kamernet listing page: {str(e)}")
        
        return listing