"""
Vesteda.nl scraper implementation with JSON API extraction.
"""

import re
import uuid
import json
import hashlib
import logging
from typing import List, Dict, Any
from urllib.parse import urljoin

from models.property import PropertyListing, PropertyType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the telegram logger
logger = get_scraper_logger("vesteda_scraper")


class VestedaScraper(BaseScraperStrategy):
    """Scraper strategy for Vesteda.nl that extracts data from their JSON API response"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for Vesteda"""
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
    
    def _map_property_type(self, entity_subtype_label: str) -> PropertyType:
        """
        Map Vesteda property types to our PropertyType enum
        
        Args:
            entity_subtype_label: Vesteda's property type label
            
        Returns:
            PropertyType enum value
        """
        # Mapping according to requirements
        if entity_subtype_label in ["Eengezinswoning", "Maisonette"]:
            return PropertyType.HOUSE
        elif entity_subtype_label in ["Appartement", "Zorgwoning", "Penthouse"]:
            return PropertyType.APARTMENT
        elif entity_subtype_label == "Studio":
            return PropertyType.STUDIO
        else:
            # Default to apartment if unknown
            return PropertyType.APARTMENT
    
    async def parse_search_page(self, html: str) -> List[PropertyListing]:
        """
        Parse the Vesteda search results page (JSON response) and extract listings
        
        Returns a list of PropertyListing objects with data extracted from the JSON
        """
        listings = []
        
        try:
            # Parse the JSON response
            data = json.loads(html)
            
            # We're only interested in "today" properties based on requirements
            today_objects = data.get("results", {}).get("objects", {}).get("today", [])
            # There are not always listing for today, fallback to week to avoid "HTML structure may have changed!" warning
            if not today_objects:
                today_objects = data.get("results", {}).get("objects", {}).get("week", [])
            
            if not today_objects:
                return []
            
            for item in today_objects:
                try:
                    # Create a new property listing
                    listing = PropertyListing(source="vesteda")
                    
                    # Extract basic property information
                    listing.source_id = str(item.get("id", ""))
                    
                    # Build the complete URL
                    relative_url = item.get("url", "")
                    if relative_url:
                        listing.url = urljoin(self.base_url, relative_url)
                    
                    # Extract address components
                    street = item.get("street", "")
                    house_number = item.get("houseNumber", "")
                    house_number_addition = item.get("houseNumberAddition", "")
                    
                    # Combine address components
                    address_parts = [street, house_number]
                    if house_number_addition:
                        address_parts.append(house_number_addition)
                    
                    full_address = " ".join([str(part) for part in address_parts if part])
                    listing.address = full_address
                    listing.title = full_address
                    
                    # Extract postal code and city
                    postal_code = item.get("postalCode", "")
                    if postal_code:
                        # Insert a space between numbers and letters in the postal code
                        formatted_postal_code = re.sub(r'(\d+)([A-Za-z]+)', r'\1 \2', postal_code)
                        listing.postal_code = formatted_postal_code
                    else:
                        listing.postal_code = ""
                    
                    listing.city = item.get("city", "").upper()
                    
                    # Extract neighborhood/district
                    listing.neighborhood = item.get("district", "")
                    
                    # Extract price information
                    listing.price = item.get("price", "").replace("\u20AC", "€")
                    price_numeric = item.get("priceUnformatted")
                    if price_numeric is not None:
                        listing.price_numeric = int(float(price_numeric))
                    listing.price_period = "month"  # Vesteda prices are always per month
                    
                    # Extract property details
                    listing.living_area = item.get("size")
                    
                    # Extract rooms/bedrooms
                    bedrooms = item.get("numberOfBedRooms")
                    if bedrooms is not None:
                        listing.bedrooms = bedrooms
                        listing.rooms = bedrooms
                    
                    # Map property type
                    entity_subtype_label = item.get("entitysubtypelabel")
                    if entity_subtype_label:
                        listing.property_type = self._map_property_type(entity_subtype_label)
                    
                    # Extract property features / requirements
                    features = []
                    
                    # Check for age requirements
                    age_from = item.get("ageFrom", 0)
                    if age_from > 0:
                        features.append({"age_from_requirement": f"{age_from}+"})
                    
                    # Check for other special requirements
                    if item.get("onlySixtyFivePlus"):
                        # It is actually 55+
                        features.append({"age_requirement": "55+"})
                    
                    if item.get("prioritizeKeyProfessions"):
                        features.append({"key_profession_requirement": "Key workers"})
                    
                    if item.get("suitedForHomeSharers"):
                        features.append({"suited_homesharing": "Home sharing"})
                    
                    if item.get("onlyMiddleRent"):
                        features.append({"only_middle_rent_requirement": "Middle rent"})
                    
                    if item.get("priorityArrangement"):
                        features.append({"priority_arr_requirement": item.get("priorityArrangement")})
                    
                    # Add complex name as a feature
                    complex_name = item.get("complex")
                    if complex_name:
                        features.append({"complex": complex_name})
                    
                    if features:
                        listing.features = features
                    
                    # Extract image
                    image_url = item.get("imageBig") or item.get("imageSmall")
                    if image_url:
                        listing.images = [image_url]
                    
                    # Generate custom property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the completed listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    # Log error and continue with next listing
                    logger.error(f"Error extracting listing from Vesteda JSON: {e}")
                    continue
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from Vesteda: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing Vesteda data: {e}")
        
        return listings
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """
        This method is included for compatibility but should not be called
        since we extract all information from the search page JSON
        """
        # Create a minimal listing with just the URL and source
        listing = PropertyListing(source="vesteda", url=url)
        
        # Extract source ID from URL
        url_match = re.search(r'-(\d+)$', url)
        if url_match:
            listing.source_id = url_match.group(1)
        else:
            listing.source_id = str(uuid.uuid4())
        
        # Generate a custom property hash
        listing.property_hash = self._generate_property_hash(listing)
        
        return listing