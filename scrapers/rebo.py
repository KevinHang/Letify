"""
REBO Groep scraper implementation with JSON API extraction.
"""

import re
import uuid
import json
import hashlib
import logging
from typing import List, Dict, Any
from urllib.parse import urljoin
from datetime import datetime

from models.property import PropertyListing, PropertyType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the telegram logger
logger = get_scraper_logger("rebo_scraper")


class REBOScraper(BaseScraperStrategy):
    """Scraper strategy for REBO Groep that extracts data from their JSON API response"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for REBO Groep"""
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
    
    def _map_property_type(self, object_type: str, object_subtype: str) -> PropertyType:
        """
        Map REBO property types to our PropertyType enum
        
        Args:
            object_type: REBO's property type
            object_subtype: REBO's property subtype
            
        Returns:
            PropertyType enum value
        """
        # First check object_type
        if object_type in ["Appartement"]:
            return PropertyType.APARTMENT
        elif object_type in ["Woonhuis", "Eengezinswoning"]:
            return PropertyType.HOUSE
        
        # If object_type doesn't provide enough information, check object_subtype
        if object_subtype in ["Appartement", "portiekflat", "galerijflat", "portiekwoning", "APP", "Appartementen"]:
            return PropertyType.APARTMENT
        elif object_subtype in ["Eengezinswoning", "Tussenwoning", "Hoekwoning", "Eindwoning", "2-onder-1-kapwoning"]:
            return PropertyType.HOUSE
        elif object_subtype in ["maisonnette"]:
            return PropertyType.APARTMENT
        
        # Default to apartment if unknown
        return PropertyType.APARTMENT
    
    def _extract_postal_code(self, title: str) -> str:
        """Extract postal code from title if possible"""
        if not title:
            return ""
            
        postal_code_match = re.search(r'(\d{4}\s*[A-Z]{2})', title)
        if postal_code_match:
            return postal_code_match.group(1)
        return ""
    
    async def parse_search_page(self, html: str) -> List[PropertyListing]:
        """
        Parse the REBO search results page (JSON response) and extract listings
        
        Returns a list of PropertyListing objects with data extracted from the JSON
        """
        listings = []
        
        try:
            # Parse the JSON response
            data = json.loads(html)
            
            # Extract all properties
            all_properties = data.get("hits", [])
            
            if not all_properties:
                return []
            
            # Sort properties by source_created_at (newest first)
            sorted_properties = sorted(
                all_properties, 
                key=lambda x: x.get("source_created_at", 0), 
                reverse=True
            )
            
            for item in sorted_properties:
                try:
                    # Create a new property listing
                    listing = PropertyListing(source="rebo")
                    
                    # Extract ID from objectID or slug
                    listing.source_id = item.get("objectID", "") or item.get("slug", "")
                    
                    # Build the complete URL
                    relative_url = item.get("uri", "")
                    if relative_url:
                        listing.url = urljoin(self.base_url, relative_url)
                    
                    # Extract address
                    address = item.get("address", "")
                    if address:
                        listing.address = address
                        listing.title = address
                    
                    postal_code = self._extract_postal_code(item.get("title", ""))
                    if postal_code:
                        # Insert a space between numbers and letters in the postal code
                        formatted_postal_code = re.sub(r'(\d+)([A-Za-z]+)', r'\1 \2', postal_code)
                        listing.postal_code = formatted_postal_code
                    else:
                        listing.postal_code = ""
                    listing.city = item.get("city", "").upper()
                    
                    # Extract price information
                    price = item.get("price")
                    if price is not None:
                        listing.price_numeric = int(float(price))
                        listing.price = f"€ {price},-"
                        
                    price_type = item.get("price_type", "").lower()
                    if "maand" in price_type:
                        listing.price_period = "month"
                    elif "week" in price_type:
                        listing.price_period = "week"
                    
                    # Extract property details
                    living_area = item.get("surface_living")
                    if living_area and living_area > 0:
                        listing.living_area = living_area
                    
                    # Extract rooms/bedrooms
                    bedrooms = item.get("number_of_bedrooms")
                    if bedrooms:
                        listing.bedrooms = bedrooms
                        listing.rooms = bedrooms  # For simplicity, we'll set rooms equal to bedrooms
                    
                    # Map property type
                    object_type = item.get("object_type", "")
                    object_subtype = item.get("object_subtype", "")
                    listing.property_type = self._map_property_type(object_type, object_subtype)
                    
                    # Extract construction year
                    construction_year = item.get("construction_year")
                    if construction_year and construction_year.isdigit():
                        listing.construction_year = int(construction_year)
                    
                    # Extract date listed (convert timestamp to date)
                    source_created_at = item.get("source_created_at")
                    if source_created_at:
                        try:
                            # Convert Unix timestamp to datetime
                            listing.date_listed = datetime.fromtimestamp(source_created_at)
                        except (ValueError, TypeError):
                            pass
                    
                    # Collect features
                    features = []
                    
                    # Add property subtype as a feature
                    if object_subtype and object_subtype != "Onbekend":
                        features.append({"property_subtype": object_subtype})
                    
                    if features:
                        listing.features = features
                    
                    # Extract image
                    main_image = item.get("main_image")
                    if main_image:
                        listing.images = [main_image]
                    
                    # Extract geolocation
                    geoloc = item.get("_geoloc", {})
                    if geoloc:
                        lat = geoloc.get("lat")
                        lng = geoloc.get("lng")
                        if lat and lng:
                            listing.latitude = lat
                            listing.longitude = lng
                    
                    # Generate custom property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the completed listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    # Log error and continue with next listing
                    logger.error(f"Error extracting listing from REBO JSON: {e}")
                    continue
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from REBO: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing REBO data: {e}")
        
        return listings
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """
        This method is included for compatibility but should not be called
        since we extract all information from the search page JSON
        """
        # Create a minimal listing with just the URL and source
        listing = PropertyListing(source="rebo", url=url)
        
        # Extract source ID from URL
        url_match = re.search(r'/aanbod/([^/]+)', url)
        if url_match:
            listing.source_id = url_match.group(1)
        else:
            listing.source_id = str(uuid.uuid4())
        
        # Generate a custom property hash
        listing.property_hash = self._generate_property_hash(listing)
        
        return listing