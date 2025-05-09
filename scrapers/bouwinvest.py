"""
WonenBijBouwinvest.nl scraper implementation for JSON API data.
Extracts rental properties with relevant features.
"""

import re
import uuid
import hashlib
import json
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin
from datetime import datetime

from models.property import PropertyListing, PropertyType, InteriorType, OfferingType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the main scraper logger
logger = get_scraper_logger("wonenbijbouwinvest_scraper")


class WonenBijBouwinvestScraper(BaseScraperStrategy):
    """Scraper strategy for WonenBijBouwinvest.nl that extracts rental properties"""
    
    async def build_search_url(self, city: str, page: int = 1, **kwargs) -> str:
        """Build an API URL for WonenBijBouwinvest.nl"""
        # Format: https://www.wonenbijbouwinvest.nl/api/filter?city=City&page=1
        base_url = "https://www.wonenbijbouwinvest.nl/api/filter"
        
        params = []
        if city:
            city_slug = city.lower().replace(' ', '-')
            params.append(f"city={city_slug}")
        
        params.append(f"page={page}")
        params.append("order=created_at")
        params.append("dir=desc")
        
        url = f"{base_url}?{'&'.join(params)}"
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
    
    def _map_property_type(self, type_text: str) -> PropertyType:
        """
        Map WonenBijBouwinvest property types to our PropertyType enum
        
        Args:
            type_text: WonenBijBouwinvest's property type text
            
        Returns:
            PropertyType enum value
        """
        type_text = type_text.lower() if type_text else ""
        
        if "appartement" in type_text:
            return PropertyType.APARTMENT
        elif "studio" in type_text:
            return PropertyType.STUDIO
        elif "woonhuis" in type_text or "eengezins" in type_text or "tussenwoning" in type_text or "hoek" in type_text:
            return PropertyType.HOUSE
        elif "kamer" in type_text:
            return PropertyType.ROOM
        else:
            # Default to apartment if unknown
            return PropertyType.APARTMENT
    
    def _extract_price(self, price_data: Dict[str, Any]) -> tuple:
        """
        Extract price information from price data
        
        Args:
            price_data: Price data dictionary
            
        Returns:
            Tuple of (price_numeric, price_text, service_cost)
        """
        price_numeric = None
        price_text = None
        service_cost = 0
        
        try:
            if price_data:
                if "price" in price_data and price_data["price"]:
                    price_numeric = int(float(price_data["price"]))
                    price_text = f"€ {price_numeric} per maand"
                
                if "service_cost" in price_data and price_data["service_cost"]:
                    service_cost = int(float(price_data["service_cost"]))
        except (ValueError, TypeError) as e:
            logger.error(f"Error extracting price: {str(e)}")
            
        return price_numeric, price_text, service_cost
    
    def _add_feature(self, listing: PropertyListing, name: str, value: Any) -> None:
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
    
    def _parse_json_data(self, json_data: Dict[str, Any]) -> List[PropertyListing]:
        """
        Parse the JSON data from the API response
        
        Args:
            json_data: JSON data from the API response
            
        Returns:
            List of PropertyListing objects
        """
        listings = []
        
        try:
            # Check if data exists
            if "data" not in json_data or not json_data["data"]:
                logger.warning("No data found in JSON response")
                return []
            
            # Extract properties from data
            properties_data = json_data["data"]
            
            for prop_data in properties_data:
                try:
                    # Only process ProjectProperty class items
                    if "class" not in prop_data or prop_data["class"] != "ProjectProperty":
                        continue
                    
                    # Create a new property listing
                    listing = PropertyListing(source="wonenbijbouwinvest")
                    listing.features = []
                    
                    # Extract basic information
                    if "id" in prop_data:
                        listing.source_id = str(prop_data["id"])
                    
                    if "url" in prop_data:
                        listing.url = prop_data["url"]
                    
                    if "name" in prop_data:
                        listing.title = prop_data["name"]
                        # Set address to title since they are typically the same
                        listing.address = prop_data["name"]
                    
                    # Extract description
                    if "description" in prop_data:
                        listing.description = prop_data["description"]
                    
                    # Extract address information
                    if "address" in prop_data and isinstance(prop_data["address"], dict):
                        address_data = prop_data["address"]
                        if "city" in address_data:
                            listing.city = address_data["city"].upper()
                        if "zipcode" in address_data:
                            listing.postal_code = address_data["zipcode"]
                    
                    # Extract price information
                    if "price" in prop_data and isinstance(prop_data["price"], dict):
                        price_data = prop_data["price"]
                        price_numeric, price_text, service_cost = self._extract_price(price_data)
                        listing.price_numeric = price_numeric
                        listing.price = price_text
                        listing.price_period = "month"
                        
                        # Add service cost
                        if service_cost > 0:
                            listing.service_costs = service_cost
                        
                        # Add other price-related features
                        if "wozvalue" in price_data and price_data["wozvalue"]:
                            self._add_feature(listing, "woz_value", price_data["wozvalue"])
                        if "wozdate" in price_data and price_data["wozdate"]:
                            self._add_feature(listing, "woz_date", price_data["wozdate"])
                    
                    # Extract property information
                    if "properties" in prop_data and isinstance(prop_data["properties"], dict):
                        props = prop_data["properties"]
                        
                        # Extract rooms
                        if "total_rooms" in props and props["total_rooms"]:
                            listing.rooms = int(props["total_rooms"])
                        
                        # Extract sleeping rooms
                        if "total_sleepingrooms" in props and props["total_sleepingrooms"]:
                            sleeping_rooms = int(props["total_sleepingrooms"])
                            listing.bedrooms = sleeping_rooms
                        
                        # Extract build year
                        if "build_year" in props and props["build_year"]:
                            try:
                                build_year = int(props["build_year"])
                                listing.construction_year = build_year
                            except (ValueError, TypeError):
                                pass
                        
                        # Extract total floors
                        if "total_floors" in props and props["total_floors"]:
                            try:
                                total_floors = int(props["total_floors"])
                                listing.floors = total_floors
                            except (ValueError, TypeError):
                                pass
                        
                        # Extract total interested
                        if "total_interested" in props and props["total_interested"]:
                            try:
                                total_interested = int(props["total_interested"])
                                self._add_feature(listing, "total_interested", total_interested)
                            except (ValueError, TypeError):
                                pass
                        
                    # Extract sizes information
                    if "sizes" in prop_data and isinstance(prop_data["sizes"], dict):
                        sizes = prop_data["sizes"]
                        
                        # Extract living area
                        if "surface" in sizes and sizes["surface"]:
                            try:
                                listing.living_area = int(sizes["surface"])
                            except (ValueError, TypeError):
                                pass
                        
                        # Extract total content
                        if "total_content" in sizes and sizes["total_content"]:
                            try:
                                total_content = int(sizes["total_content"])
                                self._add_feature(listing, "total_content", total_content)
                            except (ValueError, TypeError):
                                pass
                    
                    # Extract coordinates information
                    if "coordinates" in prop_data and isinstance(prop_data["coordinates"], dict):
                        coords = prop_data["coordinates"]
                        
                        if "latitude" in coords and "longitude" in coords:
                            latitude = coords.get("latitude")
                            longitude = coords.get("longitude")
                            if latitude and longitude:
                                self._add_feature(listing, "coordinates", f"{latitude},{longitude}")
                    
                    # Extract images
                    if "images" in prop_data and isinstance(prop_data["images"], dict):
                        images = prop_data["images"]
                        listing.images = []
                        
                        # Add main images
                        if "main" in images and isinstance(images["main"], list):
                            listing.images.extend(images["main"])
                        
                        # Add extra images
                        if "extra" in images and isinstance(images["extra"], list):
                            listing.images.extend(images["extra"])
                    
                    # Extract availability from labels
                    if "labels" in prop_data and isinstance(prop_data["labels"], dict):
                        labels = prop_data["labels"]
                        
                        if "stickerbar" in labels and labels["stickerbar"]:
                            availability = labels["stickerbar"]
                            
                            # Extract date if available
                            date_match = re.search(r'per (\d{2}-\d{2}-\d{4})', availability)
                            if date_match:
                                date_str = date_match.group(1)
                                try:
                                    date_obj = datetime.strptime(date_str, '%d-%m-%Y')
                                    listing.date_available = date_obj.strftime('%Y-%m-%d')
                                except ValueError:
                                    self._add_feature(listing, "availability", availability)
                            else:
                                self._add_feature(listing, "availability", availability)
                    
                    # Extract owner information
                    if "owner" in prop_data and isinstance(prop_data["owner"], dict):
                        owner = prop_data["owner"]
                        
                        if "name" in owner and owner["name"]:
                            self._add_feature(listing, "owner", owner["name"])
                    
                    # Add content features (property highlights)
                    if "content" in prop_data and isinstance(prop_data["content"], list) and len(prop_data["content"]) > 0:
                        content_list = prop_data["content"]
                        
                        for i, content_item in enumerate(content_list, start=1):
                            if "content" in content_item and content_item["content"]:
                                content_text = content_item["content"]
                                # Check if this is a property type
                                if i == 1 and isinstance(content_text, str):
                                    listing.property_type = self._map_property_type(content_text)
                                
                                # Add as a feature
                                feature_name = f"highlight_{i}"
                                self._add_feature(listing, feature_name, content_text)
                    # if we cant find property type in content, try to get from description
                    elif "description" in prop_data and len(prop_data["content"]) == 0:
                        description = prop_data["description"]
                        listing.property_type = self._map_property_type(description)
                    else:
                        listing.property_type = PropertyType.APARTMENT
 
                    # Set offering type (always rental)
                    listing.offering_type = OfferingType.RENTAL
                    
                    # Generate property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    logger.error(f"Error extracting listing from WonenBijBouwinvest data: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {len(listings)} listings from WonenBijBouwinvest JSON data")
            
        except Exception as e:
            logger.error(f"Error parsing WonenBijBouwinvest JSON data: {str(e)}")
        
        return listings
    
    async def parse_search_page(self, response_text: str) -> List[PropertyListing]:
        """
        Parse the API response to extract listings
        
        Args:
            response_text: JSON response from the API
            
        Returns:
            List of PropertyListing objects
        """
        try:
            # Parse JSON response
            json_data = json.loads(response_text)
            
            # Extract listings from JSON data
            return self._parse_json_data(json_data)
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error parsing search page: {str(e)}")
            return []
    
    async def parse_listing_page(self, response_text: str, url: str) -> PropertyListing:
        """
        Parse the individual listing page to extract detailed information
        Since the API already provides detailed information, we can reuse the same parsing logic
        
        Args:
            response_text: JSON response from the API
            url: URL of the listing page
            
        Returns:
            PropertyListing object with detailed information
        """
        try:
            # Parse JSON response
            json_data = json.loads(response_text)
            
            # Check if it's a single property response
            if "data" in json_data and isinstance(json_data["data"], dict):
                # Create a modified structure to match the search page format
                modified_data = {
                    "data": [json_data["data"]]
                }
                
                # Extract listings from JSON data
                listings = self._parse_json_data(modified_data)
                
                # Return the first listing if available
                if listings:
                    return listings[0]
            
            # Create a basic listing with source and URL
            listing = PropertyListing(source="wonenbijbouwinvest", url=url)
            if url:
                listing.source_id = url.split('/')[-1]
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {str(e)}")
            # Create a basic listing with source and URL
            listing = PropertyListing(source="wonenbijbouwinvest", url=url)
            if url:
                listing.source_id = url.split('/')[-1]
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
        except Exception as e:
            logger.error(f"Error parsing listing page: {str(e)}")
            # Create a basic listing with source and URL
            listing = PropertyListing(source="wonenbijbouwinvest", url=url)
            if url:
                listing.source_id = url.split('/')[-1]
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing