"""
VBT Verhuurmakelaars API scraper implementation.
Extracts rental properties while skipping specific categories.
"""

import re
import uuid
import hashlib
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from urllib.parse import urljoin

from models.property import PropertyListing, PropertyType, InteriorType, OfferingType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the main scraper logger
logger = get_scraper_logger("vbt_verhuurmakelaars_scraper")


class VBTVerhuurmakelaarsScraper(BaseScraperStrategy):
    """Scraper strategy for VBT Verhuurmakelaars API that extracts rental properties"""
    
    async def build_search_url(self, city: str = None, page: int = 1, **kwargs) -> str:
        """Build an API URL for VBT Verhuurmakelaars"""
        # Format: https://api.vbtverhuurmakelaars.nl/properties?city=City&page=1
        base_url = "https://api.vbtverhuurmakelaars.nl/properties"
        
        params = []
        if city:
            city_slug = city.lower().replace(' ', '-')
            params.append(f"city={city_slug}")
        
        params.append(f"page={page}")
        params.append("limit=20")
        params.append("sort=newest")
        
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
    
    def _map_property_type(self, category: str) -> PropertyType:
        """
        Map VBT Verhuurmakelaars property types to our PropertyType enum
        
        Args:
            category: Property category from API
            
        Returns:
            PropertyType enum value
        """
        category = category.lower() if category else ""
        
        if category == "apartment":
            return PropertyType.APARTMENT
        elif category == "studio":
            return PropertyType.STUDIO
        elif category == "house" or category == "family_house":
            return PropertyType.HOUSE
        elif category == "room":
            return PropertyType.ROOM
        else:
            # Default to apartment if unknown
            return PropertyType.APARTMENT
    
    def _extract_date_available(self, date_str: str) -> Optional[str]:
        """
        Convert date string to standardized format
        
        Args:
            date_str: Date string from API
            
        Returns:
            Standardized date string (YYYY-MM-DD)
        """
        if not date_str or "1970-01-01" in date_str:
            return None
            
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            logger.error(f"Could not parse date: {date_str}")
            return None
    
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
            # Check if houses exists
            if "houses" not in json_data or not json_data["houses"]:
                logger.warning("No houses found in JSON response")
                return []
            
            # Extract properties from houses
            houses_data = json_data["houses"]
            
            for house in houses_data:
                try:
                    # Skip if category is "other"
                    if (house.get("attributes", {}).get("type", {}).get("category") == "other"):
                        continue
                    
                    # Skip if status is not available
                    if (house.get("status", {}).get("name") != "available"):
                        continue
                    
                    # Skip if isBouwinvest is true
                    if house.get("isBouwinvest", False):
                        continue
                    
                    # Create a new property listing
                    listing = PropertyListing(source="vb&t")
                    listing.features = []
                    
                    # Extract basic information
                    if "id" in house:
                        listing.source_id = str(house["id"])
                    elif "sourceId" in house:
                        listing.source_id = str(house["sourceId"])
                    
                    if "url" in house:
                        # URL may be relative, ensure it's complete
                        if house["url"].startswith("/"):
                            listing.url = f"https://www.vbtverhuurmakelaars.nl{house['url']}"
                        else:
                            listing.url = house["url"]
                    
                    # Extract address information
                    if "address" in house and isinstance(house["address"], dict):
                        address_data = house["address"]
                        
                        if "city" in address_data:
                            listing.city = address_data["city"].upper()
                            
                        if "house" in address_data:
                            # Extract address from the 'house' field
                            listing.address = address_data["house"]
                            # Use as title too
                            listing.title = address_data["house"]
                    
                    # Extract price information
                    if "prices" in house and isinstance(house["prices"], dict):
                        prices = house["prices"]
                        
                        if "rental" in prices and isinstance(prices["rental"], dict):
                            rental = prices["rental"]
                            
                            if "price" in rental:
                                listing.price_numeric = int(float(rental["price"]))
                                listing.price = f"€ {listing.price_numeric} per month"
                                listing.price_period = "month"
                            
                            # Extract other rental info
                            if "serviceCharges" in rental and rental["serviceCharges"]:
                                service_charges = int(float(rental["serviceCharges"]))
                                listing.service_costs = service_charges
                            
                            if "securityDeposit" in rental and rental["securityDeposit"]:
                                deposit = int(float(rental["securityDeposit"]))
                                self._add_feature(listing, "security_deposit", deposit)
                            
                            if "minMonths" in rental and rental["minMonths"]:
                                min_months = int(rental["minMonths"])
                                self._add_feature(listing, "min_rental_months", min_months)
                        
                        # Extract WOZ information
                        if "woz" in prices and isinstance(prices["woz"], dict):
                            woz = prices["woz"]
                            
                            if "value" in woz and woz["value"]:
                                woz_value = int(float(woz["value"]))
                                self._add_feature(listing, "woz_value", woz_value)
                            
                            if "refdate" in woz and woz["refdate"]:
                                woz_date = self._extract_date_available(woz["refdate"])
                                if woz_date:
                                    self._add_feature(listing, "woz_date", woz_date)
                        
                        # Extract rental points
                        if "rentalpoints" in prices and prices["rentalpoints"]:
                            rental_points = int(prices["rentalpoints"])
                            self._add_feature(listing, "rental_points", rental_points)
                        
                        # Extract parking information
                        if "parkingCharges" in prices and prices["parkingCharges"]:
                            parking_charges = int(float(prices["parkingCharges"]))
                            self._add_feature(listing, "parking_charges", parking_charges)
                        
                        if "parkingServiceCharges" in prices and prices["parkingServiceCharges"]:
                            parking_service_charges = int(float(prices["parkingServiceCharges"]))
                            self._add_feature(listing, "parking_service_charges", parking_service_charges)
                    
                    # Extract property type
                    if "attributes" in house and isinstance(house["attributes"], dict):
                        attributes = house["attributes"]
                        
                        if "type" in attributes and isinstance(attributes["type"], dict):
                            type_info = attributes["type"]
                            
                            if "category" in type_info:
                                listing.property_type = self._map_property_type(type_info["category"])
                                
                            if "buildType" in type_info:
                                self._add_feature(listing, "build_type", type_info["buildType"])
                    
                    # Extract living area (plot)
                    if "plot" in house and house["plot"]:
                        listing.living_area = int(float(house["plot"]))
                    
                    # Extract rooms
                    if "rooms" in house and house["rooms"]:
                        listing.rooms = int(house["rooms"])
                    
                    # Extract interested parties
                    if "interestedParties" in house and house["interestedParties"]:
                        interested_parties = int(house["interestedParties"])
                        self._add_feature(listing, "interested_parties", interested_parties)
                    
                    # Extract status information
                    if "status" in house and isinstance(house["status"], dict):
                        status = house["status"]
                        
                        if "name" in status:
                            self._add_feature(listing, "status", status["name"])
                        
                        if "code" in status:
                            self._add_feature(listing, "status_code", status["code"])
                    
                    # Extract USPs (Unique Selling Points)
                    if "usps" in house and isinstance(house["usps"], list):
                        for i, usp in enumerate(house["usps"], start=1):
                            if isinstance(usp, dict) and "text" in usp:
                                usp_text = usp["text"]
                                usp_type = usp.get("type", "usp")
                                
                                feature_name = f"{usp_type}_{i}"
                                self._add_feature(listing, feature_name, usp_text)
                    
                    # Extract coordinates
                    if "coordinate" in house and isinstance(house["coordinate"], list) and len(house["coordinate"]) >= 2:
                        coordinates = house["coordinate"]
                        longitude, latitude = coordinates[0], coordinates[1]
                        coordinate_str = f"{latitude},{longitude}"
                        self._add_feature(listing, "coordinates", coordinate_str)
                    
                    # Extract image
                    if "image" in house and house["image"]:
                        image_path = house["image"]
                        # Complete URL if needed
                        if image_path.startswith("/"):
                            image_url = f"https://www.vbtverhuurmakelaars.nl{image_path}"
                            listing.images = [image_url]
                    
                    # Extract source information
                    if "source" in house and isinstance(house["source"], dict):
                        source = house["source"]
                        
                        if "externalLink" in source:
                            self._add_feature(listing, "external_link", source["externalLink"])
                        
                        if "lastImported" in source:
                            import_date = self._extract_date_available(source["lastImported"])
                            if import_date:
                                self._add_feature(listing, "last_imported", import_date)
                    
                    # Set offering type (always rental)
                    listing.offering_type = OfferingType.RENTAL
                    
                    # Generate property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    logger.error(f"Error extracting listing from VBT Verhuurmakelaars data: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {len(listings)} listings from VBT Verhuurmakelaars JSON data")
            
        except Exception as e:
            logger.error(f"Error parsing VBT Verhuurmakelaars JSON data: {str(e)}")
        
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
        
        Args:
            response_text: HTML content of the listing page
            url: URL of the listing page
            
        Returns:
            PropertyListing object with detailed information
        """
        try:
            # Parse JSON response if available
            try:
                json_data = json.loads(response_text)
                
                # Create a modified structure to match the search page format
                if "house" in json_data:
                    modified_data = {
                        "houses": [json_data["house"]]
                    }
                    
                    # Extract listings from JSON data
                    listings = self._parse_json_data(modified_data)
                    
                    # Return the first listing if available
                    if listings:
                        return listings[0]
            except json.JSONDecodeError:
                pass  # Not JSON, continue with HTML parsing
            
            # Create a basic listing with source and URL
            listing = PropertyListing(source="vb&t", url=url)
            
            # Extract ID from URL
            id_match = re.search(r'/woning/[^/]+-([^/]+)/?$', url)
            if id_match:
                listing.source_id = id_match.group(1)
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
            
        except Exception as e:
            logger.error(f"Error parsing listing page: {str(e)}")
            
            # Create a basic listing with source and URL
            listing = PropertyListing(source="vb&t", url=url)
            
            # Extract ID from URL
            id_match = re.search(r'/woning/[^/]+-([^/]+)/?$', url)
            if id_match:
                listing.source_id = id_match.group(1)
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing