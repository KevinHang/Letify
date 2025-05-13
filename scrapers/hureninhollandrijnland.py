"""
Dutch Housing Portal API scraper implementation.
Extracts rental properties from housing portals using JSON API.
"""

import re
import uuid
import hashlib
import json
import datetime
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

from models.property import PropertyListing, PropertyType, InteriorType, OfferingType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the main scraper logger
logger = get_scraper_logger("dutch_housing_portal_scraper")


class HurenInHollandRijnland(BaseScraperStrategy):
    """Scraper strategy for Dutch Housing Portal API that extracts rental properties"""
    
    async def build_search_url(self, city: str = None, page: int = 1, **kwargs) -> str:
        """Build an API URL for the Housing Portal API"""
        # Base URL - Replace with the actual API endpoint
        base_url = "https://api.housing-portal.nl/properties"
        
        params = []
        if city:
            # Format city for API query
            city_param = city.lower().replace(' ', '-')
            params.append(f"city={city_param}")
        
        params.append(f"page={page}")
        params.append("limit=20")
        
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
    
    def _map_property_type(self, dwelling_type: Dict[str, Any]) -> Optional[PropertyType]:
        """
        Map Housing Portal property types to our PropertyType enum
        
        Args:
            dwelling_type: Property dwelling type from API
            
        Returns:
            PropertyType enum value or None
        """
        if not dwelling_type or "code" not in dwelling_type:
            return None
            
        code = dwelling_type.get("code", "").lower()
        name = dwelling_type.get("name", "").lower()
        
        # Map Dutch property types to our enum values
        if "appartement" in name or "flat" in code:
            return PropertyType.APARTMENT
        elif "studio" in name:
            return PropertyType.STUDIO
        elif "eengezinswoning" in name or "woning" in code:
            return PropertyType.HOUSE
        elif "kamer" in name:
            return PropertyType.ROOM
        elif "benedenwoning" in name:
            return PropertyType.APARTMENT
        elif "bovenwoning" in name:
            return PropertyType.APARTMENT
        elif "parkeerplaats" in name:
            return None  # Skip parking
        else:
            # Default to apartment if unknown
            return PropertyType.APARTMENT
    
    def _map_interior_type(self, info_text: str) -> Optional[InteriorType]:
        """
        Try to extract interior type from the info text
        
        Args:
            info_text: Info text that might contain interior information
            
        Returns:
            InteriorType enum value or None
        """
        if not info_text:
            return None
            
        info_text = info_text.lower()
        
        if "gemeubileerd" in info_text:
            return InteriorType.FURNISHED
        elif "gestoffeerd" in info_text:
            return InteriorType.UPHOLSTERED
        elif "kaal" in info_text:
            return InteriorType.SHELL
        else:
            return None
    
    def _extract_date_available(self, date_str: Optional[str], available_text: Optional[str]) -> Optional[str]:
        """
        Extract availability date from available date string or text description
        
        Args:
            date_str: Date string from API
            available_text: Text description of availability
            
        Returns:
            Standardized date string (YYYY-MM-DD) or None
        """
        # If explicit date is provided
        if date_str and date_str != "null":
            try:
                date_obj = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return date_obj.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass
                
        # Try to extract from available text
        if available_text:
            # Direct available / Per direct
            if "direct" in available_text.lower():
                return datetime.datetime.now().strftime('%Y-%m-%d')
                
            # Try to extract date patterns
            # Pattern for DD-MM-YYYY
            date_match = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', available_text)
            if date_match:
                day, month, year = date_match.groups()
                try:
                    date_obj = datetime.datetime(int(year), int(month), int(day))
                    return date_obj.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass
                    
            # Pattern for month names
            months = {
                'januari': 1, 'februari': 2, 'maart': 3, 'april': 4, 'mei': 5, 'juni': 6,
                'juli': 7, 'augustus': 8, 'september': 9, 'oktober': 10, 'november': 11, 'december': 12
            }
            
            for month_name, month_num in months.items():
                month_match = re.search(fr'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})', available_text, re.IGNORECASE)
                if month_match:
                    day, year = month_match.groups()
                    try:
                        date_obj = datetime.datetime(int(year), month_num, int(day))
                        return date_obj.strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        pass
        
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
    
    def _extract_features(self, item: Dict[str, Any], listing: PropertyListing) -> None:
        """
        Extract feature information from the property data
        
        Args:
            item: Property data dictionary
            listing: PropertyListing object to add features to
        """
        # Extract basic features 
        if "storageRoom" in item and item["storageRoom"]:
            self._add_feature(listing, "storage", item["storageRoom"] == 1)
            
        # Floor information
        if "floor" in item and item["floor"] and "verdieping" in item["floor"]:
            self._add_feature(listing, "floor", item["floor"]["verdieping"])
            
        # Extract heating type
        if "heating" in item and item["heating"] and "localizedName" in item["heating"]:
            self._add_feature(listing, "heating_type", item["heating"]["localizedName"])
            
        # Extract specific facilities
        if "specifiekeVoorzieningen" in item and isinstance(item["specifiekeVoorzieningen"], list):
            for voorziening in item["specifiekeVoorzieningen"]:
                if "localizedName" in voorziening:
                    self._add_feature(listing, "facility", voorziening["localizedName"])
                
        # Extract service components
        if "servicecomponentenBinnenServicekosten" in item and isinstance(item["servicecomponentenBinnenServicekosten"], list):
            for component in item["servicecomponentenBinnenServicekosten"]:
                if "localizedNaam" in component:
                    self._add_feature(listing, "service_component", component["localizedNaam"])
                
        # Extract minimum income requirement
        if "minimumIncome" in item and item["minimumIncome"]:
            self._add_feature(listing, "minimum_income", item["minimumIncome"])
            
        # Extract minimum age requirement
        if "minimumAge" in item and item["minimumAge"]:
            self._add_feature(listing, "minimum_age", item["minimumAge"])
            
        # Extract maximum household size
        if "maximumHouseholdSize" in item and item["maximumHouseholdSize"]:
            self._add_feature(listing, "maximum_household_size", item["maximumHouseholdSize"])
            
        # Extract coordinates
        if "latitude" in item and "longitude" in item and item["latitude"] and item["longitude"]:
            coordinates = f"{item['latitude']},{item['longitude']}"
            self._add_feature(listing, "coordinates", coordinates)
            
        # Extract action label if available
        if "actionLabel" in item and item["actionLabel"] and "localizedLabel" in item["actionLabel"]:
            self._add_feature(listing, "action_label", item["actionLabel"]["localizedLabel"])
            
        # # Extract doelgroepen (target groups)
        # if "doelgroepen" in item and isinstance(item["doelgroepen"], list):
        #     for doelgroep in item["doelgroepen"]:
        #         if "localizedNaam" in doelgroep:
        #             self._add_feature(listing, "target_group", doelgroep["localizedNaam"])
    
    def _extract_bedrooms(self, sleeping_room: Dict[str, Any], area_sleeping_room: str) -> Optional[int]:
        """
        Extract number of bedrooms from sleeping room data
        
        Args:
            sleeping_room: Sleeping room data
            area_sleeping_room: Sleeping room area description
            
        Returns:
            Number of bedrooms or None
        """
        if sleeping_room and "amountOfRooms" in sleeping_room:
            try:
                return int(sleeping_room["amountOfRooms"])
            except (ValueError, TypeError):
                pass
                
        # Try to count bedrooms from area description
        if area_sleeping_room:
            # If format is like "7, 8 en 13", count the number of values
            commas = area_sleeping_room.count(',')
            ands = area_sleeping_room.lower().count(' en ')
            
            if commas > 0 or ands > 0:
                return commas + ands + 1
                
        return None
    
    def _extract_area(self, area_value: Any) -> Optional[int]:
        """
        Extract area in square meters
        
        Args:
            area_value: Area value from API
            
        Returns:
            Area as integer or None
        """
        if not area_value:
            return None
            
        try:
            return int(float(area_value))
        except (ValueError, TypeError):
            return None
    
    def _parse_property_item(self, item: Dict[str, Any], base_url: str) -> Optional[PropertyListing]:
        """
        Parse a property item from the API response
        
        Args:
            item: Property data dictionary
            base_url: Base URL for constructing absolute URLs
            
        Returns:
            PropertyListing object or None if parsing fails
        """
        try:
            # Create a new property listing
            listing = PropertyListing(source="HollandRijnland")
            
            # Initialize features list
            listing.features = []
            
            # Extract basic information
            if "id" in item:
                listing.source_id = str(item["id"])
                # Construct URL
                if "urlKey" in item:
                    listing.url = f"{base_url}/woningaanbod/details/{item['urlKey']}"
            
            # Extract address information
            address_parts = []
            if "street" in item and item["street"]:
                address_parts.append(item["street"])
            if "houseNumber" in item and item["houseNumber"]:
                address_parts.append(item["houseNumber"])
            if "houseNumberAddition" in item and item["houseNumberAddition"]:
                address_parts.append(item["houseNumberAddition"])
                
            listing.address = " ".join(address_parts)
            
            # Extract postal code
            if "postalcode" in item and item["postalcode"]:
                listing.postal_code = item["postalcode"]
                
            # Extract city
            if "city" in item and isinstance(item["city"], dict) and "name" in item["city"]:
                listing.city = item["city"]["name"].upper()
            elif "gemeenteGeoLocatieNaam" in item and item["gemeenteGeoLocatieNaam"]:
                listing.city = item["gemeenteGeoLocatieNaam"].upper()
                
            # Extract neighborhood
            if "quarter" in item and isinstance(item["quarter"], dict) and "name" in item["quarter"]:
                listing.neighborhood = item["quarter"]["name"]
                
            # Extract price information
            if "totalRent" in item and item["totalRent"]:
                listing.price_numeric = int(float(item["totalRent"]))
                listing.price = f"€ {listing.price_numeric}"
                listing.price_period = "month"
                
            # Extract service costs
            if "serviceCosts" in item and item["serviceCosts"]:
                listing.service_costs = float(item["serviceCosts"])
                
            # Extract property type
            if "dwellingType" in item and isinstance(item["dwellingType"], dict):
                listing.property_type = self._map_property_type(item["dwellingType"])
                
                # Set title based on property type and address
                if listing.property_type and listing.address:
                    type_name = item["dwellingType"].get("localizedName", "")
                    listing.title = f"{type_name} {listing.address}"
                    
            # Extract living area
            if "areaDwelling" in item and item["areaDwelling"]:
                listing.living_area = self._extract_area(item["areaDwelling"])
                
            # Extract plot area
            if "areaPerceel" in item and item["areaPerceel"]:
                listing.plot_area = self._extract_area(item["areaPerceel"])
                
            # Extract rooms information
            if "sleepingRoom" in item and isinstance(item["sleepingRoom"], dict):
                # Bedrooms
                listing.bedrooms = self._extract_bedrooms(
                    item["sleepingRoom"],
                    item.get("areaSleepingRoom", "")
                )
                
                # Rooms (add 1 for living room)
                if listing.bedrooms is not None:
                    listing.rooms = listing.bedrooms + 1
                    
            # Extract energy label
            if "energyLabel" in item and isinstance(item["energyLabel"], dict) and "localizedNaam" in item["energyLabel"]:
                energy_label = item["energyLabel"]["localizedNaam"]
                
                # Extract the label directly from formats like "Energielabel B" or "Energielabel A+++"
                label_match = re.search(r'Energielabel\s+([A-G](?:\+{1,4})?)', energy_label, re.IGNORECASE)
                if label_match:
                    listing.energy_label = label_match.group(1)
                else:
                    # Alternative approach: just take the last character(s) of the string
                    # This handles cases where the format might be different
                    label_text = energy_label.strip()
                    label_parts = label_text.split()
                    if label_parts:
                        potential_label = label_parts[-1]  # Take the last part
                        if re.match(r'^[A-G](\+{1,4})?$', potential_label):
                            listing.energy_label = potential_label
                    
            # Extract construction year
            if "constructionYear" in item and item["constructionYear"]:
                try:
                    listing.construction_year = int(item["constructionYear"])
                except (ValueError, TypeError):
                    pass
                    
            # Extract availability date
            listing.date_available = self._extract_date_available(
                item.get("availableFromDate"),
                item.get("availableFrom", "")
            )
            
            # Extract publication date
            if "publicationDate" in item and item["publicationDate"]:
                listing.date_listed = self._extract_date_available(item["publicationDate"], None)
                
            # Extract boolean features
            if "balcony" in item:
                listing.balcony = item["balcony"] == 1
            if "tuin" in item:
                listing.garden = item["tuin"] == 1
            if "storageRoom" in item:
                listing.parking = item["storageRoom"] == 1
                
            # Try to extract interior type from description
            if "infoveld" in item and item["infoveld"]:
                listing.description = item["infoveld"]
                listing.interior = self._map_interior_type(item["infoveld"])
                
            # Extract images
            if "pictures" in item and isinstance(item["pictures"], list):
                listing.images = []
                for picture in item["pictures"]:
                    if "uri" in picture:
                        img_url = picture["uri"]
                        # Convert relative URLs to absolute URLs
                        if img_url.startswith("/"):
                            img_url = urljoin(base_url, img_url)
                        listing.images.append(img_url)
                        
            # Extract features
            self._extract_features(item, listing)
                
            # Set offering type (always rental)
            listing.offering_type = OfferingType.RENTAL
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
            
        except Exception as e:
            logger.error(f"Error parsing property item: {str(e)}")
            return None
    
    async def parse_search_page(self, response_text: str) -> List[PropertyListing]:
        """
        Parse the API response to extract listings
        
        Args:
            response_text: JSON response from the API
            
        Returns:
            List of PropertyListing objects
        """
        base_url = "https://hureninhollandrijnland.nl"  # Adjust this to the actual base URL
        listings = []
        
        try:
            # Parse JSON response
            json_data = json.loads(response_text)
            
            # Extract data array
            if "data" in json_data and isinstance(json_data["data"], list):
                data_items = json_data["data"]
                
                for item in data_items:
                    listing = self._parse_property_item(item, base_url)
                    if listing and listing.property_type:  # Skip None values and listings without property type (like parking)
                        listings.append(listing)
                
                logger.info(f"Successfully extracted {len(listings)} listings from housing portal API")
            else:
                logger.warning("No data array found in API response")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON data: {str(e)}")
        except Exception as e:
            logger.error(f"Error parsing search page: {str(e)}")
        
        return listings
    
    async def parse_listing_page(self, response_text: str, url: str) -> PropertyListing:
        """
        Parse the individual listing page to extract detailed information
        
        Args:
            response_text: HTML or JSON content of the listing page
            url: URL of the listing page
            
        Returns:
            PropertyListing object with detailed information
        """
        base_url = "https://hureninhollandrijnland.nl"  # Adjust this to the actual base URL
        
        try:
            # Try to parse as JSON first
            try:
                json_data = json.loads(response_text)
                
                # Check if the JSON contains a single property
                if "data" in json_data:
                    item = json_data["data"]
                    
                    # Check if it's an array with one item
                    if isinstance(item, list) and len(item) > 0:
                        listing = self._parse_property_item(item[0], base_url)
                        if listing:
                            return listing
                    # Or a single object
                    elif isinstance(item, dict):
                        listing = self._parse_property_item(item, base_url)
                        if listing:
                            return listing
            except json.JSONDecodeError:
                pass  # Not JSON, continue with fallback
            
            # Create a basic listing with source and URL as fallback
            listing = PropertyListing(source="HollandRijnland", url=url)
            listing.features = []  # Initialize empty features list
            
            # Extract ID from URL
            id_match = re.search(r'/details/([^/]+)', url)
            if id_match:
                url_key = id_match.group(1)
                # Try to extract numeric ID from the URL key
                id_match2 = re.search(r'^(\d+)-', url_key)
                if id_match2:
                    listing.source_id = id_match2.group(1)
            
            # Generate property hash
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing
            
        except Exception as e:
            logger.error(f"Error parsing listing page: {str(e)}")
            
            # Create a basic listing with source and URL if all else fails
            listing = PropertyListing(source="HollandRijnland", url=url)
            listing.features = []  # Initialize empty features list
            listing.property_hash = self._generate_property_hash(listing)
            
            return listing