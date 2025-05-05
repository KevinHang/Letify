"""
WoningNet (WRB) scraper implementation parsing JSON responses.
"""

import re
import uuid
import json
import hashlib
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from models.property import PropertyListing, PropertyType, InteriorType, OfferingType
from scrapers.base import BaseScraperStrategy
from utils.logging_config import get_scraper_logger

# Use a child logger of the main scraper logger
logger = get_scraper_logger("woningnet_scraper")


class WoningNetScraper(BaseScraperStrategy):
    """Scraper strategy for WoningNet that extracts rental properties from JSON responses"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for WoningNet"""
        # The actual implementation would depend on WoningNet's API structure
        # This is a placeholder that would need to be customized
        return self.search_url_template.format(city=city.lower(), days=days)
    
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
        if listing.rooms:
            identifiers.append(f"rooms:{listing.rooms}")
            
        # Ensure we have at least something unique
        if not identifiers:
            identifiers.append(str(uuid.uuid4()))
            
        # Create hash input
        hash_input = "|".join([str(x) for x in identifiers if x])
        
        # Generate hash
        return hashlib.md5(hash_input.encode()).hexdigest()
    
    def _map_property_type(self, detail_soort: str, eenheid_soort: str) -> PropertyType:
        """
        Map WoningNet property types to our PropertyType enum
        
        Args:
            detail_soort: Property type detail
            eenheid_soort: Property unit type
            
        Returns:
            PropertyType enum value
        """
        if not detail_soort:
            return PropertyType.APARTMENT
        
        detail_soort = detail_soort.lower()
        
        # House types
        if any(x in detail_soort for x in ["woning", "eengezinswoning", "hoekwoning", "tussenwoning", "maisonette"]):
            return PropertyType.HOUSE
        
        # Room types
        elif "kamer" in detail_soort:
            return PropertyType.ROOM
        
        # Studio types
        elif "studio" in detail_soort:
            return PropertyType.STUDIO
        
        # Various apartment types
        elif any(x in detail_soort for x in ["appartement", "flat", "portiekwoning", "maisonnette", 
                                             "benedenwoning", "bovenwoning", "galerijflat"]):
            return PropertyType.APARTMENT
        
        # Default to apartment
        return PropertyType.APARTMENT
    
    def _translate_publication_label(self, label: str) -> List[Dict[str, str]]:
        """
        Translate PublicatieLabel into English and format as required
        
        Args:
            label: Original Dutch publication label
            
        Returns:
            List of dictionaries with translated labels
        """
        if not label:
            return []
        
        features = []
        # Split by ~ as specified
        labels = label.split('~')
        
        for item in labels:
            item = item.strip()
            if not item:
                continue
                
            # Map Dutch labels to English
            translated = item
            
            # Common translations
            translations = {
                "Jongerenwoning": "Youth Housing",
                "Alleen voor gezinnen": "Families Only",
                "Met situatiepunten": "With Situation Points",
                "Voorrang kleine gezinnen": "Priority for Small Families",
                "Vrije sector": "Free Sector",
                "Parkeren": "Parking"
            }
            
            # Look for match in translations dictionary
            for dutch, english in translations.items():
                if dutch in item:
                    translated = english
                    break
            
            # Add as feature
            features.append({"publication_label": translated})
            
        return features
    
    def _translate_module(self, module: str) -> str:
        """
        Translate PublicatieModule into English
        
        Args:
            module: Original Dutch publication module
            
        Returns:
            Translated module name
        """
        if not module:
            return ""
            
        translations = {
            "Sociale huur": "Social Housing",
            "Vrije sector": "Free Sector",
            "Koopwoning": "For Sale"
        }
        
        return translations.get(module, module)
    
    def _translate_contract_form(self, contract_form: str) -> str:
        """
        Translate ContractVorm into English
        
        Args:
            contract_form: Original Dutch contract form
            
        Returns:
            Translated contract form
        """
        if not contract_form:
            return ""
            
        translations = {
            "Jongerencontract": "Youth Contract",
            "Onbepaalde tijd contract": "Indefinite Contract"
        }
        
        return translations.get(contract_form, contract_form)
    
    def _translate_detail_soort(self, detail_soort: str) -> str:
        """
        Translate DetailSoort into English
        
        Args:
            detail_soort: Original Dutch property type detail
            
        Returns:
            Translated property type detail
        """
        if not detail_soort:
            return ""
            
        translations = {
            "Portiekflat": "Apartment Building",
            "Galerijflat": "Gallery Flat",
            "Benedenwoning": "Ground Floor Apartment",
            "Bovenwoning": "Upper Floor Apartment",
            "Hoekwoning": "Corner House",
            "Tussenwoning": "Terraced House",
            "Portiekwoning": "Entrance Apartment",
            "Maisonnette": "Maisonette",
            "Eengezinswoning": "Single-Family Home"
        }
        
        return translations.get(detail_soort, detail_soort)
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse ISO date string to formatted date
        
        Args:
            date_str: ISO date string
            
        Returns:
            Formatted date string or None
        """
        if not date_str or date_str == "1900-01-01T00:00:00":
            return None
            
        try:
            # Parse ISO format date
            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            # Format as readable date
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            logger.error(f"Could not parse date: {date_str}")
            return None
    
    def _extract_energy_label(self, label: str) -> Optional[str]:
        """
        Extract standardized energy label
        
        Args:
            label: Energy label string
            
        Returns:
            Standardized energy label string or None
        """
        if not label:
            return None
            
        # Extract basic labels (A-G)
        if re.match(r'^[A-G](\+{1,3})?$', label):
            return label
            
        return None
    
    def _parse_iso_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse ISO date string to datetime object
        
        Args:
            date_str: ISO date string
            
        Returns:
            datetime object or None
        """
        if not date_str:
            return None
            
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            return None
    
    async def parse_search_page(self, response: str) -> List[PropertyListing]:
        """
        Parse WoningNet JSON response and extract property listings
        
        Args:
            response: JSON response string
            
        Returns:
            List of PropertyListing objects
        """
        listings = []
        
        try:
            # Parse JSON response
            data = json.loads(response)
            
            # Extract properties list
            property_list = data.get("data", {}).get("PublicatieLijst", {}).get("List", [])
            
            if not property_list:
                logger.warning("No properties found in WoningNet response")
                return []
                
            # Sort property list by publication date (newest first)
            # Convert the date strings to datetime objects for sorting
            sorted_properties = sorted(
                property_list,
                key=lambda x: self._parse_iso_date(x.get("PublicatieDatum", "")) or datetime.min,
                reverse=True  # Descending order (newest first)
            )
            
            # Process each property
            for item in sorted_properties:
                try:
                    # Skip parking spots
                    if "Parkeren" in item.get("PublicatieLabel", ""):
                        continue
                        
                    # Skip if not a residential property
                    if item.get("EenheidSoort") != "Woonruimte":
                        continue
                    
                    # Address information
                    address_data = item.get("Adres", {})
                    street = address_data.get("Straatnaam", "")
                    
                    # Skip listings without a street address
                    if not street:
                        logger.warning(f"Skipping listing {item.get('Id')} - missing address")
                        continue
                    
                    # Create a new property listing
                    listing = PropertyListing(source=self.config.get("source_name"))
                    
                    # Basic identification
                    listing.source_id = item.get("Id")
                    
                    # Construct URL (would need actual URL structure)
                    listing.url = f"https://amsterdam.mijndak.nl/HuisDetails?PublicatieId={listing.source_id}"
                    
                    house_number = address_data.get("Huisnummer", "")
                    house_letter = address_data.get("Huisletter", "")
                    house_addition = address_data.get("HuisnummerToevoeging", "")
                    
                    # Combine address components
                    address_parts = [street]
                    if house_number:
                        address_parts.append(str(house_number))
                    if house_letter:
                        address_parts.append(house_letter)
                    if house_addition:
                        address_parts.append(house_addition)
                    
                    listing.address = " ".join(filter(None, address_parts))

                    postal_code = address_data.get("Postcode")
                    if postal_code:
                        # Insert a space between numbers and letters in the postal code
                        formatted_postal_code = re.sub(r'(\d+)([A-Za-z]+)', r'\1 \2', postal_code)
                        listing.postal_code = formatted_postal_code
                    else:
                        listing.postal_code = ""

                    city = address_data.get("Woonplaats")
                    if city:
                        listing.city = city.upper()
                    listing.neighborhood = address_data.get("Wijk")
                    
                    # Create title from address and city
                    if listing.address and listing.city:
                        listing.title = f"{listing.address}, {listing.city}"
                    
                    # Property details
                    eenheid_data = item.get("Eenheid", {})
                    detail_soort = eenheid_data.get("DetailSoort")
                    
                    # Map property type
                    listing.property_type = self._map_property_type(
                        detail_soort, 
                        item.get("EenheidSoort", "")
                    )
                    
                    # Add property type detail as feature
                    if detail_soort:
                        translated_detail = self._translate_detail_soort(detail_soort)
                        if translated_detail:
                            if not listing.features:
                                listing.features = {}
                            listing.features["property_type_detail"] = translated_detail
                    
                    # Extract rooms
                    rooms = eenheid_data.get("AantalKamers")
                    if rooms > 0:
                        listing.rooms = rooms
                    
                    # Extract size
                    if eenheid_data.get("WoonVertrekkenTotOpp") and float(eenheid_data.get("WoonVertrekkenTotOpp", 0)) > 0:
                        try:
                            listing.living_area = int(float(eenheid_data.get("WoonVertrekkenTotOpp", 0)))
                        except (ValueError, TypeError):
                            pass
                    elif item.get("Cluster"):
                        cluster_data = item.get("Cluster", {})
                        
                        # Try WoonVertrekkenTotOppMin if available and WoonOppervlakteMinBekend is true
                        if cluster_data.get("WoonOppervlakteMinBekend") and cluster_data.get("WoonVertrekkenTotOppMin"):
                            try:
                                listing.living_area = int(float(cluster_data.get("WoonVertrekkenTotOppMin")))
                            except (ValueError, TypeError):
                                pass
                        # If Min not available, try WoonVertrekkenTotOppMax if WoonOppervlakteMaxBekend is true
                        elif cluster_data.get("WoonOppervlakteMaxBekend") and cluster_data.get("WoonVertrekkenTotOppMax"):
                            try:
                                listing.living_area = int(float(cluster_data.get("WoonVertrekkenTotOppMax")))
                            except (ValueError, TypeError):
                                pass
                    
                    # Extract price information
                    # Check if NettoHuurBekend is True and NettoHuur is available
                    if eenheid_data.get("NettoHuurBekend") and eenheid_data.get("NettoHuur"):
                        listing.price = f"€{eenheid_data.get('NettoHuur')}"
                        try:
                            listing.price_numeric = int(float(eenheid_data.get("NettoHuur", 0)))
                        except (ValueError, TypeError):
                            pass
                    else:
                        # Fallback to PrijsMin or PrijsMax from Cluster data
                        cluster_data = item.get("Cluster", {})
                        
                        # Check PrijsMinBekend and PrijsMin
                        if cluster_data.get("PrijsMinBekend") and cluster_data.get("PrijsMin"):
                            try:
                                price_min = float(cluster_data.get("PrijsMin"))
                                listing.price = f"€{price_min}"
                                listing.price_numeric = int(price_min)
                            except (ValueError, TypeError):
                                pass
                        # If PrijsMin is not available, check PrijsMaxBekend and PrijsMax
                        elif cluster_data.get("PrijsMaxBekend") and cluster_data.get("PrijsMax"):
                            try:
                                price_max = float(cluster_data.get("PrijsMax"))
                                listing.price = f"€{price_max}"
                                listing.price_numeric = int(price_max)
                            except (ValueError, TypeError):
                                pass
                    
                    # Skip if we still don't have a price
                    if not listing.price_numeric:
                        logger.warning(f"Skipping listing {listing.source_id} - no price information")
                        continue
                    
                    # Always monthly rent
                    listing.price_period = "month"
                    
                    # Extract service costs if available
                    bruto_huur = eenheid_data.get("Brutohuur")
                    netto_huur = eenheid_data.get("NettoHuur")
                    if bruto_huur and netto_huur:
                        try:
                            bruto = float(bruto_huur)
                            netto = float(netto_huur)
                            if bruto > netto:
                                listing.service_costs = round(bruto - netto, 2)
                        except (ValueError, TypeError):
                            pass
                    
                    # Extract energy label
                    energy_label = eenheid_data.get("EnergieLabel")
                    listing.energy_label = self._extract_energy_label(energy_label)
                    
                    # Extract dates
                    listing.date_listed = self._parse_date(item.get("PublicatieDatum"))
                    listing.date_available = self._parse_date(item.get("Opleverdatum"))
                    
                    # Set offering type
                    listing.offering_type = OfferingType.RENTAL
                    
                    # Extract images
                    if item.get("Foto_Locatie"):
                        listing.images = [item.get("Foto_Locatie")]
                    
                    # Extract features
                    features = []
                    
                    # Include publication label
                    label_features = self._translate_publication_label(item.get("PublicatieLabel", ""))
                    if label_features:
                        features.extend(label_features)
                    
                    # Include publication module as feature
                    module = self._translate_module(item.get("PublicatieModule", ""))
                    if module:
                        features.append({"publication_module": module})
                    
                    # Include contract type
                    contract_form = self._translate_contract_form(item.get("ContractVorm", ""))
                    if contract_form:
                        features.append({"contract_type": contract_form})
                    
                    # Include publication model
                    pub_model = item.get("PublicatieModel", "")
                    if pub_model:
                        features.append({"publication_model": pub_model})
                    
                    # Include lift information
                    has_lift = item.get("HeeftLift")
                    if has_lift is not None:
                        features.append({"has_lift": "Yes" if has_lift else "No"})
                    
                    # Include floor information
                    floor = item.get("Verdieping")
                    if floor and floor != "Niet bekend":
                        try:
                            listing.floors = int(floor)
                            features.append({"floor": floor})
                        except ValueError:
                            features.append({"floor": floor})
                    
                    # Include target group
                    doelgroep = eenheid_data.get("Doelgroep")
                    if doelgroep:
                        target_group_mapping = {
                            "Jongeren": "Youth",
                            "Gezin": "Families",
                            "Senioren": "Seniors",
                            "Persoon": "Single"
                        }
                        translated_group = target_group_mapping.get(doelgroep, doelgroep)
                        features.append({"target_group": translated_group})
                    
                    # Add all features to listing
                    if features:
                        listing.features = []
                        for feature in features:
                            for key, value in feature.items():
                                listing.features.append({key: value})
                    
                    # Generate property hash
                    listing.property_hash = self._generate_property_hash(listing)
                    
                    # Add the listing to the results
                    listings.append(listing)
                    
                except Exception as e:
                    logger.error(f"Error processing WoningNet listing: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {len(listings)} listings from WoningNet response")
            
        except json.JSONDecodeError:
            logger.error("Invalid JSON response from WoningNet")
        except Exception as e:
            logger.error(f"Error parsing WoningNet response: {str(e)}")
        
        return listings
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """
        Parse individual listing page - not implemented as all data comes from search API
        
        This is a placeholder method to conform to the BaseScraperStrategy interface
        """
        # Create a minimal listing with just URL and source
        listing = PropertyListing(source=self.config.get("source_name"), url=url)
        
        # Extract source ID from URL if possible
        url_match = re.search(r'PublicatieId=(\d+)', url)
        if url_match:
            listing.source_id = url_match.group(1)
        else:
            listing.source_id = str(uuid.uuid4())  # Generate a random ID if not found
        
        # Generate property hash
        listing.property_hash = self._generate_property_hash(listing)
        
        return listing