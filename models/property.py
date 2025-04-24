"""
Property data models for the real estate scraper.
"""

import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional, Any


class OfferingType(str, Enum):
    """Type of property offering (rent or sale)"""
    RENTAL = "rental"
    SALE = "sale"


class PropertyType(str, Enum):
    """Type of property"""
    APARTMENT = "APARTMENT"
    HOUSE = "HOUSE"
    ROOM = "ROOM"
    STUDIO = "STUDIO"


class InteriorType(str, Enum):
    """Type of interior furnishing"""
    SHELL = "shell"
    UPHOLSTERED = "upholstered"
    FURNISHED = "furnished"


@dataclass
class PropertyListing:
    """Unified property listing model for all sources"""
    # Source identification
    source: str  # "funda", "pararius", etc.
    source_id: Optional[str] = None  # ID in the source system
    url: Optional[str] = None
    
    # Basic info
    title: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    
    # Price information
    price: Optional[str] = None
    price_numeric: Optional[int] = None
    price_period: Optional[str] = None  # "month", "week"
    service_costs: Optional[float] = None
    
    # Property details
    description: Optional[str] = None
    property_type: Optional[PropertyType] = None
    offering_type: Optional[OfferingType] = OfferingType.RENTAL
    living_area: Optional[int] = None  # in m²
    plot_area: Optional[int] = None  # in m²
    volume: Optional[int] = None  # in m³
    rooms: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    floors: Optional[int] = None
    balcony: Optional[bool] = None
    garden: Optional[bool] = None
    parking: Optional[bool] = None
    construction_year: Optional[int] = None
    energy_label: Optional[str] = None
    interior: Optional[InteriorType] = None
    
    # Dates
    date_listed: Optional[str] = None
    date_available: Optional[str] = None
    availability_period: Optional[str] = None
    date_scraped: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Media
    images: List[str] = field(default_factory=list)
    
    # Raw features data
    features: Dict[str, Dict[str, str]] = field(default_factory=dict)
    
    # Hash for deduplication
    property_hash: Optional[str] = None
    
    def generate_property_hash(self):
        """Generate a property hash for deduplication"""
        if not self.property_hash:
            # Create a hash based on key attributes to detect duplicates across platforms
            hash_input = f"{self.url}|{self.address}|{self.source_id}|{self.city}"
            self.property_hash = hashlib.md5(hash_input.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PropertyListing':
        """Create instance from dictionary"""
        return cls(**data)