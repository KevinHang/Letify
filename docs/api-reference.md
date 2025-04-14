# API Reference

This document provides a detailed reference of the Dutch Real Estate Scraper's API for developers who want to understand or extend the system.

## Table of Contents

- [Core Classes](#core-classes)
- [Models](#models)
- [Scrapers](#scrapers)
- [Database](#database)
- [Utilities](#utilities)
- [Configuration](#configuration)

## Core Classes

### RealEstateScraper

The main class that orchestrates the scraping process. Located in `main.py`.

```python
class RealEstateScraper:
    def __init__(self, 
                 sources: List[str], 
                 cities: List[str], 
                 db_connection_string: str = DB_CONNECTION_STRING,
                 interval: int = DEFAULT_SCAN_INTERVAL,
                 max_results_per_scan: int = MAX_RESULTS_PER_SCAN,
                 max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
                 use_proxies: bool = USE_PROXIES)
```

**Key Methods:**

- `async def scan_source_city(self, source: str, city: str, days: int = 1) -> Tuple[int, int]`
  - Scans a specific source and city for new listings
  - Returns a tuple of (new listings count, total listings count)

- `async def run_one_scan(self) -> Tuple[int, int]`
  - Runs a single scan of all sources and cities
  - Returns a tuple of (total new listings, total processed listings)

- `async def detect_duplicates(self) -> None`
  - Detects and records potential duplicate listings across sources

- `async def run_continuous(self, stop_event=None) -> None`
  - Runs the scraper continuously with the configured interval
  - Can be stopped by setting the stop_event

## Models

### PropertyListing

The main data model for property listings. Located in `models/property.py`.

```python
@dataclass
class PropertyListing:
    # Source identification
    source: str  # "funda", "pararius", etc.
    source_id: Optional[str] = None
    url: Optional[str] = None
    
    # Basic info
    title: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    
    # Price information
    price: Optional[str] = None
    price_numeric: Optional[float] = None
    price_period: Optional[str] = None  # "month", "week"
    service_costs: Optional[float] = None
    
    # Property details
    description: Optional[str] = None
    property_type: Optional[PropertyType] = None
    offering_type: Optional[OfferingType] = OfferingType.RENTAL
    living_area: Optional[int] = None  # in m²
    # ... additional fields ...
```

**Key Methods:**

- `__post_init__(self) -> None`
  - Generates a property hash for deduplication based on key attributes

- `to_dict(self) -> Dict[str, Any]`
  - Converts the listing to a dictionary representation

- `from_dict(cls, data: Dict[str, Any]) -> 'PropertyListing'`
  - Creates a PropertyListing instance from a dictionary

### Enums

Located in `models/property.py`.

```python
class OfferingType(str, Enum):
    RENTAL = "rental"
    SALE = "sale"

class PropertyType(str, Enum):
    APARTMENT = "apartment"
    HOUSE = "house"
    ROOM = "room"
    STUDIO = "studio"

class InteriorType(str, Enum):
    SHELL = "shell"
    UPHOLSTERED = "upholstered"
    FURNISHED = "furnished"
```

### ScanHistory

Located in `models/scan_history.py`.

```python
@dataclass
class ScanHistory:
    source: str
    city: str
    url: str
    scan_time: datetime = datetime.now()
    new_listings_count: int = 0
    total_listings_count: int = 0
    scan_duration_seconds: float = 0.0
    id: Optional[int] = None
```

## Scrapers

### BaseScraperStrategy

The abstract base class for all site-specific scrapers. Located in `scrapers/base.py`.

```python
class BaseScraperStrategy(ABC):
    def __init__(self, site_name: str, config: Dict[str, Any]):
        self.site_name = site_name
        self.config = config
        self.base_url = config["base_url"]
        self.search_url_template = config["search_url_template"]
        self.listing_selector = config["listing_selector"]
    
    @abstractmethod
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for the given parameters"""
        pass
    
    @abstractmethod
    async def parse_search_page(self, html: str) -> List[str]:
        """Parse the search results page and extract listing URLs"""
        pass
    
    @abstractmethod
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """Parse a listing detail page and extract property information"""
        pass
```

### RealEstateScraperFactory

Factory class to create scrapers. Located in `scrapers/factory.py`.

```python
class RealEstateScraperFactory:
    @staticmethod
    def create_scraper(site_name: str) -> BaseScraperStrategy:
        """Create a scraper for the given site"""
        # ...
    
    @staticmethod
    def get_available_scrapers() -> Dict[str, Dict[str, Any]]:
        """Return a dictionary of available scrapers and their configs"""
        # ...
```

### Site-Specific Scrapers

- `FundaScraper` (in `scrapers/funda.py`)
- `ParariusScraper` (in `scrapers/pararius.py`)

Each implements the three abstract methods from `BaseScraperStrategy` with site-specific parsing logic.

## Database

### PropertyDatabase

Main database handler. Located in `database/property_db.py`.

```python
class PropertyDatabase:
    def __init__(self, connection_string: str):
        """Initialize database connection"""
        # ...
    
    def save_listing(self, listing: PropertyListing) -> bool:
        """Save a property listing to the database. Returns True if new, False if updated."""
        # ...
    
    def update_scan_history(self, source: str, city: str, url: str, new_count: int, total_count: int, duration: float):
        """Update the scan history for a source and city."""
        # ...
    
    def get_last_scan_time(self, source: str, city: str) -> Optional[datetime]:
        """Get the last scan time for a source and city."""
        # ...
    
    def search_properties(self, 
                          city: Optional[str] = None,
                          min_price: Optional[float] = None,
                          max_price: Optional[float] = None,
                          # ... additional filters ...
                          ) -> List[Dict[str, Any]]:
        """Search for properties with specified filters."""
        # ...
    
    def search_properties_by_location(self, 
                                      lat: float, 
                                      lng: float, 
                                      radius_km: float = 1.0,
                                      limit: int = 100) -> List[Dict[str, Any]]:
        """Search for properties within a radius of a geographic point."""
        # ...
    
    def find_potential_duplicates(self, threshold: float = 0.8) -> List[Dict[str, Any]]:
        """Find potential duplicate properties across different sources."""
        # ...
    
    def record_duplicate_pair(self, source_1: str, source_id_1: str, 
                            source_2: str, source_id_2: str, 
                            property_hash: str, similarity_score: float):
        """Record a duplicate property pair in the database."""
        # ...
```

### Database Connection Management

Located in `database/connection.py`.

```python
def get_connection(connection_string: str):
    """Create and return a database connection."""
    # ...

def close_connection(conn):
    """Close a database connection."""
    # ...
```

### Database Migrations

Located in `database/migrations.py`.

```python
def initialize_db(connection_string: str):
    """Create tables and indexes if they don't exist."""
    # ...
```

## Utilities

### HttpClient

HTTP client with retry logic. Located in `utils/http.py`.

```python
class HttpClient:
    def __init__(self, 
                 timeout: float = HTTP_TIMEOUT,
                 max_retries: int = 3, 
                 retry_min_wait: int = 1,
                 retry_max_wait: int = 10,
                 semaphore: Optional[asyncio.Semaphore] = None,
                 use_proxies: bool = USE_PROXIES,
                 proxy_list: Optional[List[str]] = None):
        """Initialize the HTTP client"""
        # ...
    
    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP GET request with retries and optional proxy"""
        # ...
    
    async def get_with_fallback(self, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP GET request with proxy first, then fall back to direct connection if proxy fails"""
        # ...
```

### ProxyManager

Proxy management. Located in `utils/proxy_manager.py`.

```python
class ProxyManager:
    def __init__(self, 
                 enabled: bool = USE_PROXIES,
                 proxy_list: Optional[List[str]] = None,
                 rotation_strategy: str = PROXY_ROTATION_STRATEGY,
                 max_failures: int = MAX_PROXY_FAILURES):
        """Initialize the proxy manager"""
        # ...
    
    async def get_proxy(self) -> Optional[str]:
        """Get a proxy according to the rotation strategy"""
        # ...
    
    async def report_success(self, proxy: str, response_time: float) -> None:
        """Report a successful proxy usage"""
        # ...
    
    async def report_failure(self, proxy: str, error: Optional[Exception] = None) -> None:
        """Report a failed proxy usage"""
        # ...
    
    # ... additional methods ...
```

### Parsing Utilities

Located in `utils/parsing.py`.

```python
def safe_extract_text(node: Optional[Node]) -> str:
    """Extract text from a node safely, handling None values"""
    # ...

def safe_get_attribute(node: Optional[Node], attribute: str) -> Optional[str]:
    """Get an attribute from a node safely, handling None values"""
    # ...

def extract_number(text: str, pattern: str = r'\d+') -> Optional[int]:
    """Extract a number from text using a regex pattern"""
    # ...

def extract_price(text: str) -> Optional[float]:
    """Extract a price value from text, handling different formats"""
    # ...

# ... additional utility functions ...
```

## Configuration

### Config Module

Located in `config.py`.

```python
# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "realestate"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

# Construct database connection string
DB_CONNECTION_STRING = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# User agents to rotate for requests
USER_AGENTS = [
    # ... list of user agents ...
]

# Default scan settings
DEFAULT_SCAN_INTERVAL = int(os.getenv("DEFAULT_SCAN_INTERVAL", "3600"))
MAX_RESULTS_PER_SCAN = int(os.getenv("MAX_RESULTS_PER_SCAN", "100"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30.0"))

# Default cities to scrape if none specified
DEFAULT_CITIES = os.getenv("DEFAULT_CITIES", "amsterdam,rotterdam,utrecht,den-haag,eindhoven").split(",")

# Default sources to scrape if none specified
DEFAULT_SOURCES = os.getenv("DEFAULT_SOURCES", "funda,pararius").split(",")

# Site-specific configurations
SITE_CONFIGS = {
    "funda": {
        # ... site-specific configuration ...
    },
    "pararius": {
        # ... site-specific configuration ...
    }
}

# Proxy settings
USE_PROXIES = os.getenv("USE_PROXIES", "False").lower() == "true"
PROXY_ROTATION_STRATEGY = os.getenv("PROXY_ROTATION_STRATEGY", "round_robin")
MAX_PROXY_FAILURES = int(os.getenv("MAX_PROXY_FAILURES", "3"))
PROXY_LIST = [p.strip() for p in os.getenv("PROXY_LIST", "").split(",") if p.strip()]

# Additional configuration functions
def get_formatted_proxy_list() -> List[str]:
    """Format proxy URLs with credentials if needed"""
    # ...

def update_site_config_from_env():
    """Update site configurations from environment variables if present"""
    # ...
```

## Command-Line Interface

### CLI Module

Located in `cli.py`.

```python
def configure_logging(log_level=logging.INFO):
    """Configure logging for the application."""
    # ...

def parse_args():
    """Parse command line arguments."""
    # Creates an ArgumentParser with all available options
    # ...

async def main():
    """Main entry point for the CLI."""
    # Parses args, sets up logging, initializes and runs the scraper
    # ...
```

---

This API reference provides an overview of the key classes, methods, and modules in the Dutch Real Estate Scraper. For more detailed information, refer to the source code and docstrings within each module.