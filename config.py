#!/usr/bin/env python
"""
Configuration for Dutch Real Estate Scraper

This file contains configuration settings that can be adjusted without modifying 
the core scraper code.
"""

import os
from typing import Dict, Any, List
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
]

# Default scan settings
DEFAULT_SCAN_INTERVAL = int(os.getenv("DEFAULT_SCAN_INTERVAL", "3600"))  # 1 hour in seconds
MAX_RESULTS_PER_SCAN = int(os.getenv("MAX_RESULTS_PER_SCAN", "100"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30.0"))  # seconds

# Default cities to scrape if none specified
DEFAULT_CITIES = os.getenv("DEFAULT_CITIES", "amsterdam,rotterdam,utrecht,den-haag,eindhoven").split(",")

# Default sources to scrape if none specified
DEFAULT_SOURCES = os.getenv("DEFAULT_SOURCES", "funda,pararius").split(",")

# Site-specific configurations
SITE_CONFIGS = {
    "funda": {
        "base_url": "https://www.funda.nl",
        "search_url_template": "https://www.funda.nl/en/zoeken/huur/?selected_area=[\"{city}\"]&publication_date=\"{days}\"&sort=\"date_down\"",
        "listing_selector": ".search-result-content",
        "min_interval": 10,  # TODO
    },
    "pararius": {
        "base_url": "https://www.pararius.com",
        "search_url_template": "https://www.pararius.com/apartments/{city}",
        "listing_selector": ".listing-search-item",
        "min_interval": 10,  # TODO
    }
}

# Proxy settings
USE_PROXIES = os.getenv("USE_PROXIES", "False").lower() == "true"
PROXY_ROTATION_STRATEGY = os.getenv("PROXY_ROTATION_STRATEGY", "round_robin")  # Options: round_robin, random, fallback
MAX_PROXY_FAILURES = int(os.getenv("MAX_PROXY_FAILURES", "3"))

# Parse proxy list from environment
_proxy_list_str = os.getenv("PROXY_LIST", "")
PROXY_LIST = [p.strip() for p in _proxy_list_str.split(",") if p.strip()]

# Optional proxy provider settings
PROXY_PROVIDER = os.getenv("PROXY_PROVIDER", "")  # e.g., "luminati", "smartproxy", "brightdata"
PROXY_USERNAME = os.getenv("PROXY_USERNAME", "")
PROXY_PASSWORD = os.getenv("PROXY_PASSWORD", "")
PROXY_API_KEY = os.getenv("PROXY_API_KEY", "")
PROXY_API_ENDPOINT = os.getenv("PROXY_API_ENDPOINT", "")

# Function to format proxy URLs with credentials if provided
def get_formatted_proxy_list() -> List[str]:
    """Format proxy URLs with credentials if needed"""
    formatted_list = []
    
    # If specific provider credentials are given, use the appropriate format
    if PROXY_PROVIDER and PROXY_USERNAME and PROXY_PASSWORD:
        for proxy in PROXY_LIST:
            # Remove any existing authentication in the URL
            parts = proxy.split("@")
            host_part = parts[-1]
            
            if PROXY_PROVIDER.lower() == "luminati":
                # Format: http://username-session-{ip}:password@zproxy.lum-superproxy.io:22225
                formatted = f"http://{PROXY_USERNAME}-session-{random.randint(10000, 99999)}:{PROXY_PASSWORD}@{host_part}"
            else:
                # Default format: http://username:password@host:port
                formatted = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{host_part}"
            
            formatted_list.append(formatted)
    else:
        # Use proxy list as is
        formatted_list = PROXY_LIST
    
    return formatted_list

# Override any config settings with environment variables
def update_site_config_from_env():
    """Update site configurations from environment variables if present"""
    for site in SITE_CONFIGS:
        site_prefix = f"SITE_{site.upper()}_"
        for key in SITE_CONFIGS[site]:
            env_key = f"{site_prefix}{key.upper()}"
            if os.getenv(env_key):
                # Handle different types
                if isinstance(SITE_CONFIGS[site][key], int):
                    SITE_CONFIGS[site][key] = int(os.getenv(env_key))
                elif isinstance(SITE_CONFIGS[site][key], float):
                    SITE_CONFIGS[site][key] = float(os.getenv(env_key))
                elif isinstance(SITE_CONFIGS[site][key], bool):
                    SITE_CONFIGS[site][key] = os.getenv(env_key).lower() == "true"
                else:
                    SITE_CONFIGS[site][key] = os.getenv(env_key)

# Call the update function to apply environment variable overrides
update_site_config_from_env()