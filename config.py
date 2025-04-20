#!/usr/bin/env python
"""
Configuration for Dutch Real Estate Scraper

This file contains configuration settings that can be adjusted without modifying 
the core scraper code.
"""

import os
from typing import Dict, Any, List
from dotenv import load_dotenv
import random 

# Load environment variables from .env file if present
load_dotenv()

# Telegram bot settings
ENABLE_TELEGRAM = os.getenv("ENABLE_TELEGRAM", "True").lower() == "true"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Parse admin user IDs from environment
_admin_ids_str = os.getenv("TELEGRAM_ADMIN_USER_IDS", "")
TELEGRAM_ADMIN_USER_IDS = [int(uid.strip()) for uid in _admin_ids_str.split(",") if uid.strip().isdigit()]

# Notification settings
NOTIFICATION_INTERVAL = int(os.getenv("NOTIFICATION_INTERVAL", "300"))  # 5 minutes in seconds
MAX_NOTIFICATIONS_PER_USER_PER_DAY = int(os.getenv("MAX_NOTIFICATIONS_PER_USER_PER_DAY", "20"))
NOTIFICATION_BATCH_SIZE = int(os.getenv("NOTIFICATION_BATCH_SIZE", "50"))
NOTIFICATION_RETRY_ATTEMPTS = int(os.getenv("NOTIFICATION_RETRY_ATTEMPTS", "3"))

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


# Default scan settings
DEFAULT_SCAN_INTERVAL = int(os.getenv("DEFAULT_SCAN_INTERVAL", "3600"))  # 1 hour in seconds
MAX_RESULTS_PER_SCAN = int(os.getenv("MAX_RESULTS_PER_SCAN", "100"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30.0"))  # seconds
STOP_AFTER_NO_RESULT = os.getenv("STOP_AFTER_NO_RESULT", "True").lower() == "true"

# Default cities to scrape if none specified
DEFAULT_CITIES = os.getenv("DEFAULT_CITIES", "amsterdam,rotterdam,utrecht,den-haag,eindhoven").split(",")

# Default sources to scrape if none specified
DEFAULT_SOURCES = os.getenv("DEFAULT_SOURCES", "funda,pararius").split(",")

# Site-specific configurations
SITE_CONFIGS = {
    "funda": {
        "base_url": "https://www.funda.nl",
        "search_url_template": "https://www.funda.nl/zoeken/huur?selected_area=[\"{city}\"]&object_type=[\"house\",\"apartment\"]&sort=\"date_down\"",
        "min_interval": 10,  # TODO
    },
    "pararius": {
        "base_url": "https://www.pararius.com",
        "search_url_template": "https://www.pararius.com/apartments/{city}",
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