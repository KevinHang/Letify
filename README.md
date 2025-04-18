# Dutch Real Estate Scraper - CLI Reference

## Overview

The Dutch Real Estate Scraper is a configurable tool for scraping real estate listings from popular Dutch property websites. This document provides a complete reference for the command-line interface.

## Installation

Ensure you have Python 3.8+ installed, then:

```bash
# Clone the repository
git clone https://github.com/yourusername/dutch-real-estate-scraper.git
cd dutch-real-estate-scraper

# Install dependencies
pip install -r requirements.txt
```

## Basic Usage

### Scan Modes

The scraper supports three scanning modes:

```bash
# City-based scanning (extracts listings from city search pages)
python -m cli --city-scan --sources funda,pararius --cities amsterdam,rotterdam

# Query URL scanning (uses URLs stored in the database)
python -m cli --query-scan --sources funda,pararius

# Combined scanning (both cities and query URLs)
python -m cli --combined-scan --sources funda,pararius --cities amsterdam,rotterdam
```

Default behavior (if no mode specified):
```bash
# Defaults to city-based scanning when cities are provided
python -m cli --sources funda,pararius --cities amsterdam,rotterdam
```

### Running Options

```bash
# Run once and exit
python -m cli --city-scan --sources funda,pararius --cities amsterdam,rotterdam --once

# Run continuously with a specific interval (in seconds)
python -m cli --city-scan --sources funda,pararius --cities amsterdam,rotterdam --interval 3600
```

## Query URL Management

Query URLs allow you to scan specific search results pages, offering more flexibility than city-based scanning.

### Adding Query URLs

```bash
# Add a query URL (enabled by default)
python -m cli --add-query-url "funda:https://www.funda.nl/zoeken/huur?object_type=[\"apartment\"]&sort=\"date_down\"" --add-query-description "Amsterdam apartments"

# Add with POST method instead of GET
python -m cli --add-query-url "funda:https://www.funda.nl/zoeken/huur?object_type=[\"apartment\"]" --query-method POST

# Add in disabled state
python -m cli --add-query-url "funda:https://www.funda.nl/zoeken/huur?object_type=[\"apartment\"]" --disable
```

Special URL parameters should be escaped with backslashes before the double quotes.

### Managing Query URLs

```bash
# List all query URLs
python -m cli --list-query-urls

# Toggle the enabled status of a query URL (enable if disabled, disable if enabled)
python -m cli --toggle-query-url 1

# Delete a query URL
python -m cli --delete-query-url 1
```

## Additional Options

### Sources and Limits

```bash
# List available sources
python -m cli --list-sources

# Specify custom result limits
python -m cli --max-results 200 --max-concurrent 10
```

### Proxy Configuration

```bash
# Enable proxy usage
python -m cli --use-proxies

# Disable proxy usage
python -m cli --no-proxies

# Use specific proxies
python -m cli --proxy-list "http://proxy1.example.com,http://proxy2.example.com"

# Set proxy rotation strategy
python -m cli --proxy-rotation round_robin  # Options: round_robin, random, fallback

# Display proxy statistics after scanning
python -m cli --proxy-stats
```

### Debug Mode

```bash
# Enable debug logging
python -m cli --debug
```

## Environmental Configuration

Many settings can be configured through environment variables or a `.env` file:

```
# Database settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=realestate
DB_USER=postgres
DB_PASSWORD=postgres

# Default settings
DEFAULT_SCAN_INTERVAL=3600
DEFAULT_CITIES=amsterdam,rotterdam,utrecht,den-haag,eindhoven
DEFAULT_SOURCES=funda,pararius
MAX_RESULTS_PER_SCAN=100
MAX_CONCURRENT_REQUESTS=5

# Proxy settings
USE_PROXIES=False
PROXY_LIST=http://proxy1.example.com,http://proxy2.example.com
PROXY_ROTATION_STRATEGY=round_robin
```

## Command Reference

| Option | Description |
|--------|-------------|
| `--city-scan` | City-based scanning mode |
| `--query-scan` | Query URL scanning mode |
| `--combined-scan` | Combined scanning mode |
| `--sources SOURCES` | Comma-separated list of sources to scrape |
| `--cities CITIES` | Comma-separated list of cities to scrape |
| `--interval INTERVAL` | Scraping interval in seconds |
| `--once` | Run only one scan cycle |
| `--add-query-url URL` | Add a query URL (format: source:url) |
| `--query-method {GET,POST}` | HTTP method for the query URL |
| `--disable` | Add the query URL in disabled state |
| `--add-query-description DESC` | Description for the query URL |
| `--list-query-urls` | List all query URLs in the database |
| `--toggle-query-url ID` | Toggle the enabled status of a query URL |
| `--delete-query-url ID` | Delete a query URL |
| `--use-proxies` | Enable proxy usage |
| `--no-proxies` | Disable proxy usage |
| `--proxy-list PROXIES` | Comma-separated list of proxy URLs |
| `--proxy-rotation STRATEGY` | Proxy rotation strategy |
| `--proxy-stats` | Display proxy statistics after scanning |
| `--max-results LIMIT` | Maximum results to process per scan |
| `--max-concurrent LIMIT` | Maximum concurrent requests |
| `--debug` | Enable debug logging |
| `--list-sources` | List available sources |