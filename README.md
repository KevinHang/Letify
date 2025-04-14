# Dutch Real Estate Scraper

A flexible, modular system for scraping multiple Dutch real estate websites and storing listing data in a unified database.

## Features

- **Multi-Source Scraping**: Support for multiple real estate websites (Funda, Pararius, and easily expandable)
- **Multi-City Support**: Scrape properties from any number of cities
- **Unified Database**: All properties stored in a single PostgreSQL database with efficient indexing
- **Deduplication**: Cross-site property detection to identify the same property listed on multiple sites
- **Configurable Scheduling**: Different scan intervals for each source
- **Geospatial Search**: Find properties near specific locations
- **Modular Architecture**: Easy to extend with new real estate sites
- **Optional Proxy Support**: IP rotation to avoid rate limiting

## Quick Start

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/dutch-realestate-scraper.git
   cd dutch-realestate-scraper
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set up the PostgreSQL database:
   ```bash
   brew services start postgresql
   createdb realestate
   psql realestate -c "CREATE EXTENSION postgis; CREATE EXTENSION vector; CREATE EXTENSION fuzzystrmatch;"
   ```

4. Configure your environment:
   ```bash
   cp .env.template .env
   # Edit .env with your database credentials
   ```

### Basic Usage

Run a single scan:

```bash
python -m cli --once
```

Run continuous scanning:

```bash
python -m cli --interval 3600  # Scan every hour
```

Specify sources and cities:

```bash
python -m cli --sources funda,pararius --cities amsterdam,rotterdam
```

## Documentation

- [Getting Started Guide](docs/getting-started-guide.md): Comprehensive setup and usage guide
- [Multi-Site Integration Guide](docs/multi-site-integration-guide.md): How to add new real estate websites
- [Proxy Usage Guide](docs/proxy-usage-guide.md): Detailed guide for proxy configuration
- [Modular Structure Benefits](docs/modular-structure-benefit.md): Understanding the code organization

## Project Structure

```
dutch_realestate_scraper/
├── __init__.py
├── config.py                  # Configuration settings
├── models/
│   ├── __init__.py
│   ├── property.py            # PropertyListing data class and related enums
│   └── scan_history.py        # ScanHistory data class
├── scrapers/
│   ├── __init__.py
│   ├── base.py                # BaseScraperStrategy abstract class
│   ├── funda.py               # FundaScraper implementation
│   ├── pararius.py            # ParariusScraper implementation
│   └── factory.py             # RealEstateScraperFactory class
├── database/
│   ├── __init__.py
│   ├── connection.py          # Database connection management
│   ├── property_db.py         # PropertyDatabase operations
│   └── migrations.py          # Database setup and migrations
├── utils/
│   ├── __init__.py
│   ├── http.py                # HTTP request utilities
│   ├── proxy_manager.py       # Proxy management utilities
│   └── parsing.py             # HTML parsing utilities
├── cli.py                     # Command-line interface
├── main.py                    # Main application entry point
```

## Command-Line Options

- `--sources`: Comma-separated list of sources to scrape (default: funda,pararius)
- `--cities`: Comma-separated list of cities to scrape (default: amsterdam)
- `--interval`: Scraping interval in seconds (default: 3600)
- `--db`: PostgreSQL connection string
- `--max-results`: Maximum number of results to process per scan (default: 100)
- `--max-concurrent`: Maximum number of concurrent requests (default: 5)
- `--once`: Run only one scan cycle and exit
- `--debug`: Enable verbose debugging output
- `--use-proxies`: Enable proxy usage for HTTP requests
- `--proxy-list`: Comma-separated list of proxy URLs to use
- `--proxy-stats`: Display proxy statistics

## Requirements

- Python 3.8+
- PostgreSQL 12+ with PostGIS and pgvector extensions
- Dependencies listed in requirements.txt

## Extending the Scraper

See [Multi-Site Integration Guide](docs/multi-site-integration-guide.md) for instructions on adding support for new real estate websites.

## Running as a Service

### Systemd Service (Linux)

```bash
sudo nano /etc/systemd/system/realestate-scraper.service
```

Content:
```ini
[Unit]
Description=Dutch Real Estate Scraper
After=network.target postgresql.service

[Service]
User=youruser
WorkingDirectory=/path/to/dutch-realestate-scraper
ExecStart=/path/to/dutch-realestate-scraper/venv/bin/python -m dutch_realestate_scraper.cli
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable realestate-scraper
sudo systemctl start realestate-scraper
```

## Disclaimer

This tool is for educational purposes. Always respect a website's robots.txt and terms of service when scraping. Rate-limit your requests appropriately to avoid overloading the target servers.

## License

MIT License