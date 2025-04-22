# Getting Started with Dutch Real Estate Scraper

This comprehensive guide will walk you through setting up and using the Dutch Real Estate Scraper to monitor property listings from multiple real estate websites in the Netherlands.

## Table of Contents

1. [Installation](#1-installation)
2. [Database Setup](#2-database-setup)
3. [Configuration](#3-configuration)
4. [Basic Usage](#4-basic-usage)
5. [Scheduling Regular Scraping](#5-scheduling-regular-scraping)
6. [Managing Multiple Cities](#6-managing-multiple-cities)
7. [Proxy Configuration](#7-proxy-configuration)
8. [Advanced Usage](#8-advanced-usage)
9. [Troubleshooting](#9-troubleshooting)
10. [Extending the Scraper](#10-extending-the-scraper)

## 1. Installation

### Prerequisites

- Python 3.8 or higher
- Git (for cloning the repository)

### Step-by-Step Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/yourusername/dutch-realestate-scraper.git
   cd dutch-realestate-scraper
   ```

2. **Create a virtual environment**:

   ```bash
   # On Linux/macOS
   python -m venv venv
   source venv/bin/activate

   # On Windows
   python -m venv venv
   venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

## 2. Database Setup

The scraper uses PostgreSQL with PostGIS (for geospatial queries)

### PostgreSQL Installation

If you haven't installed PostgreSQL yet:

- **Linux**:
  ```bash
  sudo apt update
  sudo apt install postgresql postgresql-contrib postgis
  ```

- **macOS** (using Homebrew):
  ```bash
  brew install postgresql postgis
  ```

- **Windows**: Download and install from [PostgreSQL website](https://www.postgresql.org/download/windows/)

### Create Database and Extensions

1. **Create a database**:

   ```bash
   createdb realestate
   ```

2. **Enable required extensions**:

   ```bash
   psql -d realestate -c "CREATE EXTENSION postgis; CREATE EXTENSION vector; CREATE EXTENSION fuzzystrmatch;"
   ```

3. **Create a database user** (optional):

   ```bash
   createuser -P scraper_user  # Follow the prompts to set a password
   psql -d realestate -c "GRANT ALL PRIVILEGES ON DATABASE realestate TO scraper_user;"
   ```

## 3. Configuration

### Environment Configuration

1. **Create your environment file**:

   ```bash
   cp .env.template .env
   ```

2. **Edit the `.env` file** with your database credentials and preferences:

   ```ini
   # Database configuration
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=realestate
   DB_USER=scraper_user  # Use the user you created
   DB_PASSWORD=your_password

   # Scraper configuration
   DEFAULT_SCAN_INTERVAL=3600  # In seconds (1 hour)
   MAX_RESULTS_PER_SCAN=100
   MAX_CONCURRENT_REQUESTS=5
   DEFAULT_CITIES=amsterdam,rotterdam,utrecht  # Cities to scrape by default
   DEFAULT_SOURCES=funda,pararius  # Sources to scrape by default

   # Proxy configuration (optional)
   USE_PROXIES=False
   ```

### Site-Specific Configuration

The scraper is pre-configured for Funda.nl and Pararius.com. You can adjust their settings in `config.py` if needed:

```python
SITE_CONFIGS = {
    "funda": {
        "base_url": "https://www.funda.nl",
        "search_url_template": "https://www.funda.nl/en/zoeken/huur/?selected_area=[\"{city}\"]&publication_date=\"{days}\"&sort=\"date_down\"",
        "min_interval": 300,  # 5 minutes 
    },
    "pararius": {
        "base_url": "https://www.pararius.com",
        "search_url_template": "https://www.pararius.com/apartments/{city}",
        "min_interval": 600,  # 10 minutes
    }
}
```

## 4. Basic Usage

### Running a Single Scan

To run a single scan of the default sources and cities:

```bash
python -m dutch_realestate_scraper.cli --once
```

### Specifying Sources and Cities

```bash
python -m dutch_realestate_scraper.cli --sources funda,pararius --cities amsterdam,rotterdam --once
```

### Continuous Scanning

To run the scraper continuously with periodic scans:

```bash
python -m dutch_realestate_scraper.cli --interval 1800  # Scan every 30 minutes
```

### Output and Logging

The scraper logs information to both the console and a log file (`realestate_scraper.log`). For more detailed logging:

```bash
python -m dutch_realestate_scraper.cli --debug
```

## 5. Scheduling Regular Scraping

### Using Systemd (Linux)

1. **Create a systemd service file**:

   ```bash
   sudo nano /etc/systemd/system/realestate-scraper.service
   ```

2. **Add the following content**:

   ```ini
   [Unit]
   Description=Dutch Real Estate Scraper
   After=network.target postgresql.service

   [Service]
   User=your_username
   WorkingDirectory=/path/to/dutch-realestate-scraper
   ExecStart=/path/to/dutch-realestate-scraper/venv/bin/python -m dutch_realestate_scraper.cli
   Restart=always
   RestartSec=5
   Environment=PYTHONUNBUFFERED=1

   [Install]
   WantedBy=multi-user.target
   ```

3. **Enable and start the service**:

   ```bash
   sudo systemctl enable realestate-scraper
   sudo systemctl start realestate-scraper
   ```

4. **Check the status**:

   ```bash
   sudo systemctl status realestate-scraper
   ```

### Using Cron (Linux/macOS)

1. **Open your crontab**:

   ```bash
   crontab -e
   ```

2. **Add a scheduled task** (example: run every hour):

   ```
   0 * * * * cd /path/to/dutch-realestate-scraper && /path/to/dutch-realestate-scraper/venv/bin/python -m dutch_realestate_scraper.cli --once
   ```

### Using Task Scheduler (Windows)

1. Open Task Scheduler
2. Create a Basic Task:
   - Name: "Real Estate Scraper"
   - Trigger: Daily, recur every 1 hour
   - Action: Start a program
   - Program/script: `C:\path\to\venv\Scripts\python.exe`
   - Arguments: `-m dutch_realestate_scraper.cli --once`
   - Start in: `C:\path\to\dutch-realestate-scraper`

## 6. Managing Multiple Cities

### Adding New Cities

You can add any Dutch city by specifying it in the command line:

```bash
python -m dutch_realestate_scraper.cli --cities amsterdam,rotterdam,utrecht,groningen,eindhoven
```

Or by updating your `.env` file:

```ini
DEFAULT_CITIES=amsterdam,rotterdam,utrecht,groningen,eindhoven
```

### City-Specific Scraping

You can run a scan for a specific city only:

```bash
python -m dutch_realestate_scraper.cli --cities amsterdam --once
```

### Multiple Scraper Instances

For complex setups, you can run multiple instances with different configurations:

```bash
# Instance 1: Amsterdam and Rotterdam, every 30 minutes
python -m dutch_realestate_scraper.cli --cities amsterdam,rotterdam --interval 1800

# Instance 2: Utrecht and Eindhoven, every hour
python -m dutch_realestate_scraper.cli --cities utrecht,eindhoven --interval 3600
```

## 7. Proxy Configuration

### Enabling Proxies

You can enable proxy usage if you're experiencing IP-based rate limiting:

```bash
python -m dutch_realestate_scraper.cli --use-proxies --proxy-list "http://proxy1:8080,http://proxy2:8080"
```

Or through your `.env` file:

```ini
USE_PROXIES=True
PROXY_LIST=http://proxy1:8080,http://proxy2:8080
PROXY_ROTATION_STRATEGY=round_robin
```

### Proxy Rotation Strategies

Choose from different proxy rotation strategies:

```bash
python -m dutch_realestate_scraper.cli --use-proxies --proxy-rotation round_robin
```

Options include:
- `round_robin`: Cycle through proxies in order
- `random`: Select a random proxy for each request
- `fallback`: Try direct connection if proxy fails

### Checking Proxy Health

Monitor proxy performance with:

```bash
python -m dutch_realestate_scraper.cli --once --use-proxies --proxy-stats
```

See the [Proxy Usage Guide](proxy-usage-guide.md) for more detailed information about working with proxies.

## 8. Advanced Usage

### Limiting Results

Control how many listings to process per scan:

```bash
python -m dutch_realestate_scraper.cli --max-results 50
```

### Concurrent Requests

Adjust the number of concurrent requests:

```bash
python -m dutch_realestate_scraper.cli --max-concurrent 10
```

### Listing Available Sources

To see what sources are available:

```bash
python -m dutch_realestate_scraper.cli --list-sources
```

### Database Connection

Specify a custom database connection string:

```bash
python -m dutch_realestate_scraper.cli --db "postgresql://user:pass@host:port/dbname"
```

## 9. Troubleshooting

### Common Issues and Solutions

1. **Database Connection Errors**:
   
   ```
   Error connecting to database: could not connect to server
   ```
   
   Solution: Check your PostgreSQL service is running and credentials are correct.

2. **HTTP Errors**:
   
   ```
   Error scanning funda for amsterdam: HTTP 403 for https://www.funda.nl/...
   ```
   
   Solution: The website might be blocking scraping. Try enabling proxies or increasing scan intervals.

3. **Parser Errors**:
   
   ```
   Error processing listing: 'NoneType' object has no attribute 'text'
   ```
   
   Solution: The website structure might have changed. Check for updates or open an issue.

### Debugging

For detailed debugging information:

```bash
python -m dutch_realestate_scraper.cli --debug
```

This will output more verbose logs to help identify issues.

### Logs

Check the log file for detailed information:

```bash
tail -f realestate_scraper.log
```

## 10. Extending the Scraper

### Adding a New Real Estate Website

1. Create a new scraper class in the `scrapers` directory based on the template
2. Implement the required methods
3. Add your site to the `SITE_CONFIGS` dictionary
4. Register it in the `RealEstateScraperFactory`

See [Guide: Integrating Multiple Real Estate Websites](multi-site-integration-guide.md) for a detailed walkthrough.

### Customizing Database Schema

If you need to extend the database schema:

1. Modify the table creation in `database/migrations.py`
2. Update the `PropertyListing` class in `models/property.py`
3. Run a custom migration script or reinitialize the database

### Building a Frontend

You can build a frontend to display scraped data:

1. Create a simple web application using Flask or Django
2. Connect it to the same database
3. Implement search functionality using the existing indexes

Example Flask setup:

```python
from flask import Flask, render_template
import psycopg
from psycopg.rows import dict_row

app = Flask(__name__)

@app.route('/')
def index():
    with psycopg.connect("postgresql://user:pass@localhost/realestate") as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM properties ORDER BY date_scraped DESC LIMIT 10")
            properties = cur.fetchall()
    return render_template('index.html', properties=properties)

if __name__ == '__main__':
    app.run(debug=True)
```

## Additional Resources

- [Project README](README.md): Overview and basic information
- [Proxy Usage Guide](proxy-usage-guide.md): Detailed guide for proxy configuration
- [Multi-Site Integration Guide](multi-site-integration-guide.md): How to add new websites
- [Modular Structure Benefits](modular-structure-benefit.md): Understanding the code organization

---

Congratulations! You now have a fully operational Dutch Real Estate Scraper. Happy house hunting!