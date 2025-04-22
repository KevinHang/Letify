# Installation and Configuration Guide

This guide walks you through the complete installation and configuration process for the Dutch Real Estate Scraper.

## Prerequisites

Before you begin, ensure you have the following installed:

1. **Python 3.8+**
   - [Download Python](https://www.python.org/downloads/)
   - Verify with: `python --version`

2. **PostgreSQL 12+**
   - [Download PostgreSQL](https://www.postgresql.org/download/)
   - Required extensions: PostGIS, fuzzystrmatch
   - Verify with: `psql --version`

3. **Git** (for cloning the repository)
   - [Download Git](https://git-scm.com/downloads)
   - Verify with: `git --version`

## Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/dutch-realestate-scraper.git
cd dutch-realestate-scraper
```

## Step 2: Set Up Python Environment

### Create Virtual Environment

```bash
# On Linux/macOS
python -m venv venv
source venv/bin/activate

# On Windows
python -m venv venv
venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 3: Database Setup

### Create Database

```bash
# For Linux/macOS with default PostgreSQL setup
createdb realestate

# If you need to specify credentials
createdb -U postgres realestate
```

### Enable Required Extensions

```bash
# For Linux/macOS
psql -d realestate -c "CREATE EXTENSION postgis; CREATE EXTENSION vector; CREATE EXTENSION fuzzystrmatch;"

# If you need to specify credentials
psql -U postgres -d realestate -c "CREATE EXTENSION postgis; CREATE EXTENSION vector; CREATE EXTENSION fuzzystrmatch;"
```

### Create User and Set Permissions (Optional)

```bash
# Create a dedicated database user
createuser -P scraper_user  # You'll be prompted to set a password

# Grant privileges to the user
psql -d realestate -c "GRANT ALL PRIVILEGES ON DATABASE realestate TO scraper_user;"
```

## Step 4: Configuration

### Environment Configuration

1. Copy the environment template:

```bash
cp .env.template .env
```

2. Edit the `.env` file with your database credentials and preferences:

```ini
# Database configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=realestate
DB_USER=scraper_user  # Or your PostgreSQL username
DB_PASSWORD=your_password

# Scraper configuration
DEFAULT_SCAN_INTERVAL=3600  # How often to scan (in seconds)
MAX_RESULTS_PER_SCAN=100    # How many listings to process per scan
MAX_CONCURRENT_REQUESTS=5   # How many concurrent requests to make
DEFAULT_CITIES=amsterdam,rotterdam,utrecht
DEFAULT_SOURCES=funda,pararius

# HTTP configuration
HTTP_TIMEOUT=30.0

# Proxy configuration (optional)
USE_PROXIES=False
# PROXY_LIST=http://proxy1.example.com:8080,http://proxy2.example.com:8080
```

### Site-Specific Configuration

The scraper is pre-configured for Funda.nl and Pararius.com. If you need to modify these settings, you can edit the `config.py` file:

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

## Step 5: Verify Installation

Run a test scan to verify everything is working:

```bash
python -m dutch_realestate_scraper.cli --once --cities amsterdam --max-results 5
```

You should see output indicating the scraper is running and connecting to the database. Check the log file (`realestate_scraper.log`) for detailed information.

## Advanced Configuration

### PostgreSQL Tuning

For optimal performance with large datasets, consider adding these settings to your `postgresql.conf`:

```
# Memory settings
shared_buffers = 256MB             # Adjust based on available RAM (1/4 of system memory)
work_mem = 16MB                    # Adjust based on query complexity
maintenance_work_mem = 64MB        # For maintenance operations
effective_cache_size = 768MB       # Estimate of memory available for disk caching

# Query planner
random_page_cost = 1.1             # Lower for SSDs (default is 4.0)
effective_io_concurrency = 200     # Higher for SSDs

# Logging for debugging (disable in production)
log_min_duration_statement = 200   # Log queries taking longer than 200ms
```

### PostGIS Configuration

If you'll be doing extensive geospatial queries:

```
# Add to postgresql.conf
max_parallel_workers_per_gather = 4  # If you have multiple CPUs
```

### Python Environment Variables

Additional environment variables that can be set in your `.env` file:

```ini
# Logging
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FILE=realestate_scraper.log

# Performance
MAX_QUEUE_SIZE=1000  # Maximum number of URLs to queue
RETRY_COUNT=3        # Number of times to retry failed requests
RETRY_DELAY=5        # Delay between retries in seconds

# Database
DB_POOL_SIZE=5       # Connection pool size
```

## Next Steps

Once you've completed the installation and configuration:

1. Read the [Getting Started Guide](getting-started-guide.md) for usage instructions
2. Explore the [Multi-Site Integration Guide](multi-site-integration-guide.md) if you want to add more real estate websites
3. Configure [automated scraping](getting-started-guide.md#5-scheduling-regular-scraping) for continuous monitoring

## Troubleshooting

### Common Installation Issues

#### Python Dependency Issues

If you encounter issues with the dependencies:

```bash
# Update pip first
pip install --upgrade pip

# Install prerequisites for some packages
# Ubuntu/Debian
sudo apt-get install python3-dev libpq-dev

# macOS (with Homebrew)
brew install postgresql
brew services start postgresql

pip install -e .

# Windows
# Install the PostgreSQL binary package which includes the necessary libraries
```

#### Database Connection Issues

If you can't connect to the database:

1. Check PostgreSQL is running: `pg_isready`
2. Verify your credentials in `.env`
3. Ensure PostgreSQL is configured to accept connections:
   - Check `pg_hba.conf` for connection permissions
   - Ensure PostgreSQL is listening on the expected address in `postgresql.conf`

For more troubleshooting tips, see the [Getting Started Guide](getting-started-guide.md#9-troubleshooting).