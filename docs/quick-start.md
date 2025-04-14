# Quick Start Guide

This guide will help you get up and running with the Dutch Real Estate Scraper in just a few minutes.

## Prerequisites

- Python 3.8 or higher
- PostgreSQL 12 or higher with PostGIS extension
- Git (for cloning the repository)

## 1. Clone and Install

```bash
# Clone the repository
git clone https://github.com/yourusername/dutch-realestate-scraper.git
cd dutch-realestate-scraper

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Linux/macOS:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## 2. Set Up Database

```bash
# Create a PostgreSQL database
createdb realestate

# Enable required extensions
psql -d realestate -c "CREATE EXTENSION postgis; CREATE EXTENSION vector; CREATE EXTENSION fuzzystrmatch;"
```

## 3. Configure Environment

```bash
# Copy the environment template
cp .env.template .env

# Edit the .env file with your database credentials
# On Linux/macOS:
nano .env
# On Windows:
notepad .env
```

Minimum required settings in `.env`:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=realestate
DB_USER=your_postgres_username
DB_PASSWORD=your_postgres_password
```

## 4. Run Your First Scan

```bash
# Run a single scan of Amsterdam with default settings
python -m dutch_realestate_scraper.cli --cities amsterdam --once
```

## 5. View the Results

Check the log file for information about the scan:

```bash
# On Linux/macOS:
cat realestate_scraper.log
# On Windows:
type realestate_scraper.log
```

## 6. Run Continuous Scanning

```bash
# Start continuous scanning every hour
python -m dutch_realestate_scraper.cli
```

## Common Command Examples

### Scan Multiple Cities

```bash
python -m dutch_realestate_scraper.cli --cities amsterdam,rotterdam,utrecht --once
```

### Scan Specific Sources

```bash
python -m dutch_realestate_scraper.cli --sources funda,pararius --once
```

### Limit Results Per Scan

```bash
python -m dutch_realestate_scraper.cli --max-results 50 --once
```

### Change Scan Interval

```bash
python -m dutch_realestate_scraper.cli --interval 1800  # 30 minutes
```

### Debug Mode

```bash
python -m dutch_realestate_scraper.cli --debug --once
```

### Using Prox