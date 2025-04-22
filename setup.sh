#!/bin/bash

# Setup script for Dutch Real Estate Scraper on Hetzner Cloud
set -e

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Dutch Real Estate Scraper - Setup Script${NC}"
echo "========================================"
echo

# Create necessary directories
echo -e "${YELLOW}Creating required directories...${NC}"
mkdir -p logs
mkdir -p init-db

# Check if .env file exists, if not create it
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file...${NC}"
    cat > .env << EOL
# Database configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=realestate
DB_USER=postgres
DB_PASSWORD=postgres

# Scraper configuration
DEFAULT_SCAN_INTERVAL=3600
MAX_RESULTS_PER_SCAN=100
MAX_CONCURRENT_REQUESTS=5
DEFAULT_CITIES=amsterdam,rotterdam,utrecht,den-haag,eindhoven
DEFAULT_SOURCES=funda,pararius
STOP_AFTER_NO_RESULT=True

# HTTP configuration
HTTP_TIMEOUT=30.0

# Proxy configuration
USE_PROXIES=False
PROXY_ROTATION_STRATEGY=round_robin

# Telegram Bot configuration
ENABLE_TELEGRAM=True
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_ADMIN_USER_IDS=123456789
NOTIFICATION_INTERVAL=300
MAX_NOTIFICATIONS_PER_USER_PER_DAY=20
NOTIFICATION_BATCH_SIZE=50
NOTIFICATION_RETRY_ATTEMPTS=3
EOL
    echo -e "${GREEN}Created .env file. Please edit it with your configuration.${NC}"
else
    echo -e "${GREEN}.env file already exists.${NC}"
fi

# Create database initialization script
echo -e "${YELLOW}Creating database initialization script...${NC}"
cat > init-db/01-extensions.sql << EOL
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS vector;

-- Grant privileges
ALTER ROLE postgres WITH SUPERUSER;
EOL

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

# Ask if user wants to start the services
read -p "Do you want to start the services now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Starting services...${NC}"
    docker-compose up -d
    
    echo -e "${GREEN}Services are starting up!${NC}"
    echo "You can check the status with: docker-compose ps"
    echo "You can view logs with: docker-compose logs -f"
else
    echo -e "${GREEN}Setup completed. You can start the services later with:${NC}"
    echo "docker-compose up -d"
fi

echo
echo -e "${GREEN}Setup completed successfully!${NC}"
echo "Please check the deployment guide for next steps."
echo