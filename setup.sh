#!/bin/bash
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
chmod 777 logs  # Ensure logs directory is writable

# Create database initialization script
echo -e "${YELLOW}Ensuring database initialization scripts exist...${NC}"
# Create the SQL file if it doesn't exist
if [ ! -f init-db/00-extensions.sql ]; then
    cat > init-db/00-extensions.sql << 'SQLEOF'
-- Try to install required packages first using apt (will be executed before initializing database)
CREATE OR REPLACE FUNCTION pg_temp.install_extensions() RETURNS VOID AS $$
BEGIN
    -- Attempt to install postgis
    PERFORM pg_catalog.pg_extension_config_dump('postgis', '');
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error installing postgis extension: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

SELECT pg_temp.install_extensions();

-- Enable extensions if available
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Let's make the vector extension optional
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Vector extension not available. Vector search features will be disabled.';
END
$$;
SQLEOF
fi

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# Create default .env if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file...${NC}"
    cat > .env << 'ENVEOF'
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
ENVEOF
    echo -e "${GREEN}Created .env file. Please edit it with your configuration.${NC}"
else
    echo -e "${GREEN}.env file already exists.${NC}"
fi

# Stop existing containers
echo -e "${YELLOW}Stopping any existing containers...${NC}"
docker compose down -v

# Clean up any existing state
echo -e "${YELLOW}Cleaning up...${NC}"
docker compose rm -f postgres || true

# Start PostgreSQL
echo -e "${YELLOW}Starting PostgreSQL container...${NC}"
docker compose up -d postgres

# Wait for PostgreSQL to be ready
echo -e "${YELLOW}Waiting for PostgreSQL to be ready...${NC}"
sleep 20

# Run migrations
echo -e "${YELLOW}Running database migrations...${NC}"
docker compose run --rm scraper python -m database.migrations || true

# Verify database setup
echo -e "${YELLOW}Verifying database setup...${NC}"
docker compose exec postgres psql -U postgres -d realestate -c "\dx" || true

# Ask if user wants to start the services
read -p "Do you want to start the services now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Starting all services...${NC}"
    docker compose up -d
    
    echo -e "${GREEN}Services are starting up!${NC}"
    echo "You can check the status with: docker compose ps"
    echo "You can view logs with: docker compose logs -f"
else
    echo -e "${GREEN}Setup completed. You can start the services later with:${NC}"
    echo "docker compose up -d"
fi

echo
echo -e "${GREEN}Setup completed successfully!${NC}"