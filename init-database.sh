#!/bin/bash
set -e

echo "Starting PostgreSQL container..."
docker compose up -d postgres

echo "Waiting for PostgreSQL to be ready..."
sleep 10

echo "Initializing main database tables..."
docker compose exec postgres psql -U postgres -d realestate -c "CREATE EXTENSION IF NOT EXISTS postgis;"
docker compose exec postgres psql -U postgres -d realestate -c "CREATE EXTENSION IF NOT EXISTS vector;"
docker compose exec postgres psql -U postgres -d realestate -c "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;"

echo "Running database migrations..."
docker compose run --rm scraper python -m database.migrations

echo "Verifying tables..."
docker compose exec postgres psql -U postgres -d realestate -c "\dt"

echo "Database initialization complete. Starting remaining services..."
docker compose up -d

echo "Done!"