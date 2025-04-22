#!/bin/bash
set -e

echo "Starting PostgreSQL container..."
docker compose up -d postgres

echo "Waiting for PostgreSQL to be ready (30 seconds)..."
sleep 30

echo "Initializing main database tables..."
# Try to create extensions, but don't fail if they already exist
docker compose exec postgres psql -U postgres -d realestate -c "CREATE EXTENSION IF NOT EXISTS postgis;" || true
docker compose exec postgres psql -U postgres -d realestate -c "CREATE EXTENSION IF NOT EXISTS vector;" || true
docker compose exec postgres psql -U postgres -d realestate -c "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;" || true

echo "Running database migrations..."
docker compose run --rm scraper python -m cli --init-telegram-db

echo "Verifying tables..."
docker compose exec postgres psql -U postgres -d realestate -c "\dt"

echo "Database initialization complete. Starting remaining services..."
docker compose up -d

echo "Done!"