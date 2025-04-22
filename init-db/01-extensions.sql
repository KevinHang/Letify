-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS vector;

-- Create realestate database if it doesn't exist
-- This is actually handled by the POSTGRES_DB environment variable
-- but including for documentation purposes
-- CREATE DATABASE realestate;

-- Grant privileges
ALTER ROLE postgres WITH SUPERUSER;