-- Create extensions if available
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Grant privileges
ALTER ROLE postgres WITH SUPERUSER;