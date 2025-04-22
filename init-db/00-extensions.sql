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