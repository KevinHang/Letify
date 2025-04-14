# Database Schema Reference

This document provides a detailed reference of all tables created by the Dutch Real Estate Scraper in the PostgreSQL database.

## Table of Contents

1. [Properties Table](#properties-table)
2. [Scan History Table](#scan-history-table)
3. [Duplicate Properties Table](#duplicate-properties-table)
4. [Table Relationships](#table-relationships)
5. [Indexes](#indexes)
6. [Data Types](#data-types)

## Properties Table

The `properties` table stores all property listings from various sources.

```sql
CREATE TABLE IF NOT EXISTS properties (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    source_id TEXT,
    property_hash TEXT UNIQUE,
    url TEXT,
    title TEXT,
    address TEXT,
    postal_code TEXT,
    city TEXT,
    neighborhood TEXT,
    price TEXT,
    price_numeric FLOAT,
    price_period TEXT,
    service_costs FLOAT,
    description TEXT,
    property_type TEXT,
    offering_type TEXT,
    living_area INTEGER,
    plot_area INTEGER,
    volume INTEGER,
    rooms INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    floors INTEGER,
    balcony BOOLEAN,
    garden BOOLEAN,
    parking BOOLEAN,
    construction_year INTEGER,
    energy_label TEXT,
    interior TEXT,
    coordinates JSONB,
    location GEOGRAPHY(POINT),
    date_listed TEXT,
    date_available TEXT,
    date_scraped TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    images JSONB,
    features JSONB,
    description_embedding vector(384),
    UNIQUE (source, source_id)
);
```

### Key Fields Description

- `id`: Auto-incrementing primary key
- `source`: Source website (e.g., "funda", "pararius")
- `source_id`: ID of the property in the source website
- `property_hash`: Unique hash for property deduplication
- `coordinates`: JSON object with lat/lng values
- `location`: PostGIS geography point for spatial queries
- `images`: JSONB array of image URLs
- `features`: JSONB object of additional property features
- `description_embedding`: Vector for similarity search

## Scan History Table

The `scan_history` table tracks when each source/city combination was scanned.

```sql
CREATE TABLE IF NOT EXISTS scan_history (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    city TEXT NOT NULL,
    scan_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    url TEXT,
    new_listings_count INTEGER DEFAULT 0,
    total_listings_count INTEGER DEFAULT 0,
    scan_duration_seconds FLOAT,
    UNIQUE (source, city)
);
```

### Key Fields Description

- `source`: Source website that was scanned
- `city`: City that was scanned
- `scan_time`: When the scan was performed
- `url`: URL that was used for scanning
- `new_listings_count`: Number of new listings found
- `total_listings_count`: Total number of listings processed
- `scan_duration_seconds`: How long the scan took

## Duplicate Properties Table

The `duplicate_properties` table records potential duplicate listings across different sources.

```sql
CREATE TABLE IF NOT EXISTS duplicate_properties (
    id SERIAL PRIMARY KEY,
    property_hash TEXT NOT NULL,
    source_1 TEXT NOT NULL,
    source_id_1 TEXT NOT NULL,
    source_2 TEXT NOT NULL,
    source_id_2 TEXT NOT NULL,
    similarity_score FLOAT,
    date_detected TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (source_1, source_id_1, source_2, source_id_2)
);
```

### Key Fields Description

- `property_hash`: The hash that matched between properties
- `source_1`, `source_id_1`: Source and ID of the first property
- `source_2`, `source_id_2`: Source and ID of the second property
- `similarity_score`: Calculated similarity between properties (0.0-1.0)
- `date_detected`: When the duplicate was detected

## Table Relationships

- `properties` and `duplicate_properties` are related through `source` and `source_id` fields
- `properties` and `scan_history` are related through `source` and `city` fields

## Indexes

The following indexes are created to optimize query performance:

### Properties Indexes

```sql
CREATE INDEX IF NOT EXISTS idx_properties_source ON properties(source);
CREATE INDEX IF NOT EXISTS idx_properties_city ON properties(city);
CREATE INDEX IF NOT EXISTS idx_properties_postal_code ON properties(postal_code);
CREATE INDEX IF NOT EXISTS idx_properties_price_numeric ON properties(price_numeric);
CREATE INDEX IF NOT EXISTS idx_properties_bedrooms ON properties(bedrooms);
CREATE INDEX IF NOT EXISTS idx_properties_living_area ON properties(living_area);
CREATE INDEX IF NOT EXISTS idx_properties_offering_type ON properties(offering_type);
CREATE INDEX IF NOT EXISTS idx_properties_property_type ON properties(property_type);
CREATE INDEX IF NOT EXISTS idx_properties_date_scraped ON properties(date_scraped);
CREATE INDEX IF NOT EXISTS idx_properties_property_hash ON properties(property_hash);
```

### Spatial Index

```sql
CREATE INDEX IF NOT EXISTS idx_properties_location ON properties USING GIST(location);
```

## Data Types

### Special Types Used

- `GEOGRAPHY(POINT)`: PostGIS data type for storing geographic coordinates
- `JSONB`: PostgreSQL binary JSON format for efficient storage of structured data
- `vector(384)`: pgvector type for storing embedding vectors for similarity search

### Enums Used in Code

While not defined as PostgreSQL ENUMs, the application uses these string enums:

#### OfferingType
- `rental`: Property for rent
- `sale`: Property for sale

#### PropertyType
- `apartment`: Apartment
- `house`: House
- `room`: Room
- `studio`: Studio

#### InteriorType
- `shell`: Unfurnished/shell
- `upholstered`: Upholstered
- `furnished`: Fully furnished