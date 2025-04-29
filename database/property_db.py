"""
Property database operations.
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

from models.property import PropertyListing
from utils.logging_config import get_scraper_logger

# Use a child logger of the telegram logger
logger = get_scraper_logger("property_db")


class PropertyDatabase:
    """Database handler for property listings"""
    
    def __init__(self, connection_string: str):
        """Initialize database connection"""
        self.connection_string = connection_string
        self.conn = psycopg.connect(connection_string)
        
    def save_listing(self, listing: PropertyListing) -> bool:
        """Save a property listing to the database. Returns True if new, False if updated."""
        is_new = False
        
        try:
            with self.conn.cursor() as cur:
                # Ensure property_hash is generated
                if not listing.property_hash:
                    listing.generate_property_hash()
                
                # Check if the listing already exists
                cur.execute(
                    "SELECT id FROM properties WHERE (source = %s AND source_id = %s) OR property_hash = %s",
                    (listing.source, listing.source_id, listing.property_hash)
                )
                result = cur.fetchone()
                
                if result:
                    # Update existing listing
                    property_id = result[0]
                    cur.execute("""
                    UPDATE properties SET
                        source_id = %s,
                        url = %s,
                        title = %s,
                        address = %s,
                        postal_code = %s,
                        city = %s,
                        neighborhood = %s,
                        price = %s,
                        price_numeric = %s,
                        price_period = %s,
                        service_costs = %s,
                        description = %s,
                        property_type = %s,
                        offering_type = %s,
                        living_area = %s,
                        plot_area = %s,
                        volume = %s,
                        rooms = %s,
                        bedrooms = %s,
                        bathrooms = %s,
                        floors = %s,
                        balcony = %s,
                        garden = %s,
                        parking = %s,
                        construction_year = %s,
                        energy_label = %s,
                        interior = %s,
                        date_listed = %s,
                        date_available = %s,
                        availability_period = %s,
                        date_scraped = %s,
                        images = %s,
                        features = %s
                    WHERE id = %s
                    """, (
                        listing.source_id,
                        listing.url,
                        listing.title,
                        listing.address,
                        listing.postal_code,
                        listing.city,
                        listing.neighborhood,
                        listing.price,
                        listing.price_numeric,
                        listing.price_period,
                        listing.service_costs,
                        listing.description,
                        listing.property_type.value if listing.property_type else None,
                        listing.offering_type.value if listing.offering_type else None,
                        listing.living_area,
                        listing.plot_area,
                        listing.volume,
                        listing.rooms,
                        listing.bedrooms,
                        listing.bathrooms,
                        listing.floors,
                        listing.balcony,
                        listing.garden,
                        listing.parking,
                        listing.construction_year,
                        listing.energy_label,
                        listing.interior.value if listing.interior else None,
                        listing.date_listed,
                        listing.date_available,
                        listing.availability_period,
                        datetime.now(),
                        json.dumps(listing.images),
                        json.dumps(listing.features),
                        property_id
                    ))
                else:
                    # Insert new listing
                    is_new = True
                    cur.execute("""
                    INSERT INTO properties (
                        source, source_id, property_hash, url, title, address, postal_code, city, neighborhood,
                        price, price_numeric, price_period, service_costs, description, property_type, offering_type,
                        living_area, plot_area, volume, rooms, bedrooms, bathrooms, floors, balcony, garden, parking,
                        construction_year, energy_label, interior, date_listed, date_available, availability_period,
                        date_scraped, images, features
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    """, (
                        listing.source,
                        listing.source_id,
                        listing.property_hash,
                        listing.url,
                        listing.title,
                        listing.address,
                        listing.postal_code,
                        listing.city,
                        listing.neighborhood,
                        listing.price,
                        listing.price_numeric,
                        listing.price_period,
                        listing.service_costs,
                        listing.description,
                        listing.property_type.value if listing.property_type else None,
                        listing.offering_type.value if listing.offering_type else None,
                        listing.living_area,
                        listing.plot_area,
                        listing.volume,
                        listing.rooms,
                        listing.bedrooms,
                        listing.bathrooms,
                        listing.floors,
                        listing.balcony,
                        listing.garden,
                        listing.parking,
                        listing.construction_year,
                        listing.energy_label,
                        listing.interior.value if listing.interior else None,
                        listing.date_listed,
                        listing.date_available,
                        listing.availability_period,
                        datetime.now(),
                        json.dumps(listing.images),
                        json.dumps(listing.features)
                    ))
                
                self.conn.commit()
                return is_new
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error saving listing: {e}")
            raise
    
    def update_scan_history(self, source: str, city: str, url: str, new_count: int, total_count: int, duration: float):
        """Update the scan history for a source and city."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO scan_history 
                    (source, city, url, new_listings_count, total_listings_count, scan_duration_seconds) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, city) DO UPDATE 
                SET 
                    scan_time = NOW(), 
                    url = EXCLUDED.url,
                    new_listings_count = EXCLUDED.new_listings_count,
                    total_listings_count = EXCLUDED.total_listings_count,
                    scan_duration_seconds = EXCLUDED.scan_duration_seconds
                """, (source, city, url, new_count, total_count, duration))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error updating scan history: {e}")
            raise
    
    def update_query_url_scan_time(self, query_url_id: int):
        """Update the last scan time for a query URL."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE query_urls
                SET last_scan_time = NOW()
                WHERE id = %s
                """, (query_url_id,))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error updating query URL scan time: {e}")
            raise
    
    def get_last_scan_time(self, source: str, city: str) -> Optional[datetime]:
        """Get the last scan time for a source and city."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT scan_time FROM scan_history WHERE source = %s AND city = %s",
                    (source, city)
                )
                result = cur.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting last scan time: {e}")
            return None
    
    def get_enabled_query_urls(self, sources: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get all enabled query URLs, optionally filtered by source.
        
        Args:
            sources: Optional list of sources to filter by
            
        Returns:
            List of dictionaries containing query URL information
        """
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                if sources:
                    cur.execute(
                        "SELECT * FROM query_urls WHERE enabled = TRUE AND source = ANY(%s) ORDER BY id ASC",
                        (sources,)
                    )
                else:
                    cur.execute(
                        "SELECT * FROM query_urls WHERE enabled = TRUE ORDER BY id ASC"
                    )
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting enabled query URLs: {e}")
            return []
    
    def add_query_url(self, source: str, query_url: str, method: str = 'GET', enabled: bool = True, description: str = None, request_body: dict = None, custom_headers: dict = None) -> int:
        """
        Add a new query URL to the database.
        
        Args:
            source: Source name (e.g., "funda")
            query_url: URL to scrape
            method: HTTP method (GET or POST)
            enabled: Whether the URL is enabled for scraping
            description: Optional description
            request_body: Body of the request
            custom_headers: Extra headers that might be needed
            
        Returns:
            ID of the newly created entry, or -1 on error
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO query_urls (source, queryurl, method, enabled, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, queryurl) DO UPDATE
                SET method = EXCLUDED.method, enabled = EXCLUDED.enabled, description = EXCLUDED.description, request_body = EXCLUDED.request_body, custom_headers = EXCLUDED.custom_headers
                RETURNING id
                """, (source, query_url, method, enabled, description, json.dumps(request_body), json.dumps(custom_headers)))
                result = cur.fetchone()
                self.conn.commit()
                return result[0] if result else -1
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error adding query URL: {e}")
            return -1
    
    def toggle_query_url(self, query_url_id: int, enabled: bool) -> bool:
        """
        Enable or disable a query URL.
        
        Args:
            query_url_id: ID of the query URL
            enabled: Whether to enable or disable
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                UPDATE query_urls
                SET enabled = %s
                WHERE id = %s
                """, (enabled, query_url_id))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error toggling query URL: {e}")
            return False
    
    def delete_query_url(self, query_url_id: int) -> bool:
        """
        Delete a query URL.
        
        Args:
            query_url_id: ID of the query URL
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                DELETE FROM query_urls
                WHERE id = %s
                """, (query_url_id,))
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error deleting query URL: {e}")
            return False
    
    def search_properties(self, 
                          city: Optional[str] = None,
                          min_price: Optional[float] = None,
                          max_price: Optional[float] = None,
                          min_rooms: Optional[int] = None,
                          max_rooms: Optional[int] = None,
                          property_type: Optional[str] = None,
                          min_area: Optional[int] = None,
                          max_area: Optional[int] = None,
                          interior_type: Optional[str] = None,
                          neighborhood: Optional[str] = None,
                          limit: int = 100,
                          offset: int = 0) -> List[Dict[str, Any]]:
        """
        Search for properties with specified filters.
        Returns a list of property listings.
        """
        query = "SELECT * FROM properties WHERE 1=1"
        params = []
        
        # Add filters
        if city:
            query += " AND city ILIKE %s"
            params.append(f"%{city}%")
        
        if min_price is not None:
            query += " AND price_numeric >= %s"
            params.append(min_price)
        
        if max_price is not None:
            query += " AND price_numeric <= %s"
            params.append(max_price)
        
        if min_rooms is not None:
            query += " AND rooms >= %s"
            params.append(min_rooms)
        
        if max_rooms is not None:
            query += " AND rooms <= %s"
            params.append(max_rooms)
        
        if property_type:
            query += " AND property_type = %s"
            params.append(property_type)
        
        if min_area is not None:
            query += " AND living_area >= %s"
            params.append(min_area)
        
        if max_area is not None:
            query += " AND living_area <= %s"
            params.append(max_area)
        
        if interior_type:
            query += " AND interior = %s"
            params.append(interior_type)
        
        if neighborhood:
            query += " AND neighborhood ILIKE %s"
            params.append(f"%{neighborhood}%")
        
        # Add sorting and pagination
        query += " ORDER BY date_scraped DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Error searching properties: {e}")
            return []
    
    def find_potential_duplicates(self, threshold: float = 0.8) -> List[Dict[str, Any]]:
        """
        Find potential duplicate properties across different sources 
        based on property_hash and other similarity measures.
        """
        query = """
        SELECT 
            a.id as id_1, 
            b.id as id_2,
            a.source as source_1,
            a.source_id as source_id_1,
            b.source as source_2,
            b.source_id as source_id_2,
            a.property_hash,
            a.address as address_1,
            b.address as address_2,
            a.living_area as area_1,
            b.living_area as area_2,
            a.price_numeric as price_1,
            b.price_numeric as price_2
        FROM 
            properties a
        JOIN 
            properties b 
        ON 
            a.source < b.source
            AND a.property_hash = b.property_hash
            AND (a.address IS NOT NULL AND b.address IS NOT NULL AND 
                levenshtein(lower(a.address), lower(b.address)) / 
                GREATEST(length(a.address), length(b.address), 1) < %s)
        ORDER BY a.property_hash
        """
        
        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, [1.0 - threshold])
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Error finding potential duplicates: {e}")
            return []
    
    def record_duplicate_pair(self, source_1: str, source_id_1: str, 
                            source_2: str, source_id_2: str, 
                            property_hash: str, similarity_score: float):
        """Record a duplicate property pair in the database."""
        try:
            with self.conn.cursor() as cur:
                # Sort sources to ensure consistent ordering
                if source_1 > source_2:
                    source_1, source_2 = source_2, source_1
                    source_id_1, source_id_2 = source_id_2, source_id_1
                
                cur.execute("""
                INSERT INTO duplicate_properties 
                    (property_hash, source_1, source_id_1, source_2, source_id_2, similarity_score) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_1, source_id_1, source_2, source_id_2) DO UPDATE 
                SET 
                    similarity_score = EXCLUDED.similarity_score,
                    date_detected = NOW()
                """, (property_hash, source_1, source_id_1, source_2, source_id_2, similarity_score))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error recording duplicate pair: {e}")
            raise
        
    def get_property_id_by_source_id(self, source: str, source_id: str) -> Optional[int]:
        """
        Get property ID from source and source_id.
        
        Args:
            source: Source name (e.g., "funda")
            source_id: Source-specific ID
            
        Returns:
            Property ID if found, None otherwise
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM properties WHERE source = %s AND source_id = %s",
                    (source, source_id)
                )
                result = cur.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting property ID for {source}/{source_id}: {e}")
            return None