"""
Utilities for formatting messages and property listings.
"""

import re
from typing import Dict, Any, Optional
from datetime import datetime


def format_currency(amount: Optional[int]) -> str:
    """
    Format a numeric amount as currency.
    
    Args:
        amount: Amount in cents/eurocents
        
    Returns:
        Formatted currency string
    """
    if amount is None:
        return "N/A"
    
    # Format with thousand separators
    return f"€{amount:,}".replace(",", ".")


def format_date(date_str: Optional[str]) -> str:
    """
    Format a date string into a readable format.
    
    Args:
        date_str: Date string in any parseable format
        
    Returns:
        Formatted date string
    """
    if not date_str:
        return "N/A"
    
    try:
        # Try parsing different date formats
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                date_obj = datetime.strptime(date_str, fmt)
                return date_obj.strftime("%d %b %Y")  # e.g., "15 Jan 2023"
            except ValueError:
                continue
        
        # If none of the formats worked, return as is
        return date_str
    except Exception:
        return date_str


def clean_html(text: str) -> str:
    """
    Remove HTML tags from text.
    
    Args:
        text: Text with potential HTML tags
        
    Returns:
        Cleaned text without HTML tags
    """
    if not text:
        return ""
    
    # Remove HTML tags
    clean = re.sub(r'<[^>]+>', '', text)
    
    # Convert HTML entities
    clean = clean.replace('&nbsp;', ' ')
    clean = clean.replace('&amp;', '&')
    clean = clean.replace('&lt;', '<')
    clean = clean.replace('&gt;', '>')
    clean = clean.replace('&quot;', '"')
    
    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    return clean


def truncate_text(text: str, max_length: int = 100) -> str:
    """
    Truncate text to a maximum length with ellipsis.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        
    Returns:
        Truncated text
    """
    if not text:
        return ""
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length-3] + "..."


def format_listing_message(property_data: Dict[str, Any]) -> str:
    # Extract property data with explicit None handling
    title = property_data.get('title', 'Property Listing') or 'Property Listing'
    address = property_data.get('address', 'Unknown Address') or 'Unknown Address'
    city = property_data.get('city', '') or ''
    neighborhood = property_data.get('neighborhood', '') or ''
    
    # Format full location
    location_parts = []
    if address:
        location_parts.append(address)
    if neighborhood and isinstance(neighborhood, str) and neighborhood not in address:
        location_parts.append(neighborhood)
    if city and isinstance(city, str) and city not in address and (not neighborhood or city not in neighborhood):
        location_parts.append(city)
    location = ", ".join(location_parts) or "Unknown Location"
    
    # Price info
    price = property_data.get('price', 'Price not specified') or 'Price not specified'
    price_numeric = property_data.get('price_numeric')
    if price_numeric is not None:
        price = format_currency(price_numeric)
    
    # Property details
    property_type = property_data.get('property_type', 'Not specified') or 'Not specified'
    offering_type = property_data.get('offering_type', 'Not specified') or 'Not specified'
    
    # Size and rooms
    living_area = property_data.get('living_area', 0) or 0
    living_area_str = f"{living_area} m²" if living_area else "N/A"
    
    rooms = property_data.get('rooms', 0) or 0
    rooms_str = f"{rooms}" if rooms else "N/A"
    
    bedrooms = property_data.get('bedrooms', 0) or 0
    bedrooms_str = f"{bedrooms}" if bedrooms else "N/A"
    
    # Dates
    date_listed = format_date(property_data.get('date_listed', '') or '')
    date_available = format_date(property_data.get('date_available', '') or '')
    
    # Energy label
    energy_label = property_data.get('energy_label', 'N/A') or 'N/A'
    
    # Additional features
    features = []
    if property_data.get('balcony'):
        features.append("Balcony")
    if property_data.get('garden'):
        features.append("Garden")
    if property_data.get('parking'):
        features.append("Parking")
    
    # Description (truncated)
    description = property_data.get('description', '') or ''
    if description:
        description = clean_html(description)
        description = truncate_text(description, 200)
    
    # URL
    url = property_data.get('url', '') or ''
    source = property_data.get('source', '').capitalize() or ''
    
    # Create message with HTML formatting
    message = (
        f"<b>🏠 {title}</b>\n\n"
        f"📍 <b>Location:</b> {location}\n"
        f"💰 <b>Price:</b> {price}\n\n"
        f"<b>Details:</b>\n"
        f"• Type: {property_type} ({offering_type})\n"
        f"• Size: {living_area_str}\n"
        f"• Rooms: {rooms_str} (Bedrooms: {bedrooms_str})\n"
        f"• Energy label: {energy_label}\n"
    )
    
    # Add dates if available
    if date_listed != "N/A":
        message += f"• Listed on: {date_listed}\n"
    if date_available != "N/A":
        message += f"• Available from: {date_available}\n"
    
    # Add description if available
    if description:
        message += f"\n<i>{description}</i>\n"
    
    # Add source info
    if source and url:
        message += f"\nSource: {source}"
    
    return message