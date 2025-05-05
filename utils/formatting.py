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
    postal_code = property_data.get('postal_code', '') or ''
    
    # Format full location
    location_parts = []
    if address:
        location_parts.append(address)
    if neighborhood and isinstance(neighborhood, str) and neighborhood not in address:
        location_parts.append(neighborhood)
    if postal_code and isinstance(postal_code, str):
        location_parts.append(postal_code)
    if city and isinstance(city, str):
        location_parts.append(city.title())
    location = ", ".join(location_parts) or "Unknown Location"
    
    # Price info
    price = property_data.get('price', 'Price not specified') or 'Price not specified'
    price_numeric = property_data.get('price_numeric')
    if price_numeric is not None:
        price = format_currency(price_numeric)
    
    # Property details
    property_type = property_data.get('property_type', 'Not specified').capitalize() or 'Not specified'
    offering_type = property_data.get('offering_type', 'Not specified').capitalize() or 'Not specified'
    
    # Rooms
    living_area = property_data.get('living_area', 0) or 0
    
    rooms = property_data.get('rooms', 0) or 0
    rooms_str = f"{rooms}" if rooms else "N/A"
    
    bedrooms = property_data.get('bedrooms', 0) or 0
    bedrooms_str = f"{bedrooms}" if bedrooms else "N/A"

    interior = property_data.get('interior', 'N/A') or 'N/A'
    
    # Dates
    date_listed = format_date(property_data.get('date_listed', '') or '')
    date_available = format_date(property_data.get('date_available', '') or '')
    availability_period = format_date(property_data.get('availability_period', '') or '')
    
    # Energy label
    energy_label = property_data.get('energy_label', 'N/A') or 'N/A'

    # Service costs
    service_costs = property_data.get('service_costs', 0) or 0

    # Contrusction year
    construction_year = property_data.get('construction_year', 0) or 0
    construction_year_str = f"{construction_year}" if construction_year else "N/A"
    
    # Additional extras
    extras = []
    if property_data.get('balcony'):
        extras.append("Balcony")
    if property_data.get('garden'):
        extras.append("Garden")
    if property_data.get('parking'):
        extras.append("Parking")

    # Requirements
    requirements = property_data.get('features')
    requirements_parts = []

    if requirements:
        all_req_keys = set()
        for item in requirements:
            all_req_keys.update(item.keys())
        
        # Helper function to get value for a specific key
        def get_value_for_key(data_array, key):
            for item in data_array:
                if key in item:
                    return item[key]
            return None
        
        # Check for age requirements
        if 'age_requirement' in all_req_keys:
            requirements_parts.append(f"• Age requirement: {get_value_for_key(requirements, 'age_requirement')}")
        
        # Check for key profession requirement
        if 'key_profession_requirement' in all_req_keys:
            requirements_parts.append(f"• Profession: {get_value_for_key(requirements, 'key_profession_requirement')}")

        if 'utilities_included' in all_req_keys:
            requirements_parts.append(f"• Utilities included: {get_value_for_key(requirements, 'utilities_included')}")

        if 'has_lift' in all_req_keys:
            requirements_parts.append(f"• Has lift: {get_value_for_key(requirements, 'has_lift')}")

        if 'floor' in all_req_keys:
            requirements_parts.append(f"• Floor: {get_value_for_key(requirements, 'floor')}")

        if 'student_housing' in all_req_keys:
            requirements_parts.append(f"• Student housing: {get_value_for_key(requirements, 'student_housing')}")

        if 'target_group' in all_req_keys:
            requirements_parts.append(f"• Target group: {get_value_for_key(requirements, 'target_group')}")

        if 'contract_type' in all_req_keys:
            requirements_parts.append(f"• Contract type: {get_value_for_key(requirements, 'contract_type')}")

        if 'publication_module' in all_req_keys:
            requirements_parts.append(f"• Module: {get_value_for_key(requirements, 'publication_module')}")

    # Description (truncated)
    description = property_data.get('description', '') or ''
    if description:
        description = clean_html(description)
        description = truncate_text(description, 200)

    # URL
    url = property_data.get('url', '') or ''
    source = property_data.get('source', '').capitalize() or ''

    # Handle Regio source names
    if "Regio" in source:
        # Find where "regio" ends
        split_point = len("Regio")
        # Split and capitalize appropriately
        source = source[:split_point].capitalize() + " " + source[split_point:].capitalize()

    # Create message with HTML formatting
    message = (
        f"🏠 <b>{location}</b>\n"
        f"💰 <b>€{int(price_numeric)} per month</b>\n\n"
        f"<b>Details:</b>\n"
        f"• Type: {property_type}\n"
    )

    living_area_str = f"{living_area} m²" if living_area else ""
    if living_area_str:
        message += f"• Size: {living_area_str}\n"

    # Custom
    rooms_part = ""
    if rooms and bedrooms:
        rooms_part = f"• Rooms: {rooms_str} (Bedrooms: {bedrooms_str})\n"
    elif rooms and not bedrooms:
        rooms_part = f"• Rooms: {rooms_str}\n"
    elif not rooms and bedrooms:
        rooms_part = f"• Bedrooms: {bedrooms_str}\n"

    if rooms_part:
        message += f"{rooms_part}"

    service_costs_part = f"• Service costs: €{service_costs} per month\n" if service_costs != 0 else ""

    if service_costs_part:
        message += f"{service_costs_part}"

    interior_part = f"• Interior: {interior.title()}\n" if interior != "N/A" else ""

    if interior_part:
        message += f"{interior_part}"

    energy_label_part = f"• Energy label: {energy_label}\n" if energy_label != "N/A" else ""

    if energy_label_part:
        message += f"{energy_label_part}"

    construction_year_part = f"• Construction year: {construction_year_str}\n" if construction_year_str != "N/A" else ""

    if construction_year_part:
        message += f"{construction_year_part}"
    
    # Add dates if available
    if date_available != "N/A":
        message += f"• Available from: {date_available}\n"
    if availability_period != "N/A":
        message += f"• Availability period: {availability_period}\n"

    # Add requirements section if we have any
    if requirements_parts:
        message += f"\n<b>Additional information:</b>\n"
        for req in requirements_parts:
            message += f"{req}\n"
    
    # Add description if available
    # if description:
    #     message += f"\n<i>{description}</i>\n"
    
    # Add source info
    if source and url:
        message += f"\nSource: {source}"
    
    return message