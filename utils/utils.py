from config import ALL_CITIES
from typing import Dict, Any, Optional

def levenshtein_distance(s1, s2):
    """
    Calculate the Levenshtein distance between two strings.
    This measures how many single-character edits are needed to change one string into another.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Calculate insertions, deletions, and substitutions
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            
            # Get the minimum of the three operations
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]

def suggest_city(query, max_distance=3, max_suggestions=3):
    """
    Suggest similar cities based on string similarity.
    
    Args:
        query: The city name to look for
        max_distance: Maximum Levenshtein distance to consider
        max_suggestions: Maximum number of suggestions to return
    
    Returns:
        List of suggested cities
    """
    query = query.upper()
    
    # If exact match exists, no need for suggestions
    if query in ALL_CITIES:
        return []
    
    # Calculate distances to all cities
    distances = [(city, levenshtein_distance(query, city)) for city in ALL_CITIES]
    
    # Sort by distance (closest first) and filter by max_distance
    suggestions = [city for city, distance in sorted(distances, key=lambda x: x[1]) 
                  if distance <= max_distance]
    
    # Return limited number of suggestions
    return suggestions[:max_suggestions]

def construct_full_address(property_data: Dict[str, Any], include_neighborhood: bool = True) -> str:
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
    if include_neighborhood and neighborhood and isinstance(neighborhood, str) and neighborhood not in address:
        location_parts.append(neighborhood)
    if postal_code and isinstance(postal_code, str):
        location_parts.append(postal_code)
    if city and isinstance(city, str):
        location_parts.append(city.title())
    return ", ".join(location_parts) or "Unknown Location"  