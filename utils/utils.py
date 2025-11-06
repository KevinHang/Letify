from config import ALL_CITIES
from typing import Dict, Any, List


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


def get_source_status_summary(scan_rows: List[dict], properties: List[dict]) -> str:
    """
    Generate a clean, two-section status report:
    • Scraper Status (S): based on total_listings_count
    • Formatter Status (F): based on data quality of latest 3 properties
        - red circle if source missing from properties OR any required field missing
    """
    # === Source Name Mappings ===
    SCAN_NAME_MAP = {
        '123wonen': '123Wonen',
        'bouwinvest': 'Bouwinvest',
        'funda': 'Funda',
        'hollandrijnland': 'Holland Rijnland',
        'huurwoningenappartement': 'Huurwoningen (Apartment)',
        'huurwoningenhuis': 'Huurwoningen (House)',
        'huurwoningenkamer': 'Huurwoningen (Room)',
        'huurwoningenstudio': 'Huurwoningen (Studio)',
        'kamernet': 'Kamernet',
        'pararius': 'Pararius',
        'rebo': 'REBO',
        'regioalmere': 'Regio Almere',
        'regioamsterdam': 'Regio Amsterdam',
        'regioeemvallei': 'Regio Eemvallei',
        'regiogooienvecht': 'Regio Gooi en Vecht',
        'regiogroningen': 'Regio Groningen',
        'regiohuiswaarts': 'Regio Huiswaarts',
        'regiomiddenholland': 'Regio Midden-Holland',
        'regioutrecht': 'Regio Utrecht',
        'regiowoongaard': 'Regio Woongaard',
        'regiowoonkeus': 'Regio Woonkeus',
        'vbt': 'VB&T',
        'vesteda': 'Vesteda',
    }

    PROP_NAME_MAP = {
        '123wonen': '123Wonen',
        'hollandrijnland': 'Holland Rijnland',
        'funda': 'Funda',
        'huurwoningen': 'Huurwoningen',
        'pararius': 'Pararius',
        'rebo': 'REBO',
        'regioalmere': 'Regio Almere',
        'regioamsterdam': 'Regio Amsterdam',
        'regioeemvallei': 'Regio Eemvallei',
        'regiogooienvecht': 'Regio Gooi en Vecht',
        'regiogroningen': 'Regio Groningen',
        'regiohuiswaarts': 'Regio Huiswaarts',
        'regiomiddenholland': 'Regio Midden-Holland',
        'regioutrecht': 'Regio Utrecht',
        'regiowoongaard': 'Regio Woongaard',
        'regiowoonkeus': 'Regio Woonkeus',
        'vb&t': 'VB&T',
        'vesteda': 'Vesteda',
        'wonenbijbouwinvest': 'Bouwinvest',
        'kamernet': 'Kamernet',
    }

    # === Extract actual sources from properties (lowercase) ===
    actual_prop_sources = {p.get('source', '').strip().lower() for p in properties if p.get('source')}

    # === Group properties by source ===
    props_by_source = {}
    for p in properties:
        src = p.get('source', '').strip()
        if src:
            key = src.lower()
            props_by_source.setdefault(key, []).append(p)

    # === Scraper Status (S) ===
    scraper_lines = []
    for row in scan_rows:
        source = row.get('source', '').strip()
        if not source:
            continue
        count = row.get('total_listings_count', 0)
        icon = "🔴" if count == 0 else "🟢"
        name = SCAN_NAME_MAP.get(source, source.replace('_', ' ').title())
        scraper_lines.append(f"{icon} {name}")

    # === Formatter Status (F) ===
    formatter_lines = []
    for key, display_name in sorted(PROP_NAME_MAP.items(), key=lambda x: x[1]):
        # Check if this source exists in properties
        if key not in actual_prop_sources:
            formatter_lines.append(f"🔴 {display_name}")
            continue

        latest = props_by_source.get(key, [])[:3]
        required = {'source', 'url', 'title', 'address', 'city', 'price_numeric'}

        all_valid = bool(latest) and all(
            all(
                str(p.get(f) or '').strip() and
                (f != 'price_numeric' or p.get(f) not in (None, 0))
                for f in required
            )
            for p in latest
        )

        icon = "🟢" if all_valid else "🔴"
        formatter_lines.append(f"{icon} {display_name}")

    # === Sort lines ===
    scraper_lines.sort()
    formatter_lines.sort()

    # === Build final output ===
    output = ["<b>Scraper Status</b>:"]
    output.extend(scraper_lines)
    output.append("")
    output.append("<b>Formatter Status</b>:")
    output.extend(formatter_lines)

    return "\n".join(output)