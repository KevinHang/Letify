"""
HTML parsing utilities.
"""

import re
from typing import Optional, Dict, Any, List, Union

from selectolax.parser import HTMLParser, Node


def safe_extract_text(node: Optional[Node]) -> str:
    """Extract text from a node safely, handling None values"""
    if node is None:
        return ""
    return node.text().strip()


def safe_get_attribute(node: Optional[Node], attribute: str) -> Optional[str]:
    """Get an attribute from a node safely, handling None values"""
    if node is None:
        return None
    return node.attributes.get(attribute)


def extract_number(text: str, pattern: str = r'\d+') -> Optional[int]:
    """Extract a number from text using a regex pattern"""
    if not text:
        return None
    
    match = re.search(pattern, text)
    if match:
        try:
            return int(match.group())
        except ValueError:
            return None
    return None


def extract_price(text: str) -> Optional[float]:
    """Extract a price value from text, handling different formats"""
    if not text:
        return None
    
    # Look for price patterns like €1,234.56 or €1.234,56
    match = re.search(r'€\s*([\d\.,]+)', text)
    if match:
        # Handle different number formats
        price_str = match.group(1)
        
        # European format (1.234,56)
        if "," in price_str and "." in price_str and price_str.rindex(".") < price_str.rindex(","):
            price_str = price_str.replace(".", "").replace(",", ".")
        # US/UK format (1,234.56)
        elif "," in price_str and "." in price_str:
            price_str = price_str.replace(",", "")
        # Only commas (1,234)
        elif "," in price_str:
            price_str = price_str.replace(",", ".")
        
        try:
            return float(price_str)
        except ValueError:
            return None
    
    return None


def extract_area(text: str) -> Optional[int]:
    """Extract an area value in square meters from text"""
    if not text:
        return None
    
    # Look for patterns like 100 m², 100m2, 100 sq.m
    match = re.search(r'(\d+)\s*(?:m²|m2|sq\.?m)', text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    
    return None


def extract_rooms(text: str) -> Optional[int]:
    """Extract room count from text"""
    if not text:
        return None
    
    # Look for patterns like 3 rooms, 3 kamers, etc.
    match = re.search(r'(\d+)\s+(?:room|rooms|kamer|kamers|zimmer)', text.lower())
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    
    return None