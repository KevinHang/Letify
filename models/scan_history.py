"""
Scan history data models.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ScanHistory:
    """Record of a scraping scan for a source and city"""
    source: str
    city: str
    url: str
    scan_time: datetime = datetime.now()
    new_listings_count: int = 0
    total_listings_count: int = 0
    scan_duration_seconds: float = 0.0
    id: Optional[int] = None