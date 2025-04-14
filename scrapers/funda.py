"""
Funda.nl scraper implementation.
"""

import re
from typing import List
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from models.property import PropertyListing, PropertyType, InteriorType
from scrapers.base import BaseScraperStrategy


class FundaScraper(BaseScraperStrategy):
    """Scraper strategy for Funda.nl"""
    
    async def build_search_url(self, city: str, days: int = 1, **kwargs) -> str:
        """Build a search URL for Funda"""
        return self.search_url_template.format(city=city.lower(), days=days)
    
    async def parse_search_page(self, html: str) -> List[str]:
        """Parse the Funda search results page for listing URLs"""
        parser = HTMLParser(html)
        listing_urls = []
        
        # Extract listing URLs from search page
        for link in parser.css("a[data-test-id='object-image-link']"):
            href = link.attributes.get("href")
            if href and "/detail/" in href:
                full_url = urljoin(self.base_url, href)
                listing_urls.append(full_url)
        
        return listing_urls
    
    async def parse_listing_page(self, html: str, url: str) -> PropertyListing:
        """Parse a Funda listing detail page"""
        parser = HTMLParser(html)
        listing = PropertyListing(source="funda", url=url)
        
        # Extract basic info
        title_elem = parser.css_first("h1 span:first-child")
        if title_elem:
            listing.title = title_elem.text().strip()
            listing.address = listing.title
        
        # Extract postal code and city
        address_elem = parser.css_first("h1 span.text-neutral-40")
        if address_elem:
            address_text = address_elem.text().strip()
            postal_city_match = re.match(r'(\d{4}\s*[A-Z]{2})\s+(.+)', address_text)
            if postal_city_match:
                listing.postal_code = postal_city_match.group(1)
                listing.city = postal_city_match.group(2)
        
        # Extract neighborhood
        neighborhood_elem = parser.css_first("h1 a[aria-label]")
        if neighborhood_elem:
            listing.neighborhood = neighborhood_elem.text().strip()
        
        # Extract price
        price_elem = parser.css_first(".mt-5 .flex.flex-col div")
        if price_elem:
            price_text = price_elem.text().strip()
            listing.price = price_text
            
            # Extract numeric price
            price_match = re.search(r'€\s*([\d\.,]+)', price_text)
            if price_match:
                price_str = price_match.group(1).replace(".", "").replace(",", ".")
                try:
                    listing.price_numeric = float(price_str)
                except ValueError:
                    pass
            
            # Extract price period
            if "/mnd" in price_text or "per month" in price_text:
                listing.price_period = "month"
            elif "/week" in price_text or "per week" in price_text:
                listing.price_period = "week"
        
        # Extract description
        description_elem = parser.css_first(".listing-description-text")
        if description_elem:
            listing.description = description_elem.text().strip()
        
        # Extract living area and bedrooms from the feature list
        for feature_elem in parser.css("ul.mt-2 li"):
            feature_text = feature_elem.text().strip()
            
            # Extract living area
            area_match = re.search(r'(\d+)\s*m²', feature_text)
            if area_match and not listing.living_area:
                listing.living_area = int(area_match.group(1))
            
            # Extract bedrooms
            bedrooms_match = re.search(r'(\d+)\s+slaapkamers', feature_text)
            if bedrooms_match and not listing.bedrooms:
                listing.bedrooms = int(bedrooms_match.group(1))
        
        # Extract property type
        property_type_elem = parser.css_first("dt:-soup-contains('Soort') + dd")
        if property_type_elem:
            property_text = property_type_elem.text().strip().lower()
            if "appartement" in property_text or "apartment" in property_text:
                listing.property_type = PropertyType.APARTMENT
            elif "woonhuis" in property_text or "house" in property_text:
                listing.property_type = PropertyType.HOUSE
            elif "studio" in property_text:
                listing.property_type = PropertyType.STUDIO
            elif "kamer" in property_text or "room" in property_text:
                listing.property_type = PropertyType.ROOM
        
        # Extract interior type
        interior_elem = parser.css_first("dt:-soup-contains('Specificaties') + dd")
        if interior_elem:
            interior_text = interior_elem.text().strip().lower()
            if "gemeubileerd" in interior_text or "furnished" in interior_text:
                listing.interior = InteriorType.FURNISHED
            elif "gestoffeerd" in interior_text or "upholstered" in interior_text:
                listing.interior = InteriorType.UPHOLSTERED
            elif "kaal" in interior_text or "shell" in interior_text:
                listing.interior = InteriorType.SHELL
        
        # Extract construction year
        year_elem = parser.css_first("dt:-soup-contains('Bouwjaar') + dd")
        if year_elem:
            year_match = re.search(r'\d{4}', year_elem.text())
            if year_match:
                listing.construction_year = int(year_match.group())
        
        # Extract energy label
        energy_label_elem = parser.css_first("span[class*='bg-[#']")
        if energy_label_elem:
            listing.energy_label = energy_label_elem.text().strip()
        
        # Extract coordinates from NUXT data
        nuxt_script = parser.css_first('script#__NUXT_DATA__')
        if nuxt_script:
            script_text = nuxt_script.text()
            lat_match = re.search(r'"lat":([\d\.]+)', script_text)
            lng_match = re.search(r'"lng":([\d\.]+)', script_text)
            
            if lat_match and lng_match:
                listing.coordinates = {
                    "lat": float(lat_match.group(1)),
                    "lng": float(lng_match.group(1))
                }
            
            # Extract source ID
            id_match = re.search(r'"globalId":(\d+)', script_text)
            if id_match:
                listing.source_id = id_match.group(1)
        
        # Extract image URLs
        for img in parser.css("img[alt^='Foto']"):
            src = img.attributes.get('src')
            if src and src not in listing.images:
                listing.images.append(src)
        
        return listing