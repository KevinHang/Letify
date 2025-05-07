"""
Factory for creating scraper instances.
"""

from typing import Dict, Any

from config import SITE_CONFIGS
from scrapers.base import BaseScraperStrategy
from scrapers.funda import FundaScraper
from scrapers.pararius import ParariusScraper
from scrapers.vesteda import VestedaScraper
from scrapers.rebo import REBOScraper
from scrapers.kamernet import KamernetScraper
from scrapers.woningnet import WoningNetScraper
from scrapers.huurwoningen import HuurwoningenScraper
from scrapers.bouwinvest import WonenBijBouwinvestScraper


class RealEstateScraperFactory:
    """Factory class to create the appropriate scraper based on the site name"""
    
    @staticmethod
    def create_scraper(site_name: str) -> BaseScraperStrategy:
        """Create a scraper for the given site"""
        site_name = site_name.lower()
        
        if site_name not in SITE_CONFIGS:
            raise ValueError(f"Unsupported site: {site_name}")
        
        if site_name == "funda":
            return FundaScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name == "pararius":
            return ParariusScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name == "vesteda":
            return VestedaScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name == "rebo":
            return REBOScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name == "kamernet":
            return KamernetScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name == "bouwinvest":
            return WonenBijBouwinvestScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name in ["huurwoningenappartement", "huurwoningenhuis", "huurwoningenstudio", "huurwoningenkamer"]:
            return HuurwoningenScraper(site_name, SITE_CONFIGS[site_name])
        elif site_name in ["regioamsterdam", "regioutrecht", "regiogroningen", "regiogooienvecht", "regioalmere", "regiomiddenholland", "regioeemvallei", "regiobovengroningen", "regiowoonkeus", "regiowoongaard", "regiohuiswaarts"]:
            return WoningNetScraper(site_name, SITE_CONFIGS[site_name])
        else:
            raise ValueError(f"No scraper implementation for site: {site_name}")

    @staticmethod
    def get_available_scrapers() -> Dict[str, Dict[str, Any]]:
        """Return a dictionary of available scrapers and their configs"""
        return SITE_CONFIGS