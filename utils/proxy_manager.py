"""
Proxy management for HTTP requests.

Provides functionality for managing and rotating proxies,
tracking proxy health, and fetching new proxies as needed.
"""

import random
import logging
import asyncio
import time
from typing import List, Dict, Any, Optional, Set, Tuple

import httpx

from config import (
    USE_PROXIES,
    PROXY_LIST,
    PROXY_ROTATION_STRATEGY,
    MAX_PROXY_FAILURES,
    PROXY_API_ENDPOINT,
    PROXY_API_KEY,
    get_formatted_proxy_list
)

logger = logging.getLogger(__name__)


class ProxyManager:
    """Manages a pool of proxies with health tracking and rotation"""
    
    def __init__(self, 
                 enabled: bool = USE_PROXIES,
                 proxy_list: Optional[List[str]] = None,
                 rotation_strategy: str = PROXY_ROTATION_STRATEGY,
                 max_failures: int = MAX_PROXY_FAILURES):
        """
        Initialize the proxy manager
        
        Args:
            enabled: Whether to use proxies
            proxy_list: List of proxy URLs (if None, uses the formatted list from config)
            rotation_strategy: Strategy for rotating proxies ("round_robin", "random", "fallback")
            max_failures: Maximum failures before marking a proxy as unhealthy
        """
        self.enabled = enabled
        self._proxy_list = proxy_list if proxy_list is not None else get_formatted_proxy_list()
        self.rotation_strategy = rotation_strategy
        self.max_failures = max_failures
        
        # Track proxy health and usage
        self._proxy_health: Dict[str, Dict[str, Any]] = {}
        self._proxy_index = 0
        self._active_proxies: Set[str] = set()
        self._lock = asyncio.Lock()
        
        # Initialize health tracking for all proxies
        for proxy in self._proxy_list:
            self._proxy_health[proxy] = {
                "failures": 0,
                "successes": 0,
                "last_used": 0,
                "last_success": 0,
                "avg_response_time": 0,
                "healthy": True
            }
        
        if self.enabled and not self._proxy_list:
            logger.warning("Proxy usage enabled but no proxies provided. Disabling proxy usage.")
            self.enabled = False
    
    @property
    def healthy_proxies(self) -> List[str]:
        """Get a list of healthy proxies"""
        return [p for p in self._proxy_list if self._proxy_health[p]["healthy"]]
    
    @property
    def proxy_count(self) -> int:
        """Get the total number of proxies"""
        return len(self._proxy_list)
    
    @property
    def healthy_count(self) -> int:
        """Get the number of healthy proxies"""
        return len(self.healthy_proxies)
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """Get statistics about proxy usage"""
        return {
            "total_proxies": self.proxy_count,
            "healthy_proxies": self.healthy_count,
            "enabled": self.enabled,
            "rotation_strategy": self.rotation_strategy,
            "proxy_health": self._proxy_health
        }
    
    async def get_proxy(self) -> Optional[str]:
        """
        Get a proxy according to the rotation strategy
        
        Returns:
            Optional[str]: A proxy URL or None if proxies are disabled or no healthy proxies
        """
        if not self.enabled:
            return None
        
        async with self._lock:
            healthy_proxies = self.healthy_proxies
            if not healthy_proxies:
                logger.warning("No healthy proxies available")
                return None
            
            if self.rotation_strategy == "random":
                proxy = random.choice(healthy_proxies)
            elif self.rotation_strategy == "round_robin":
                self._proxy_index = (self._proxy_index + 1) % len(healthy_proxies)
                proxy = healthy_proxies[self._proxy_index]
            else:  # fallback or any other strategy defaults to random
                proxy = random.choice(healthy_proxies)
            
            # Update usage tracking
            self._proxy_health[proxy]["last_used"] = time.time()
            self._active_proxies.add(proxy)
            
            return proxy
    
    async def report_success(self, proxy: str, response_time: float) -> None:
        """
        Report a successful proxy usage
        
        Args:
            proxy: The proxy URL that was used
            response_time: The response time in seconds
        """
        if not proxy or proxy not in self._proxy_health:
            return
        
        async with self._lock:
            stats = self._proxy_health[proxy]
            stats["successes"] += 1
            stats["last_success"] = time.time()
            stats["healthy"] = True
            
            # Update average response time using weighted average
            if stats["avg_response_time"] == 0:
                stats["avg_response_time"] = response_time
            else:
                stats["avg_response_time"] = (stats["avg_response_time"] * 0.9) + (response_time * 0.1)
            
            if proxy in self._active_proxies:
                self._active_proxies.remove(proxy)
    
    async def report_failure(self, proxy: str, error: Optional[Exception] = None) -> None:
        """
        Report a failed proxy usage
        
        Args:
            proxy: The proxy URL that was used
            error: The exception that occurred (if any)
        """
        if not proxy or proxy not in self._proxy_health:
            return
        
        async with self._lock:
            stats = self._proxy_health[proxy]
            stats["failures"] += 1
            
            # Mark proxy as unhealthy if it has too many failures
            if stats["failures"] >= self.max_failures:
                stats["healthy"] = False
                logger.warning(f"Proxy {proxy} marked as unhealthy after {stats['failures']} failures")
            
            if proxy in self._active_proxies:
                self._active_proxies.remove(proxy)
    
    async def reset_proxy(self, proxy: str) -> None:
        """
        Reset a proxy's health stats
        
        Args:
            proxy: The proxy URL to reset
        """
        if not proxy or proxy not in self._proxy_health:
            return
        
        async with self._lock:
            stats = self._proxy_health[proxy]
            stats["failures"] = 0
            stats["healthy"] = True
    
    async def reset_all_proxies(self) -> None:
        """Reset health stats for all proxies"""
        async with self._lock:
            for proxy in self._proxy_health:
                self._proxy_health[proxy]["failures"] = 0
                self._proxy_health[proxy]["healthy"] = True
    
    async def add_proxy(self, proxy: str) -> None:
        """
        Add a new proxy to the pool
        
        Args:
            proxy: The proxy URL to add
        """
        async with self._lock:
            if proxy not in self._proxy_health:
                self._proxy_list.append(proxy)
                self._proxy_health[proxy] = {
                    "failures": 0,
                    "successes": 0,
                    "last_used": 0,
                    "last_success": 0,
                    "avg_response_time": 0,
                    "healthy": True
                }
    
    async def remove_proxy(self, proxy: str) -> None:
        """
        Remove a proxy from the pool
        
        Args:
            proxy: The proxy URL to remove
        """
        async with self._lock:
            if proxy in self._proxy_list:
                self._proxy_list.remove(proxy)
            if proxy in self._proxy_health:
                del self._proxy_health[proxy]
            if proxy in self._active_proxies:
                self._active_proxies.remove(proxy)
    
    async def fetch_new_proxies(self) -> bool:
        """
        Fetch new proxies from the API endpoint if configured
        
        Returns:
            bool: True if new proxies were fetched successfully, False otherwise
        """
        if not PROXY_API_ENDPOINT or not PROXY_API_KEY:
            logger.warning("Proxy API endpoint or key not configured")
            return False
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    PROXY_API_ENDPOINT,
                    headers={"Authorization": f"Bearer {PROXY_API_KEY}"}
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to fetch proxies: HTTP {response.status_code}")
                    return False
                
                data = response.json()
                new_proxies = data.get("proxies", [])
                
                if not new_proxies:
                    logger.warning("No proxies returned from API")
                    return False
                
                # Add new proxies to the pool
                for proxy in new_proxies:
                    await self.add_proxy(proxy)
                
                logger.info(f"Added {len(new_proxies)} new proxies from API")
                return True
                
        except Exception as e:
            logger.error(f"Error fetching new proxies: {e}")
            return False