"""
Enhanced HTTP client utility with robust compression handling and browser emulation.
"""

import asyncio
import logging
import random
import time
from typing import Optional, Dict, Any, List, Union

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import USER_AGENTS, HTTP_TIMEOUT, USE_PROXIES, PROXY_LIST

logger = logging.getLogger(__name__)


class EnhancedHttpClient:
    """HTTP client with enhanced decompression, browser emulation, and proxy support"""
    
    def __init__(self, 
                 timeout: float = HTTP_TIMEOUT,
                 max_retries: int = 3, 
                 retry_min_wait: int = 1,
                 retry_max_wait: int = 10,
                 semaphore: Optional[asyncio.Semaphore] = None,
                 use_proxies: bool = USE_PROXIES,
                 proxy_list: Optional[List[str]] = None):
        """
        Initialize the HTTP client
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_min_wait: Minimum wait time between retries in seconds
            retry_max_wait: Maximum wait time between retries in seconds
            semaphore: Optional semaphore for limiting concurrent requests
            use_proxies: Whether to use proxies for requests
            proxy_list: List of proxy URLs to use (if None, uses PROXY_LIST from config)
        """
        self.timeout = httpx.Timeout(timeout)
        self.max_retries = max_retries
        self.retry_min_wait = retry_min_wait
        self.retry_max_wait = retry_max_wait
        self.semaphore = semaphore or asyncio.Semaphore(10)
        self.use_proxies = use_proxies
        self.proxy_list = proxy_list if proxy_list is not None else PROXY_LIST
        
        # Import compression libraries
        self._import_compression_libs()
        
        if self.use_proxies and not self.proxy_list:
            logger.warning("Proxy usage enabled but no proxies provided. Disabling proxy usage.")
            self.use_proxies = False
    
    def _import_compression_libs(self):
        """Import compression libraries if available"""
        self.gzip_available = False
        self.brotli_available = False
        self.zlib_available = False
        
        try:
            import gzip
            self.gzip = gzip
            self.gzip_available = True
        except ImportError:
            logger.warning("gzip module not available")
        
        try:
            import brotli
            self.brotli = brotli
            self.brotli_available = True
        except ImportError:
            logger.warning("brotli module not available")
        
        try:
            import zlib
            self.zlib = zlib
            self.zlib_available = True
        except ImportError:
            logger.warning("zlib module not available")
    
    def _get_browser_headers(self) -> Dict[str, str]:
        """Get full browser-like headers to avoid detection"""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "sec-ch-ua": "\"Google Chrome\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "DNT": "1",
            "Cache-Control": "max-age=0",
            "Referer": "https://www.google.com/",
            "Priority": "u=0, i"
        }
    
    def _get_random_proxy(self) -> Optional[str]:
        """Get a random proxy from the list"""
        if not self.use_proxies or not self.proxy_list:
            return None
        return random.choice(self.proxy_list)
    
    def _try_decompress_content(self, content: bytes, encoding: Optional[str] = None) -> bytes:
        """
        Try to decompress content using multiple methods
        
        Args:
            content: The compressed content
            encoding: Content-Encoding header value
            
        Returns:
            bytes: The decompressed content or original content if decompression fails
        """
        if not encoding:
            return content
        
        encoding = encoding.lower()
        
        # Try gzip decompression
        if 'gzip' in encoding and self.gzip_available:
            try:
                return self.gzip.decompress(content)
            except Exception as e:
                logger.warning(f"gzip decompression failed: {e}")
        
        # Try brotli decompression
        if 'br' in encoding and self.brotli_available:
            try:
                return self.brotli.decompress(content)
            except Exception as e:
                logger.warning(f"brotli decompression failed: {e}")
        
        # Try deflate decompression
        if 'deflate' in encoding and self.zlib_available:
            try:
                return self.zlib.decompress(content)
            except Exception as e:
                # Try with raw deflate (no zlib header)
                try:
                    return self.zlib.decompress(content, -self.zlib.MAX_WBITS)
                except Exception as e2:
                    logger.warning(f"deflate decompression failed: {e}, {e2}")
        
        # Return original content if all decompression methods failed
        return content
    
    def _decode_content(self, content: bytes, charset: Optional[str] = None) -> str:
        """
        Decode bytes to string using various encodings
        
        Args:
            content: The content bytes
            charset: The charset from Content-Type header
            
        Returns:
            str: The decoded string
        """
        if charset:
            try:
                return content.decode(charset)
            except UnicodeDecodeError:
                logger.warning(f"Failed to decode with charset {charset}")
        
        # Try common encodings
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                return content.decode(encoding)
            except UnicodeDecodeError:
                continue
        
        # If all decodings fail, use latin1 as a fallback (it never fails)
        return content.decode('latin1')
    
    def _extract_charset(self, content_type: Optional[str]) -> Optional[str]:
        """Extract charset from Content-Type header"""
        if not content_type:
            return None
        
        if "charset=" in content_type.lower():
            parts = content_type.split(';')
            for part in parts:
                if "charset=" in part.lower():
                    return part.split('=')[-1].strip().lower()
        
        return None
    
    async def get(self, url: str, retry_anti_bot: bool = True, **kwargs) -> httpx.Response:
        """
        Make an HTTP GET request with advanced handling for compressed responses and anti-bot measures
        
        Args:
            url: URL to request
            retry_anti_bot: Whether to retry with different headers if anti-bot detection is suspected
            **kwargs: Additional keyword arguments for httpx.AsyncClient.get
            
        Returns:
            httpx.Response: Response object with text properly decoded
            
        Raises:
            httpx.RequestError: If the request fails after all retries
        """
        headers = self._get_browser_headers()
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        
        # Add cookies if needed for sites that use cookie-based sessions
        cookies = kwargs.pop("cookies", {})
        
        # Get a random proxy if enabled
        proxy = self._get_random_proxy()
        if proxy:
            logger.debug(f"Using proxy: {proxy}")
        
        client_kwargs = {
            "timeout": self.timeout,
            "follow_redirects": True,
        }
        if proxy:
            client_kwargs["proxies"] = proxy
        
        async with self.semaphore:
            try:
                async with httpx.AsyncClient(**client_kwargs) as client:
                    # Add random delay to seem more human-like
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                    
                    # Make the request
                    response = await client.get(url, headers=headers, cookies=cookies, **kwargs)
                    
                    # Check for too many redirects
                    if len(response.history) > 10:
                        logger.warning(f"Too many redirects for {url}")
                        raise httpx.RequestError(f"Too many redirects", request=response.request)
                    
                    # Check for common error responses
                    if response.status_code == 429:  # Too Many Requests
                        logger.warning(f"Rate limited on {url}. Waiting before retry.")
                        retry_after = int(response.headers.get("Retry-After", "5"))
                        await asyncio.sleep(retry_after)
                        raise httpx.RequestError(f"Rate limited: {response.status_code}", request=response.request)
                    
                    elif response.status_code >= 400:
                        logger.warning(f"HTTP {response.status_code} for {url}")
                        if response.status_code == 404:  # Not Found
                            # Don't retry 404s
                            return response
                        raise httpx.RequestError(f"HTTP error: {response.status_code}", request=response.request)
                    
                    # Check for anti-bot measures
                    if retry_anti_bot and "Je bent bijna op de pagina die je zoekt" in response.text or "robot" in response.text.lower() or "captcha" in response.text.lower():
                        logger.warning(f"Bot detection triggered for {url}. Retrying with different approach.")
                        
                        # Wait longer before retrying
                        await asyncio.sleep(random.uniform(3.0, 6.0))
                        
                        # Try again with different user-agent and IP
                        new_headers = self._get_browser_headers()
                        if self.use_proxies:
                            new_proxy = self._get_random_proxy() 
                            client_kwargs["proxies"] = new_proxy
                        
                        # Add additional cookies for the retry
                        cookies = {
                            'session_depth': '1',
                            'has_js': '1',
                            'resolution': '1920x1080',
                            'accept_cookies': 'true'
                        }
                        
                        async with httpx.AsyncClient(**client_kwargs) as retry_client:
                            response = await retry_client.get(url, headers=new_headers, cookies=cookies, **kwargs)
                    
                    # Handle content decoding/decompression
                    content_encoding = response.headers.get("content-encoding", "")
                    content_type = response.headers.get("content-type", "")
                    charset = self._extract_charset(content_type)
                    
                    # If content is empty or seems binary, try manual decompression
                    if response.status_code == 200 and (not response.text or len(response.text) < 100 or b'\x00' in response.content):
                        # Try manual decompression
                        decompressed_content = self._try_decompress_content(response.content, content_encoding)
                        
                        # Decode decompressed content
                        text = self._decode_content(decompressed_content, charset)
                        
                        # Override response text
                        response._text = text
                    
                    return response
            
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.error(f"Request error for {url}: {e}")
                raise
    
    async def get_with_fallback(self, url: str, **kwargs) -> httpx.Response:
        """
        Make an HTTP GET request with proxy first, then fall back to direct connection if proxy fails
        
        Args:
            url: URL to request
            **kwargs: Additional keyword arguments for httpx.AsyncClient.get
            
        Returns:
            httpx.Response: Response object if successful
            
        Raises:
            httpx.RequestError: If both proxy and direct requests fail
        """
        if not self.use_proxies:
            return await self.get(url, **kwargs)
        
        try:
            # First try with proxy
            return await self.get(url, **kwargs)
        except httpx.RequestError as e:
            logger.warning(f"Proxy request failed for {url}. Falling back to direct connection.")
            # Temporarily disable proxies and try again
            self.use_proxies = False
            try:
                return await self.get(url, **kwargs)
            finally:
                # Re-enable proxies for future requests
                self.use_proxies = True