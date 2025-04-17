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

from config import HTTP_TIMEOUT, USE_PROXIES, PROXY_LIST

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
        """Get full browser-like headers with consistent platform and browser information"""
        # Randomly select a browser profile
        browser_profile = random.choice([
            {
                "name": "Chrome Windows",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
                "platform": "Windows",
                "sec_ch_ua": "\"Chromium\";v=\"112\", \"Google Chrome\";v=\"112\", \"Not:A-Brand\";v=\"99\"",
                "sec_ch_ua_mobile": "?0",
                "sec_ch_ua_platform": "\"Windows\""
            },
            {
                "name": "Chrome macOS",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
                "platform": "macOS",
                "sec_ch_ua": "\"Chromium\";v=\"112\", \"Google Chrome\";v=\"112\", \"Not:A-Brand\";v=\"99\"",
                "sec_ch_ua_mobile": "?0",
                "sec_ch_ua_platform": "\"macOS\""
            },
            {
                "name": "Chrome Linux",
                "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
                "platform": "Linux",
                "sec_ch_ua": "\"Chromium\";v=\"112\", \"Google Chrome\";v=\"112\", \"Not:A-Brand\";v=\"99\"",
                "sec_ch_ua_mobile": "?0",
                "sec_ch_ua_platform": "\"Linux\""
            },
            {
                "name": "Firefox Windows",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
                "platform": "Windows",
                # Firefox doesn't send sec-ch-ua headers
                "sec_ch_ua": None,
                "sec_ch_ua_mobile": None,
                "sec_ch_ua_platform": None
            },
            {
                "name": "Safari macOS",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
                "platform": "macOS",
                # Safari doesn't send sec-ch-ua headers
                "sec_ch_ua": None,
                "sec_ch_ua_mobile": None,
                "sec_ch_ua_platform": None
            },
            {
                "name": "Edge Windows",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48",
                "platform": "Windows",
                "sec_ch_ua": "\"Chromium\";v=\"112\", \"Microsoft Edge\";v=\"112\", \"Not:A-Brand\";v=\"99\"",
                "sec_ch_ua_mobile": "?0",
                "sec_ch_ua_platform": "\"Windows\""
            }
        ])
        
        # Build base headers
        headers = {
            "User-Agent": browser_profile["user_agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
            "DNT": "1"
        }
        
        # Add browser-specific headers
        if "Chrome" in browser_profile["name"] or "Edge" in browser_profile["name"]:
            headers.update({
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate", 
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Priority": "u=0, i"
            })
        
        # Add sec-ch-ua headers for browsers that support them
        if browser_profile["sec_ch_ua"]:
            headers["sec-ch-ua"] = browser_profile["sec_ch_ua"]
            headers["sec-ch-ua-mobile"] = browser_profile["sec_ch_ua_mobile"]
            headers["sec-ch-ua-platform"] = browser_profile["sec_ch_ua_platform"]
        
        # Add a referer (50% chance of Google, 50% chance of another popular site)
        referers = [
            "https://www.google.com/",
            "https://www.google.nl/",
            "https://www.bing.com/",
            "https://duckduckgo.com/",
            "https://www.startpage.com/"
        ]
        headers["Referer"] = random.choice(referers)
        
        return headers
    
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
        
    async def get(self, url: str, retry_anti_bot: bool = True, max_antibot_retries: int = 8, **kwargs) -> httpx.Response:
        """
        Make an HTTP GET request with advanced handling for compressed responses and anti-bot measures
        
        Args:
            url: URL to request
            retry_anti_bot: Whether to retry with different headers if anti-bot detection is suspected
            max_antibot_retries: Maximum number of anti-bot retry attempts
            **kwargs: Additional keyword arguments for httpx.AsyncClient.get
            
        Returns:
            httpx.Response: Response object with text properly decoded
            
        Raises:
            httpx.RequestError: If the request fails after all retries
        """
        # Track anti-bot retries
        antibot_retry_count = 0
        
        # Create a list of browser profiles to rotate through
        browser_profiles = [
            # Regular profiles from _get_browser_headers
            {"name": "Default Browser", "custom": False},
            # Custom profiles for anti-bot evasion
            {
                "name": "Desktop Chrome",
                "custom": True,
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": url.split('/')[0] + '//' + url.split('/')[2] + '/',
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "Cache-Control": "max-age=0"
                },
                "cookies": {
                    'session_depth': str(random.randint(1, 5)),
                    'has_js': '1',
                    'resolution': f"{random.choice([1920, 1440, 1366, 1280])}x{random.choice([1080, 900, 768, 720])}",
                    'accept_cookies': 'true'
                }
            },
            {
                "name": "Mobile Safari",
                "custom": True,
                "headers": {
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "nl-NL,nl;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": "https://www.google.com/",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache"
                },
                "cookies": {
                    'session_depth': '3',
                    'has_js': '1',
                    'resolution': '375x812',
                    'accept_cookies': 'true',
                    'cookieConsent': 'true',
                    'device': 'mobile'
                }
            },
            {
                "name": "Desktop Safari",
                "custom": True,
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "nl-NL,nl;q=0.8,en-US;q=0.5,en;q=0.3",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "DNT": "1",
                    "Upgrade-Insecure-Requests": "1"
                },
                "cookies": {
                    'session_depth': '5',
                    'has_js': '1',
                    'resolution': '1440x900',
                    'accept_cookies': 'true',
                    'visited_before': 'true',
                    'lastVisit': str(int(time.time())),
                    'consent_level': 'ALL'
                }
            },
            {
                "name": "Firefox Linux",
                "custom": True,
                "headers": {
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "DNT": "1",
                    "Upgrade-Insecure-Requests": "1"
                },
                "cookies": {
                    'session_depth': '2',
                    'has_js': '1',
                    'resolution': '1920x1080',
                    'accept_cookies': 'true'
                }
            },
            {
                "name": "Edge Windows",
                "custom": True,
                "headers": {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.57",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Referer": "https://www.bing.com/",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "cross-site",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "sec-ch-ua": "\"Microsoft Edge\";v=\"113\", \"Chromium\";v=\"113\", \"Not-A.Brand\";v=\"24\"",
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": "\"Windows\""
                },
                "cookies": {
                    'session_depth': '4',
                    'has_js': '1',
                    'resolution': '1366x768',
                    'accept_cookies': 'true',
                    'visited_before': 'true'
                }
            }
        ]
        
        # Keep trying until we exhaust anti-bot retries
        while antibot_retry_count <= max_antibot_retries:
            # Select appropriate browser profile based on retry count
            if antibot_retry_count == 0:
                # For first attempt, use standard browser headers
                current_profile = browser_profiles[0]
                headers = self._get_browser_headers()
                cookies = kwargs.pop("cookies", {})
            else:
                # For retries, rotate through different profiles
                profile_index = min(antibot_retry_count, len(browser_profiles) - 1)
                current_profile = browser_profiles[profile_index]
                
                if current_profile["custom"]:
                    headers = current_profile["headers"]
                    cookies = current_profile["cookies"].copy()
                else:
                    # If we've gone through all custom profiles, use _get_browser_headers again
                    # but force a different profile than before
                    headers = self._get_browser_headers()
                    cookies = kwargs.pop("cookies", {})
                    # Add some custom cookies to make it more distinct
                    cookies.update({
                        'session_depth': str(random.randint(5, 10)),
                        'has_js': '1',
                        'resolution': f"{random.choice([1920, 1440, 1366, 1280])}x{random.choice([1080, 900, 768, 720])}",
                        'accept_cookies': 'true',
                        'visited_before': 'true',
                        'lastVisit': str(int(time.time()) - random.randint(3600, 86400))
                    })
            
            # Apply any custom headers from kwargs
            if "headers" in kwargs:
                custom_headers = kwargs.pop("headers")
                headers.update(custom_headers)
            
            # Get a random proxy if enabled
            proxy = self._get_random_proxy() if self.use_proxies else None
            if proxy and antibot_retry_count > 0:
                # Always try to get a new proxy on anti-bot retries
                proxy = self._get_random_proxy()
                logger.debug(f"Using proxy for anti-bot retry {antibot_retry_count}: {proxy}")
            
            client_kwargs = {
                "timeout": self.timeout,
                "follow_redirects": True,
            }
            if proxy:
                client_kwargs["proxies"] = proxy
            
            # Add progressively more wait time with each retry
            if antibot_retry_count > 0:
                retry_delay = random.uniform(2.0 * antibot_retry_count, 5.0 * antibot_retry_count)
                logger.info(f"Anti-bot retry {antibot_retry_count}/{max_antibot_retries} using {current_profile['name']} profile for {url}, waiting {retry_delay:.1f} seconds...")
                await asyncio.sleep(retry_delay)
            
            async with self.semaphore:
                try:
                    async with httpx.AsyncClient(**client_kwargs) as client:
                        # Add random delay to seem more human-like (longer with each retry)
                        await asyncio.sleep(random.uniform(0.5, 2.0) * (1 + antibot_retry_count))
                        
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
                        
                        # Check for anti-bot measures if enabled
                        if retry_anti_bot and antibot_retry_count < max_antibot_retries:
                            # Patterns that indicate anti-bot measures
                            anti_bot_patterns = [
                                "Je bent bijna op de pagina die je zoekt",
                                "We houden ons platform graag veilig en spamvrij",
                                "robot",
                                "captcha",
                                "CloudFare",
                                "DDoS protection",
                                "Ik ben geen robot",
                                "Just a moment"
                            ]
                            
                            # Check if any anti-bot pattern is found in the response
                            anti_bot_detected = False
                            for pattern in anti_bot_patterns:
                                if pattern.lower() in response.text.lower():
                                    logger.warning(f"Anti-bot pattern detected: '{pattern}'")
                                    anti_bot_detected = True
                                    break
                            
                            if anti_bot_detected:
                                logger.warning(f"Anti-bot measures detected (retry {antibot_retry_count}/{max_antibot_retries}) for {url}")
                                antibot_retry_count += 1
                                continue  # Skip to next retry
                        
                        # If we got here, the request was successful or we're out of retries
                        if antibot_retry_count > 0:
                            logger.info(f"Successfully bypassed anti-bot measures after {antibot_retry_count} retries for {url}")
                        
                        return response
                
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    logger.error(f"Request error for {url}: {e}")
                    raise
            
            # If we're not retrying anti-bot or didn't detect anti-bot measures, exit the loop
            break
        
        # If we've exhausted all retries and still hit anti-bot measures, return the last response
        return response
    
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