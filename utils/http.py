"""
Enhanced HTTP client utility with robust compression handling and browser emulation.
"""

import asyncio
import random
import time
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
import uuid
import base64
import hashlib
import os

import httpx

from config import HTTP_TIMEOUT, USE_PROXIES, PROXY_LIST
from utils.logging_config import configure_logging

# Use a child logger of the scraper logger
logger = configure_logging(
    name="realestate_scraper.http_client", 
    log_file="logs/scraper.log"
)

class EnhancedHttpClient:
    """HTTP client with enhanced decompression, browser emulation, and proxy support"""
    
    # Browser profiles consolidated in one place
    BROWSER_PROFILES = [
        {
            "name": "Chrome Windows",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "platform": "Windows",
            "sec_ch_ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "sec_ch_ua_mobile": "?0",
            "sec_ch_ua_platform": '"Windows"',
            "sec_ch_ua_full_version": '"125.0.6422.113"',
            "sec_ch_ua_platform_version": '"10.0.0"',
            "sec_ch_ua_arch": '"x86"',
            "sec_ch_ua_bitness": '"64"',
            "is_mobile": False,
            "resolution": ["1920x1080", "1440x900", "1366x768", "1280x720"],
        },
        {
            "name": "Chrome macOS",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "platform": "macOS",
            "sec_ch_ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "sec_ch_ua_mobile": "?0",
            "sec_ch_ua_platform": '"macOS"',
            "sec_ch_ua_full_version": '"125.0.6422.113"',
            "sec_ch_ua_platform_version": '"14.5.0"',
            "sec_ch_ua_arch": '"arm"',
            "sec_ch_ua_bitness": '"64"',
            "is_mobile": False,
            "resolution": ["1920x1080", "1440x900", "1366x768", "1280x720"],
        },
        {
            "name": "Firefox Windows",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
            "platform": "Windows",
            "is_mobile": False,
            "resolution": ["1920x1080", "1440x900", "1366x768", "1280x720"],
        },
        {
            "name": "Safari macOS",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
            "platform": "macOS",
            "is_mobile": False,
            "resolution": ["1920x1080", "1440x900", "1366x768", "1280x720"],
        },
        {
            "name": "Edge Windows",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.2535.85",
            "platform": "Windows",
            "sec_ch_ua": '"Microsoft Edge";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "sec_ch_ua_mobile": "?0",
            "sec_ch_ua_platform": '"Windows"',
            "sec_ch_ua_full_version": '"125.0.2535.85"',
            "sec_ch_ua_platform_version": '"10.0.0"',
            "sec_ch_ua_arch": '"x86"',
            "sec_ch_ua_bitness": '"64"',
            "is_mobile": False,
            "resolution": ["1920x1080", "1440x900", "1366x768", "1280x720"],
        },
        {
            "name": "Mobile Chrome Android",
            "user_agent": "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
            "platform": "Android",
            "sec_ch_ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "sec_ch_ua_mobile": "?1",
            "sec_ch_ua_platform": '"Android"',
            "sec_ch_ua_full_version": '"125.0.6422.113"',
            "sec_ch_ua_platform_version": '"14.0.0"',
            "sec_ch_ua_arch": '"arm"',
            "sec_ch_ua_bitness": '"64"',
            "is_mobile": True,
            "resolution": ["375x812", "414x896", "360x800", "390x844"],
        },
        {
            "name": "Mobile Safari iOS",
            "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
            "platform": "iOS",
            "is_mobile": True,
            "resolution": ["375x812", "414x896", "360x800", "390x844"],
        },
        {
            "name": "Firefox Linux",
            "user_agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
            "platform": "Linux",
            "is_mobile": False,
            "resolution": ["1920x1080", "1440x900", "1366x768", "1280x720"],
        },
    ]
    
    # Language preferences
    LANGUAGE_PREFERENCES = [
        "en-US,en;q=0.9",
        "en-GB,en;q=0.9",
        "nl-NL,nl;q=0.9,en-US;q=0.8",
        "de-DE,de;q=0.9,en;q=0.8",
        "fr-FR,fr;q=0.9,en;q=0.8",
        "es-ES,es;q=0.9,en;q=0.8"
    ]
    
    # Common referers
    COMMON_REFERERS = [
        "https://www.google.com/",
        "https://www.bing.com/",
        "https://duckduckgo.com/",
        "https://www.yahoo.com/",
    ]
    
    # Anti-bot patterns
    ANTI_BOT_PATTERNS = [
        "Je bent bijna op de pagina die je zoekt",
        "We houden ons platform graag veilig en spamvrij",
        "captcha",
        "Cloudflare",
        "DDoS protection",
        "Ik ben geen robot",
        "Just a moment",
        "checking your browser",
        "security check",
        "challenge",
        "human verification",
    ]
    
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
        self.session_history = []  # Initialize session history for referer tracking
        
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
    
    def _get_browser_profile(self, profile_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a browser profile by name or a random one if name is None
        
        Args:
            profile_name: Name of the profile to get or None for random
            
        Returns:
            Dict[str, Any]: Browser profile
        """
        if profile_name:
            for profile in self.BROWSER_PROFILES:
                if profile["name"] == profile_name:
                    return profile
            logger.warning(f"Profile {profile_name} not found, using random profile")
            
        return random.choice(self.BROWSER_PROFILES)
            
    def _get_browser_headers(self, profile: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Get full browser-like headers for the specified profile or a random one"""
        if profile is None:
            profile = random.choice(self.BROWSER_PROFILES)
        
        # Define header order to mimic real browsers
        header_order = [
            "Host",
            "Connection",
            "Cache-Control",
            "sec-ch-ua",
            "sec-ch-ua-mobile",
            "sec-ch-ua-platform",
            "sec-ch-ua-full-version",
            "sec-ch-ua-platform-version",
            "sec-ch-ua-arch",
            "sec-ch-ua-bitness",
            "Upgrade-Insecure-Requests",
            "User-Agent",
            "Accept",
            "Sec-Fetch-Site",
            "Sec-Fetch-Mode",
            "Sec-Fetch-User",
            "Sec-Fetch-Dest",
            "Referer",
            "Accept-Encoding",
            "Accept-Language",
            "Cookie",
        ]

        # Build headers with consistent order
        headers = {
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": profile["user_agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": random.choice(self.LANGUAGE_PREFERENCES),
        }

        # Add browser-specific headers for Chromium browsers
        if "Chrome" in profile["name"] or "Edge" in profile["name"]:
            headers.update({
                "Sec-Fetch-Site": random.choice(["none", "same-origin", "cross-site"]),
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-User": "?1",
                "Sec-Fetch-Dest": "document",
            })

        # Add client hints if available in the profile
        for key in ["sec_ch_ua", "sec_ch_ua_mobile", "sec_ch_ua_platform", 
                   "sec_ch_ua_full_version", "sec_ch_ua_platform_version", 
                   "sec_ch_ua_arch", "sec_ch_ua_bitness"]:
            if value := profile.get(key):  
                headers[key.replace("_", "-")] = value

        # Add realistic referer (50% chance)
        if random.choice([True, False]):
            headers["Referer"] = random.choice(self.COMMON_REFERERS)

        # Ensure header order matches real browsers
        ordered_headers = {}
        for header in header_order:
            if header in headers:
                ordered_headers[header] = headers[header]
        for header, value in headers.items():
            if header not in ordered_headers:
                ordered_headers[header] = value

        return ordered_headers

    def _generate_cookies(self, url: str, profile: Dict[str, Any]) -> Dict[str, str]:
        """Generate realistic cookies to mimic a human user."""
        domain = urlparse(url).netloc
        timestamp = int(time.time())
        session_id = uuid.uuid4().hex[:16]  
        
        # Get appropriate resolution based on device type
        resolution = random.choice(profile["resolution"])

        # Base cookies
        cookies = {
            "session_id": session_id,
            "has_js": "1",
            "resolution": resolution,
            "accept_cookies": "true",
            "visited_before": "true",
            "last_visit": str(timestamp - random.randint(3600, 86400 * 7)),  
            "session_depth": str(random.randint(1, 10)),
            "_js_enabled": "true",
        }

        # Add analytics cookies
        cookies.update({
            "_ga": f"GA1.2.{random.randint(1000000000, 9999999999)}.{timestamp - random.randint(3600, 86400)}",  
            "_gid": f"GA1.2.{random.randint(1000000000, 9999999999)}.{timestamp - random.randint(3600, 86400)}",  
            "CookieConsent": "{stamp:'randomStamp',necessary:true,preferences:false,statistics:true,marketing:false}",  
        })

        # Add anti-bot cookies (50% chance)
        if random.choice([True, False]):  
            cookies.update({
                "__cf_bm": base64.urlsafe_b64encode(os.urandom(22)).decode('utf-8')[:30],  
                "bm_sz": hashlib.sha256(f"{session_id}{timestamp}".encode()).hexdigest()[:32],  
            })

        # Add site-specific cookies
        cookies[f"{domain}_session"] = session_id
        cookies[f"{domain}_consent"] = "granted"
        
        # Add mobile-specific cookies if applicable
        if profile.get("is_mobile", False):
            cookies["device"] = "mobile"

        return cookies
    
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
    
    def _detect_anti_bot(self, response: httpx.Response) -> bool:
        """
        Detect anti-bot measures in a response
        
        Args:
            response: HTTP response to check
            
        Returns:
            bool: True if anti-bot measures detected, False otherwise
        """
        # Check HTTP status that might indicate anti-bot measures
        if response.status_code in (403, 429, 503):
            logger.warning(f"Possible anti-bot response: HTTP {response.status_code}")
            return True
        
        # Check for anti-bot patterns in the response text
        for pattern in self.ANTI_BOT_PATTERNS:
            if pattern.lower() in response.text.lower():
                logger.warning(f"Anti-bot pattern detected: '{pattern}'")
                return True
        
        # Check for very short responses that might be anti-bot redirects
        if response.status_code == 200 and len(response.text) < 500 and any(p.lower() in response.text.lower() for p in ("javascript", "cookie", "redirect")):
            logger.warning("Possible anti-bot response: short content with JS/cookie/redirect")
            return True
        
        return False
        
    async def get(self, url: str, retry_anti_bot: bool = True, max_antibot_retries: int = 3, **kwargs) -> httpx.Response:
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
        # Track anti-bot retries and initialize session cookies
        antibot_retry_count = 0
        session_cookies = kwargs.pop("cookies", {})
        response = None  # Initialize response variable
        
        # Keep trying until we exhaust anti-bot retries
        while antibot_retry_count < max_antibot_retries + 1:  # +1 to ensure we try exactly max_antibot_retries times
            # For first attempt or retries, select different browser profiles
            if antibot_retry_count == 0:
                profile = self._get_browser_profile()
            else:
                # Avoid using the same profile for retries
                used_profiles = [p for p in self.BROWSER_PROFILES if p["name"] != profile["name"]]
                if not used_profiles:  # Fallback if somehow we don't have different profiles
                    used_profiles = self.BROWSER_PROFILES
                profile = random.choice(used_profiles)
                
            headers = self._get_browser_headers(profile)
            cookies = self._generate_cookies(url, profile)
            cookies.update(session_cookies)
            
            # If we're on a retry, add additional evasion cookies
            if antibot_retry_count > 0:
                cookies.update({
                    'session_depth': str(random.randint(5, 10)),
                    'visited_before': 'true',
                    'lastVisit': str(int(time.time()) - random.randint(3600, 86400)),
                    '_js_enabled': 'true',
                    'challengeSuccess': 'true',
                    'challenge_bypass': base64.urlsafe_b64encode(os.urandom(16)).decode('utf-8')[:22],
                })
            
            # Apply any custom headers from kwargs
            if "headers" in kwargs:
                custom_headers = kwargs.pop("headers")
                headers.update(custom_headers)
            
            # Set referer from session history if available
            if self.session_history:
                headers["Referer"] = self.session_history[-1]
            
            # Get a random proxy if enabled
            proxy = self._get_random_proxy() if self.use_proxies else None
            if proxy and antibot_retry_count > 0:
                proxy = self._get_random_proxy()  # Ensure new proxy on retries
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
                logger.info(f"Anti-bot retry {antibot_retry_count}/{max_antibot_retries} using {profile['name']} profile for {url}, waiting {retry_delay:.1f} seconds...")
                await asyncio.sleep(retry_delay)
            
            async with self.semaphore:
                try:
                    async with httpx.AsyncClient(**client_kwargs) as client:
                        # Add human-like random delay
                        await asyncio.sleep(random.uniform(0.3, 1.0) * (1 + antibot_retry_count * 0.5))
                        
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
                                return response
                            raise httpx.RequestError(f"HTTP error: {response.status_code}", request=response.request)
                        
                        # Handle content decoding/decompression
                        content_encoding = response.headers.get("content-encoding", "")
                        content_type = response.headers.get("content-type", "")
                        charset = self._extract_charset(content_type)
                        
                        # If content is empty or seems binary, try manual decompression
                        if response.status_code == 200 and (not response.text or len(response.text) < 100 or b'\x00' in response.content):
                            decompressed_content = self._try_decompress_content(response.content, content_encoding)
                            text = self._decode_content(decompressed_content, charset)
                            response._text = text
                        
                        # Update session cookies with any new cookies from the response
                        if response.cookies:
                            session_cookies.update(response.cookies)
                        
                        # Add URL to session history
                        self.session_history.append(url)
                        if len(self.session_history) > 5:
                            self.session_history.pop(0)
                        
                        # Check for anti-bot measures if enabled
                        if retry_anti_bot:
                            if self._detect_anti_bot(response):
                                # Add Cloudflare-specific cookies if detected
                                if "Cloudflare" in response.text.lower():
                                    session_cookies["__cf_chl"] = base64.urlsafe_b64encode(os.urandom(16)).decode('utf-8')[:22]
                                
                                # If we still have retries left, continue
                                if antibot_retry_count < max_antibot_retries:
                                    logger.warning(f"Anti-bot measures detected (retry {antibot_retry_count + 1}/{max_antibot_retries}) for {url}")
                                    antibot_retry_count += 1
                                    continue
                                else:
                                    # We've exhausted retries but still hit anti-bot
                                    logger.error(f"Anti-bot measures still detected after {max_antibot_retries} retries for {url}")
                                    raise httpx.RequestError(f"Failed to bypass anti-bot measures after {max_antibot_retries} retries", 
                                                           request=response.request)
                        
                        # Log success if retries were needed
                        if antibot_retry_count > 0:
                            logger.info(f"Successfully bypassed anti-bot measures after {antibot_retry_count} retries for {url}")
                        
                        return response
                    
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    logger.error(f"Request error for {url}: {e}")
                    # Only continue retrying if we haven't exceeded max retries
                    if antibot_retry_count < max_antibot_retries:
                        antibot_retry_count += 1
                        continue
                    raise
        
        # This should not be reached due to the loop and exception conditions above
        # but included as a safeguard
        if response:
            return response
        
        raise httpx.RequestError(f"Exceeded maximum anti-bot retries ({max_antibot_retries}) for {url}", 
                               request=None)
    
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