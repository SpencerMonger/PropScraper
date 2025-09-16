#!/usr/bin/env python3
"""
Captcha solver integration for Inmuebles24 scraper
Uses SolveCaptcha API to bypass Cloudflare Turnstile challenges
"""

import asyncio
import logging
import os
from typing import Optional, Dict, Any
from solvecaptcha import SolveCaptcha
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

logger = logging.getLogger(__name__)

class CaptchaSolver:
    """
    Handles captcha solving for web scraping using SolveCaptcha API
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the captcha solver
        
        Args:
            api_key: SolveCaptcha API key. If None, reads from environment variable SOLVECAPTCHA_API_KEY
        """
        self.api_key = api_key or os.getenv('SOLVECAPTCHA_API_KEY')
        if not self.api_key:
            raise ValueError("SolveCaptcha API key is required. Set SOLVECAPTCHA_API_KEY environment variable or pass api_key parameter.")
        
        self.solver = SolveCaptcha(self.api_key)
        logger.info("CaptchaSolver initialized with SolveCaptcha API")
    
    async def solve_cloudflare_turnstile(self, site_key: str, page_url: str, page_data: Optional[str] = None) -> Optional[str]:
        """
        Solve Cloudflare Turnstile captcha
        
        Args:
            site_key: The site key found in the captcha
            page_url: URL of the page containing the captcha
            page_data: Optional additional page data
            
        Returns:
            Captcha solution token or None if failed
        """
        try:
            logger.info(f"Solving Cloudflare Turnstile for {page_url}")
            
            # Submit captcha for solving
            task_data = {
                'type': 'TurnstileTaskProxyless',
                'websiteURL': page_url,
                'websiteKey': site_key,
            }
            
            if page_data:
                task_data['pagedata'] = page_data
            
            # Create task
            task_id = await asyncio.to_thread(self.solver.create_task, task_data)
            logger.info(f"Created captcha solving task: {task_id}")
            
            # Wait for solution
            solution = await asyncio.to_thread(self.solver.join_task_result, task_id)
            
            if solution and solution.get('status') == 'ready':
                token = solution.get('solution', {}).get('token')
                if token:
                    logger.info("Cloudflare Turnstile solved successfully")
                    return token
                else:
                    logger.error("No token in captcha solution")
            else:
                logger.error(f"Captcha solving failed: {solution}")
                
        except Exception as e:
            logger.error(f"Error solving Cloudflare Turnstile: {e}")
        
        return None
    
    async def solve_recaptcha_v2(self, site_key: str, page_url: str, invisible: bool = False) -> Optional[str]:
        """
        Solve reCAPTCHA v2 captcha
        
        Args:
            site_key: The site key found in the captcha
            page_url: URL of the page containing the captcha
            invisible: Whether it's invisible reCAPTCHA
            
        Returns:
            Captcha solution token or None if failed
        """
        try:
            logger.info(f"Solving reCAPTCHA v2 for {page_url}")
            
            task_data = {
                'type': 'NoCaptchaTaskProxyless',
                'websiteURL': page_url,
                'websiteKey': site_key,
                'isInvisible': invisible
            }
            
            # Create task
            task_id = await asyncio.to_thread(self.solver.create_task, task_data)
            logger.info(f"Created reCAPTCHA solving task: {task_id}")
            
            # Wait for solution
            solution = await asyncio.to_thread(self.solver.join_task_result, task_id)
            
            if solution and solution.get('status') == 'ready':
                token = solution.get('solution', {}).get('gRecaptchaResponse')
                if token:
                    logger.info("reCAPTCHA v2 solved successfully")
                    return token
                else:
                    logger.error("No token in reCAPTCHA solution")
            else:
                logger.error(f"reCAPTCHA solving failed: {solution}")
                
        except Exception as e:
            logger.error(f"Error solving reCAPTCHA v2: {e}")
        
        return None

class CrawlerWithCaptchaSolver:
    """
    Enhanced AsyncWebCrawler with integrated captcha solving capabilities
    """
    
    def __init__(self, captcha_solver: CaptchaSolver, browser_config: Optional[BrowserConfig] = None):
        """
        Initialize crawler with captcha solver
        
        Args:
            captcha_solver: CaptchaSolver instance
            browser_config: Optional browser configuration
        """
        self.captcha_solver = captcha_solver
        self.browser_config = browser_config or BrowserConfig(
            headless=True,
            browser_type="chromium",
            verbose=True
        )
        
    async def crawl_with_captcha_handling(self, url: str, crawler_config: Optional[CrawlerRunConfig] = None, max_captcha_attempts: int = 3) -> Any:
        """
        Crawl a URL with automatic captcha detection and solving
        
        Args:
            url: URL to crawl
            crawler_config: Optional crawler configuration
            max_captcha_attempts: Maximum number of captcha solving attempts
            
        Returns:
            Crawl result or None if failed
        """
        crawler_config = crawler_config or CrawlerRunConfig(
            page_timeout=30000,
            verbose=True
        )
        
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            for attempt in range(max_captcha_attempts):
                try:
                    logger.info(f"Crawling {url} (attempt {attempt + 1}/{max_captcha_attempts})")
                    
                    # Try to crawl the page
                    result = await crawler.arun(url, config=crawler_config)
                    
                    if result.success:
                        # Check if page contains captcha
                        if self._has_captcha_challenge(result.html):
                            logger.warning("Captcha detected on page")
                            
                            # Extract captcha details
                            captcha_info = self._extract_captcha_info(result.html, url)
                            if captcha_info:
                                # Solve captcha
                                token = await self._solve_captcha(captcha_info)
                                if token:
                                    # Inject captcha solution and retry
                                    result = await self._retry_with_captcha_solution(crawler, url, captcha_info, token, crawler_config)
                                    if result and result.success:
                                        logger.info("Successfully crawled page after solving captcha")
                                        return result
                                else:
                                    logger.error("Failed to solve captcha")
                            else:
                                logger.error("Could not extract captcha information")
                        else:
                            # No captcha detected, return result
                            logger.info("Successfully crawled page without captcha")
                            return result
                    else:
                        logger.warning(f"Crawl failed: {result.error_message}")
                        
                except Exception as e:
                    logger.error(f"Error during crawl attempt {attempt + 1}: {e}")
                
                if attempt < max_captcha_attempts - 1:
                    logger.info(f"Waiting before retry attempt {attempt + 2}")
                    await asyncio.sleep(5)  # Wait before retry
            
            logger.error(f"Failed to crawl {url} after {max_captcha_attempts} attempts")
            return None
    
    def _has_captcha_challenge(self, html: str) -> bool:
        """
        Check if the HTML contains captcha challenges
        
        Args:
            html: HTML content to check
            
        Returns:
            True if captcha is detected
        """
        captcha_indicators = [
            'cf-turnstile',  # Cloudflare Turnstile
            'g-recaptcha',   # Google reCAPTCHA
            'h-captcha',     # hCaptcha
            'challenge-form', # Generic challenge form
            'captcha',       # Generic captcha
            'verify you are human',  # Common text
            'checking your browser',  # Cloudflare text
        ]
        
        html_lower = html.lower()
        return any(indicator in html_lower for indicator in captcha_indicators)
    
    def _extract_captcha_info(self, html: str, page_url: str) -> Optional[Dict[str, Any]]:
        """
        Extract captcha information from HTML
        
        Args:
            html: HTML content
            page_url: URL of the page
            
        Returns:
            Dictionary with captcha information or None
        """
        import re
        
        # Look for Cloudflare Turnstile
        turnstile_match = re.search(r'data-sitekey="([^"]+)".*?cf-turnstile', html, re.IGNORECASE | re.DOTALL)
        if turnstile_match:
            return {
                'type': 'turnstile',
                'site_key': turnstile_match.group(1),
                'page_url': page_url
            }
        
        # Look for reCAPTCHA
        recaptcha_match = re.search(r'data-sitekey="([^"]+)".*?g-recaptcha', html, re.IGNORECASE | re.DOTALL)
        if recaptcha_match:
            invisible = 'invisible' in html.lower()
            return {
                'type': 'recaptcha_v2',
                'site_key': recaptcha_match.group(1),
                'page_url': page_url,
                'invisible': invisible
            }
        
        logger.warning("Could not extract captcha information from HTML")
        return None
    
    async def _solve_captcha(self, captcha_info: Dict[str, Any]) -> Optional[str]:
        """
        Solve captcha based on extracted information
        
        Args:
            captcha_info: Captcha information dictionary
            
        Returns:
            Captcha solution token or None
        """
        captcha_type = captcha_info.get('type')
        
        if captcha_type == 'turnstile':
            return await self.captcha_solver.solve_cloudflare_turnstile(
                captcha_info['site_key'],
                captcha_info['page_url']
            )
        elif captcha_type == 'recaptcha_v2':
            return await self.captcha_solver.solve_recaptcha_v2(
                captcha_info['site_key'],
                captcha_info['page_url'],
                captcha_info.get('invisible', False)
            )
        else:
            logger.error(f"Unsupported captcha type: {captcha_type}")
            return None
    
    async def _retry_with_captcha_solution(self, crawler, url: str, captcha_info: Dict[str, Any], token: str, crawler_config: CrawlerRunConfig) -> Any:
        """
        Retry crawling with captcha solution injected
        
        Args:
            crawler: AsyncWebCrawler instance
            url: URL to crawl
            captcha_info: Captcha information
            token: Captcha solution token
            crawler_config: Crawler configuration
            
        Returns:
            Crawl result or None
        """
        try:
            # Create JavaScript to inject captcha solution
            captcha_type = captcha_info.get('type')
            
            if captcha_type == 'turnstile':
                js_code = f"""
                // Wait for Turnstile widget and inject solution
                const widget = document.querySelector('[data-sitekey="{captcha_info["site_key"]}"]');
                if (widget) {{
                    // Create hidden input with solution
                    const input = document.createElement('input');
                    input.type = 'hidden';
                    input.name = 'cf-turnstile-response';
                    input.value = '{token}';
                    widget.appendChild(input);
                    
                    // Trigger form submission if form exists
                    const form = widget.closest('form');
                    if (form) {{
                        form.submit();
                    }}
                }}
                """
            elif captcha_type == 'recaptcha_v2':
                js_code = f"""
                // Inject reCAPTCHA solution
                document.getElementById('g-recaptcha-response').innerHTML = '{token}';
                if (typeof grecaptcha !== 'undefined') {{
                    grecaptcha.getResponse = function() {{ return '{token}'; }};
                }}
                
                // Trigger form submission if form exists
                const form = document.querySelector('form');
                if (form) {{
                    form.submit();
                }}
                """
            else:
                logger.error(f"Cannot inject solution for captcha type: {captcha_type}")
                return None
            
            # Update crawler config to include JavaScript
            updated_config = CrawlerRunConfig(
                page_timeout=crawler_config.page_timeout,
                verbose=crawler_config.verbose,
                js_code=[js_code],
                wait_for_selector="body",  # Wait for page to load
                delay_before_return_html=3.0  # Give time for form submission
            )
            
            # Retry crawling with injected solution
            result = await crawler.arun(url, config=updated_config)
            return result
            
        except Exception as e:
            logger.error(f"Error injecting captcha solution: {e}")
            return None


# Example usage
async def example_usage():
    """
    Example of how to use the captcha solver with your scraper
    """
    # Initialize captcha solver with API key
    captcha_solver = CaptchaSolver()  # Reads from SOLVECAPTCHA_API_KEY env var
    
    # Create enhanced crawler
    crawler_with_captcha = CrawlerWithCaptchaSolver(captcha_solver)
    
    # Crawl with automatic captcha handling
    result = await crawler_with_captcha.crawl_with_captcha_handling(
        "https://www.inmuebles24.com/casas-en-venta.html"
    )
    
    if result and result.success:
        print("Successfully crawled with captcha handling!")
        print(f"Title: {result.metadata.get('title', 'N/A')}")
    else:
        print("Failed to crawl even with captcha solving")

if __name__ == "__main__":
    asyncio.run(example_usage()) 