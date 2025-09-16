#!/usr/bin/env python3
"""
Working anti-captcha strategies for Crawl4AI
"""

import asyncio
import random
import time
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WorkingAntiCaptchaScraper:
    """Working scraper with anti-captcha strategies"""
    
    def __init__(self):
        # Simple, working browser config
        self.browser_config = BrowserConfig(
            headless=True,
            browser_type="chromium",
            verbose=True
        )
    
    async def scrape_with_retry(self, url: str, max_retries: int = 2):
        """Scraping with retry logic and anti-captcha measures"""
        
        for attempt in range(max_retries):
            logger.info(f"🔄 Attempt {attempt + 1}/{max_retries} for {url}")
            
            try:
                # Add random delay between attempts to look human
                if attempt > 0:
                    delay = random.uniform(5, 15)
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before retry...")
                    await asyncio.sleep(delay)
                
                # Anti-captcha crawler configuration
                crawler_config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    page_timeout=30000,  # 30 seconds
                    delay_before_return_html=random.randint(2000, 5000),  # Random delay
                    wait_for="body",
                    verbose=True,
                    
                    # Anti-bot measures
                    simulate_user=True,  # If available
                    magic=True,  # Enable magic mode for human-like behavior
                    
                    # Custom headers to look more human
                    extra_headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "DNT": "1",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Cache-Control": "max-age=0"
                    }
                )
                
                # Use context manager (proven to work)
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    logger.info(f"🚀 Starting crawl...")
                    result = await crawler.arun(url, config=crawler_config)
                    
                    if result.success:
                        # Check for captcha/blocking indicators
                        content_lower = result.html.lower()
                        captcha_indicators = [
                            'captcha', 'recaptcha', 'hcaptcha', 
                            'verify you are human', 'verify that you are human',
                            'bot protection', 'cloudflare', 'access denied', 
                            'blocked', 'rate limit', 'too many requests',
                            'please complete the security check',
                            'checking your browser before accessing'
                        ]
                        
                        captcha_detected = any(indicator in content_lower for indicator in captcha_indicators)
                        
                        if captcha_detected:
                            logger.warning(f"⚠️  Captcha/blocking detected on attempt {attempt + 1}")
                            logger.warning(f"Content preview: {result.html[:200]}...")
                            
                            if attempt < max_retries - 1:
                                # Exponential backoff with randomization
                                wait_time = (2 ** attempt) * random.uniform(30, 60)
                                logger.info(f"⏳ Waiting {wait_time:.1f} seconds before retry...")
                                await asyncio.sleep(wait_time)
                                continue
                        else:
                            logger.info("✅ Successfully scraped without captcha detection")
                            title = result.metadata.get('title') if result.metadata else 'No title'
                            logger.info(f"📄 Title: {title}")
                            return result
                    else:
                        logger.error(f"❌ Crawl failed: {result.error_message}")
                        
            except Exception as e:
                logger.error(f"❌ Exception on attempt {attempt + 1}: {e}")
                
            # Wait before next retry
            if attempt < max_retries - 1:
                await asyncio.sleep(random.uniform(10, 20))
        
        logger.error("❌ All attempts failed")
        return None
    
    async def test_simple_site(self):
        """Test with a simple, reliable site first"""
        logger.info("🧪 Testing with simple site...")
        
        test_url = "https://httpbin.org/html"
        result = await self.scrape_with_retry(test_url, max_retries=1)
        
        if result and result.success:
            logger.info("✅ Simple site test passed")
            return True
        else:
            logger.error("❌ Simple site test failed")
            return False

async def main():
    """Main function with proper error handling"""
    logger.info("=" * 60)
    logger.info("🚀 WORKING ANTI-CAPTCHA SCRAPER")
    logger.info("=" * 60)
    
    scraper = WorkingAntiCaptchaScraper()
    
    try:
        # Step 1: Test with simple site
        logger.info("\n📋 Step 1: Testing basic functionality...")
        if not await scraper.test_simple_site():
            logger.error("❌ Basic test failed. Check your setup.")
            return
        
        # Step 2: Try target site
        logger.info("\n📋 Step 2: Testing target site...")
        target_url = "https://www.inmuebles24.com"
        
        result = await scraper.scrape_with_retry(target_url, max_retries=3)
        
        if result and result.success:
            logger.info("🎉 SUCCESS!")
            title = result.metadata.get('title') if result.metadata else 'No title'
            logger.info(f"📄 Title: {title}")
            logger.info(f"📊 Content length: {len(result.html)} characters")
            
            # Check for signs of blocking
            content_lower = result.html.lower()
            blocking_indicators = ['blocked', 'captcha', 'access denied', 'cloudflare']
            if any(word in content_lower for word in blocking_indicators):
                logger.warning("⚠️  Content may indicate blocking/captcha")
            else:
                logger.info("✅ Content looks normal")
                
            # Save a sample of the content for inspection
            with open('scraped_content_sample.html', 'w', encoding='utf-8') as f:
                f.write(result.html[:5000])  # First 5000 chars
            logger.info("📝 Saved content sample to 'scraped_content_sample.html'")
            
        else:
            logger.error("❌ Failed to scrape target site")
            
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted by user")
    except Exception as e:
        print(f"�� Fatal error: {e}") 