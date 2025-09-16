#!/usr/bin/env python3
"""
Simplified anti-captcha strategies for Crawl4AI
"""

import asyncio
import random
import time
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleAntiCaptchaScraper:
    """Simplified scraper with basic anti-captcha strategies"""
    
    def __init__(self):
        # Keep it simple - no managed browser initially
        self.browser_config = BrowserConfig(
            headless=True,
            browser_type="chromium",
            verbose=True  # Enable logging to see what's happening
        )
    
    async def scrape_with_retry(self, url: str, max_retries: int = 2):
        """Simple scraping with retry logic"""
        
        for attempt in range(max_retries):
            logger.info(f"🔄 Attempt {attempt + 1}/{max_retries} for {url}")
            
            try:
                # Add random delay between attempts
                if attempt > 0:
                    delay = random.uniform(5, 10)
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before retry...")
                    await asyncio.sleep(delay)
                
                # Simple crawler config with reasonable timeouts
                crawler_config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    page_timeout=20000,  # 20 seconds
                    delay_before_return_html=2000,  # Wait 2 seconds
                    wait_for="body",
                    verbose=True
                )
                
                # Set overall timeout for the entire operation
                timeout_seconds = 30
                
                async def do_crawl():
                    async with AsyncWebCrawler(config=self.browser_config) as crawler:
                        return await crawler.arun(url, config=crawler_config)
                
                logger.info(f"🚀 Starting crawl with {timeout_seconds}s timeout...")
                result = await asyncio.wait_for(do_crawl(), timeout=timeout_seconds)
                
                if result.success:
                    # Simple captcha detection
                    content_lower = result.html.lower()
                    captcha_keywords = ['captcha', 'recaptcha', 'verify you are human']
                    
                    if any(keyword in content_lower for keyword in captcha_keywords):
                        logger.warning(f"⚠️  Possible captcha detected on attempt {attempt + 1}")
                        if attempt < max_retries - 1:
                            continue
                    else:
                        logger.info("✅ Successfully scraped without captcha detection")
                        return result
                else:
                    logger.error(f"❌ Crawl failed: {result.error_message}")
                    
            except asyncio.TimeoutError:
                logger.error(f"❌ Timeout on attempt {attempt + 1}")
            except Exception as e:
                logger.error(f"❌ Exception on attempt {attempt + 1}: {e}")
        
        logger.error("❌ All attempts failed")
        return None
    
    async def test_simple_site(self):
        """Test with a simple, reliable site first"""
        logger.info("🧪 Testing with simple site...")
        
        test_url = "https://httpbin.org/html"
        result = await self.scrape_with_retry(test_url, max_retries=1)
        
        if result and result.success:
            logger.info("✅ Simple site test passed")
            title = result.metadata.get('title') if result.metadata else 'No title'
            logger.info(f"📄 Title: {title}")
            return True
        else:
            logger.error("❌ Simple site test failed")
            return False

async def main():
    """Main function with proper error handling"""
    logger.info("=" * 60)
    logger.info("🚀 SIMPLIFIED ANTI-CAPTCHA SCRAPER")
    logger.info("=" * 60)
    
    scraper = SimpleAntiCaptchaScraper()
    
    try:
        # Step 1: Test with simple site
        logger.info("\n📋 Step 1: Testing basic functionality...")
        if not await scraper.test_simple_site():
            logger.error("❌ Basic test failed. Check your setup.")
            return
        
        # Step 2: Try target site
        logger.info("\n📋 Step 2: Testing target site...")
        target_url = "https://www.inmuebles24.com"
        
        # Set a timeout for the entire operation
        total_timeout = 120  # 2 minutes max
        
        logger.info(f"⏱️  Setting {total_timeout}s timeout for entire operation...")
        result = await asyncio.wait_for(
            scraper.scrape_with_retry(target_url, max_retries=2),
            timeout=total_timeout
        )
        
        if result and result.success:
            logger.info("🎉 SUCCESS!")
            title = result.metadata.get('title') if result.metadata else 'No title'
            logger.info(f"📄 Title: {title}")
            logger.info(f"📊 Content length: {len(result.html)} characters")
            
            # Check for signs of blocking
            content_lower = result.html.lower()
            if any(word in content_lower for word in ['blocked', 'captcha', 'access denied']):
                logger.warning("⚠️  Content may indicate blocking/captcha")
            else:
                logger.info("✅ Content looks normal")
        else:
            logger.error("❌ Failed to scrape target site")
            
    except asyncio.TimeoutError:
        logger.error(f"❌ Entire operation timed out after {total_timeout} seconds")
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