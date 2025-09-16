#!/usr/bin/env python3
"""
Minimal working anti-captcha scraper
"""

import asyncio
import random
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_basic():
    """Test basic functionality"""
    logger.info("🧪 Testing basic functionality...")
    
    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun("https://httpbin.org/html")
            
            if result.success:
                logger.info("✅ Basic test passed")
                return True
            else:
                logger.error(f"❌ Basic test failed: {result.error_message}")
                return False
    except Exception as e:
        logger.error(f"❌ Basic test exception: {e}")
        return False

async def scrape_with_simple_anti_captcha(url: str):
    """Scrape with minimal anti-captcha measures"""
    logger.info(f"🚀 Scraping {url}...")
    
    try:
        # Simple configuration that we know works
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=30000,
            delay_before_return_html=3000,
            wait_for="body"
        )
        
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url, config=config)
            
            if result.success:
                logger.info("✅ Scraping successful!")
                
                # Check content length
                content_length = len(result.html)
                logger.info(f"📊 Content length: {content_length} characters")
                
                # Get title from metadata
                title = result.metadata.get('title') if result.metadata else 'No title'
                logger.info(f"📄 Title: {title}")
                
                # Simple captcha detection
                content_lower = result.html.lower()
                captcha_indicators = ['captcha', 'verify you are human', 'cloudflare']
                
                if any(indicator in content_lower for indicator in captcha_indicators):
                    logger.warning("⚠️  Possible captcha detected")
                    logger.info("First 500 chars of content:")
                    logger.info(result.html[:500])
                else:
                    logger.info("✅ No obvious captcha detected")
                
                # Save sample for inspection
                with open('scraped_sample.html', 'w', encoding='utf-8') as f:
                    f.write(result.html)
                logger.info("📝 Saved full content to 'scraped_sample.html'")
                
                return result
            else:
                logger.error(f"❌ Scraping failed: {result.error_message}")
                return None
                
    except Exception as e:
        logger.error(f"❌ Exception during scraping: {e}")
        return None

async def main():
    logger.info("=" * 50)
    logger.info("🚀 MINIMAL ANTI-CAPTCHA SCRAPER")
    logger.info("=" * 50)
    
    # Step 1: Basic test
    if not await test_basic():
        logger.error("❌ Basic test failed, stopping")
        return
    
    # Step 2: Target site
    logger.info("\n📋 Testing target site...")
    target_url = "https://www.inmuebles24.com"
    
    result = await scrape_with_simple_anti_captcha(target_url)
    
    if result:
        logger.info("🎉 SUCCESS! Check 'scraped_sample.html' for results")
    else:
        logger.error("❌ Failed to scrape target site")

if __name__ == "__main__":
    asyncio.run(main()) 