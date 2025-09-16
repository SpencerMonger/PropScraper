#!/usr/bin/env python3
"""
Simple test script to verify Crawl4AI can access inmuebles24.com
This will help debug any connectivity issues
"""

import asyncio
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_website_access():
    """Test if we can access the target website with Crawl4AI"""
    
    target_url = "https://www.inmuebles24.com/casas-en-venta.html"
    
    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        java_script_enabled=True,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        browser_type="chromium"
    )
    
    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_for="body",
        page_timeout=30000,
        delay_before_return_html=3000,
        verbose=False
    )
    
    logger.info(f"Testing access to: {target_url}")
    logger.info("Initializing browser...")
    
    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            logger.info("Browser initialized successfully")
            logger.info("Attempting to crawl the page...")
            
            result = await crawler.arun(target_url, config=crawler_config)
            
            if result.success:
                logger.info(f"✅ SUCCESS! Page crawled successfully")
                logger.info(f"Page title: {result.title[:100]}...")
                logger.info(f"Content length: {len(result.html)} characters")
                logger.info(f"Markdown length: {len(result.markdown)} characters")
                
                # Check if we got actual content
                if "casa" in result.html.lower() or "propiedad" in result.html.lower():
                    logger.info("✅ Found property-related content on the page")
                else:
                    logger.warning("⚠️  No obvious property content found - might be blocked or different structure")
                
                return True
            else:
                logger.error(f"❌ FAILED to crawl page: {result.error_message}")
                return False
                
    except Exception as e:
        logger.error(f"❌ EXCEPTION during crawling: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("=" * 60)
    logger.info("INMUEBLES24 WEBSITE ACCESS TEST")
    logger.info("=" * 60)
    
    success = await test_website_access()
    
    logger.info("=" * 60)
    if success:
        logger.info("🎉 Test completed successfully! The scraper should work.")
    else:
        logger.info("❌ Test failed. There may be connectivity or blocking issues.")
        logger.info("Try running the full scraper anyway - it might still work with different settings.")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main()) 