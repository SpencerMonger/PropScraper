#!/usr/bin/env python3
"""
Debug scraper to identify connectivity issues
"""

import asyncio
import logging
import requests
import time
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_minimal_crawl():
    """Test with minimal settings"""
    logger.info("Testing minimal Crawl4AI setup...")
    
    try:
        # Minimal browser config
        browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            browser_type="chromium"
        )
        
        # Very short timeout
        crawler_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=5000,  # Just 5 seconds
            verbose=False
        )
        
        logger.info("Initializing crawler...")
        start_time = time.time()
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            init_time = time.time() - start_time
            logger.info(f"Crawler initialized in {init_time:.2f} seconds")
            
            # Test with a simple page first
            test_url = "https://httpbin.org/html"
            logger.info(f"Testing simple page: {test_url}")
            
            result = await crawler.arun(test_url, config=crawler_config)
            
            if result.success:
                logger.info("✅ Simple page test successful")
                
                # Now try the actual target
                target_url = "https://www.inmuebles24.com"
                logger.info(f"Testing target site: {target_url}")
                
                result2 = await crawler.arun(target_url, config=crawler_config)
                
                if result2.success:
                    logger.info("✅ Target site accessible!")
                    logger.info(f"Title: {result2.title}")
                    logger.info(f"Content length: {len(result2.html)}")
                else:
                    logger.error(f"❌ Target site failed: {result2.error_message}")
            else:
                logger.error(f"❌ Simple page test failed: {result.error_message}")
                
    except asyncio.TimeoutError:
        logger.error("❌ Timeout occurred")
    except Exception as e:
        logger.error(f"❌ Exception: {e}")

def test_basic_requests():
    """Test basic HTTP connectivity"""
    logger.info("Testing basic HTTP requests...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    test_urls = [
        "https://httpbin.org/get",
        "https://www.google.com",
        "https://www.inmuebles24.com"
    ]
    
    for url in test_urls:
        try:
            logger.info(f"Testing: {url}")
            response = requests.get(url, headers=headers, timeout=10)
            logger.info(f"✅ {url} - Status: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ {url} - Error: {e}")

async def main():
    """Main debug function"""
    logger.info("=" * 60)
    logger.info("DEBUGGING SCRAPER CONNECTIVITY")
    logger.info("=" * 60)
    
    # Test 1: Basic HTTP requests
    logger.info("\n--- Test 1: Basic HTTP Requests ---")
    test_basic_requests()
    
    # Test 2: Minimal Crawl4AI
    logger.info("\n--- Test 2: Minimal Crawl4AI Test ---")
    await test_minimal_crawl()
    
    logger.info("\n" + "=" * 60)
    logger.info("DEBUG COMPLETED")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main()) 