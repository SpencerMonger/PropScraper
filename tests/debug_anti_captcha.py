#!/usr/bin/env python3
"""
Debug script to identify hanging issues with Crawl4AI
"""

import asyncio
import logging
import time
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Enable detailed logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_basic_crawl():
    """Test basic crawling without advanced features"""
    logger.info("🔍 Testing basic Crawl4AI functionality...")
    
    try:
        # Very simple browser config
        browser_config = BrowserConfig(
            headless=True,
            verbose=True,  # Enable verbose logging
            browser_type="chromium"
        )
        
        # Simple crawler config with short timeout
        crawler_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=10000,  # 10 seconds only
            verbose=True
        )
        
        logger.info("🚀 Initializing crawler...")
        start_time = time.time()
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            init_time = time.time() - start_time
            logger.info(f"✅ Crawler initialized in {init_time:.2f} seconds")
            
            # Test with a simple, reliable site
            test_url = "https://httpbin.org/html"
            logger.info(f"📡 Testing with: {test_url}")
            
            crawl_start = time.time()
            result = await crawler.arun(test_url, config=crawler_config)
            crawl_time = time.time() - crawl_start
            
            if result.success:
                logger.info(f"✅ Basic crawl successful in {crawl_time:.2f} seconds")
                title = result.metadata.get('title') if result.metadata else 'No title'
                logger.info(f"📄 Title: {title}")
                logger.info(f"📊 Content length: {len(result.html)}")
                return True
            else:
                logger.error(f"❌ Basic crawl failed: {result.error_message}")
                return False
                
    except asyncio.TimeoutError:
        logger.error("❌ Timeout during basic test")
        return False
    except Exception as e:
        logger.error(f"❌ Exception during basic test: {e}")
        return False

async def test_managed_browser():
    """Test managed browser functionality"""
    logger.info("🔍 Testing managed browser...")
    
    try:
        browser_config = BrowserConfig(
            headless=True,
            use_managed_browser=True,
            user_data_dir="./test_profile",
            browser_type="chromium",
            verbose=True
        )
        
        crawler_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=15000,  # 15 seconds
            verbose=True
        )
        
        logger.info("🚀 Initializing managed browser...")
        start_time = time.time()
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            init_time = time.time() - start_time
            logger.info(f"✅ Managed browser initialized in {init_time:.2f} seconds")
            
            test_url = "https://httpbin.org/headers"
            logger.info(f"📡 Testing managed browser with: {test_url}")
            
            result = await crawler.arun(test_url, config=crawler_config)
            
            if result.success:
                logger.info("✅ Managed browser test successful")
                return True
            else:
                logger.error(f"❌ Managed browser test failed: {result.error_message}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Exception during managed browser test: {e}")
        return False

async def test_with_timeout():
    """Test with explicit timeout handling"""
    logger.info("🔍 Testing with timeout handling...")
    
    try:
        # Set a maximum time limit for the entire operation
        timeout_seconds = 30
        
        async def timed_crawl():
            browser_config = BrowserConfig(
                headless=True,
                verbose=True,
                browser_type="chromium"
            )
            
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(
                    "https://www.google.com",
                    config=CrawlerRunConfig(
                        page_timeout=10000,
                        verbose=True
                    )
                )
                return result
        
        logger.info(f"⏱️  Setting {timeout_seconds}s timeout for entire operation...")
        result = await asyncio.wait_for(timed_crawl(), timeout=timeout_seconds)
        
        if result and result.success:
            logger.info("✅ Timeout test successful")
            return True
        else:
            logger.error("❌ Timeout test failed")
            return False
            
    except asyncio.TimeoutError:
        logger.error(f"❌ Operation timed out after {timeout_seconds} seconds")
        return False
    except Exception as e:
        logger.error(f"❌ Exception during timeout test: {e}")
        return False

async def main():
    """Run diagnostic tests"""
    logger.info("=" * 60)
    logger.info("🔧 CRAWL4AI DIAGNOSTIC TESTS")
    logger.info("=" * 60)
    
    tests = [
        ("Basic Crawl", test_basic_crawl),
        ("Timeout Handling", test_with_timeout),
        ("Managed Browser", test_managed_browser),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running: {test_name}")
        logger.info("-" * 40)
        
        try:
            start_time = time.time()
            success = await test_func()
            duration = time.time() - start_time
            
            results[test_name] = {
                'success': success,
                'duration': duration
            }
            
            status = "✅ PASSED" if success else "❌ FAILED"
            logger.info(f"{status} - {test_name} ({duration:.2f}s)")
            
        except Exception as e:
            results[test_name] = {
                'success': False,
                'duration': 0,
                'error': str(e)
            }
            logger.error(f"❌ FAILED - {test_name}: {e}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST RESULTS SUMMARY")
    logger.info("=" * 60)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result['success'] else "❌ FAIL"
        duration = result.get('duration', 0)
        logger.info(f"{status} {test_name:<20} ({duration:.2f}s)")
        
        if 'error' in result:
            logger.info(f"     Error: {result['error']}")
    
    # Recommendations
    logger.info("\n🔍 RECOMMENDATIONS:")
    if not any(r['success'] for r in results.values()):
        logger.info("- All tests failed. Check your Crawl4AI installation:")
        logger.info("  pip install --upgrade crawl4ai")
        logger.info("  playwright install")
    elif results.get("Basic Crawl", {}).get('success'):
        logger.info("- Basic crawling works. Use simpler configurations.")
    
    if not results.get("Managed Browser", {}).get('success'):
        logger.info("- Managed browser issues. Try without user_data_dir first.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"�� Fatal error: {e}") 