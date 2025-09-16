#!/usr/bin/env python3
"""
Timeout-safe scraper with aggressive anti-hanging measures
"""

import asyncio
import logging
import signal
import sys
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TimeoutSafeScraper:
    def __init__(self):
        self.max_total_time = 60  # Maximum 60 seconds per attempt
        
    async def scrape_with_aggressive_timeout(self, url: str):
        """Scrape with very aggressive timeout handling"""
        logger.info(f"🚀 Attempting to scrape {url} with {self.max_total_time}s timeout...")
        
        try:
            # Create a task for the scraping operation
            scrape_task = asyncio.create_task(self._do_scrape(url))
            
            # Wait for either completion or timeout
            try:
                result = await asyncio.wait_for(scrape_task, timeout=self.max_total_time)
                return result
            except asyncio.TimeoutError:
                logger.error(f"❌ Operation timed out after {self.max_total_time} seconds")
                # Cancel the task
                scrape_task.cancel()
                try:
                    await scrape_task
                except asyncio.CancelledError:
                    logger.info("🛑 Scraping task cancelled successfully")
                return None
                
        except Exception as e:
            logger.error(f"❌ Exception in timeout handler: {e}")
            return None
    
    async def _do_scrape(self, url: str):
        """Internal scraping method"""
        try:
            # Very minimal config to avoid hanging
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=15000,  # Only 15 seconds for page load
                delay_before_return_html=1000,  # Minimal delay
                wait_for="body",
                verbose=False  # Reduce logging noise
            )
            
            # Use minimal browser config
            browser_config = BrowserConfig(
                headless=True,
                browser_type="chromium",
                verbose=False
            )
            
            logger.info("🔄 Starting crawler...")
            async with AsyncWebCrawler(config=browser_config) as crawler:
                logger.info("📡 Performing crawl...")
                result = await crawler.arun(url, config=config)
                
                if result.success:
                    logger.info("✅ Crawl completed successfully")
                    return result
                else:
                    logger.error(f"❌ Crawl failed: {result.error_message}")
                    return None
                    
        except Exception as e:
            logger.error(f"❌ Exception in scraping: {e}")
            return None

async def test_different_sites():
    """Test with different sites to see which ones work"""
    scraper = TimeoutSafeScraper()
    
    test_sites = [
        "https://httpbin.org/html",  # Should work
        "https://example.com",       # Should work
        "https://www.google.com",    # Might work
        "https://www.inmuebles24.com"  # Target site
    ]
    
    results = {}
    
    for url in test_sites:
        logger.info(f"\n{'='*50}")
        logger.info(f"🧪 Testing: {url}")
        logger.info(f"{'='*50}")
        
        start_time = asyncio.get_event_loop().time()
        result = await scraper.scrape_with_aggressive_timeout(url)
        end_time = asyncio.get_event_loop().time()
        
        duration = end_time - start_time
        
        if result and result.success:
            title = result.metadata.get('title') if result.metadata else 'No title'
            content_length = len(result.html)
            
            logger.info(f"✅ SUCCESS ({duration:.1f}s)")
            logger.info(f"📄 Title: {title}")
            logger.info(f"📊 Content: {content_length} chars")
            
            # Check for blocking indicators
            content_lower = result.html.lower()
            blocking_terms = ['captcha', 'blocked', 'cloudflare', 'access denied']
            blocked = any(term in content_lower for term in blocking_terms)
            
            if blocked:
                logger.warning("⚠️  Possible blocking detected")
            else:
                logger.info("✅ No obvious blocking")
            
            results[url] = {
                'success': True,
                'duration': duration,
                'title': title,
                'content_length': content_length,
                'blocked': blocked
            }
        else:
            logger.error(f"❌ FAILED ({duration:.1f}s)")
            results[url] = {
                'success': False,
                'duration': duration
            }
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("📊 RESULTS SUMMARY")
    logger.info(f"{'='*60}")
    
    for url, result in results.items():
        status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
        duration = result['duration']
        logger.info(f"{status} {url} ({duration:.1f}s)")
        
        if result['success']:
            if result.get('blocked'):
                logger.info(f"     ⚠️  Blocking detected")
            else:
                logger.info(f"     📊 {result['content_length']} chars")

def setup_signal_handler():
    """Setup signal handler for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info("\n🛑 Received interrupt signal, shutting down...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)

async def main():
    logger.info("🚀 TIMEOUT-SAFE SCRAPER TEST")
    logger.info("Press Ctrl+C to interrupt at any time")
    
    setup_signal_handler()
    
    try:
        await test_different_sites()
    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Script interrupted")
    except Exception as e:
        print(f"�� Fatal error: {e}") 