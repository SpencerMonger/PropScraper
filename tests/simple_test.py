#!/usr/bin/env python3
"""
Very simple test to identify hanging issues
"""

import asyncio
import logging
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def simple_test():
    """Most basic possible test"""
    logger.info("🚀 Starting simple test...")
    
    try:
        # Create crawler instance
        logger.info("Creating crawler...")
        crawler = AsyncWebCrawler()
        
        # Start crawler
        logger.info("Starting crawler...")
        await crawler.start()
        
        # Simple crawl
        logger.info("Performing crawl...")
        result = await crawler.arun("https://httpbin.org/html")
        
        if result.success:
            logger.info("✅ Success!")
            title = result.metadata.get('title') if result.metadata else 'No title'
            logger.info(f"Title: {title}")
        else:
            logger.error(f"❌ Failed: {result.error_message}")
        
        # Close crawler
        logger.info("Closing crawler...")
        await crawler.close()
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")

async def simple_context_test():
    """Test with context manager (recommended approach)"""
    logger.info("🚀 Testing with context manager...")
    
    try:
        async with AsyncWebCrawler() as crawler:
            logger.info("Performing crawl...")
            result = await crawler.arun("https://httpbin.org/html")
            
            if result.success:
                logger.info("✅ Context manager test success!")
                title = result.metadata.get('title') if result.metadata else 'No title'
                logger.info(f"Title: {title}")
                return True
            else:
                logger.error(f"❌ Failed: {result.error_message}")
                return False
    except Exception as e:
        logger.error(f"❌ Context manager error: {e}")
        return False

async def main():
    logger.info("Starting main...")
    
    # Test with context manager first (recommended approach)
    try:
        logger.info("=== Testing Context Manager Approach ===")
        success = await asyncio.wait_for(simple_context_test(), timeout=60)
        
        if success:
            logger.info("✅ Context manager approach works!")
        else:
            logger.error("❌ Context manager approach failed")
            
    except asyncio.TimeoutError:
        logger.error("❌ Context manager test timed out after 60 seconds")
    except Exception as e:
        logger.error(f"❌ Error in context manager test: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 