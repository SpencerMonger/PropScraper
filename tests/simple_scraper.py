#!/usr/bin/env python3
"""
Simplified Inmuebles24 scraper with better error handling and fallback options
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client, Client
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleInmuebles24Scraper:
    """Simplified scraper with better error handling"""
    
    def __init__(self):
        self.base_url = "https://www.inmuebles24.com"
        
        # Initialize Supabase client
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Please set SUPABASE_URL and SUPABASE_ANON_KEY environment variables")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
        # Simple browser config
        self.browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            java_script_enabled=True,
            browser_type="chromium"
        )
    
    async def test_basic_access(self, url: str) -> bool:
        """Test basic website access with short timeout"""
        logger.info(f"Testing basic access to: {url}")
        
        try:
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=15000,  # Short timeout
                delay_before_return_html=2000,
                verbose=False
            )
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(url, config=crawler_config)
                
                if result.success:
                    logger.info(f"✅ Basic access successful - Content length: {len(result.html)}")
                    logger.info(f"Title: {result.title}")
                    return True
                else:
                    logger.error(f"❌ Basic access failed: {result.error_message}")
                    return False
        
        except asyncio.TimeoutError:
            logger.error("❌ Timeout during basic access test")
            return False
        except Exception as e:
            logger.error(f"❌ Exception during basic access: {e}")
            return False
    
    async def scrape_with_simple_extraction(self, url: str) -> List[Dict]:
        """Scrape page with simple text extraction instead of complex selectors"""
        logger.info(f"Scraping with simple extraction: {url}")
        
        try:
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=20000,
                delay_before_return_html=3000,
                verbose=False
            )
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(url, config=crawler_config)
                
                if result.success:
                    logger.info(f"✅ Page scraped successfully")
                    logger.info(f"Content length: {len(result.html)} chars")
                    logger.info(f"Markdown length: {len(result.markdown)} chars")
                    
                    # Simple property extraction from markdown
                    properties = self.extract_properties_from_markdown(result.markdown, url)
                    logger.info(f"Found {len(properties)} potential properties")
                    
                    return properties
                else:
                    logger.error(f"❌ Scraping failed: {result.error_message}")
                    return []
        
        except Exception as e:
            logger.error(f"❌ Exception during scraping: {e}")
            return []
    
    def extract_properties_from_markdown(self, markdown: str, source_url: str) -> List[Dict]:
        """Extract property data from markdown text"""
        properties = []
        
        # Split markdown into potential property sections
        lines = markdown.split('\n')
        current_property = {}
        
        for line in lines:
            line = line.strip()
            
            # Look for price patterns
            if '$' in line and any(char.isdigit() for char in line):
                if current_property:
                    properties.append(current_property)
                current_property = {
                    'title': line,
                    'price_text': line,
                    'source_url': source_url,
                    'scraped_at': datetime.utcnow().isoformat()
                }
            
            # Look for location patterns
            elif any(word in line.lower() for word in ['colonia', 'delegación', 'municipio', 'estado']):
                if current_property:
                    current_property['location'] = line
        
        # Add the last property
        if current_property:
            properties.append(current_property)
        
        return properties[:10]  # Limit to first 10 for testing
    
    async def save_property_simple(self, property_data: Dict) -> bool:
        """Save property with minimal data validation"""
        try:
            # Simple data structure for testing
            clean_data = {
                'title': property_data.get('title', 'Unknown')[:255],
                'description': property_data.get('location', '')[:500],
                'source_url': property_data.get('source_url', ''),
                'scraped_at': property_data.get('scraped_at'),
                'operation_type': 'venta'
            }
            
            # Insert into database
            result = self.supabase.table("pulled_properties").insert(clean_data).execute()
            logger.info(f"✅ Saved property: {clean_data['title'][:50]}...")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save property: {e}")
            return False
    
    async def run_simple_test(self):
        """Run a simple test scrape"""
        logger.info("=" * 60)
        logger.info("SIMPLE INMUEBLES24 SCRAPER TEST")
        logger.info("=" * 60)
        
        test_urls = [
            "https://www.inmuebles24.com/casas-en-venta.html",
            "https://www.inmuebles24.com/departamentos-en-venta.html"
        ]
        
        for url in test_urls:
            logger.info(f"\n--- Testing URL: {url} ---")
            
            # Step 1: Test basic access
            if not await self.test_basic_access(url):
                logger.warning(f"Skipping {url} - basic access failed")
                continue
            
            # Step 2: Try scraping
            properties = await self.scrape_with_simple_extraction(url)
            
            if properties:
                logger.info(f"Found {len(properties)} properties, saving to database...")
                
                saved_count = 0
                for prop in properties:
                    if await self.save_property_simple(prop):
                        saved_count += 1
                
                logger.info(f"✅ Saved {saved_count} properties from {url}")
            else:
                logger.warning(f"No properties found on {url}")
        
        logger.info("=" * 60)
        logger.info("TEST COMPLETED")
        logger.info("=" * 60)

async def main():
    """Main function"""
    try:
        scraper = SimpleInmuebles24Scraper()
        await scraper.run_simple_test()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 