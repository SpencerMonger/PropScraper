#!/usr/bin/env python3
"""
Inmuebles24.com Property Scraper using Crawl4AI
Scrapes property listings and stores them in Supabase database

Requirements:
- pip install crawl4ai supabase python-dotenv requests beautifulsoup4 lxml
- Set up environment variables for Supabase connection
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inmuebles24_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Inmuebles24Scraper:
    """Main scraper class for Inmuebles24.com"""
    
    def __init__(self):
        self.base_url = "https://www.inmuebles24.com"
        self.target_url = "https://www.inmuebles24.com/casas-en-venta.html"
        
        # Initialize Supabase client
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Please set SUPABASE_URL and SUPABASE_ANON_KEY environment variables")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
        # Scraping session tracking
        self.session_id = None
        self.properties_scraped = 0
        self.properties_inserted = 0
        self.properties_updated = 0
        self.errors_count = 0
        
        # Browser configuration for Crawl4AI
        self.browser_config = BrowserConfig(
            headless=True,
            verbose=False,  # Reduce verbosity for cleaner output
            java_script_enabled=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            browser_type="chromium"
        )
        
        # CSS selectors for property extraction
        self.property_schema = {
            "name": "Properties",
            "baseSelector": ".posting-card, .property-card, article[data-qa='posting PROPERTY']",
            "fields": [
                {"name": "title", "selector": "h2, .posting-title, .property-title", "type": "text"},
                {"name": "price", "selector": ".price, .posting-price, .property-price", "type": "text"},
                {"name": "location", "selector": ".location, .posting-location, .property-location", "type": "text"},
                {"name": "details", "selector": ".details, .posting-details, .property-details", "type": "text"},
                {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                {"name": "image", "selector": "img", "type": "attribute", "attribute": "src"},
                {"name": "bedrooms", "selector": ".bedrooms, .rooms", "type": "text"},
                {"name": "bathrooms", "selector": ".bathrooms, .baths", "type": "text"},
                {"name": "area", "selector": ".area, .surface", "type": "text"},
                {"name": "property_type", "selector": ".property-type, .type", "type": "text"}
            ]
        }
    
    async def create_scraping_session(self, filters_applied: Dict = None) -> str:
        """Create a new scraping session in the database"""
        try:
            session_data = {
                "session_name": f"Inmuebles24 Scrape {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "base_url": self.target_url,
                "filters_applied": filters_applied or {},
                "status": "running"
            }
            
            result = self.supabase.table("scraping_sessions").insert(session_data).execute()
            self.session_id = result.data[0]["id"]
            logger.info(f"Created scraping session: {self.session_id}")
            return self.session_id
            
        except Exception as e:
            logger.error(f"Failed to create scraping session: {e}")
            raise
    
    async def update_session_progress(self, **kwargs):
        """Update scraping session progress"""
        if not self.session_id:
            return
        
        try:
            update_data = {
                "pages_scraped": self.properties_scraped,
                "properties_inserted": self.properties_inserted,
                "properties_updated": self.properties_updated,
                **kwargs
            }
            
            self.supabase.table("scraping_sessions").update(update_data).eq("id", self.session_id).execute()
            
        except Exception as e:
            logger.error(f"Failed to update session progress: {e}")
    
    async def log_error(self, url: str, error_type: str, error_message: str):
        """Log scraping errors to database"""
        try:
            error_data = {
                "session_id": self.session_id,
                "url": url,
                "error_type": error_type,
                "error_message": str(error_message)
            }
            
            self.supabase.table("scraping_errors").insert(error_data).execute()
            self.errors_count += 1
            
        except Exception as e:
            logger.error(f"Failed to log error: {e}")
    
    def extract_property_details(self, property_data: Dict) -> Dict:
        """Extract and clean property details from raw data"""
        cleaned_data = {
            "title": self.clean_text(property_data.get("title", "")),
            "description": self.clean_text(property_data.get("details", "")),
            "source_url": urljoin(self.base_url, property_data.get("link", "")),
            "main_image_url": property_data.get("image", ""),
            "scraped_at": datetime.utcnow().isoformat(),
        }
        
        # Extract price information
        price_text = property_data.get("price", "")
        price_info = self.extract_price(price_text)
        cleaned_data.update(price_info)
        
        # Extract location information
        location_text = property_data.get("location", "")
        location_info = self.extract_location(location_text)
        cleaned_data.update(location_info)
        
        # Extract property specifications
        bedrooms = self.extract_number(property_data.get("bedrooms", ""))
        if bedrooms:
            cleaned_data["bedrooms"] = bedrooms
        
        bathrooms = self.extract_number(property_data.get("bathrooms", ""))
        if bathrooms:
            cleaned_data["bathrooms"] = bathrooms
        
        area = self.extract_area(property_data.get("area", ""))
        if area:
            cleaned_data["total_area_m2"] = area
        
        # Extract property type
        prop_type = self.clean_text(property_data.get("property_type", ""))
        if prop_type:
            cleaned_data["property_type"] = prop_type
        
        # Set operation type based on URL
        cleaned_data["operation_type"] = "venta"  # Since we're scraping sales
        
        return cleaned_data
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove special characters that might cause issues
        text = re.sub(r'[^\w\s\-.,áéíóúüñÁÉÍÓÚÜÑ$€¢£¥₹₽₩₪₫₡]', '', text)
        return text
    
    def extract_price(self, price_text: str) -> Dict:
        """Extract price information from text"""
        result = {"currency": "MXN"}
        
        if not price_text:
            return result
        
        # Remove currency symbols and clean text
        price_clean = re.sub(r'[^\d,.]', '', price_text.replace(',', ''))
        
        # Extract numeric price
        price_match = re.search(r'(\d+(?:\.\d+)?)', price_clean)
        if price_match:
            try:
                result["price"] = float(price_match.group(1))
            except ValueError:
                pass
        
        # Detect currency
        if any(symbol in price_text.upper() for symbol in ['USD', 'DOLLAR', '$']):
            result["currency"] = "USD"
        elif any(symbol in price_text.upper() for symbol in ['EUR', 'EURO', '€']):
            result["currency"] = "EUR"
        
        return result
    
    def extract_location(self, location_text: str) -> Dict:
        """Extract location information from text"""
        result = {}
        
        if not location_text:
            return result
        
        # Split location by common separators
        parts = re.split(r'[,\-|]', location_text)
        parts = [part.strip() for part in parts if part.strip()]
        
        if parts:
            result["address"] = parts[0]
            if len(parts) > 1:
                result["neighborhood"] = parts[1]
            if len(parts) > 2:
                result["city"] = parts[2]
            if len(parts) > 3:
                result["state"] = parts[3]
        
        return result
    
    def extract_number(self, text: str) -> Optional[int]:
        """Extract number from text"""
        if not text:
            return None
        
        match = re.search(r'(\d+)', text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        
        return None
    
    def extract_area(self, area_text: str) -> Optional[float]:
        """Extract area in square meters from text"""
        if not area_text:
            return None
        
        # Look for patterns like "120 m2", "120m²", "120 sq m"
        patterns = [
            r'(\d+(?:\.\d+)?)\s*m[²2]',
            r'(\d+(?:\.\d+)?)\s*sq\s*m',
            r'(\d+(?:\.\d+)?)\s*metros'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, area_text.lower())
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    continue
        
        return None
    
    async def save_property(self, property_data: Dict) -> bool:
        """Save property to Supabase database"""
        try:
            # Check if property already exists
            existing = self.supabase.table("pulled_properties").select("id").eq("source_url", property_data["source_url"]).execute()
            
            if existing.data:
                # Update existing property
                result = self.supabase.table("pulled_properties").update(property_data).eq("source_url", property_data["source_url"]).execute()
                self.properties_updated += 1
                logger.debug(f"Updated property: {property_data['title']}")
            else:
                # Insert new property
                result = self.supabase.table("pulled_properties").insert(property_data).execute()
                self.properties_inserted += 1
                logger.debug(f"Inserted property: {property_data['title']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save property {property_data.get('title', 'Unknown')}: {e}")
            await self.log_error(property_data.get("source_url", ""), "database_error", str(e))
            return False
    
    async def scrape_property_list_page(self, url: str, page_num: int = 1) -> List[Dict]:
        """Scrape a single property listing page"""
        logger.info(f"Scraping page {page_num}: {url}")
        
        try:
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=JsonCssExtractionStrategy(self.property_schema),
                wait_for="body",
                page_timeout=45000,  # Increased timeout
                delay_before_return_html=5000,  # Wait longer for dynamic content
                verbose=False  # Reduce log noise
            )
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(url, config=crawler_config)
                
                if not result.success:
                    logger.error(f"Failed to crawl {url}: {result.error_message}")
                    await self.log_error(url, "crawl_error", result.error_message)
                    return []
                
                # Parse extracted data
                try:
                    extracted_data = json.loads(result.extracted_content)
                    properties = extracted_data if isinstance(extracted_data, list) else [extracted_data]
                    
                    logger.info(f"Found {len(properties)} properties on page {page_num}")
                    return properties
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON from {url}: {e}")
                    await self.log_error(url, "json_parse_error", str(e))
                    return []
        
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            await self.log_error(url, "general_error", str(e))
            return []
    
    async def get_total_pages(self, base_url: str) -> int:
        """Determine total number of pages to scrape"""
        try:
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                wait_for="body",
                page_timeout=20000
            )
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(base_url, config=crawler_config)
                
                if not result.success:
                    logger.warning(f"Could not determine total pages, defaulting to 50")
                    return 50
                
                # Parse HTML to find pagination
                soup = BeautifulSoup(result.html, 'html.parser')
                
                # Look for pagination elements
                pagination_selectors = [
                    '.pagination a',
                    '.pager a',
                    '.page-numbers a',
                    '[data-qa="pagination"] a'
                ]
                
                max_page = 1
                for selector in pagination_selectors:
                    links = soup.select(selector)
                    for link in links:
                        text = link.get_text().strip()
                        if text.isdigit():
                            max_page = max(max_page, int(text))
                
                logger.info(f"Detected {max_page} total pages")
                return max_page
        
        except Exception as e:
            logger.error(f"Error determining total pages: {e}")
            return 50  # Default fallback
    
    async def scrape_all_pages(self, max_pages: Optional[int] = None, start_page: int = 1):
        """Scrape all property listing pages"""
        try:
            # Create scraping session
            await self.create_scraping_session()
            
            # Determine total pages
            if max_pages is None:
                max_pages = await self.get_total_pages(self.target_url)
            
            await self.update_session_progress(total_pages=max_pages)
            
            logger.info(f"Starting to scrape {max_pages} pages, starting from page {start_page}")
            
            for page_num in range(start_page, max_pages + 1):
                try:
                    # Construct page URL
                    if page_num == 1:
                        page_url = self.target_url
                    else:
                        # Common pagination patterns
                        if '?' in self.target_url:
                            page_url = f"{self.target_url}&pagina={page_num}"
                        else:
                            page_url = f"{self.target_url}?pagina={page_num}"
                    
                    # Scrape the page
                    properties = await self.scrape_property_list_page(page_url, page_num)
                    
                    # Process and save each property
                    for prop_data in properties:
                        if not prop_data.get("title") or not prop_data.get("link"):
                            continue
                        
                        try:
                            cleaned_property = self.extract_property_details(prop_data)
                            cleaned_property["page_number"] = page_num
                            
                            await self.save_property(cleaned_property)
                            self.properties_scraped += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing property: {e}")
                            continue
                    
                    # Update progress
                    await self.update_session_progress()
                    
                    # Rate limiting
                    await asyncio.sleep(2)  # 2 second delay between pages
                    
                    logger.info(f"Completed page {page_num}/{max_pages}. "
                              f"Properties: {self.properties_scraped}, "
                              f"Inserted: {self.properties_inserted}, "
                              f"Updated: {self.properties_updated}")
                
                except Exception as e:
                    logger.error(f"Error on page {page_num}: {e}")
                    continue
            
            # Mark session as completed
            await self.update_session_progress(
                status="completed",
                completed_at=datetime.utcnow().isoformat()
            )
            
            logger.info(f"Scraping completed! "
                       f"Total properties: {self.properties_scraped}, "
                       f"Inserted: {self.properties_inserted}, "
                       f"Updated: {self.properties_updated}, "
                       f"Errors: {self.errors_count}")
        
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}")
            await self.update_session_progress(
                status="failed",
                error_message=str(e),
                completed_at=datetime.utcnow().isoformat()
            )
            raise

async def main():
    """Main function to run the scraper"""
    scraper = Inmuebles24Scraper()
    
    # You can customize these parameters
    MAX_PAGES = 10  # Start with 10 pages for testing
    START_PAGE = 1
    
    try:
        await scraper.scrape_all_pages(max_pages=MAX_PAGES, start_page=START_PAGE)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        await scraper.update_session_progress(
            status="paused",
            completed_at=datetime.utcnow().isoformat()
        )
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 