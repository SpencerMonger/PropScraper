#!/usr/bin/env python3
"""
Pincali.com Property Scraper using Crawl4AI
Scrapes property listings from Pincali.com and stores them in Supabase database

Requirements:
- pip install crawl4ai supabase python-dotenv requests beautifulsoup4 lxml
- Set up environment variables for Supabase connection
"""

import asyncio
import hashlib
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
        logging.FileHandler('pincali_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PincaliScraper:
    """Main scraper class for Pincali.com"""
    
    def __init__(self):
        self.base_url = "https://www.pincali.com"
        self.target_url = "https://www.pincali.com/en/properties/residential-listings-for-sale-or-rent"
        
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
            verbose=False,
            java_script_enabled=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            browser_type="chromium"
        )
        
        # CSS selectors for property extraction - based on actual Pincali structure
        self.property_schema = {
            "name": "Properties",
            "baseSelector": "li.property__component",
            "fields": [
                {"name": "title", "selector": ".title", "type": "text"},
                {"name": "price", "selector": "li.price", "type": "text"},
                {"name": "location", "selector": ".location", "type": "text"},
                {"name": "features", "selector": ".features", "type": "text"},
                {"name": "link", "selector": "a.property__content", "type": "attribute", "attribute": "href"},
                {"name": "image", "selector": ".property__media img", "type": "attribute", "attribute": "data-src"},
                {"name": "latitude", "selector": "", "type": "attribute", "attribute": "data-lat"},
                {"name": "longitude", "selector": "", "type": "attribute", "attribute": "data-long"},
                {"name": "bedrooms", "selector": ".features div:contains('bedroom')", "type": "text"},
                {"name": "bathrooms", "selector": ".features div:contains('bathroom')", "type": "text"},
                {"name": "area", "selector": ".features div:contains('m²')", "type": "text"}
            ]
        }
    
    async def create_scraping_session(self, filters_applied: Dict = None) -> str:
        """Create a new scraping session in the database"""
        try:
            session_data = {
                "session_name": f"Pincali Scrape {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
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
    
    async def drop_all_properties(self):
        """Drop all existing properties from the database (for testing)"""
        try:
            logger.warning("WARNING: DROPPING ALL PROPERTIES FROM DATABASE...")
            
            # First get count for confirmation
            count_result = self.supabase.table("pulled_properties").select("id", count="exact").execute()
            total_count = count_result.count
            
            if total_count == 0:
                logger.info("Database is already empty")
                return
            
            logger.warning(f"About to delete {total_count} properties...")
            
            # Use a simple delete all approach - Supabase supports this
            # Delete all records at once using a condition that matches all records
            try:
                # Delete all records where id is not null (which should be all records)
                result = self.supabase.table("pulled_properties").delete().neq("id", "").execute()
                logger.info(f"Successfully deleted all properties from database")
                
                # Verify deletion
                verify_result = self.supabase.table("pulled_properties").select("id", count="exact").execute()
                remaining_count = verify_result.count
                
                if remaining_count == 0:
                    logger.info("✅ Database is now empty")
                else:
                    logger.warning(f"⚠️  {remaining_count} properties still remain in database")
                    
            except Exception as delete_error:
                logger.error(f"Bulk delete failed: {delete_error}")
                logger.info("Falling back to batch delete method...")
                
                # Fallback: Delete in larger batches
                batch_size = 100  # Larger batch size
                deleted_count = 0
                max_iterations = 50  # Prevent infinite loops
                iteration = 0
                
                while iteration < max_iterations:
                    # Get a batch of IDs
                    batch_result = self.supabase.table("pulled_properties").select("id").limit(batch_size).execute()
                    
                    if not batch_result.data:
                        break
                    
                    # Delete this batch using a single query with OR conditions
                    ids_to_delete = [record["id"] for record in batch_result.data]
                    
                    # Use the 'in' operator for bulk delete
                    delete_result = self.supabase.table("pulled_properties").delete().in_("id", ids_to_delete).execute()
                    
                    deleted_count += len(ids_to_delete)
                    logger.info(f"Deleted batch {iteration + 1}: {deleted_count} properties...")
                    
                    iteration += 1
                
                if iteration >= max_iterations:
                    logger.error("⚠️  Reached maximum iterations, stopping delete process")
                else:
                    logger.info(f"✅ Successfully deleted {deleted_count} properties from database")
            
        except Exception as e:
            logger.error(f"Failed to drop properties: {e}")
            raise
    
    def extract_property_details(self, property_data: Dict) -> Dict:
        """Extract and clean property details from raw data"""
        source_url = urljoin(self.base_url, property_data.get("link", ""))
        
        cleaned_data = {
            "property_id": self.generate_property_id(source_url),
            "title": self.clean_text(property_data.get("title", "")),
            "description": self.clean_text(property_data.get("details", "")),
            "source_url": source_url,
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
        
        # Extract coordinates if available
        if property_data.get("latitude"):
            try:
                cleaned_data["latitude"] = float(property_data["latitude"])
            except (ValueError, TypeError):
                pass
        
        if property_data.get("longitude"):
            try:
                cleaned_data["longitude"] = float(property_data["longitude"])
            except (ValueError, TypeError):
                pass
        
        # Extract property type
        prop_type = self.clean_text(property_data.get("property_type", ""))
        if prop_type:
            cleaned_data["property_type"] = prop_type
        
        # Extract operation type (sale/rent)
        operation = self.extract_operation_type(property_data.get("operation_type", ""))
        if operation:
            cleaned_data["operation_type"] = operation
        
        # Extract agent information
        agent_info = property_data.get("agent_info", "")
        if agent_info:
            agent_data = self.extract_agent_info(agent_info)
            cleaned_data.update(agent_data)
        
        # Extract features/amenities
        features_text = property_data.get("features", "")
        if features_text:
            features = self.extract_features(features_text)
            cleaned_data["features"] = features
            
            # Set boolean flags based on features
            cleaned_data.update(self.extract_feature_flags(features_text))
        
        return cleaned_data
    
    def generate_property_id(self, source_url: str) -> str:
        """Generate a unique property ID from the source URL"""
        
        if not source_url:
            # Fallback for empty URLs - should never happen but just in case
            return f"pincali_empty_{hashlib.md5('empty'.encode()).hexdigest()[:8]}"
        
        # Extract the path part of the URL which should be unique for each property
        parsed_url = urlparse(source_url)
        
        # Use the path as the base for the property ID
        # For Pincali URLs like "/en/home/property-name", we want the "property-name" part
        path_parts = parsed_url.path.strip('/').split('/')
        
        if len(path_parts) >= 3 and path_parts[0] == 'en' and path_parts[1] == 'home':
            # Use the property slug from the URL (everything after /en/home/)
            property_slug = '/'.join(path_parts[2:])  # Join all parts after 'home' in case there are multiple segments
            if property_slug:  # Make sure we have a non-empty slug
                return f"pincali_{property_slug}"
            else:
                # Fallback if somehow the slug is empty
                url_hash = hashlib.md5(source_url.encode()).hexdigest()[:16]
                return f"pincali_hash_{url_hash}"
        else:
            # Fallback: use hash of the full URL for any unexpected URL patterns
            url_hash = hashlib.md5(source_url.encode()).hexdigest()[:16]
            return f"pincali_hash_{url_hash}"
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove special characters that might cause issues but preserve international characters
        text = re.sub(r'[^\w\s\-.,áéíóúüñÁÉÍÓÚÜÑ$€¢£¥₹₽₩₪₫₡]', '', text)
        return text
    
    def extract_price(self, price_text: str) -> Dict:
        """Extract price information from text"""
        result = {"currency": "MXN"}  # Default to MXN for Mexico
        
        if not price_text:
            return result
        
        # Detect currency first
        if any(symbol in price_text.upper() for symbol in ['USD', 'DOLLAR', '$']):
            if 'US$' in price_text or 'USD' in price_text:
                result["currency"] = "USD"
        elif any(symbol in price_text.upper() for symbol in ['EUR', 'EURO', '€']):
            result["currency"] = "EUR"
        elif 'MXN' in price_text.upper() or '$' in price_text:
            result["currency"] = "MXN"
        
        # Remove currency symbols and clean text
        price_clean = re.sub(r'[^\d,.]', '', price_text.replace(',', ''))
        
        # Extract numeric price
        price_match = re.search(r'(\d+(?:\.\d+)?)', price_clean)
        if price_match:
            try:
                result["price"] = float(price_match.group(1))
            except ValueError:
                pass
        
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
    
    def extract_operation_type(self, operation_text: str) -> str:
        """Extract operation type (sale/rent) from text"""
        if not operation_text:
            return "sale"  # Default
        
        text_lower = operation_text.lower()
        if any(word in text_lower for word in ['rent', 'renta', 'alquiler', 'arrendamiento']):
            return "rent"
        elif any(word in text_lower for word in ['sale', 'venta', 'sell']):
            return "sale"
        
        return "sale"  # Default
    
    def extract_agent_info(self, agent_text: str) -> Dict:
        """Extract agent information from text"""
        result = {}
        
        if not agent_text:
            return result
        
        # Extract email
        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', agent_text)
        if email_match:
            result["agent_email"] = email_match.group()
        
        # Extract phone number
        phone_match = re.search(r'[\+]?[\d\s\-\(\)]{7,15}', agent_text)
        if phone_match:
            result["agent_phone"] = phone_match.group().strip()
        
        # Extract agent name (simple heuristic)
        lines = agent_text.split('\n')
        for line in lines:
            line = line.strip()
            if line and not re.search(r'[@\+\d]', line) and len(line) < 50:
                result["agent_name"] = line
                break
        
        return result
    
    def extract_features(self, features_text: str) -> List[str]:
        """Extract features/amenities from text"""
        if not features_text:
            return []
        
        # Common feature keywords
        feature_keywords = [
            'pool', 'piscina', 'gym', 'gimnasio', 'garden', 'jardín',
            'elevator', 'elevador', 'balcony', 'balcón', 'terrace', 'terraza',
            'security', 'seguridad', 'parking', 'estacionamiento', 'garage',
            'furnished', 'amueblado', 'air conditioning', 'aire acondicionado'
        ]
        
        features = []
        text_lower = features_text.lower()
        
        for keyword in feature_keywords:
            if keyword in text_lower:
                features.append(keyword)
        
        return features
    
    def extract_feature_flags(self, features_text: str) -> Dict:
        """Extract boolean feature flags from text"""
        result = {}
        
        if not features_text:
            return result
        
        text_lower = features_text.lower()
        
        # Feature mappings
        feature_flags = {
            'has_pool': ['pool', 'piscina'],
            'has_garden': ['garden', 'jardín'],
            'has_elevator': ['elevator', 'elevador'],
            'has_balcony': ['balcony', 'balcón'],
            'has_terrace': ['terrace', 'terraza'],
            'has_gym': ['gym', 'gimnasio'],
            'has_security': ['security', 'seguridad'],
            'pet_friendly': ['pet', 'mascota'],
            'furnished': ['furnished', 'amueblado']
        }
        
        for flag, keywords in feature_flags.items():
            result[flag] = any(keyword in text_lower for keyword in keywords)
        
        return result
    
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
        """Scrape a single property listing page using HTTP fallback directly"""
        logger.info(f"Scraping page {page_num}: {url}")
        
        # Skip browser method and go straight to HTTP since we know it works
        logger.debug("Using HTTP method directly (browser method times out)")
        return await self.http_scrape_method(url, page_num)
    
    async def http_scrape_method(self, url: str, page_num: int) -> List[Dict]:
        """HTTP-based scraping method"""
        try:
            import requests
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.pincali.com',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                logger.debug(f"HTTP request successful for page {page_num}")
                return await self.fallback_html_parsing(response.text, url)
            else:
                logger.error(f"HTTP request failed for {url}: {response.status_code}")
                await self.log_error(url, "http_error", f"Status code: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"HTTP scraping error for {url}: {e}")
            await self.log_error(url, "http_error", str(e))
            return []
    
    async def scrape_property_details(self, detail_url: str) -> Dict:
        """Scrape detailed information from individual property page"""
        logger.debug(f"Scraping property details: {detail_url}")
        
        try:
            import requests
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.pincali.com',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(detail_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return await self.extract_detailed_property_info(response.text, detail_url)
            else:
                logger.warning(f"Failed to fetch property details {detail_url}: {response.status_code}")
                await self.log_error(detail_url, "detail_http_error", f"Status code: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error scraping property details {detail_url}: {e}")
            await self.log_error(detail_url, "detail_error", str(e))
            return {}
    
    async def extract_detailed_property_info(self, html: str, url: str) -> Dict:
        """Extract detailed information from property detail page HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            details = {}
            
            # Note: We don't extract property_id here since it's already generated from the URL
            # in the extract_property_details method and we don't want to overwrite it
            
            # Extract full description
            desc_selectors = [
                '.description',
                '[class*="description"]',
                '.property-description',
                '.details-section',
                '.property-details'
            ]
            
            for selector in desc_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    details["description"] = self.clean_text(desc_elem.get_text())
                    break
            
            # Extract agent information
            agent_selectors = [
                '.agent-info',
                '.contact-info',
                '[class*="agent"]',
                '[class*="contact"]'
            ]
            
            for selector in agent_selectors:
                agent_elem = soup.select_one(selector)
                if agent_elem:
                    agent_data = self.extract_agent_info(agent_elem.get_text())
                    details.update(agent_data)
                    break
            
            # Extract all images
            image_elements = soup.select('img[src], img[data-src]')
            image_urls = []
            
            for img in image_elements:
                img_url = img.get('data-src') or img.get('src')
                if img_url and 'property_images' in img_url:  # Filter for property images
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = 'https://assets.easybroker.com' + img_url
                    
                    if img_url not in image_urls:
                        image_urls.append(img_url)
            
            if image_urls:
                details["image_urls"] = image_urls
                if not details.get("main_image_url"):
                    details["main_image_url"] = image_urls[0]
            
            # Extract specific property features
            feature_sections = soup.select('.features, .amenities, .characteristics, [class*="feature"]')
            
            all_features = []
            for section in feature_sections:
                feature_items = section.select('li, div, span')
                for item in feature_items:
                    feature_text = self.clean_text(item.get_text())
                    if feature_text and len(feature_text) > 2:
                        all_features.append(feature_text)
            
            if all_features:
                details["features"] = all_features
                # Set boolean flags based on features
                features_text = ' '.join(all_features).lower()
                details.update(self.extract_feature_flags(features_text))
            
            # Extract additional specifications
            spec_selectors = [
                '.specifications',
                '.property-specs',
                '.details-grid',
                '[class*="spec"]'
            ]
            
            for selector in spec_selectors:
                spec_section = soup.select_one(selector)
                if spec_section:
                    # Look for specific data points
                    spec_items = spec_section.select('div, li, tr')
                    for item in spec_items:
                        text = item.get_text().lower()
                        
                        # Extract parking spaces
                        if 'parking' in text and not details.get('parking_spaces'):
                            parking_num = self.extract_number(text)
                            if parking_num:
                                details['parking_spaces'] = parking_num
                        
                        # Extract floor information
                        if 'floor' in text and not details.get('floor_number'):
                            floor_num = self.extract_number(text)
                            if floor_num:
                                details['floor_number'] = floor_num
                        
                        # Extract year built
                        if any(word in text for word in ['built', 'year', 'construction']) and not details.get('construction_year'):
                            year_match = re.search(r'(19|20)\d{2}', text)
                            if year_match:
                                try:
                                    details['construction_year'] = int(year_match.group())
                                except ValueError:
                                    pass
            
            # Extract price per m2 if available
            price_per_m2_elem = soup.select_one('[class*="price-per"], .price-m2')
            if price_per_m2_elem:
                price_per_m2_text = price_per_m2_elem.get_text()
                price_per_m2 = self.extract_number(price_per_m2_text.replace(',', ''))
                if price_per_m2:
                    details['price_per_m2'] = price_per_m2
            
            # Extract listing date if available
            date_elem = soup.select_one('.listing-date, .published-date, [class*="date"]')
            if date_elem:
                date_text = date_elem.get_text()
                # Try to parse date - this is basic, could be improved
                date_match = re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{4}', date_text)
                if date_match:
                    details['listing_date'] = date_match.group()
            
            logger.debug(f"Extracted {len(details)} additional details from {url}")
            return details
            
        except Exception as e:
            logger.error(f"Error extracting detailed property info from {url}: {e}")
            return {}
    
    async def fallback_html_parsing(self, html: str, url: str) -> List[Dict]:
        """Parse HTML directly using the actual Pincali structure"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            properties = []
            
            # Use the correct selector for Pincali property listings
            property_elements = soup.select('li.property__component')
            
            if property_elements:
                logger.info(f"Found {len(property_elements)} properties using Pincali structure")
                
                for elem in property_elements:
                    prop_data = {}
                    
                    # Extract title
                    title_elem = elem.select_one('.title')
                    if title_elem:
                        prop_data['title'] = title_elem.get_text(strip=True)
                    
                    # Extract price
                    price_elem = elem.select_one('li.price')
                    if price_elem:
                        prop_data['price'] = price_elem.get_text(strip=True)
                    
                    # Extract location
                    location_elem = elem.select_one('.location')
                    if location_elem:
                        prop_data['location'] = location_elem.get_text(strip=True)
                    
                    # Extract link
                    link_elem = elem.select_one('a.property__content')
                    if link_elem and link_elem.get('href'):
                        prop_data['link'] = link_elem.get('href')
                    
                    # Extract main image
                    image_elem = elem.select_one('.property__media img')
                    if image_elem:
                        # Try data-src first (lazy loading), then src
                        image_url = image_elem.get('data-src') or image_elem.get('src')
                        if image_url:
                            prop_data['image'] = image_url
                    
                    # Extract coordinates from the li element itself
                    lat = elem.get('data-lat')
                    lng = elem.get('data-long')
                    if lat:
                        prop_data['latitude'] = lat
                    if lng:
                        prop_data['longitude'] = lng
                    
                    # Extract features (bedrooms, bathrooms, area)
                    features_elem = elem.select_one('.features')
                    if features_elem:
                        features_text = features_elem.get_text(strip=True)
                        prop_data['features'] = features_text
                        
                        # Parse individual features
                        feature_divs = features_elem.select('div')
                        for div in feature_divs:
                            text = div.get_text(strip=True).lower()
                            if 'bedroom' in text:
                                prop_data['bedrooms'] = text
                            elif 'bathroom' in text:
                                prop_data['bathrooms'] = text
                            elif 'm²' in text:
                                prop_data['area'] = text
                    
                    # Extract operation type from price section
                    if price_elem:
                        price_text = price_elem.get_text(strip=True).lower()
                        if 'for sale' in price_text:
                            prop_data['operation_type'] = 'For Sale'
                        elif 'for rent' in price_text:
                            prop_data['operation_type'] = 'For Rent'
                    
                    # Only add if we have essential data
                    if prop_data.get('title') and prop_data.get('link'):
                        properties.append(prop_data)
            
            else:
                logger.warning(f"No properties found with Pincali selector for {url}")
            
            return properties
            
        except Exception as e:
            logger.error(f"HTML parsing failed for {url}: {e}")
            return []
    
    async def get_total_pages(self, base_url: str) -> int:
        """Determine total number of pages to scrape"""
        try:
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                wait_for="body",
                page_timeout=30000,
                delay_before_return_html=5000
            )
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(base_url, config=crawler_config)
                
                if not result.success:
                    logger.warning(f"Could not determine total pages, defaulting to 100")
                    return 100
                
                # Parse HTML to find pagination
                soup = BeautifulSoup(result.html, 'html.parser')
                
                # Look for pagination elements - adapted for Pincali
                pagination_selectors = [
                    '.pagination a',
                    '.pager a',
                    '.page-numbers a',
                    '[class*="pagination"] a',
                    '[class*="page"] a',
                    'nav a'
                ]
                
                max_page = 1
                for selector in pagination_selectors:
                    links = soup.select(selector)
                    for link in links:
                        text = link.get_text().strip()
                        if text.isdigit():
                            max_page = max(max_page, int(text))
                        
                        # Also check href for page numbers
                        href = link.get('href', '')
                        page_match = re.search(r'page=(\d+)', href)
                        if page_match:
                            max_page = max(max_page, int(page_match.group(1)))
                
                logger.info(f"Detected {max_page} total pages")
                return min(max_page, 500)  # Cap at 500 pages for safety
        
        except Exception as e:
            logger.error(f"Error determining total pages: {e}")
            return 100  # Default fallback
    
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
                    # Construct page URL for Pincali
                    if page_num == 1:
                        page_url = self.target_url
                    else:
                        # Pincali uses page parameter
                        if '?' in self.target_url:
                            page_url = f"{self.target_url}&page={page_num}"
                        else:
                            page_url = f"{self.target_url}?page={page_num}"
                    
                    # Scrape the page
                    properties = await self.scrape_property_list_page(page_url, page_num)
                    
                    # Process and save each property
                    for prop_data in properties:
                        if not prop_data.get("title") and not prop_data.get("link"):
                            continue
                        
                        try:
                            # First extract basic data from listing page
                            cleaned_property = self.extract_property_details(prop_data)
                            cleaned_property["page_number"] = page_num
                            
                            # Then scrape detailed information from property detail page
                            if prop_data.get("link"):
                                detail_url = urljoin(self.base_url, prop_data["link"])
                                detailed_data = await self.scrape_property_details(detail_url)
                                
                                # Merge detailed data with basic data
                                if detailed_data:
                                    cleaned_property.update(detailed_data)
                                
                                # Rate limiting for detail page requests
                                await asyncio.sleep(1)  # 1 second delay between detail requests
                            
                            await self.save_property(cleaned_property)
                            self.properties_scraped += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing property: {e}")
                            continue
                    
                    # Update progress
                    await self.update_session_progress()
                    
                    # Rate limiting - be respectful to Pincali
                    await asyncio.sleep(3)  # 3 second delay between pages
                    
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
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description='Pincali.com Property Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pincali_scraper.py --pages 1          # Scrape only 1 page
  python pincali_scraper.py --pages 5          # Scrape 5 pages
  python pincali_scraper.py --pages 10 --start 3  # Scrape pages 3-12
  python pincali_scraper.py                    # Scrape default 10 pages
  python pincali_scraper.py --drop --pages 1   # Drop all data and scrape 1 page fresh
        """
    )
    
    parser.add_argument(
        '--pages', 
        type=int, 
        default=10,
        help='Number of pages to scrape (default: 10)'
    )
    
    parser.add_argument(
        '--start', 
        type=int, 
        default=1,
        help='Starting page number (default: 1)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop all existing properties from database before scraping (WARNING: This deletes all data!)'
    )
    
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Verbose logging enabled")
    
    # Validate arguments
    if args.pages < 1:
        logger.error("Number of pages must be at least 1")
        return
    
    if args.start < 1:
        logger.error("Starting page must be at least 1")
        return
    
    # Initialize scraper
    scraper = PincaliScraper()
    
    # Log scraping parameters
    logger.info(f"Starting Pincali scraper")
    logger.info(f"Pages to scrape: {args.pages}")
    logger.info(f"Starting from page: {args.start}")
    logger.info(f"Total pages: {args.start} to {args.start + args.pages - 1}")
    
    try:
        if args.drop:
            await scraper.drop_all_properties()
            logger.info("Database dropped. Starting fresh scrape.")
        
        await scraper.scrape_all_pages(max_pages=args.pages, start_page=args.start)
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