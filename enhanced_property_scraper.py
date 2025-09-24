#!/usr/bin/env python3
"""
Enhanced Pincali.com Property Scraper using Staging Table Architecture
Scrapes property listings from Pincali.com and stores them in staging table,
then uses the services architecture to sync with live table.

This is based on the working pincali_scraper.py but enhanced to use the 
dual-table architecture with staging and live tables.

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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

from services.property_sync_orchestrator import PropertySyncOrchestrator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_pincali_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedPincaliScraper:
    """Enhanced scraper class for Pincali.com using staging table architecture"""
    
    def __init__(self):
        self.base_url = "https://www.pincali.com"
        self.target_url = "https://www.pincali.com/en/properties/residential-listings-for-sale-or-rent"
        
        # Initialize Supabase client
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_ANON_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Please set SUPABASE_URL and SUPABASE_ANON_KEY environment variables")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        
        # Initialize services orchestrator
        self.orchestrator = PropertySyncOrchestrator(self.supabase)
        
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
        
        # CSS selectors for property extraction - based on actual Pincali structure from working Scrapy code
        self.property_schema = {
            "name": "Properties",
            "baseSelector": "li.property__component",
            "fields": [
                {"name": "title", "selector": ".title", "type": "text"},
                {"name": "price", "selector": "li.price", "type": "text"},
                {"name": "location", "selector": ".location", "type": "text"},
                {"name": "features", "selector": ".features", "type": "text"},
                {"name": "link", "selector": "a", "type": "attribute", "attribute": "href"},
                {"name": "image", "selector": ".property__media img", "type": "attribute", "attribute": "src"},
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
                "session_name": f"Enhanced Pincali Scrape {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
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
        """Extract and clean property details from raw data - same as working scraper"""
        source_url = urljoin(self.base_url, property_data.get("link", ""))
        
        cleaned_data = {
            "property_id": self.generate_property_id(source_url),
            "title": self.clean_text(property_data.get("title", "")),
            "description": self.clean_text(property_data.get("details", "")),
            "source_url": source_url,
            "main_image_url": property_data.get("image", ""),
            "scraped_at": datetime.utcnow().isoformat(),
            "session_id": self.session_id,  # Add session_id for staging table
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
        
        # Extract coordinates and GPS string
        if property_data.get("latitude") and property_data.get("longitude"):
            try:
                lat = float(property_data["latitude"])
                lng = float(property_data["longitude"])
                cleaned_data["latitude"] = lat
                cleaned_data["longitude"] = lng
                cleaned_data["gps_coordinates"] = f"{lat},{lng}"
            except (ValueError, TypeError):
                pass
        
        # Extract property type from description/title
        property_type = self.extract_property_type_from_text(
            cleaned_data.get("title", "") + " " + cleaned_data.get("description", "")
        )
        if property_type:
            cleaned_data["property_type"] = property_type
        
        # Extract operation type (sale/rent)
        operation = self.extract_operation_type(property_data.get("operation_type", ""))
        if operation:
            cleaned_data["operation_type"] = operation
        
        # Extract agent and agency information (with "published by" removal)
        agent_info = property_data.get("agent_info", "")
        if agent_info:
            agent_data = self.extract_agent_and_agency_info(agent_info)
            cleaned_data.update(agent_data)
        
        # Extract listing date from "published X ago" text
        published_text = property_data.get("published_text", "")
        if published_text:
            listing_date = self.extract_listing_date(published_text, cleaned_data["scraped_at"])
            if listing_date:
                cleaned_data["listing_date"] = listing_date
        
        # Extract message URL
        message_url = property_data.get("message_url", "")
        if message_url:
            cleaned_data["message_url"] = urljoin(self.base_url, message_url)
        
        # Extract structured amenities (replacing boolean flags)
        amenities_data = property_data.get("amenities_data", {})
        if amenities_data:
            cleaned_data["amenities"] = self.structure_amenities(amenities_data)
        
        # Extract features
        features_text = property_data.get("features", "")
        if features_text:
            features = self.extract_features(features_text)
            cleaned_data["features"] = features
        
        return cleaned_data
    
    def generate_property_id(self, source_url: str) -> str:
        """Generate a unique property ID from the source URL - same as working scraper"""
        
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
        """Clean and normalize text data - same as working scraper"""
        if not text:
            return ""
        
        # Remove 'Description' prefix if it's at the start of the text
        text = re.sub(r'^Description\s*:?\s*', '', text, flags=re.IGNORECASE)
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove special characters that might cause issues but preserve international characters
        text = re.sub(r'[^\w\s\-.,áéíóúüñÁÉÍÓÚÜÑ$€¢£¥₹₽₩₪₫₡]', '', text)
        return text
    
    def extract_price(self, price_text: str) -> Dict:
        """Extract price information from text - same as working scraper"""
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
        """Extract location information from text including postal code parsing - same as working scraper"""
        result = {}
        
        if not location_text:
            return result
        
        # First, extract and remove postal code to avoid confusion
        postal_code = None
        postal_match = re.search(r'\b(\d{5})\b', location_text)
        if postal_match:
            postal_code = postal_match.group(1)
            result["postal_code"] = postal_code
            # Remove postal code from text for further processing
            location_text = re.sub(r'\b\d{5}\b', '', location_text).strip()
        
        # Split location by common separators
        parts = re.split(r'[,\-|]', location_text)
        parts = [part.strip() for part in parts if part.strip()]
        
        if not parts:
            return result
        
        # First part is always the address
        result["address"] = parts[0]
        
        # Common Mexican states for validation
        mexican_states = [
            'aguascalientes', 'baja california', 'baja california sur', 'campeche',
            'chiapas', 'chihuahua', 'coahuila', 'colima', 'durango', 'guanajuato',
            'guerrero', 'hidalgo', 'jalisco', 'méxico', 'michoacán', 'morelos',
            'nayarit', 'nuevo león', 'oaxaca', 'puebla', 'querétaro', 'quintana roo',
            'san luis potosí', 'sinaloa', 'sonora', 'tabasco', 'tamaulipas',
            'tlaxcala', 'veracruz', 'yucatán', 'zacatecas', 'ciudad de méxico',
            'cdmx', 'df', 'estado de méxico'
        ]
        
        # Process remaining parts
        remaining_parts = parts[1:] if len(parts) > 1 else []
        
        if len(remaining_parts) == 1:
            # Only one remaining part - could be neighborhood, city, or state
            part_lower = remaining_parts[0].lower()
            if any(state in part_lower for state in mexican_states):
                result["state"] = remaining_parts[0]
            else:
                result["neighborhood"] = remaining_parts[0]
                
        elif len(remaining_parts) == 2:
            # Two remaining parts - likely neighborhood/city and state
            first_part = remaining_parts[0]
            second_part = remaining_parts[1]
            second_part_lower = second_part.lower()
            
            if any(state in second_part_lower for state in mexican_states):
                result["neighborhood"] = first_part
                result["state"] = second_part
            else:
                result["neighborhood"] = first_part
                result["city"] = second_part
                
        elif len(remaining_parts) >= 3:
            # Three or more parts - neighborhood, city, state
            result["neighborhood"] = remaining_parts[0]
            result["city"] = remaining_parts[1]
            result["state"] = remaining_parts[2]
        
        return result
    
    def extract_number(self, text: str) -> Optional[int]:
        """Extract number from text - same as working scraper"""
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
        """Extract area in square meters from text - same as working scraper"""
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
        """Extract operation type (sale/rent) from text - same as working scraper"""
        if not operation_text:
            return "sale"  # Default
        
        text_lower = operation_text.lower()
        if any(word in text_lower for word in ['rent', 'renta', 'alquiler', 'arrendamiento']):
            return "rent"
        elif any(word in text_lower for word in ['sale', 'venta', 'sell']):
            return "sale"
        
        return "sale"  # Default
    
    def extract_agent_and_agency_info(self, agent_text: str) -> Dict:
        """Extract agent and agency information from text, removing 'published by' text - same as working scraper"""
        result = {}
        
        if not agent_text:
            return result
        
        # Clean text by removing "published by" and similar prefixes
        cleaned_text = re.sub(r'(?i)published\s+by\s*:?\s*', '', agent_text)
        cleaned_text = re.sub(r'(?i)contact\s*:?\s*', '', cleaned_text)
        cleaned_text = cleaned_text.strip()
        
        # Extract email
        email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', cleaned_text)
        if email_match:
            result["agent_email"] = email_match.group()
        
        # Extract phone number
        phone_match = re.search(r'[\+]?[\d\s\-\(\)]{7,15}', cleaned_text)
        if phone_match:
            result["agent_phone"] = phone_match.group().strip()
        
        # Split lines to extract agent and agency names
        lines = [line.strip() for line in cleaned_text.split('\n') if line.strip()]
        
        # Look for agent name and agency name patterns
        agent_name = None
        agency_name = None
        
        for line in lines:
            # Skip lines with contact info
            if re.search(r'[@\+\d]', line) or len(line) > 100:
                continue
                
            # First clean line without contact info is likely the agent name
            if not agent_name and len(line) < 60:
                # Check if it looks like a person's name (has space, proper case, etc.)
                if ' ' in line and line.replace(' ', '').isalpha():
                    agent_name = line
                elif not agency_name:  # If no space, might be agency
                    agency_name = line
            # Second clean line is likely the agency name
            elif not agency_name and len(line) < 100:
                agency_name = line
        
        # Sometimes the format is "Agent Name\nAgency Name" or "Agency Name\nAgent Name"
        # Use heuristics to determine which is which
        if len(lines) >= 2:
            first_line = lines[0]
            second_line = lines[1]
            
            # If first line looks like a person name (has space, proper case)
            if ' ' in first_line and first_line.replace(' ', '').replace('.', '').isalpha():
                result["agent_name"] = first_line
                result["agency_name"] = second_line
            # If second line looks more like a person name
            elif ' ' in second_line and second_line.replace(' ', '').replace('.', '').isalpha():
                result["agency_name"] = first_line
                result["agent_name"] = second_line
            # Default: first is agency, second is agent
            else:
                result["agency_name"] = first_line
                if len(lines) > 1:
                    result["agent_name"] = second_line
        elif agent_name:
            result["agent_name"] = agent_name
        elif agency_name:
            result["agency_name"] = agency_name
        
        return result
    
    def extract_features(self, features_text: str) -> List[str]:
        """Extract features/amenities from text - same as working scraper"""
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
    
    def extract_property_type_from_text(self, text: str) -> str:
        """Extract property type from title/description text - same as working scraper"""
        if not text:
            return ""
        
        text_lower = text.lower()
        
        # Property type mappings based on common Spanish/English terms
        type_mappings = {
            'house': ['house', 'casa', 'home', 'villa', 'chalet'],
            'apartment': ['apartment', 'departamento', 'depto', 'condo', 'condominium', 'flat'],
            'lot': ['lot', 'lote', 'terreno', 'land', 'plot', 'terrain']
        }
        
        for prop_type, keywords in type_mappings.items():
            if any(keyword in text_lower for keyword in keywords):
                return prop_type
        
        return "house"  # Default fallback
    
    def extract_listing_date(self, published_text: str, scraped_at: str) -> Optional[str]:
        """Extract listing date from 'Published X ago' text by subtracting from scraped_at time - same as working scraper"""
        if not published_text:
            return None
        
        try:
            # Parse the scraped_at timestamp (this is when we scraped the data)
            if scraped_at.endswith('Z'):
                scraped_datetime = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
            elif '+' in scraped_at or scraped_at.endswith('00:00'):
                scraped_datetime = datetime.fromisoformat(scraped_at)
            else:
                # Assume UTC if no timezone info
                scraped_datetime = datetime.fromisoformat(scraped_at)
                if scraped_datetime.tzinfo is None:
                    from datetime import timezone
                    scraped_datetime = scraped_datetime.replace(tzinfo=timezone.utc)
            
            # Extract time information from published text
            text_lower = published_text.lower().strip()
            
            # Patterns for different time units with more flexible matching
            time_patterns = [
                # Numbered patterns
                (r'(\d+)\s*minute[s]?\s*ago', 'minutes'),
                (r'(\d+)\s*hour[s]?\s*ago', 'hours'), 
                (r'(\d+)\s*day[s]?\s*ago', 'days'),
                (r'(\d+)\s*week[s]?\s*ago', 'weeks'),
                (r'(\d+)\s*month[s]?\s*ago', 'months'),
                (r'(\d+)\s*year[s]?\s*ago', 'years'),
                # "a" or "an" patterns
                (r'\ba\s+minute\s+ago', 'minutes'),
                (r'\ban\s+hour\s+ago', 'hours'),
                (r'\ba\s+day\s+ago', 'days'),
                (r'\ba\s+week\s+ago', 'weeks'),
                (r'\ba\s+month\s+ago', 'months'),
                (r'\ba\s+year\s+ago', 'years'),
                # Alternative "an" patterns
                (r'\ban\s+hour\s+ago', 'hours'),
            ]
            
            for pattern, unit in time_patterns:
                match = re.search(pattern, text_lower)
                if match:
                    try:
                        # Try to extract the number from the match
                        if match.groups() and match.group(1) and match.group(1).isdigit():
                            amount = int(match.group(1))
                        else:
                            amount = 1  # For "a minute ago", "an hour ago", etc.
                    except (IndexError, AttributeError):
                        amount = 1  # Default for patterns without capture groups
                    
                    # Calculate the listing date by subtracting from scraped time
                    if unit == 'minutes':
                        listing_datetime = scraped_datetime - timedelta(minutes=amount)
                    elif unit == 'hours':
                        listing_datetime = scraped_datetime - timedelta(hours=amount)
                    elif unit == 'days':
                        listing_datetime = scraped_datetime - timedelta(days=amount)
                    elif unit == 'weeks':
                        listing_datetime = scraped_datetime - timedelta(weeks=amount)
                    elif unit == 'months':
                        listing_datetime = scraped_datetime - timedelta(days=amount * 30)  # Approximate
                    elif unit == 'years':
                        listing_datetime = scraped_datetime - timedelta(days=amount * 365)  # Approximate
                    
                    # Return as date string (not datetime) to match schema
                    return listing_datetime.date().isoformat()
            
            # If no pattern matched, log for debugging
            logger.debug(f"No time pattern matched for: '{published_text}'")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing listing date from '{published_text}': {e}")
            return None
    
    def structure_amenities(self, amenities_data: Dict) -> Dict:
        """Structure amenities data to mirror webpage categories - same as working scraper"""
        structured = {
            "exterior": [],
            "general": [],
            "policies": [],
            "recreation": []
        }
        
        if not amenities_data:
            return structured
        
        # Mapping of amenity keywords to categories
        category_mappings = {
            "exterior": [
                "covered_parking", "covered parking", "estacionamiento cubierto",
                "street_parking", "street parking", "estacionamiento en calle",
                "patio", "garden", "jardín", "yard", "balcony", "balcón",
                "terrace", "terraza", "garage", "carport"
            ],
            "general": [
                "accessibility_for_elderly", "accessibility for elderly", "accesibilidad adultos mayores",
                "storage_unit", "storage unit", "bodega", "cuarto de servicio",
                "laundry_room", "laundry room", "cuarto de lavado",
                "elevator", "elevador", "ascensor",
                "equipped_kitchen", "equipped kitchen", "cocina equipada",
                "study", "estudio", "office", "oficina",
                "24_hour_security", "24 hour security", "seguridad 24 horas",
                "furnished", "amueblado", "semi-furnished", "semi amueblado"
            ],
            "policies": [
                "pets_allowed", "pets allowed", "mascotas permitidas",
                "no_pets", "no pets", "no mascotas",
                "no_smoking", "no smoking", "no fumar"
            ],
            "recreation": [
                "pool", "piscina", "swimming pool", "alberca",
                "playground", "área de juegos", "parque infantil",
                "tennis_court", "tennis court", "cancha de tenis",
                "gym", "gimnasio", "fitness center", "centro de fitness",
                "sauna", "spa", "jacuzzi",
                "games_room", "games room", "sala de juegos",
                "multipurpose_room", "multipurpose room", "salón de usos múltiples",
                "padel_courts", "padel courts", "canchas de pádel",
                "barbecue_area", "barbecue area", "área de asado"
            ]
        }
        
        # Process the amenities data
        for amenity_text in amenities_data.get("amenities", []):
            if not amenity_text:
                continue
                
            amenity_lower = amenity_text.lower().strip()
            
            # Find the appropriate category for this amenity
            categorized = False
            for category, keywords in category_mappings.items():
                for keyword in keywords:
                    if keyword.lower() in amenity_lower:
                        if amenity_text not in structured[category]:
                            structured[category].append(amenity_text)
                        categorized = True
                        break
                if categorized:
                    break
            
            # If not categorized, add to general
            if not categorized:
                structured["general"].append(amenity_text)
        
        # Remove empty categories
        return {k: v for k, v in structured.items() if v}
    
    async def save_property_to_staging(self, property_data: Dict) -> bool:
        """Save property to staging table instead of live table"""
        try:
            # Insert into staging table
            result = self.supabase.table("property_scrapes_staging").insert(property_data).execute()
            self.properties_inserted += 1
            logger.debug(f"Inserted property to staging: {property_data['title']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save property to staging {property_data.get('title', 'Unknown')}: {e}")
            await self.log_error(property_data.get("source_url", ""), "database_error", str(e))
            return False
    
    async def scrape_property_list_page(self, url: str, page_num: int = 1) -> List[Dict]:
        """Scrape a single property listing page using HTTP fallback directly - same as working scraper"""
        logger.info(f"Scraping page {page_num}: {url}")
        
        # Skip browser method and go straight to HTTP since we know it works
        logger.debug("Using HTTP method directly (browser method times out)")
        return await self.http_scrape_method(url, page_num)
    
    async def http_scrape_method(self, url: str, page_num: int) -> List[Dict]:
        """HTTP-based scraping method - same as working scraper"""
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
        """Scrape detailed information from individual property page - same as working scraper"""
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
        """Extract detailed information from property detail page HTML using correct Pincali selectors - same as working scraper"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            details = {}
            
            # Extract property ID
            listing_id_elem = soup.select_one('.listing-id span')
            if listing_id_elem:
                property_id = listing_id_elem.get_text().replace('ID: ', '').strip()
                if property_id:
                    details["property_id"] = f"pincali_{property_id}"
            
            # Extract title (h1)
            title_elem = soup.select_one('h1')
            if title_elem:
                title = self.clean_text(title_elem.get_text())
                details["title"] = title
                
                # Check for pre-sale indicator
                if "PREVENTA" in title.upper():
                    details["is_presale"] = True
            
            # Extract full description using correct selector
            desc_elem = soup.select_one('.text-description')
            if desc_elem:
                description_parts = desc_elem.get_text(separator=' ', strip=True)
                details["description"] = self.clean_text(description_parts)
            
            # Extract location information using correct selectors
            location_elem = soup.select_one('.map-address-info')
            if location_elem:
                location_text = location_elem.get_text(strip=True)
                location_info = self.extract_location(location_text)
                details.update(location_info)
            
            # Extract property type from h2.location
            location_type_elem = soup.select_one('h2.location')
            if location_type_elem:
                location_type_text = location_type_elem.get_text(strip=True)
                if location_type_text:
                    # Extract first word which should be the property type
                    property_type = location_type_text.split()[0].lower()
                    type_mappings = {
                        'departamento': 'apartment',
                        'casa': 'house', 
                        'terreno': 'lot',
                        'lote': 'lot'
                    }
                    details["property_type"] = type_mappings.get(property_type, property_type)
            
            # Extract coordinates from map URL
            map_elem = soup.select_one('.map-container div[data-lazy-iframe-url]')
            if map_elem:
                map_url = map_elem.get('data-lazy-iframe-url')
                if map_url:
                    try:
                        from urllib.parse import urlparse, parse_qs
                        parsed_url = urlparse(map_url)
                        query_params = parse_qs(parsed_url.query)
                        center = query_params.get('q', [None])[0]
                        if center and ',' in center:
                            lat, lng = center.split(',')
                            details["latitude"] = float(lat.strip())
                            details["longitude"] = float(lng.strip())
                            details["gps_coordinates"] = f"{lat.strip()},{lng.strip()}"
                    except (ValueError, AttributeError):
                        pass
            
            # Extract price using correct selector
            price_elem = soup.select_one('div.listing__price div.price div.digits')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                if price_text:
                    # Determine currency
                    currency = "MXN"  # Default
                    if "US$" in price_text:
                        currency = "USD"
                        price_text = price_text.replace("US$", "")
                    
                    # Clean and extract numeric price
                    price_clean = price_text.replace(",", "").replace("$", "").strip()
                    try:
                        details["price"] = float(price_clean)
                        details["currency"] = currency
                    except ValueError:
                        pass
            
            # Extract operation type
            operation_elem = soup.select_one('div.listing__price div.price div.operation-type')
            if operation_elem:
                operation_text = operation_elem.get_text(strip=True).replace("En ", "")
                operation_mappings = {
                    'venta': 'sale',
                    'renta': 'rent',
                    'sale': 'sale',
                    'rent': 'rent'
                }
                details["operation_type"] = operation_mappings.get(operation_text.lower(), operation_text.lower())
            
            # Extract property features using correct icon-based selectors
            feature_icons = soup.select('div.listing__features div.feature-icon')
            for icon_div in feature_icons:
                icon_elem = icon_div.select_one('i')
                if icon_elem:
                    icon_class = icon_elem.get('class', [])
                    icon_class_str = ' '.join(icon_class) if isinstance(icon_class, list) else str(icon_class)
                    
                    # Get the text value (second text node)
                    text_nodes = [t.strip() for t in icon_div.get_text(separator='|').split('|') if t.strip()]
                    if len(text_nodes) > 1:
                        value_text = text_nodes[1].replace('\n', '').strip()
                        
                        try:
                            if "fa-bed" in icon_class_str:
                                details["bedrooms"] = int(value_text)
                            elif "fa-bath" in icon_class_str:
                                details["bathrooms"] = int(value_text)
                            elif "fa-car" in icon_class_str:
                                details["parking_spaces"] = int(value_text)
                            elif "fa-cube" in icon_class_str:
                                # Construction area in m²
                                area_match = re.search(r'(\d+(?:\.\d+)?)', value_text)
                                if area_match:
                                    details["covered_area_m2"] = float(area_match.group(1))
                            elif "fa-expand" in icon_class_str:
                                # Total area in m²
                                area_match = re.search(r'(\d+(?:\.\d+)?)', value_text)
                                if area_match:
                                    details["total_area_m2"] = float(area_match.group(1))
                            elif "fa-building" in icon_class_str:
                                details["floor_number"] = int(value_text)
                            elif "fa-calendar" in icon_class_str:
                                # Construction year
                                year_match = re.search(r'(\d{4})', value_text)
                                if year_match:
                                    details["construction_year"] = int(year_match.group(1))
                        except (ValueError, AttributeError):
                            continue
            
            # Extract structured amenities using correct selectors
            amenities_groups = soup.select('div.listing__amenities div.amenities-group')
            structured_amenities = {}
            
            for group in amenities_groups:
                # Get group title
                title_elem = group.select_one('div.amenities-group-title')
                if title_elem:
                    group_title = title_elem.get_text(strip=True)
                    
                    # Get amenities in this group
                    amenity_items = group.select('div.amenities-list li')
                    group_amenities = []
                    
                    for item in amenity_items:
                        span_elem = item.select_one('span')
                        if span_elem:
                            amenity_text = span_elem.get_text(strip=True)
                            if amenity_text:
                                group_amenities.append(amenity_text)
                    
                    if group_amenities:
                        # Map Spanish group names to English
                        group_mappings = {
                            'Exterior': 'exterior',
                            'General': 'general', 
                            'Políticas': 'policies',
                            'Recreación': 'recreation'
                        }
                        mapped_title = group_mappings.get(group_title, group_title.lower())
                        structured_amenities[mapped_title] = group_amenities
            
            if structured_amenities:
                details["amenities"] = structured_amenities
            
            # Extract all images using correct selector
            image_elements = soup.select('div.property__gallery div.picture img')
            image_urls = []
            
            for img in image_elements:
                img_url = img.get('src') or img.get('data-src')
                if img_url and 'placeholder' not in img_url:
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = 'https://www.pincali.com' + img_url
                    
                    if img_url not in image_urls:
                        image_urls.append(img_url)
            
            if image_urls:
                details["image_urls"] = image_urls
                if not details.get("main_image_url"):
                    details["main_image_url"] = image_urls[0]
            
            # Extract publisher/agent information using correct selectors
            publisher_name_elem = soup.select_one('.publisher-name')
            publisher_org_elem = soup.select_one('.publisher-organization-name')
            publisher_phones = soup.select('.publisher-phones')
            
            if publisher_name_elem:
                details["agent_name"] = self.clean_text(publisher_name_elem.get_text())
            
            if publisher_org_elem:
                details["agency_name"] = self.clean_text(publisher_org_elem.get_text())
            
            if publisher_phones:
                phone_numbers = []
                for phone_elem in publisher_phones:
                    phone = phone_elem.get_text(strip=True)
                    if phone:
                        phone_numbers.append(phone)
                if phone_numbers:
                    details["agent_phone"] = phone_numbers[0]  # Take first phone number
            
            logger.debug(f"Extracted {len(details)} details from {url}")
            return details
            
        except Exception as e:
            logger.error(f"Error extracting detailed property info from {url}: {e}")
            return {}
    
    async def fallback_html_parsing(self, html: str, url: str) -> List[Dict]:
        """Parse HTML directly using the actual Pincali structure - same as working scraper"""
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
                    
                    # Extract link (first anchor tag in the property component)
                    link_elem = elem.select_one('a')
                    if link_elem and link_elem.get('href'):
                        prop_data['link'] = link_elem.get('href')
                    
                    # Extract main image
                    image_elem = elem.select_one('.property__media img')
                    if image_elem:
                        # Try src first, then data-src (lazy loading)
                        image_url = image_elem.get('src') or image_elem.get('data-src')
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
                    
                    # Extract published text (for listing date calculation)
                    published_elem = elem.select_one('.published, .date, [class*="published"], [class*="date"]')
                    if published_elem:
                        prop_data['published_text'] = published_elem.get_text(strip=True)
                    
                    # Extract agent/agency info (usually in a contact or agent section)
                    agent_elem = elem.select_one('.agent, .contact, [class*="agent"], [class*="contact"]')
                    if agent_elem:
                        # Store raw agent info for processing in extract_property_details
                        prop_data['agent_info'] = agent_elem.get_text(strip=True)
                    
                    # Extract message URL (send message button)
                    message_elem = elem.select_one('a[href*="message"], a[href*="contact"], .message-btn, .contact-btn')
                    if message_elem and message_elem.get('href'):
                        prop_data['message_url'] = message_elem.get('href')
                    
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
        """Determine total number of pages to scrape - same as working scraper"""
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
    
    async def scrape_all_pages(self, max_pages: Optional[int] = None, start_page: int = 1, auto_sync: bool = True):
        """Scrape all property listing pages and optionally run sync workflow"""
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
                            
                            # Save to staging table instead of live table
                            await self.save_property_to_staging(cleaned_property)
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
                              f"Inserted to staging: {self.properties_inserted}")
                
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
                       f"Inserted to staging: {self.properties_inserted}, "
                       f"Errors: {self.errors_count}")
            
            # Run sync workflow if enabled
            if auto_sync and self.session_id:
                logger.info("Starting automatic sync workflow...")
                workflow_result = await self.orchestrator.daily_sync_workflow(self.session_id)
                
                if workflow_result.success:
                    logger.info("✅ Sync workflow completed successfully!")
                    if workflow_result.sync_result:
                        metrics = workflow_result.sync_result.metrics
                        logger.info(f"✅ New properties: {metrics.new_properties}")
                        logger.info(f"✅ Updated properties: {metrics.updated_properties}")
                        logger.info(f"✅ Removed properties: {metrics.removed_properties}")
                        logger.info(f"✅ Data quality score: {metrics.data_quality_score:.2f}")
                else:
                    logger.error(f"❌ Sync workflow failed: {workflow_result.error_message}")
            else:
                logger.info("ℹ️  Auto-sync disabled. Data is in staging table.")
                logger.info(f"ℹ️  To sync manually, run: python -c \"from services import PropertySyncOrchestrator; import asyncio; asyncio.run(PropertySyncOrchestrator(supabase).daily_sync_workflow('{self.session_id}'))\"")
        
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}")
            await self.update_session_progress(
                status="failed",
                error_message=str(e),
                completed_at=datetime.utcnow().isoformat()
            )
            raise

async def main():
    """Main function to run the enhanced scraper"""
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description='Enhanced Pincali.com Property Scraper with Staging Architecture',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python enhanced_property_scraper.py --pages 1          # Scrape only 1 page
  python enhanced_property_scraper.py --pages 5          # Scrape 5 pages
  python enhanced_property_scraper.py --pages 10 --start 3  # Scrape pages 3-12
  python enhanced_property_scraper.py                    # Scrape default 10 pages
  python enhanced_property_scraper.py --no-sync --pages 3   # Scrape without auto-sync
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
        '--no-sync',
        action='store_true',
        help='Skip automatic sync after scraping'
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
    scraper = EnhancedPincaliScraper()
    
    # Log scraping parameters
    logger.info(f"Starting Enhanced Pincali scraper with staging architecture")
    logger.info(f"Pages to scrape: {args.pages}")
    logger.info(f"Starting from page: {args.start}")
    logger.info(f"Total pages: {args.start} to {args.start + args.pages - 1}")
    logger.info(f"Auto-sync: {'No' if args.no_sync else 'Yes'}")
    
    try:
        await scraper.scrape_all_pages(
            max_pages=args.pages, 
            start_page=args.start, 
            auto_sync=not args.no_sync
        )
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