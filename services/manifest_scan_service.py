"""
Manifest Scan Service for Hybrid 4-Tier Property Sync System

This service performs fast scanning of listing pages to collect property IDs,
URLs, and visible prices WITHOUT visiting detail pages.

Based on HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from supabase import Client

from config.tier_config import get_config, TierConfig

logger = logging.getLogger(__name__)


@dataclass
class ManifestEntry:
    """Represents a single property entry from manifest scan"""
    property_id: str
    source_url: str
    listing_page_price: Optional[float] = None
    listing_page_title: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    operation_type: Optional[str] = None


@dataclass
class ManifestScanResult:
    """Results from a manifest scan operation"""
    pages_scanned: int = 0
    properties_found: int = 0
    new_properties: int = 0
    price_changes: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    session_id: Optional[str] = None


class ManifestScanService:
    """
    Service for fast scanning of listing pages to build property manifest.
    
    This service is optimized for speed - it only scrapes listing pages (not detail pages)
    and extracts minimal data needed for change detection.
    """
    
    def __init__(self, supabase_client: Client, config: Optional[TierConfig] = None):
        self.supabase = supabase_client
        self.config = config or get_config()
        self.base_url = self.config.base_url
        
        # HTTP session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': self.base_url,
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Tracking
        self._failed_pages: List[str] = []
        self._retry_queue: List[tuple] = []
    
    async def run_manifest_scan(
        self,
        max_pages: int,
        session_id: str,
        source_url: Optional[str] = None,
        operation_type: Optional[str] = None,
        delay_between_pages: float = 2.0
    ) -> ManifestScanResult:
        """
        Run a manifest scan on listing pages.
        
        Args:
            max_pages: Maximum number of pages to scan (0 = all pages)
            session_id: Scraping session ID for tracking
            source_url: URL to scan (defaults to properties-for-sale)
            operation_type: Operation type for properties (sale, rent, etc.)
            delay_between_pages: Seconds to wait between page requests
            
        Returns:
            ManifestScanResult with scan statistics
        """
        start_time = time.time()
        result = ManifestScanResult(session_id=session_id)
        
        target_url = source_url or self.config.listing_sources[0]["url"]
        op_type = operation_type or "sale"
        
        logger.info(f"Starting manifest scan: {target_url}")
        logger.info(f"Max pages: {max_pages if max_pages > 0 else 'all'}, operation_type: {op_type}")
        
        try:
            # Determine total pages if max_pages is 0 (scan all)
            if max_pages == 0:
                max_pages = await self._get_total_pages(target_url)
                logger.info(f"Auto-detected {max_pages} total pages")
            
            all_entries: List[ManifestEntry] = []
            
            # Scan each page
            for page_num in range(1, max_pages + 1):
                try:
                    # Construct page URL
                    if page_num == 1:
                        page_url = target_url
                    else:
                        separator = '&' if '?' in target_url else '?'
                        page_url = f"{target_url}{separator}page={page_num}"
                    
                    # Fetch and parse page
                    entries = await self._scan_single_page(page_url, op_type)
                    
                    if entries:
                        all_entries.extend(entries)
                        result.pages_scanned += 1
                        logger.debug(f"Page {page_num}: Found {len(entries)} properties")
                    else:
                        self._failed_pages.append(page_url)
                        result.errors.append(f"No properties found on page {page_num}")
                    
                    # Check failure threshold
                    if len(self._failed_pages) > self.config.get_tier(1).max_page_failures:
                        error_msg = f"Too many page failures ({len(self._failed_pages)}), aborting scan"
                        logger.error(error_msg)
                        result.errors.append(error_msg)
                        break
                    
                    # Rate limiting
                    if page_num < max_pages:
                        await asyncio.sleep(delay_between_pages)
                    
                    # Progress logging
                    if page_num % 10 == 0:
                        logger.info(f"Progress: {page_num}/{max_pages} pages, {len(all_entries)} properties found")
                
                except Exception as e:
                    error_msg = f"Error on page {page_num}: {str(e)}"
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    self._failed_pages.append(page_url)
                    continue
            
            # Retry failed pages once
            if self._retry_queue:
                logger.info(f"Retrying {len(self._retry_queue)} failed pages...")
                retry_entries = await self._retry_failed_pages(op_type, delay_between_pages)
                all_entries.extend(retry_entries)
            
            # Deduplicate entries
            unique_entries = self._deduplicate_entries(all_entries)
            result.properties_found = len(unique_entries)
            
            logger.info(f"Manifest scan found {result.properties_found} unique properties from {result.pages_scanned} pages")
            
            # Upsert to database
            if unique_entries:
                new_count, price_changes = await self.upsert_manifest_entries(unique_entries, session_id)
                result.new_properties = new_count
                result.price_changes = price_changes
            
        except Exception as e:
            error_msg = f"Fatal error in manifest scan: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
        
        finally:
            result.duration_seconds = time.time() - start_time
            self._failed_pages = []
            self._retry_queue = []
        
        logger.info(f"Manifest scan completed in {result.duration_seconds:.1f}s: "
                   f"{result.properties_found} properties, {result.new_properties} new, "
                   f"{result.price_changes} price changes")
        
        return result
    
    async def run_multi_source_manifest_scan(
        self,
        max_pages_per_source: int,
        session_id: str,
        sources: Optional[List[str]] = None,
        delay_between_pages: float = 2.0
    ) -> ManifestScanResult:
        """
        Run manifest scan across multiple listing sources.
        
        Args:
            max_pages_per_source: Max pages per source (0 = all pages)
            session_id: Scraping session ID
            sources: List of operation_types to scan (None = all)
            delay_between_pages: Delay between page requests
            
        Returns:
            Combined ManifestScanResult
        """
        start_time = time.time()
        combined_result = ManifestScanResult(session_id=session_id)
        
        # Determine which sources to scan
        sources_to_scan = self.config.listing_sources
        if sources:
            sources_to_scan = [s for s in sources_to_scan if s["operation_type"] in sources]
        
        logger.info(f"Starting multi-source manifest scan: {len(sources_to_scan)} sources")
        
        for source in sources_to_scan:
            source_name = source["name"]
            source_url = source["url"]
            operation_type = source["operation_type"]
            
            logger.info(f"Scanning source: {source_name} ({operation_type})")
            
            source_result = await self.run_manifest_scan(
                max_pages=max_pages_per_source,
                session_id=session_id,
                source_url=source_url,
                operation_type=operation_type,
                delay_between_pages=delay_between_pages
            )
            
            # Aggregate results
            combined_result.pages_scanned += source_result.pages_scanned
            combined_result.properties_found += source_result.properties_found
            combined_result.new_properties += source_result.new_properties
            combined_result.price_changes += source_result.price_changes
            combined_result.errors.extend(source_result.errors)
        
        combined_result.duration_seconds = time.time() - start_time
        
        logger.info(f"Multi-source scan completed: {combined_result.pages_scanned} pages, "
                   f"{combined_result.properties_found} properties")
        
        return combined_result
    
    async def _scan_single_page(self, url: str, operation_type: str) -> List[ManifestEntry]:
        """
        Scan a single listing page and extract property entries.
        
        Args:
            url: Page URL to scan
            operation_type: Operation type for properties
            
        Returns:
            List of ManifestEntry objects
        """
        try:
            response = self.session.get(url, timeout=self.config.request_timeout)
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} for {url}")
                self._retry_queue.append((url, operation_type))
                return []
            
            return self.parse_listing_page_for_manifest(response.text, url, operation_type)
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching {url}")
            self._retry_queue.append((url, operation_type))
            return []
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return []
    
    def parse_listing_page_for_manifest(
        self,
        html: str,
        page_url: str,
        operation_type: str
    ) -> List[ManifestEntry]:
        """
        Parse listing page HTML and extract minimal manifest data.
        
        Args:
            html: Raw HTML content
            page_url: URL of the page (for relative link resolution)
            operation_type: Operation type for properties
            
        Returns:
            List of ManifestEntry objects
        """
        entries = []
        
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            # Use Pincali's property listing selector
            property_elements = soup.select('li.property__component')
            
            for elem in property_elements:
                try:
                    entry = self._extract_manifest_entry(elem, page_url, operation_type)
                    if entry and entry.property_id:
                        entries.append(entry)
                except Exception as e:
                    logger.debug(f"Error extracting property entry: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error parsing HTML from {page_url}: {e}")
        
        return entries
    
    def _extract_manifest_entry(
        self,
        element: Any,
        page_url: str,
        operation_type: str
    ) -> Optional[ManifestEntry]:
        """
        Extract a single ManifestEntry from a property element.
        
        Args:
            element: BeautifulSoup element for the property
            page_url: Page URL for relative link resolution
            operation_type: Operation type for the property
            
        Returns:
            ManifestEntry or None if extraction fails
        """
        # Extract link and generate property ID
        link_elem = element.select_one('a')
        if not link_elem or not link_elem.get('href'):
            return None
        
        relative_url = link_elem.get('href')
        source_url = urljoin(self.base_url, relative_url)
        property_id = self._generate_property_id(source_url)
        
        if not property_id:
            return None
        
        entry = ManifestEntry(
            property_id=property_id,
            source_url=source_url,
            operation_type=operation_type
        )
        
        # Extract title
        title_elem = element.select_one('.title')
        if title_elem:
            entry.listing_page_title = title_elem.get_text(strip=True)[:500]
        
        # Extract price
        price_elem = element.select_one('li.price')
        if price_elem:
            entry.listing_page_price = self._extract_price(price_elem.get_text(strip=True))
        
        # Extract coordinates from data attributes
        lat = element.get('data-lat')
        lng = element.get('data-long')
        if lat and lng:
            try:
                entry.latitude = float(lat)
                entry.longitude = float(lng)
            except (ValueError, TypeError):
                pass
        
        return entry
    
    def _generate_property_id(self, source_url: str) -> Optional[str]:
        """Generate unique property ID from source URL"""
        if not source_url:
            return None
        
        parsed_url = urlparse(source_url)
        path_parts = parsed_url.path.strip('/').split('/')
        
        # For Pincali URLs like "/en/home/property-name"
        if len(path_parts) >= 3 and path_parts[0] == 'en' and path_parts[1] == 'home':
            property_slug = '/'.join(path_parts[2:])
            if property_slug:
                return f"pincali_{property_slug}"
        
        # Fallback: use hash
        import hashlib
        url_hash = hashlib.md5(source_url.encode()).hexdigest()[:16]
        return f"pincali_hash_{url_hash}"
    
    def _extract_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        if not price_text:
            return None
        
        # Remove currency symbols and clean
        price_clean = re.sub(r'[^\d,.]', '', price_text.replace(',', ''))
        
        # Extract numeric value
        match = re.search(r'(\d+(?:\.\d+)?)', price_clean)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                pass
        
        return None
    
    async def _get_total_pages(self, base_url: str) -> int:
        """Determine total number of pages for a listing source"""
        try:
            response = self.session.get(base_url, timeout=self.config.request_timeout)
            
            if response.status_code != 200:
                logger.warning(f"Could not determine total pages (HTTP {response.status_code})")
                return 100  # Default fallback
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Look for Pincali's pagination summary
            pagination_summary = soup.select_one('.pagination-summary')
            if pagination_summary:
                summary_text = pagination_summary.get_text(strip=True)
                match = re.search(r'Page\s+\d+\s+of\s+([\d,]+)', summary_text, re.IGNORECASE)
                if match:
                    total_pages = int(match.group(1).replace(',', ''))
                    logger.debug(f"Detected {total_pages} total pages")
                    return total_pages
            
            # Fallback: look for pagination links
            max_page = 1
            for link in soup.select('.pagination a, .pager a'):
                text = link.get_text().strip()
                if text.isdigit():
                    max_page = max(max_page, int(text))
                
                href = link.get('href', '')
                page_match = re.search(r'page=(\d+)', href)
                if page_match:
                    max_page = max(max_page, int(page_match.group(1)))
            
            if max_page > 1:
                return max_page
            
        except Exception as e:
            logger.error(f"Error determining total pages: {e}")
        
        return 100  # Default fallback
    
    async def _retry_failed_pages(
        self,
        operation_type: str,
        delay: float
    ) -> List[ManifestEntry]:
        """Retry failed pages with exponential backoff"""
        entries = []
        
        for url, op_type in self._retry_queue:
            try:
                await asyncio.sleep(delay * 2)  # Double delay for retry
                page_entries = await self._scan_single_page(url, op_type)
                entries.extend(page_entries)
            except Exception as e:
                logger.warning(f"Retry failed for {url}: {e}")
        
        self._retry_queue = []
        return entries
    
    def _deduplicate_entries(self, entries: List[ManifestEntry]) -> List[ManifestEntry]:
        """Remove duplicate entries, keeping the most recent"""
        seen: Dict[str, ManifestEntry] = {}
        
        for entry in entries:
            if entry.property_id not in seen:
                seen[entry.property_id] = entry
            else:
                # Update if this entry has more data
                existing = seen[entry.property_id]
                if entry.listing_page_price and not existing.listing_page_price:
                    seen[entry.property_id] = entry
        
        return list(seen.values())
    
    async def upsert_manifest_entries(
        self,
        entries: List[ManifestEntry],
        session_id: str
    ) -> tuple:
        """
        Upsert manifest entries to database.
        
        Args:
            entries: List of ManifestEntry objects
            session_id: Session ID for tracking
            
        Returns:
            Tuple of (new_count, price_change_count)
        """
        if not entries:
            return 0, 0
        
        new_count = 0
        price_change_count = 0
        
        # Use smaller batch size for queries to avoid URL length limits
        query_batch_size = 200  # Small enough to avoid URL query string limits
        upsert_batch_size = 50  # Batch size for upserts
        
        try:
            property_ids = [e.property_id for e in entries]
            
            # Get existing manifest entries in batches
            existing_map = {}
            for i in range(0, len(property_ids), query_batch_size):
                batch_ids = property_ids[i:i + query_batch_size]
                try:
                    existing_response = self.supabase.table('property_manifest').select(
                        'property_id, listing_page_price'
                    ).in_('property_id', batch_ids).execute()
                    
                    for item in existing_response.data:
                        existing_map[item['property_id']] = item.get('listing_page_price')
                except Exception as e:
                    logger.warning(f"Error fetching manifest batch {i}: {e}")
            
            # Get properties in live table in batches
            live_map = {}
            for i in range(0, len(property_ids), query_batch_size):
                batch_ids = property_ids[i:i + query_batch_size]
                try:
                    live_response = self.supabase.table('properties_live').select(
                        'property_id, price'
                    ).in_('property_id', batch_ids).execute()
                    
                    for item in live_response.data:
                        live_map[item['property_id']] = item.get('price')
                except Exception as e:
                    logger.warning(f"Error fetching live batch {i}: {e}")
            
            # Prepare upsert data
            now = datetime.utcnow().isoformat()
            upsert_records = []
            
            for entry in entries:
                is_new = entry.property_id not in live_map
                is_manifest_new = entry.property_id not in existing_map
                
                # Detect price changes
                live_price = live_map.get(entry.property_id)
                price_changed = False
                if not is_new and live_price and entry.listing_page_price:
                    price_changed = self.config.should_flag_price_change(
                        live_price, entry.listing_page_price
                    )
                
                if is_manifest_new:
                    new_count += 1
                if price_changed:
                    price_change_count += 1
                
                record = {
                    'property_id': entry.property_id,
                    'source_url': entry.source_url,
                    'listing_page_price': entry.listing_page_price,
                    'listing_page_title': entry.listing_page_title,
                    'latitude': entry.latitude,
                    'longitude': entry.longitude,
                    'last_seen_at': now,
                    'seen_in_session_id': session_id,
                    'is_new': is_new,
                    'needs_full_scrape': is_new or price_changed,
                    'price_changed': price_changed,
                    'updated_at': now,
                }
                
                if is_manifest_new:
                    record['first_seen_at'] = now
                    record['created_at'] = now
                
                upsert_records.append(record)
            
            # Batch upsert with smaller batches
            for i in range(0, len(upsert_records), upsert_batch_size):
                batch = upsert_records[i:i + upsert_batch_size]
                try:
                    self.supabase.table('property_manifest').upsert(
                        batch,
                        on_conflict='property_id'
                    ).execute()
                except Exception as e:
                    logger.warning(f"Error upserting batch {i}: {e}")
            
            logger.info(f"Upserted {len(upsert_records)} manifest entries: "
                       f"{new_count} new, {price_change_count} price changes")
            
        except Exception as e:
            logger.error(f"Error upserting manifest entries: {e}")
            raise
        
        return new_count, price_change_count
    
    async def get_manifest_stats(self) -> Dict:
        """Get statistics about the current manifest"""
        try:
            response = self.supabase.rpc('get_manifest_stats').execute()
            if response.data:
                return response.data[0]
            return {}
        except Exception as e:
            logger.error(f"Error getting manifest stats: {e}")
            return {}
    
    async def clear_manifest_flags(self, session_id: str):
        """Clear is_new and needs_full_scrape flags after processing"""
        try:
            self.supabase.table('property_manifest').update({
                'is_new': False,
                'needs_full_scrape': False,
                'price_changed': False,
                'updated_at': datetime.utcnow().isoformat()
            }).eq('seen_in_session_id', session_id).execute()
            
            logger.info(f"Cleared manifest flags for session {session_id}")
        except Exception as e:
            logger.error(f"Error clearing manifest flags: {e}")

