"""
Change Detection Service for Property Data Management

This service implements intelligent change detection between staging and live property data,
including confidence scoring and change reason analysis.

Based on property_data_management_architecture.md
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from supabase import Client
import json

logger = logging.getLogger(__name__)


@dataclass
class PropertyChange:
    """Represents a detected property change"""
    property_id: str
    change_type: str  # created, updated, removed, reactivated
    field_name: Optional[str] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    confidence_score: float = 1.0
    change_reason: str = ""


@dataclass
class ChangeDetectionResult:
    """Results from change detection process"""
    new_properties: List[str]
    updated_properties: List[str]
    removed_properties: List[str]
    unchanged_properties: List[str]
    changes: List[PropertyChange]
    total_processed: int


class ChangeDetectionService:
    """
    Service for detecting changes between staging and live property data
    """
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.confidence_factors = {
            'price_change': 0.9,  # Price changes are highly reliable
            'title_change': 0.8,  # Title changes are usually significant
            'description_change': 0.6,  # Descriptions might have minor updates
            'image_change': 0.7,  # Image changes indicate real updates
            'contact_change': 0.85,  # Agent/contact changes are significant
            'status_change': 0.95,  # Status changes are very reliable
            'location_change': 0.9,  # Location changes are highly significant
            'amenities_change': 0.7,  # Amenities changes are moderately significant
            'features_change': 0.7,  # Features changes are moderately significant
        }
        
        # Fields to monitor for changes (with their confidence weights)
        self.monitored_fields = {
            'title': 'title_change',
            'description': 'description_change',
            'price': 'price_change',
            'price_per_m2': 'price_change',
            'bedrooms': 'status_change',
            'bathrooms': 'status_change',
            'total_area_m2': 'status_change',
            'agent_name': 'contact_change',
            'agent_phone': 'contact_change',
            'agent_email': 'contact_change',
            'agency_name': 'contact_change',
            'main_image_url': 'image_change',
            'image_urls': 'image_change',
            'address': 'location_change',
            'latitude': 'location_change',
            'longitude': 'location_change',
            'amenities': 'amenities_change',
            'features': 'features_change',
            'status': 'status_change',
        }

    async def detect_changes(self, session_id: str) -> ChangeDetectionResult:
        """
        Main method to detect all types of changes for a scraping session
        
        Args:
            session_id: The scraping session ID to process
            
        Returns:
            ChangeDetectionResult containing all detected changes
        """
        logger.info(f"Starting change detection for session {session_id}")
        
        try:
            # Get staging data for this session
            staging_data = await self._get_staging_data(session_id)
            if not staging_data:
                logger.warning(f"No staging data found for session {session_id}")
                return ChangeDetectionResult([], [], [], [], [], 0)
            
            # Detect different types of changes
            new_properties = await self._detect_new_properties(session_id)
            updated_properties = await self._detect_updated_properties(session_id)
            removed_properties = await self._detect_removed_properties()
            
            # Get all property IDs from staging for unchanged detection
            staging_property_ids = {item['property_id'] for item in staging_data}
            unchanged_properties = list(
                staging_property_ids - set(new_properties) - set(updated_properties)
            )
            
            # Generate detailed change records
            changes = []
            changes.extend(await self._create_change_records(new_properties, 'created', session_id))
            changes.extend(await self._create_detailed_update_records(updated_properties, session_id))
            changes.extend(await self._create_change_records(removed_properties, 'removed', session_id))
            
            # Update staging table with change types
            await self._update_staging_change_types(session_id, new_properties, updated_properties, unchanged_properties)
            
            result = ChangeDetectionResult(
                new_properties=new_properties,
                updated_properties=updated_properties,
                removed_properties=removed_properties,
                unchanged_properties=unchanged_properties,
                changes=changes,
                total_processed=len(staging_data)
            )
            
            logger.info(f"Change detection completed: {len(new_properties)} new, "
                       f"{len(updated_properties)} updated, {len(removed_properties)} removed, "
                       f"{len(unchanged_properties)} unchanged")
            
            return result
            
        except Exception as e:
            logger.error(f"Error during change detection: {str(e)}")
            raise

    async def _get_staging_data(self, session_id: str) -> List[Dict]:
        """Get all staging data for a session"""
        try:
            response = self.supabase.table('property_scrapes_staging').select('*').eq('session_id', session_id).execute()
            return response.data
        except Exception as e:
            logger.error(f"Error fetching staging data: {str(e)}")
            return []

    async def _detect_new_properties(self, session_id: str) -> List[str]:
        """
        Detect properties that exist in staging but not in live table
        
        Args:
            session_id: The scraping session ID
            
        Returns:
            List of property IDs for new properties
        """
        try:
            # Try RPC method first (if available)
            try:
                query = """
                SELECT s.property_id 
                FROM property_scrapes_staging s
                LEFT JOIN properties_live l ON s.property_id = l.property_id
                WHERE l.property_id IS NULL AND s.session_id = %s
                """
                
                response = self.supabase.rpc('execute_sql', {
                    'query': query,
                    'params': [session_id]
                }).execute()
                
                if response.data:
                    return [row['property_id'] for row in response.data]
                    
            except Exception as rpc_error:
                logger.debug(f"RPC method failed, using fallback: {str(rpc_error)}")
            
            # Fallback to regular query method
            staging_response = self.supabase.table('property_scrapes_staging').select('property_id').eq('session_id', session_id).execute()
            live_response = self.supabase.table('properties_live').select('property_id').execute()
            
            staging_ids = {item['property_id'] for item in staging_response.data}
            live_ids = {item['property_id'] for item in live_response.data}
            
            new_properties = list(staging_ids - live_ids)
            logger.info(f"Found {len(new_properties)} new properties using fallback method")
            
            return new_properties
            
        except Exception as e:
            logger.error(f"Error detecting new properties: {str(e)}")
            return []

    async def _detect_updated_properties(self, session_id: str) -> List[str]:
        """
        Detect properties that have changes between staging and live
        
        Args:
            session_id: The scraping session ID
            
        Returns:
            List of property IDs for updated properties
        """
        try:
            # Get properties that exist in both staging and live
            staging_response = self.supabase.table('property_scrapes_staging').select('*').eq('session_id', session_id).execute()
            staging_data = {item['property_id']: item for item in staging_response.data}
            
            if not staging_data:
                return []
            
            # Get corresponding live data
            property_ids = list(staging_data.keys())
            live_response = self.supabase.table('properties_live').select('*').in_('property_id', property_ids).execute()
            live_data = {item['property_id']: item for item in live_response.data}
            
            updated_properties = []
            
            for property_id in staging_data:
                if property_id in live_data:
                    if await self._has_significant_changes(staging_data[property_id], live_data[property_id]):
                        updated_properties.append(property_id)
            
            return updated_properties
            
        except Exception as e:
            logger.error(f"Error detecting updated properties: {str(e)}")
            return []

    async def _detect_removed_properties(self) -> List[str]:
        """
        Detect properties that haven't been seen in recent scraping sessions
        
        Returns:
            List of property IDs for potentially removed properties
        """
        try:
            # Properties not seen in the last 7 days of completed sessions
            # and haven't been seen in the last 3 days
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            last_seen_cutoff = datetime.utcnow() - timedelta(days=3)
            
            # Get recent completed sessions
            recent_sessions_response = self.supabase.table('scraping_sessions').select('id').gte('created_at', cutoff_date.isoformat()).eq('status', 'completed').execute()
            recent_session_ids = [session['id'] for session in recent_sessions_response.data]
            
            if not recent_session_ids:
                logger.warning("No recent completed sessions found for removal detection")
                return []
            
            # Get properties from recent staging data
            recent_staging_response = self.supabase.table('property_scrapes_staging').select('property_id').in_('session_id', recent_session_ids).execute()
            recent_property_ids = {item['property_id'] for item in recent_staging_response.data}
            
            # Find active properties not in recent scrapes and not seen recently
            live_response = self.supabase.table('properties_live').select('property_id, last_seen_at').eq('status', 'active').lt('last_seen_at', last_seen_cutoff.isoformat()).execute()
            
            removed_candidates = []
            for property_data in live_response.data:
                if property_data['property_id'] not in recent_property_ids:
                    removed_candidates.append(property_data['property_id'])
            
            return removed_candidates
            
        except Exception as e:
            logger.error(f"Error detecting removed properties: {str(e)}")
            return []

    async def _has_significant_changes(self, staging_item: Dict, live_item: Dict) -> bool:
        """
        Determine if there are significant changes between staging and live data
        
        Args:
            staging_item: Property data from staging
            live_item: Property data from live table
            
        Returns:
            True if significant changes detected
        """
        for field_name in self.monitored_fields:
            staging_value = staging_item.get(field_name)
            live_value = live_item.get(field_name)
            
            if await self._values_differ_significantly(field_name, staging_value, live_value):
                return True
                
        return False

    async def _values_differ_significantly(self, field_name: str, staging_value: Any, live_value: Any) -> bool:
        """
        Check if two values differ significantly for a given field
        
        Args:
            field_name: Name of the field being compared
            staging_value: Value from staging
            live_value: Value from live
            
        Returns:
            True if values differ significantly
        """
        # Handle None values
        if staging_value is None and live_value is None:
            return False
        if staging_value is None or live_value is None:
            return True
            
        # Handle different data types
        if field_name == 'price' or field_name == 'price_per_m2':
            # For prices, consider changes > 1% significant
            try:
                staging_price = float(staging_value)
                live_price = float(live_value)
                if live_price == 0:
                    return staging_price != 0
                change_percent = abs(staging_price - live_price) / live_price
                return change_percent > 0.01  # 1% threshold
            except (ValueError, TypeError):
                return str(staging_value) != str(live_value)
                
        elif field_name in ['image_urls', 'amenities', 'features']:
            # For JSON fields, do deep comparison
            return json.dumps(staging_value, sort_keys=True) != json.dumps(live_value, sort_keys=True)
            
        elif field_name in ['latitude', 'longitude']:
            # For coordinates, consider changes > 0.0001 degrees significant (~10 meters)
            try:
                staging_coord = float(staging_value)
                live_coord = float(live_value)
                return abs(staging_coord - live_coord) > 0.0001
            except (ValueError, TypeError):
                return str(staging_value) != str(live_value)
                
        else:
            # For other fields, do string comparison
            return str(staging_value).strip() != str(live_value).strip()

    def _calculate_change_confidence(self, field_name: str, old_value: Any, new_value: Any) -> float:
        """
        Calculate confidence score for a detected change
        
        Args:
            field_name: Name of the changed field
            old_value: Previous value
            new_value: New value
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        base_confidence = self.confidence_factors.get(
            self.monitored_fields.get(field_name, 'status_change'), 0.7
        )
        
        # Adjust confidence based on change characteristics
        if field_name == 'price':
            # Higher confidence for larger price changes
            try:
                if old_value and new_value:
                    old_price = float(old_value)
                    new_price = float(new_value)
                    if old_price > 0:
                        change_percent = abs(new_price - old_price) / old_price
                        if change_percent > 0.1:  # >10% change
                            base_confidence = min(0.95, base_confidence + 0.1)
            except (ValueError, TypeError):
                pass
                
        elif field_name in ['title', 'description']:
            # Lower confidence for minor text changes
            if old_value and new_value:
                old_text = str(old_value).strip().lower()
                new_text = str(new_value).strip().lower()
                
                # Calculate similarity (simple approach)
                if len(old_text) > 0:
                    common_chars = sum(1 for a, b in zip(old_text, new_text) if a == b)
                    similarity = common_chars / max(len(old_text), len(new_text))
                    if similarity > 0.9:  # Very similar text
                        base_confidence *= 0.7
                        
        return min(1.0, max(0.0, base_confidence))

    async def _create_change_records(self, property_ids: List[str], change_type: str, session_id: str) -> List[PropertyChange]:
        """
        Create PropertyChange records for a list of properties
        
        Args:
            property_ids: List of property IDs
            change_type: Type of change (created, removed, etc.)
            session_id: Session ID
            
        Returns:
            List of PropertyChange objects
        """
        changes = []
        for property_id in property_ids:
            change_reason = f"Property {change_type} in session {session_id}"
            confidence = 0.95 if change_type in ['created', 'removed'] else 0.8
            
            changes.append(PropertyChange(
                property_id=property_id,
                change_type=change_type,
                confidence_score=confidence,
                change_reason=change_reason
            ))
            
        return changes

    async def _create_detailed_update_records(self, property_ids: List[str], session_id: str) -> List[PropertyChange]:
        """
        Create detailed PropertyChange records for updated properties
        
        Args:
            property_ids: List of updated property IDs
            session_id: Session ID
            
        Returns:
            List of PropertyChange objects with field-level details
        """
        changes = []
        
        if not property_ids:
            return changes
            
        try:
            # Get staging and live data for comparison
            staging_response = self.supabase.table('property_scrapes_staging').select('*').eq('session_id', session_id).in_('property_id', property_ids).execute()
            staging_data = {item['property_id']: item for item in staging_response.data}
            
            live_response = self.supabase.table('properties_live').select('*').in_('property_id', property_ids).execute()
            live_data = {item['property_id']: item for item in live_response.data}
            
            for property_id in property_ids:
                if property_id in staging_data and property_id in live_data:
                    staging_item = staging_data[property_id]
                    live_item = live_data[property_id]
                    
                    # Check each monitored field for changes
                    for field_name in self.monitored_fields:
                        staging_value = staging_item.get(field_name)
                        live_value = live_item.get(field_name)
                        
                        if await self._values_differ_significantly(field_name, staging_value, live_value):
                            confidence = self._calculate_change_confidence(field_name, live_value, staging_value)
                            change_reason = f"Field '{field_name}' changed from '{live_value}' to '{staging_value}'"
                            
                            changes.append(PropertyChange(
                                property_id=property_id,
                                change_type='updated',
                                field_name=field_name,
                                old_value=live_value,
                                new_value=staging_value,
                                confidence_score=confidence,
                                change_reason=change_reason
                            ))
                            
        except Exception as e:
            logger.error(f"Error creating detailed update records: {str(e)}")
            
        return changes

    async def _update_staging_change_types(self, session_id: str, new_properties: List[str], 
                                         updated_properties: List[str], unchanged_properties: List[str]):
        """
        Update the change_type field in staging table
        
        Args:
            session_id: Session ID
            new_properties: List of new property IDs
            updated_properties: List of updated property IDs
            unchanged_properties: List of unchanged property IDs
        """
        try:
            # Update new properties
            if new_properties:
                self.supabase.table('property_scrapes_staging').update({'change_type': 'new'}).eq('session_id', session_id).in_('property_id', new_properties).execute()
            
            # Update changed properties
            if updated_properties:
                self.supabase.table('property_scrapes_staging').update({'change_type': 'updated'}).eq('session_id', session_id).in_('property_id', updated_properties).execute()
            
            # Update unchanged properties
            if unchanged_properties:
                self.supabase.table('property_scrapes_staging').update({'change_type': 'unchanged'}).eq('session_id', session_id).in_('property_id', unchanged_properties).execute()
                
        except Exception as e:
            logger.error(f"Error updating staging change types: {str(e)}")

    async def save_change_records(self, changes: List[PropertyChange], session_id: str):
        """
        Save change records to the property_changes table
        
        Args:
            changes: List of PropertyChange objects
            session_id: Session ID
        """
        if not changes:
            return
            
        try:
            change_records = []
            for change in changes:
                record = {
                    'property_id': change.property_id,
                    'session_id': session_id,
                    'change_type': change.change_type,
                    'field_name': change.field_name,
                    'old_value': change.old_value,
                    'new_value': change.new_value,
                    'confidence_score': change.confidence_score,
                    'change_reason': change.change_reason
                }
                change_records.append(record)
            
            # Insert in batches to avoid overwhelming the database
            batch_size = 100
            for i in range(0, len(change_records), batch_size):
                batch = change_records[i:i + batch_size]
                self.supabase.table('property_changes').insert(batch).execute()
                
            logger.info(f"Saved {len(change_records)} change records to database")
            
        except Exception as e:
            logger.error(f"Error saving change records: {str(e)}")
            raise
    
    # ========== Manifest-based Change Detection Methods ==========
    # These methods support the Hybrid 4-Tier Sync System
    
    async def detect_new_from_manifest(self, manifest_entries: List[Dict]) -> List[str]:
        """
        Detect new properties from manifest data by comparing against properties_live.
        
        This method is used by the ManifestScanService to quickly identify
        new listings without fetching full property details.
        
        Args:
            manifest_entries: List of manifest entries with 'property_id' field
            
        Returns:
            List of property IDs that are new (not in properties_live)
        """
        if not manifest_entries:
            return []
        
        try:
            # Extract property IDs from manifest
            manifest_ids = [entry.get('property_id') for entry in manifest_entries if entry.get('property_id')]
            
            if not manifest_ids:
                return []
            
            # Query properties_live for existing IDs
            # Process in batches to avoid query size limits
            batch_size = 500
            existing_ids = set()
            
            for i in range(0, len(manifest_ids), batch_size):
                batch = manifest_ids[i:i + batch_size]
                response = self.supabase.table('properties_live').select('property_id').in_('property_id', batch).execute()
                existing_ids.update(item['property_id'] for item in response.data)
            
            # Find IDs not in live table
            new_ids = [pid for pid in manifest_ids if pid not in existing_ids]
            
            logger.info(f"Manifest check: {len(manifest_ids)} entries, {len(new_ids)} new properties")
            return new_ids
            
        except Exception as e:
            logger.error(f"Error detecting new properties from manifest: {str(e)}")
            return []
    
    async def detect_price_changes_from_manifest(self, manifest_entries: List[Dict]) -> List[Dict]:
        """
        Detect price changes by comparing manifest prices against properties_live.
        
        This method enables fast price change detection without full scraping.
        Only properties with visible price changes are flagged for full scrape verification.
        
        Args:
            manifest_entries: List of manifest entries with 'property_id' and 'price' fields
            
        Returns:
            List of dicts with property_id, old_price, new_price, and price_change_percent
        """
        if not manifest_entries:
            return []
        
        try:
            # Filter entries that have both property_id and price
            entries_with_price = [
                entry for entry in manifest_entries 
                if entry.get('property_id') and entry.get('price') is not None
            ]
            
            if not entries_with_price:
                return []
            
            property_ids = [entry['property_id'] for entry in entries_with_price]
            manifest_prices = {entry['property_id']: entry.get('price') for entry in entries_with_price}
            
            # Query live prices in batches
            batch_size = 500
            live_prices = {}
            
            for i in range(0, len(property_ids), batch_size):
                batch = property_ids[i:i + batch_size]
                response = self.supabase.table('properties_live').select(
                    'property_id, price, price_at_last_manifest'
                ).in_('property_id', batch).execute()
                
                for item in response.data:
                    live_prices[item['property_id']] = {
                        'price': item.get('price'),
                        'price_at_last_manifest': item.get('price_at_last_manifest')
                    }
            
            # Compare prices and detect changes
            price_changes = []
            price_change_threshold = 0.01  # 1% threshold
            
            for property_id, manifest_price in manifest_prices.items():
                if property_id not in live_prices:
                    continue  # Skip new properties (handled by detect_new_from_manifest)
                
                live_data = live_prices[property_id]
                live_price = live_data.get('price')
                
                if live_price is None or live_price == 0:
                    continue
                
                try:
                    manifest_price_float = float(manifest_price)
                    live_price_float = float(live_price)
                    
                    if live_price_float == 0:
                        continue
                    
                    price_diff = manifest_price_float - live_price_float
                    price_change_percent = abs(price_diff) / live_price_float
                    
                    if price_change_percent > price_change_threshold:
                        price_changes.append({
                            'property_id': property_id,
                            'old_price': live_price_float,
                            'new_price': manifest_price_float,
                            'price_change_percent': price_change_percent,
                            'price_diff': price_diff,
                            'direction': 'increase' if price_diff > 0 else 'decrease'
                        })
                        
                except (ValueError, TypeError):
                    continue
            
            logger.info(f"Price change detection: {len(entries_with_price)} entries checked, "
                       f"{len(price_changes)} price changes detected")
            return price_changes
            
        except Exception as e:
            logger.error(f"Error detecting price changes from manifest: {str(e)}")
            return []
    
    async def get_stale_properties(self, stale_days: int = 14, limit: int = 1000) -> List[Dict]:
        """
        Get properties that haven't been fully scraped recently.
        
        Used by Tier 3 (Weekly Deep Scan) to identify properties needing refresh.
        
        Args:
            stale_days: Number of days after which a property is considered stale
            limit: Maximum number of properties to return
            
        Returns:
            List of property dicts with property_id, source_url, and last_full_scrape_at
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=stale_days)
            
            # Find active properties not scraped recently
            response = self.supabase.table('properties_live').select(
                'property_id, source_url, last_full_scrape_at, data_staleness_days'
            ).eq('status', 'active').or_(
                f"last_full_scrape_at.is.null,last_full_scrape_at.lt.{cutoff_date.isoformat()}"
            ).order('last_full_scrape_at', nullsfirst=True).limit(limit).execute()
            
            logger.info(f"Found {len(response.data)} stale properties (>{stale_days} days old)")
            return response.data
            
        except Exception as e:
            logger.error(f"Error getting stale properties: {str(e)}")
            return []
    
    async def get_random_sample_for_verification(self, sample_size: int = 100) -> List[Dict]:
        """
        Get a random sample of active properties for data quality verification.
        
        Used by Tier 4 (Monthly Refresh) to verify data accuracy across the dataset.
        
        Args:
            sample_size: Number of random properties to sample
            
        Returns:
            List of property dicts for verification
        """
        try:
            # Use database random sampling if available, otherwise fetch and sample
            response = self.supabase.table('properties_live').select(
                'property_id, source_url, price, title, last_full_scrape_at'
            ).eq('status', 'active').limit(sample_size * 3).execute()  # Fetch extra for randomization
            
            if not response.data:
                return []
            
            # Random sample from results
            import random
            sample = random.sample(response.data, min(sample_size, len(response.data)))
            
            logger.info(f"Selected {len(sample)} random properties for verification")
            return sample
            
        except Exception as e:
            logger.error(f"Error getting random sample: {str(e)}")
            return [] 