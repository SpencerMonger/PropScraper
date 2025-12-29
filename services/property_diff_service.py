"""
Property Diff Service for Hybrid 4-Tier Property Sync System

This service compares manifest data against live database to detect
new, changed, and removed properties.

Based on HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Set

import requests
from supabase import Client

from config.tier_config import get_config, TierConfig

logger = logging.getLogger(__name__)


@dataclass
class PropertyPriceChange:
    """Represents a detected price change"""
    property_id: str
    old_price: float
    new_price: float
    percent_change: float
    source_url: Optional[str] = None


@dataclass
class PropertyRemovalCandidate:
    """Represents a property that might be removed"""
    property_id: str
    source_url: str
    last_seen_at: Optional[datetime] = None
    consecutive_missing_count: int = 0


@dataclass
class PropertyRemovalResult:
    """Result of removal verification"""
    property_id: str
    confirmed_removed: bool
    http_status: Optional[int] = None
    redirect_url: Optional[str] = None
    reason: str = ""


@dataclass
class DiffResult:
    """Complete result of diff detection"""
    new_properties: List[str] = field(default_factory=list)
    price_changes: List[PropertyPriceChange] = field(default_factory=list)
    removal_candidates: List[PropertyRemovalCandidate] = field(default_factory=list)
    confirmed_removals: List[PropertyRemovalResult] = field(default_factory=list)
    relisted_properties: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0


class PropertyDiffService:
    """
    Service for detecting differences between manifest and live database.
    
    Detects:
    - New properties (in manifest but not in live)
    - Price changes (manifest price differs from live price)
    - Removed properties (in live but not seen in manifest)
    - Relisted properties (was removed but now appears again)
    """
    
    def __init__(self, supabase_client: Client, config: Optional[TierConfig] = None):
        self.supabase = supabase_client
        self.config = config or get_config()
        
        # HTTP session for removal verification
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
    
    async def detect_new_properties(self, session_id: str) -> List[str]:
        """
        Detect properties in manifest that are not in live table.
        
        Args:
            session_id: Session ID to filter manifest entries
            
        Returns:
            List of property IDs for new properties
        """
        try:
            # Query manifest for entries marked as new
            response = self.supabase.table('property_manifest').select(
                'property_id'
            ).eq('is_new', True).execute()
            
            new_property_ids = [item['property_id'] for item in response.data]
            
            logger.info(f"Detected {len(new_property_ids)} new properties")
            return new_property_ids
            
        except Exception as e:
            logger.error(f"Error detecting new properties: {e}")
            return []
    
    async def detect_price_changes(self, session_id: str) -> List[PropertyPriceChange]:
        """
        Detect properties where manifest price differs from live price.
        
        Args:
            session_id: Session ID to filter manifest entries
            
        Returns:
            List of PropertyPriceChange objects
        """
        price_changes = []
        
        try:
            # Get manifest entries with price_changed flag
            manifest_response = self.supabase.table('property_manifest').select(
                'property_id, listing_page_price, source_url'
            ).eq('price_changed', True).execute()
            
            if not manifest_response.data:
                return []
            
            # Get corresponding live prices
            property_ids = [item['property_id'] for item in manifest_response.data]
            live_response = self.supabase.table('properties_live').select(
                'property_id, price'
            ).in_('property_id', property_ids).execute()
            
            live_prices = {
                item['property_id']: item.get('price')
                for item in live_response.data
            }
            
            # Build price change list
            for manifest_item in manifest_response.data:
                prop_id = manifest_item['property_id']
                new_price = manifest_item.get('listing_page_price')
                old_price = live_prices.get(prop_id)
                
                if old_price and new_price and old_price != new_price:
                    try:
                        percent_change = abs(new_price - old_price) / old_price * 100
                        
                        price_changes.append(PropertyPriceChange(
                            property_id=prop_id,
                            old_price=float(old_price),
                            new_price=float(new_price),
                            percent_change=percent_change,
                            source_url=manifest_item.get('source_url')
                        ))
                    except (ValueError, ZeroDivisionError):
                        continue
            
            logger.info(f"Detected {len(price_changes)} price changes")
            return price_changes
            
        except Exception as e:
            logger.error(f"Error detecting price changes: {e}")
            return []
    
    async def detect_removed_properties(
        self,
        session_id: str,
        min_missing_count: int = 2
    ) -> List[PropertyRemovalCandidate]:
        """
        Detect properties that might have been removed.
        
        A property is flagged for removal if:
        - It's marked as 'active' in live table
        - It was NOT seen in the current manifest scan
        - It has been missing for at least min_missing_count consecutive scans
        
        Args:
            session_id: Current session ID
            min_missing_count: Minimum consecutive misses to flag
            
        Returns:
            List of PropertyRemovalCandidate objects
        """
        candidates = []
        
        try:
            # First, increment missing count for properties not seen
            increment_result = self.supabase.rpc(
                'increment_missing_count',
                {'p_session_id': session_id}
            ).execute()
            
            logger.debug(f"Incremented missing count for properties not in session")
            
            # Reset count for properties that were seen
            reset_result = self.supabase.rpc(
                'reset_missing_count',
                {'p_session_id': session_id}
            ).execute()
            
            logger.debug(f"Reset missing count for properties seen in session")
            
            # Query for removal candidates
            response = self.supabase.table('properties_live').select(
                'property_id, source_url, last_manifest_seen_at, consecutive_missing_count'
            ).eq('listing_status', 'active').gte(
                'consecutive_missing_count', min_missing_count
            ).execute()
            
            for item in response.data:
                last_seen = None
                if item.get('last_manifest_seen_at'):
                    try:
                        last_seen = datetime.fromisoformat(
                            item['last_manifest_seen_at'].replace('Z', '+00:00')
                        )
                    except (ValueError, TypeError):
                        pass
                
                candidates.append(PropertyRemovalCandidate(
                    property_id=item['property_id'],
                    source_url=item.get('source_url', ''),
                    last_seen_at=last_seen,
                    consecutive_missing_count=item.get('consecutive_missing_count', 0)
                ))
            
            logger.info(f"Found {len(candidates)} removal candidates "
                       f"(missing >= {min_missing_count} consecutive scans)")
            return candidates
            
        except Exception as e:
            logger.error(f"Error detecting removed properties: {e}")
            return []
    
    async def confirm_removals(
        self,
        candidates: List[PropertyRemovalCandidate],
        delay_between_checks: float = 1.0
    ) -> List[PropertyRemovalResult]:
        """
        Verify removal candidates by checking if their URLs still exist.
        
        Args:
            candidates: List of removal candidates to verify
            delay_between_checks: Delay between HTTP requests
            
        Returns:
            List of PropertyRemovalResult objects
        """
        results = []
        
        logger.info(f"Verifying {len(candidates)} removal candidates...")
        
        for i, candidate in enumerate(candidates):
            try:
                result = await self._check_property_url(candidate)
                results.append(result)
                
                if (i + 1) % 10 == 0:
                    logger.debug(f"Verified {i + 1}/{len(candidates)} removal candidates")
                
                if i < len(candidates) - 1:
                    await asyncio.sleep(delay_between_checks)
                    
            except Exception as e:
                logger.warning(f"Error checking {candidate.property_id}: {e}")
                results.append(PropertyRemovalResult(
                    property_id=candidate.property_id,
                    confirmed_removed=False,
                    reason=f"Error: {str(e)}"
                ))
        
        confirmed_count = len([r for r in results if r.confirmed_removed])
        logger.info(f"Confirmed {confirmed_count}/{len(candidates)} removals")
        
        return results
    
    async def _check_property_url(
        self,
        candidate: PropertyRemovalCandidate
    ) -> PropertyRemovalResult:
        """
        Check if a property URL still exists or redirects.
        
        Args:
            candidate: Removal candidate to check
            
        Returns:
            PropertyRemovalResult with verification details
        """
        if not candidate.source_url:
            return PropertyRemovalResult(
                property_id=candidate.property_id,
                confirmed_removed=True,
                reason="No source URL available"
            )
        
        try:
            # Use HEAD request first (faster)
            response = self.session.head(
                candidate.source_url,
                allow_redirects=False,
                timeout=10
            )
            
            status_code = response.status_code
            
            # Check for 404 - confirmed removed
            if status_code == 404:
                return PropertyRemovalResult(
                    property_id=candidate.property_id,
                    confirmed_removed=True,
                    http_status=status_code,
                    reason="Page not found (404)"
                )
            
            # Check for redirect
            if status_code in (301, 302, 303, 307, 308):
                redirect_url = response.headers.get('Location', '')
                
                # If redirected to a search page, it's likely removed
                if any(term in redirect_url.lower() for term in 
                       ['search', 'properties', 'filter', '?']):
                    return PropertyRemovalResult(
                        property_id=candidate.property_id,
                        confirmed_removed=True,
                        http_status=status_code,
                        redirect_url=redirect_url,
                        reason="Redirected to search/listing page"
                    )
                
                # Redirect to another property page - not necessarily removed
                return PropertyRemovalResult(
                    property_id=candidate.property_id,
                    confirmed_removed=False,
                    http_status=status_code,
                    redirect_url=redirect_url,
                    reason="Redirected to another page"
                )
            
            # 200 OK - property still exists
            if status_code == 200:
                return PropertyRemovalResult(
                    property_id=candidate.property_id,
                    confirmed_removed=False,
                    http_status=status_code,
                    reason="Page still exists (200 OK)"
                )
            
            # Other status codes - inconclusive
            return PropertyRemovalResult(
                property_id=candidate.property_id,
                confirmed_removed=False,
                http_status=status_code,
                reason=f"Unexpected status code: {status_code}"
            )
            
        except requests.exceptions.Timeout:
            return PropertyRemovalResult(
                property_id=candidate.property_id,
                confirmed_removed=False,
                reason="Request timed out"
            )
        except requests.exceptions.ConnectionError:
            return PropertyRemovalResult(
                property_id=candidate.property_id,
                confirmed_removed=False,
                reason="Connection error"
            )
        except Exception as e:
            return PropertyRemovalResult(
                property_id=candidate.property_id,
                confirmed_removed=False,
                reason=f"Error: {str(e)}"
            )
    
    async def detect_relisted_properties(self, session_id: str) -> List[str]:
        """
        Detect properties that were previously removed but now appear in manifest.
        
        Args:
            session_id: Current session ID
            
        Returns:
            List of property IDs for relisted properties
        """
        try:
            # Find properties in manifest that are marked as removed in live
            manifest_response = self.supabase.table('property_manifest').select(
                'property_id'
            ).eq('seen_in_session_id', session_id).execute()
            
            manifest_ids = [item['property_id'] for item in manifest_response.data]
            
            if not manifest_ids:
                return []
            
            # Find which of these are marked as removed in live (batch queries)
            relisted = []
            batch_size = 200  # Avoid URL length limits
            
            for i in range(0, len(manifest_ids), batch_size):
                batch_ids = manifest_ids[i:i + batch_size]
                try:
                    live_response = self.supabase.table('properties_live').select(
                        'property_id'
                    ).in_('property_id', batch_ids).in_(
                        'listing_status', ['confirmed_removed', 'sold', 'likely_removed']
                    ).execute()
                    
                    relisted.extend([item['property_id'] for item in live_response.data])
                except Exception as e:
                    logger.warning(f"Error checking relisted batch {i}: {e}")
            
            if relisted:
                logger.info(f"Detected {len(relisted)} relisted properties")
            
            return relisted
            
        except Exception as e:
            logger.error(f"Error detecting relisted properties: {e}")
            return []
    
    async def update_removal_status(
        self,
        property_id: str,
        confirmed: bool,
        reason: str = ""
    ):
        """
        Update the listing status of a property based on removal confirmation.
        
        Args:
            property_id: Property ID to update
            confirmed: Whether removal is confirmed
            reason: Reason for the status change
        """
        try:
            now = datetime.utcnow().isoformat()
            
            if confirmed:
                # Mark as confirmed removed
                update_data = {
                    'listing_status': 'confirmed_removed',
                    'status': 'removed',
                    'last_updated_at': now,
                    'updated_at': now
                }
                logger.debug(f"Marking {property_id} as confirmed_removed: {reason}")
            else:
                # Reset consecutive missing count, keep as active
                update_data = {
                    'consecutive_missing_count': 0,
                    'last_manifest_seen_at': now,
                    'updated_at': now
                }
                logger.debug(f"Resetting missing count for {property_id}: {reason}")
            
            self.supabase.table('properties_live').update(
                update_data
            ).eq('property_id', property_id).execute()
            
        except Exception as e:
            logger.error(f"Error updating removal status for {property_id}: {e}")
    
    async def update_relisted_status(self, property_id: str):
        """
        Update a property that has been relisted.
        
        Args:
            property_id: Property ID to update
        """
        try:
            now = datetime.utcnow().isoformat()
            
            self.supabase.table('properties_live').update({
                'listing_status': 'relisted',
                'status': 'active',
                'consecutive_missing_count': 0,
                'last_manifest_seen_at': now,
                'last_updated_at': now,
                'updated_at': now
            }).eq('property_id', property_id).execute()
            
            logger.debug(f"Marked {property_id} as relisted")
            
        except Exception as e:
            logger.error(f"Error updating relisted status for {property_id}: {e}")
    
    async def batch_update_removal_status(
        self,
        results: List[PropertyRemovalResult]
    ):
        """
        Batch update removal status for multiple properties.
        
        Args:
            results: List of PropertyRemovalResult objects
        """
        confirmed_removed = []
        still_exists = []
        
        for result in results:
            if result.confirmed_removed:
                confirmed_removed.append(result.property_id)
            else:
                still_exists.append(result.property_id)
        
        now = datetime.utcnow().isoformat()
        
        try:
            # Update confirmed removals
            if confirmed_removed:
                self.supabase.table('properties_live').update({
                    'listing_status': 'confirmed_removed',
                    'status': 'removed',
                    'last_updated_at': now,
                    'updated_at': now
                }).in_('property_id', confirmed_removed).execute()
                
                logger.info(f"Marked {len(confirmed_removed)} properties as confirmed_removed")
            
            # Reset missing count for properties that still exist
            if still_exists:
                self.supabase.table('properties_live').update({
                    'consecutive_missing_count': 0,
                    'last_manifest_seen_at': now,
                    'updated_at': now
                }).in_('property_id', still_exists).execute()
                
                logger.info(f"Reset missing count for {len(still_exists)} properties")
                
        except Exception as e:
            logger.error(f"Error in batch update of removal status: {e}")
    
    async def run_full_diff(
        self,
        session_id: str,
        verify_removals: bool = True,
        min_missing_count: int = 2
    ) -> DiffResult:
        """
        Run a complete diff detection process.
        
        Args:
            session_id: Current session ID
            verify_removals: Whether to verify removal candidates via HTTP
            min_missing_count: Minimum missing count to flag removals
            
        Returns:
            DiffResult with all detected differences
        """
        start_time = time.time()
        result = DiffResult()
        
        logger.info(f"Running full diff for session {session_id}")
        
        try:
            # Detect new properties
            result.new_properties = await self.detect_new_properties(session_id)
            
            # Detect price changes
            result.price_changes = await self.detect_price_changes(session_id)
            
            # Detect relisted properties
            result.relisted_properties = await self.detect_relisted_properties(session_id)
            
            # Detect removal candidates
            result.removal_candidates = await self.detect_removed_properties(
                session_id, min_missing_count
            )
            
            # Verify removals if requested
            if verify_removals and result.removal_candidates:
                result.confirmed_removals = await self.confirm_removals(
                    result.removal_candidates
                )
                
                # Update database with results
                await self.batch_update_removal_status(result.confirmed_removals)
            
            # Update relisted properties
            for prop_id in result.relisted_properties:
                await self.update_relisted_status(prop_id)
            
        except Exception as e:
            logger.error(f"Error in full diff: {e}")
        
        result.duration_seconds = time.time() - start_time
        
        logger.info(f"Diff completed in {result.duration_seconds:.1f}s: "
                   f"{len(result.new_properties)} new, "
                   f"{len(result.price_changes)} price changes, "
                   f"{len(result.relisted_properties)} relisted, "
                   f"{len([r for r in result.confirmed_removals if r.confirmed_removed])} confirmed removals")
        
        return result
    
    async def update_live_prices_from_manifest(self, session_id: str):
        """
        Update price_at_last_manifest in live table from manifest data.
        
        Args:
            session_id: Session ID for manifest entries
        """
        try:
            # Get manifest entries with prices
            manifest_response = self.supabase.table('property_manifest').select(
                'property_id, listing_page_price'
            ).eq('seen_in_session_id', session_id).not_.is_(
                'listing_page_price', 'null'
            ).execute()
            
            now = datetime.utcnow().isoformat()
            
            # Update in batches
            for item in manifest_response.data:
                try:
                    self.supabase.table('properties_live').update({
                        'price_at_last_manifest': item['listing_page_price'],
                        'last_manifest_seen_at': now
                    }).eq('property_id', item['property_id']).execute()
                except Exception as e:
                    logger.debug(f"Could not update manifest price for {item['property_id']}: {e}")
            
            logger.info(f"Updated manifest prices for {len(manifest_response.data)} properties")
            
        except Exception as e:
            logger.error(f"Error updating live prices from manifest: {e}")

