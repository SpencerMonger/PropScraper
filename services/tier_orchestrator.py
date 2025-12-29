"""
Tier Orchestrator for Hybrid 4-Tier Property Sync System

This service coordinates the execution of each tier's workflow,
managing the complete sync process from manifest scanning to detail scraping.

Based on HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

from supabase import Client

from config.tier_config import get_config, TierConfig, TierLevel
from config.property_id import generate_property_id
from .manifest_scan_service import ManifestScanService, ManifestScanResult
from .property_diff_service import PropertyDiffService, DiffResult
from .scrape_queue_service import ScrapeQueueService, QueuedProperty

logger = logging.getLogger(__name__)


@dataclass
class TierResult:
    """Result of a tier execution"""
    tier_level: int
    tier_name: str
    success: bool
    pages_scanned: int = 0
    new_properties: int = 0
    price_changes: int = 0
    removals_confirmed: int = 0
    relisted_count: int = 0
    properties_queued: int = 0
    properties_scraped: int = 0
    properties_updated: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    sync_run_id: Optional[str] = None


@dataclass
class QueueProcessResult:
    """Result of queue processing"""
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    duration_seconds: float = 0.0


class TierOrchestrator:
    """
    Main orchestrator for the tiered sync system.
    
    Coordinates:
    - Tier 1 (Hot Listings): Fast scan of first 10 pages every 6 hours
    - Tier 2 (Daily Sync): First 100 pages + manifest check daily
    - Tier 3 (Weekly Deep): Full manifest scan + removal detection weekly
    - Tier 4 (Monthly Refresh): Stale data refresh + random sample monthly
    """
    
    def __init__(
        self,
        supabase_client: Client,
        config: Optional[TierConfig] = None,
        scraper: Optional[Any] = None
    ):
        self.supabase = supabase_client
        self.config = config or get_config()
        self.scraper = scraper  # EnhancedPincaliScraper instance
        
        # Initialize services
        self.manifest_service = ManifestScanService(supabase_client, self.config)
        self.diff_service = PropertyDiffService(supabase_client, self.config)
        self.queue_service = ScrapeQueueService(supabase_client, self.config)
    
    def set_scraper(self, scraper: Any):
        """Set the scraper instance for detail page scraping"""
        self.scraper = scraper
    
    async def run_tier_1_hot_listings(self) -> TierResult:
        """
        Execute Tier 1: Hot Listings scan.
        
        - Runs every 6 hours
        - Scans first 10 pages of each listing source
        - Detects new properties only
        - Queues new properties for immediate scraping
        - Processes queue
        
        Returns:
            TierResult with execution details
        """
        tier_level = TierLevel.HOT_LISTINGS
        tier_settings = self.config.get_tier(tier_level)
        
        logger.info(f"=" * 60)
        logger.info(f"Starting Tier 1: {tier_settings.display_name}")
        logger.info(f"=" * 60)
        
        start_time = time.time()
        result = TierResult(
            tier_level=tier_level,
            tier_name=tier_settings.name,
            success=False
        )
        
        try:
            # Create sync run record
            sync_run_id = await self._create_sync_run(tier_level, tier_settings.name)
            result.sync_run_id = sync_run_id
            
            # Create scraping session
            session_id = await self._create_scraping_session(tier_settings.name)
            
            # Step 1: Run manifest scan on first N pages
            manifest_result = await self.manifest_service.run_multi_source_manifest_scan(
                max_pages_per_source=tier_settings.pages_to_scan,
                session_id=session_id,
                delay_between_pages=tier_settings.delay_between_pages
            )
            
            result.pages_scanned = manifest_result.pages_scanned
            result.new_properties = manifest_result.new_properties
            result.price_changes = manifest_result.price_changes
            result.errors.extend(manifest_result.errors)
            
            # Step 2: Queue new properties for scraping
            new_property_ids = await self.diff_service.detect_new_properties(session_id)
            if new_property_ids:
                queued = await self.queue_service.queue_new_properties(
                    new_property_ids, session_id
                )
                result.properties_queued = queued
            
            # Step 3: Process queue
            if result.properties_queued > 0:
                queue_result = await self.process_scrape_queue(
                    max_items=tier_settings.max_queue_items,
                    rate_limit_seconds=tier_settings.delay_between_details,
                    session_id=session_id
                )
                result.properties_scraped = queue_result.succeeded
            
            # Step 4: Clear manifest flags for processed properties
            await self.manifest_service.clear_manifest_flags(session_id)
            
            result.success = True
            
        except Exception as e:
            error_msg = f"Error in Tier 1: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
        
        finally:
            result.duration_seconds = time.time() - start_time
            
            # Update sync run record
            if result.sync_run_id:
                await self._update_sync_run(result)
            
            logger.info(f"Tier 1 completed in {result.duration_seconds:.1f}s: "
                       f"{result.new_properties} new properties found, "
                       f"{result.properties_scraped} scraped")
        
        return result
    
    async def run_tier_2_daily_sync(self) -> TierResult:
        """
        Execute Tier 2: Daily Sync.
        
        - Runs daily at midnight
        - Scans first 100 pages of each source
        - Detects new properties and price changes
        - Updates consecutive_missing_count
        - Queues new + price changed properties
        - Processes queue
        
        Returns:
            TierResult with execution details
        """
        tier_level = TierLevel.DAILY_SYNC
        tier_settings = self.config.get_tier(tier_level)
        
        logger.info(f"=" * 60)
        logger.info(f"Starting Tier 2: {tier_settings.display_name}")
        logger.info(f"=" * 60)
        
        start_time = time.time()
        result = TierResult(
            tier_level=tier_level,
            tier_name=tier_settings.name,
            success=False
        )
        
        try:
            sync_run_id = await self._create_sync_run(tier_level, tier_settings.name)
            result.sync_run_id = sync_run_id
            
            session_id = await self._create_scraping_session(tier_settings.name)
            
            # Step 1: Run manifest scan
            manifest_result = await self.manifest_service.run_multi_source_manifest_scan(
                max_pages_per_source=tier_settings.pages_to_scan,
                session_id=session_id,
                delay_between_pages=tier_settings.delay_between_pages
            )
            
            result.pages_scanned = manifest_result.pages_scanned
            result.new_properties = manifest_result.new_properties
            result.price_changes = manifest_result.price_changes
            result.errors.extend(manifest_result.errors)
            
            # Step 2: Run diff detection
            diff_result = await self.diff_service.run_full_diff(
                session_id=session_id,
                verify_removals=False,  # Don't verify removals in Tier 2
                min_missing_count=2
            )
            
            result.relisted_count = len(diff_result.relisted_properties)
            
            # Step 3: Queue new properties
            if diff_result.new_properties:
                await self.queue_service.queue_new_properties(
                    diff_result.new_properties, session_id
                )
            
            # Step 4: Queue price changes
            if diff_result.price_changes:
                await self.queue_service.queue_price_changes(
                    diff_result.price_changes, session_id
                )
            
            # Step 5: Queue relisted properties
            if diff_result.relisted_properties:
                await self.queue_service.queue_relisted_properties(
                    diff_result.relisted_properties, session_id
                )
            
            # Get total queued count
            queue_stats = await self.queue_service.get_queue_stats()
            result.properties_queued = queue_stats.pending_count
            
            # Step 6: Process queue
            if result.properties_queued > 0:
                queue_result = await self.process_scrape_queue(
                    max_items=tier_settings.max_queue_items,
                    rate_limit_seconds=tier_settings.delay_between_details,
                    session_id=session_id
                )
                result.properties_scraped = queue_result.succeeded
            
            # Step 7: Update manifest prices in live table
            await self.diff_service.update_live_prices_from_manifest(session_id)
            
            # Step 8: Clear manifest flags
            await self.manifest_service.clear_manifest_flags(session_id)
            
            result.success = True
            
        except Exception as e:
            error_msg = f"Error in Tier 2: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
        
        finally:
            result.duration_seconds = time.time() - start_time
            
            if result.sync_run_id:
                await self._update_sync_run(result)
            
            logger.info(f"Tier 2 completed in {result.duration_seconds:.1f}s: "
                       f"{result.new_properties} new, {result.price_changes} price changes, "
                       f"{result.properties_scraped} scraped")
        
        return result
    
    async def run_tier_3_weekly_deep(self) -> TierResult:
        """
        Execute Tier 3: Weekly Deep Scan.
        
        - Runs weekly on Sunday
        - Full manifest scan (all pages)
        - Detects new properties, price changes, and removals
        - Confirms removals via HTTP checks
        - Queues new + price changed + stale (7+ days)
        - Processes queue
        
        Returns:
            TierResult with execution details
        """
        tier_level = TierLevel.WEEKLY_DEEP
        tier_settings = self.config.get_tier(tier_level)
        
        logger.info(f"=" * 60)
        logger.info(f"Starting Tier 3: {tier_settings.display_name}")
        logger.info(f"=" * 60)
        
        start_time = time.time()
        result = TierResult(
            tier_level=tier_level,
            tier_name=tier_settings.name,
            success=False
        )
        
        try:
            sync_run_id = await self._create_sync_run(tier_level, tier_settings.name)
            result.sync_run_id = sync_run_id
            
            session_id = await self._create_scraping_session(tier_settings.name)
            
            # Step 1: Run FULL manifest scan (all pages)
            manifest_result = await self.manifest_service.run_multi_source_manifest_scan(
                max_pages_per_source=0,  # 0 = all pages
                session_id=session_id,
                delay_between_pages=tier_settings.delay_between_pages
            )
            
            result.pages_scanned = manifest_result.pages_scanned
            result.new_properties = manifest_result.new_properties
            result.price_changes = manifest_result.price_changes
            result.errors.extend(manifest_result.errors)
            
            # Step 2: Run full diff with removal verification
            diff_result = await self.diff_service.run_full_diff(
                session_id=session_id,
                verify_removals=True,  # Verify removals in Tier 3
                min_missing_count=self.config.min_missing_count_for_removal
            )
            
            result.removals_confirmed = len([
                r for r in diff_result.confirmed_removals 
                if r.confirmed_removed
            ])
            result.relisted_count = len(diff_result.relisted_properties)
            
            # Step 3: Queue new properties
            if diff_result.new_properties:
                await self.queue_service.queue_new_properties(
                    diff_result.new_properties, session_id
                )
            
            # Step 4: Queue price changes
            if diff_result.price_changes:
                await self.queue_service.queue_price_changes(
                    diff_result.price_changes, session_id
                )
            
            # Step 5: Queue relisted properties
            if diff_result.relisted_properties:
                await self.queue_service.queue_relisted_properties(
                    diff_result.relisted_properties, session_id
                )
            
            # Step 6: Queue stale properties
            await self.queue_service.queue_stale_properties(
                days_threshold=tier_settings.stale_days_threshold,
                limit=tier_settings.max_queue_items // 2,  # Use half capacity for stale
                session_id=session_id
            )
            
            queue_stats = await self.queue_service.get_queue_stats()
            result.properties_queued = queue_stats.pending_count
            
            # Step 7: Process queue
            if result.properties_queued > 0:
                queue_result = await self.process_scrape_queue(
                    max_items=tier_settings.max_queue_items,
                    rate_limit_seconds=tier_settings.delay_between_details,
                    session_id=session_id
                )
                result.properties_scraped = queue_result.succeeded
            
            # Step 8: Cleanup confirmed removals from manifest
            for removal in diff_result.confirmed_removals:
                if removal.confirmed_removed:
                    try:
                        self.supabase.table('property_manifest').delete().eq(
                            'property_id', removal.property_id
                        ).execute()
                    except Exception as e:
                        logger.debug(f"Could not remove {removal.property_id} from manifest: {e}")
            
            # Step 9: Clear manifest flags
            await self.manifest_service.clear_manifest_flags(session_id)
            
            result.success = True
            
        except Exception as e:
            error_msg = f"Error in Tier 3: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
        
        finally:
            result.duration_seconds = time.time() - start_time
            
            if result.sync_run_id:
                await self._update_sync_run(result)
            
            logger.info(f"Tier 3 completed in {result.duration_seconds:.1f}s: "
                       f"{result.new_properties} new, {result.price_changes} price changes, "
                       f"{result.removals_confirmed} confirmed removals, "
                       f"{result.properties_scraped} scraped")
        
        return result
    
    async def run_tier_4_monthly_refresh(self) -> TierResult:
        """
        Execute Tier 4: Monthly Refresh.
        
        - Runs monthly on the 1st
        - Identifies properties with stale data (30+ days)
        - Runs random sample verification (10% of active)
        - Processes queue with extended rate limiting
        - Generates data quality report
        
        Returns:
            TierResult with execution details
        """
        tier_level = TierLevel.MONTHLY_REFRESH
        tier_settings = self.config.get_tier(tier_level)
        
        logger.info(f"=" * 60)
        logger.info(f"Starting Tier 4: {tier_settings.display_name}")
        logger.info(f"=" * 60)
        
        start_time = time.time()
        result = TierResult(
            tier_level=tier_level,
            tier_name=tier_settings.name,
            success=False
        )
        
        try:
            sync_run_id = await self._create_sync_run(tier_level, tier_settings.name)
            result.sync_run_id = sync_run_id
            
            session_id = await self._create_scraping_session(tier_settings.name)
            
            # Step 1: Queue all stale properties
            stale_queued = await self.queue_service.queue_stale_properties(
                days_threshold=tier_settings.stale_days_threshold,
                limit=tier_settings.max_queue_items,
                session_id=session_id
            )
            
            # Step 2: Calculate and queue random sample
            count_response = self.supabase.table('properties_live').select(
                'id', count='exact'
            ).eq('listing_status', 'active').execute()
            
            total_active = count_response.count or 0
            sample_size = int(total_active * tier_settings.random_sample_percent / 100)
            
            if sample_size > 0:
                sample_queued = await self.queue_service.queue_random_sample(
                    sample_size=sample_size,
                    session_id=session_id
                )
                logger.info(f"Queued {sample_queued} random sample properties "
                           f"({tier_settings.random_sample_percent}% of {total_active} active)")
            
            queue_stats = await self.queue_service.get_queue_stats()
            result.properties_queued = queue_stats.pending_count
            
            # Step 3: Process queue with extended rate limiting
            if result.properties_queued > 0:
                queue_result = await self.process_scrape_queue(
                    max_items=tier_settings.max_queue_items,
                    rate_limit_seconds=tier_settings.delay_between_details,
                    session_id=session_id
                )
                result.properties_scraped = queue_result.succeeded
            
            # Step 4: Generate data quality report
            quality_report = await self._generate_quality_report(session_id)
            logger.info(f"Data quality report: {quality_report}")
            
            result.success = True
            
        except Exception as e:
            error_msg = f"Error in Tier 4: {str(e)}"
            logger.error(error_msg)
            result.errors.append(error_msg)
        
        finally:
            result.duration_seconds = time.time() - start_time
            
            if result.sync_run_id:
                await self._update_sync_run(result)
            
            logger.info(f"Tier 4 completed in {result.duration_seconds:.1f}s: "
                       f"{result.properties_queued} queued, "
                       f"{result.properties_scraped} scraped")
        
        return result
    
    async def process_scrape_queue(
        self,
        max_items: int,
        rate_limit_seconds: float,
        session_id: str
    ) -> QueueProcessResult:
        """
        Process items from the scrape queue.
        
        Args:
            max_items: Maximum items to process
            rate_limit_seconds: Delay between scraping each item
            session_id: Session ID for tracking
            
        Returns:
            QueueProcessResult with processing statistics
        """
        start_time = time.time()
        result = QueueProcessResult()
        
        if not self.scraper:
            logger.warning("No scraper configured, skipping queue processing")
            return result
        
        logger.info(f"Processing scrape queue (max {max_items} items)...")
        
        # Release any stale claims first
        await self.queue_service.release_stale_claims(
            minutes=self.config.queue_stale_claim_minutes
        )
        
        items_processed = 0
        batch_size = 10  # Claim in small batches
        
        while items_processed < max_items:
            # Claim next batch
            claimed = await self.queue_service.claim_next_batch(
                batch_size=min(batch_size, max_items - items_processed),
                worker_id="tier_orchestrator"
            )
            
            if not claimed:
                logger.debug("No more items in queue")
                break
            
            # Process each claimed item
            for item in claimed:
                try:
                    # Call scraper to get property details
                    property_data = await self.scraper.scrape_property_details(
                        item.source_url
                    )
                    
                    if property_data:
                        # Ensure source_url is included from queue item
                        property_data['source_url'] = item.source_url
                        
                        # Use property_id from scraped data (uses centralized hash-based ID)
                        # Fall back to queue item's property_id if not present
                        final_property_id = property_data.get('property_id') or generate_property_id(item.source_url)
                        
                        # Update live table with scraped data
                        await self._update_property_from_scrape(
                            final_property_id, property_data, session_id
                        )
                        
                        await self.queue_service.mark_completed(item.id, success=True)
                        result.succeeded += 1
                    else:
                        await self.queue_service.mark_completed(
                            item.id, success=False, error="No data returned from scraper"
                        )
                        result.failed += 1
                    
                except Exception as e:
                    logger.error(f"Error scraping {item.property_id}: {e}")
                    await self.queue_service.mark_completed(
                        item.id, success=False, error=str(e)
                    )
                    result.failed += 1
                
                result.processed += 1
                items_processed += 1
                
                # Rate limiting
                if items_processed < max_items:
                    await asyncio.sleep(rate_limit_seconds)
            
            # Progress logging
            if items_processed % 50 == 0:
                logger.info(f"Queue progress: {items_processed}/{max_items} "
                           f"({result.succeeded} succeeded, {result.failed} failed)")
        
        result.duration_seconds = time.time() - start_time
        
        logger.info(f"Queue processing completed: {result.processed} processed, "
                   f"{result.succeeded} succeeded, {result.failed} failed")
        
        return result
    
    async def _update_property_from_scrape(
        self,
        property_id: str,
        property_data: Dict,
        session_id: str
    ):
        """Insert or update live table with scraped property data"""
        try:
            now = datetime.utcnow().isoformat()
            
            # Prepare data with required fields for INSERT
            upsert_data = {
                'property_id': property_id,
                'last_full_scrape_at': now,
                'last_updated_at': now,
                'updated_at': now,
                'last_seen_at': now,
                'last_manifest_seen_at': now,
                'scrape_priority': 3,  # Reset to normal priority
                'status': 'active',
                'listing_status': 'active',
                'consecutive_missing_count': 0,
            }
            
            # Copy relevant fields from scraped data
            fields_to_copy = [
                'title', 'description', 'price', 'currency', 'price_per_m2',
                'property_type', 'operation_type',
                'bedrooms', 'bathrooms', 'half_bathrooms', 'parking_spaces',
                'total_area_m2', 'covered_area_m2', 'lot_size_m2',
                'construction_year', 'floor_number',
                'features', 'amenities', 'image_urls', 'main_image_url',
                'agent_name', 'agent_phone', 'agent_email', 'agency_name',
                'address', 'neighborhood', 'city', 'state', 'postal_code',
                'latitude', 'longitude', 'gps_coordinates',
                'source_url', 'listing_date'
            ]
            
            for field in fields_to_copy:
                if field in property_data and property_data[field] is not None:
                    upsert_data[field] = property_data[field]
            
            # Use upsert to handle both INSERT (new) and UPDATE (existing)
            self.supabase.table('properties_live').upsert(
                upsert_data,
                on_conflict='property_id'
            ).execute()
            
            logger.debug(f"Upserted property {property_id}")
            
        except Exception as e:
            logger.error(f"Error upserting property {property_id}: {e}")
            raise
    
    async def _create_sync_run(self, tier_level: int, tier_name: str) -> str:
        """Create a new sync run record"""
        try:
            response = self.supabase.table('sync_runs').insert({
                'tier_level': tier_level,
                'tier_name': tier_name,
                'status': 'running',
                'started_at': datetime.utcnow().isoformat()
            }).execute()
            
            return response.data[0]['id']
        except Exception as e:
            logger.error(f"Error creating sync run: {e}")
            return None
    
    async def _update_sync_run(self, result: TierResult):
        """Update sync run record with results"""
        if not result.sync_run_id:
            return
        
        try:
            update_data = {
                'status': 'completed' if result.success else 'failed',
                'completed_at': datetime.utcnow().isoformat(),
                'pages_scanned': result.pages_scanned,
                'new_properties_found': result.new_properties,
                'price_changes_detected': result.price_changes,
                'removals_confirmed': result.removals_confirmed,
                'properties_queued': result.properties_queued,
                'properties_scraped': result.properties_scraped,
                'properties_updated': result.properties_updated,
                'error_count': len(result.errors),
                'error_summary': '; '.join(result.errors[:5]) if result.errors else None,
                'execution_time_ms': int(result.duration_seconds * 1000)
            }
            
            self.supabase.table('sync_runs').update(
                update_data
            ).eq('id', result.sync_run_id).execute()
            
        except Exception as e:
            logger.error(f"Error updating sync run: {e}")
    
    async def _create_scraping_session(self, tier_name: str) -> str:
        """Create a scraping session for this tier run"""
        try:
            response = self.supabase.table('scraping_sessions').insert({
                'session_name': f"Tier Sync - {tier_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                'base_url': self.config.base_url,
                'status': 'running',
                'filters_applied': {'tier': tier_name}
            }).execute()
            
            return response.data[0]['id']
        except Exception as e:
            logger.error(f"Error creating scraping session: {e}")
            return None
    
    async def _generate_quality_report(self, session_id: str) -> Dict:
        """Generate a data quality report"""
        try:
            # Get overall stats
            total_response = self.supabase.table('properties_live').select(
                'id', count='exact'
            ).eq('listing_status', 'active').execute()
            
            stale_response = self.supabase.table('properties_live').select(
                'id', count='exact'
            ).eq('listing_status', 'active').lt(
                'last_full_scrape_at',
                (datetime.utcnow() - __import__('datetime').timedelta(days=30)).isoformat()
            ).execute()
            
            return {
                'total_active': total_response.count or 0,
                'stale_properties': stale_response.count or 0,
                'staleness_percent': (
                    (stale_response.count or 0) / (total_response.count or 1) * 100
                ),
                'generated_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error generating quality report: {e}")
            return {}
    
    async def get_last_tier_run(self, tier_level: int) -> Optional[Dict]:
        """Get the last successful run for a tier"""
        try:
            response = self.supabase.rpc(
                'get_last_tier_run',
                {'p_tier_level': tier_level}
            ).execute()
            
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            logger.error(f"Error getting last tier run: {e}")
            return None
    
    async def run_tier(self, tier_level: int) -> TierResult:
        """
        Run a specific tier by level.
        
        Args:
            tier_level: Tier level (1-4)
            
        Returns:
            TierResult from the tier execution
        """
        if tier_level == TierLevel.HOT_LISTINGS:
            return await self.run_tier_1_hot_listings()
        elif tier_level == TierLevel.DAILY_SYNC:
            return await self.run_tier_2_daily_sync()
        elif tier_level == TierLevel.WEEKLY_DEEP:
            return await self.run_tier_3_weekly_deep()
        elif tier_level == TierLevel.MONTHLY_REFRESH:
            return await self.run_tier_4_monthly_refresh()
        else:
            raise ValueError(f"Invalid tier level: {tier_level}")

