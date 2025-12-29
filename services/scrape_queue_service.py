"""
Scrape Queue Service for Hybrid 4-Tier Property Sync System

This service manages the priority queue of properties that need
full detail page scraping.

Based on HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from supabase import Client

from config.tier_config import get_config, TierConfig
from .property_diff_service import PropertyPriceChange

logger = logging.getLogger(__name__)


@dataclass
class QueuedProperty:
    """Represents a property in the scrape queue"""
    id: str  # Queue entry UUID
    property_id: str
    source_url: str
    priority: int
    queue_reason: str
    metadata: Dict = field(default_factory=dict)


@dataclass
class QueueStats:
    """Statistics about the scrape queue"""
    pending_count: int = 0
    in_progress_count: int = 0
    completed_today: int = 0
    failed_today: int = 0
    by_priority: Dict[int, int] = field(default_factory=dict)
    by_reason: Dict[str, int] = field(default_factory=dict)


class ScrapeQueueService:
    """
    Service for managing the property scrape queue.
    
    Handles:
    - Adding properties to queue with appropriate priority
    - Claiming properties for processing
    - Marking properties as complete/failed
    - Queue statistics and cleanup
    """
    
    def __init__(self, supabase_client: Client, config: Optional[TierConfig] = None):
        self.supabase = supabase_client
        self.config = config or get_config()
    
    async def queue_new_properties(
        self,
        property_ids: List[str],
        session_id: str
    ) -> int:
        """
        Queue new properties for full scraping.
        
        Args:
            property_ids: List of property IDs to queue
            session_id: Session ID for tracking
            
        Returns:
            Number of properties queued
        """
        if not property_ids:
            return 0
        
        return await self._add_to_queue(
            property_ids=property_ids,
            priority=self.config.priority_new_property,
            queue_reason='new_property',
            session_id=session_id
        )
    
    async def queue_price_changes(
        self,
        changes: List[PropertyPriceChange],
        session_id: str
    ) -> int:
        """
        Queue properties with price changes for verification scraping.
        
        Args:
            changes: List of PropertyPriceChange objects
            session_id: Session ID for tracking
            
        Returns:
            Number of properties queued
        """
        if not changes:
            return 0
        
        property_ids = [change.property_id for change in changes]
        
        # Include price change metadata
        metadata_map = {
            change.property_id: {
                'old_price': change.old_price,
                'new_price': change.new_price,
                'percent_change': change.percent_change
            }
            for change in changes
        }
        
        return await self._add_to_queue(
            property_ids=property_ids,
            priority=self.config.priority_price_change,
            queue_reason='price_change',
            session_id=session_id,
            metadata_map=metadata_map
        )
    
    async def queue_stale_properties(
        self,
        days_threshold: int,
        limit: int,
        session_id: str
    ) -> int:
        """
        Queue properties with stale data for refresh.
        
        Args:
            days_threshold: Days since last scrape to consider stale
            limit: Maximum number of properties to queue
            session_id: Session ID for tracking
            
        Returns:
            Number of properties queued
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_threshold)
            
            # Get stale properties
            response = self.supabase.table('properties_live').select(
                'property_id'
            ).eq('listing_status', 'active').lt(
                'last_full_scrape_at', cutoff_date.isoformat()
            ).order('last_full_scrape_at').limit(limit).execute()
            
            if not response.data:
                logger.info("No stale properties found to queue")
                return 0
            
            property_ids = [item['property_id'] for item in response.data]
            
            return await self._add_to_queue(
                property_ids=property_ids,
                priority=self.config.priority_stale_data,
                queue_reason='stale_data',
                session_id=session_id
            )
            
        except Exception as e:
            logger.error(f"Error queuing stale properties: {e}")
            return 0
    
    async def queue_random_sample(
        self,
        sample_size: int,
        session_id: str
    ) -> int:
        """
        Queue a random sample of active properties for health check.
        
        Args:
            sample_size: Number of properties to sample
            session_id: Session ID for tracking
            
        Returns:
            Number of properties queued
        """
        try:
            # Get total count of active properties
            count_response = self.supabase.table('properties_live').select(
                'id', count='exact'
            ).eq('listing_status', 'active').execute()
            
            total_count = count_response.count or 0
            
            if total_count == 0:
                logger.info("No active properties for random sample")
                return 0
            
            # Get random sample using OFFSET
            # Note: This isn't perfectly random but is efficient
            sample_offsets = random.sample(
                range(total_count),
                min(sample_size, total_count)
            )
            
            property_ids = []
            for offset in sample_offsets:
                response = self.supabase.table('properties_live').select(
                    'property_id'
                ).eq('listing_status', 'active').range(offset, offset).execute()
                
                if response.data:
                    property_ids.append(response.data[0]['property_id'])
            
            if not property_ids:
                return 0
            
            return await self._add_to_queue(
                property_ids=property_ids,
                priority=self.config.priority_random_sample,
                queue_reason='random_sample',
                session_id=session_id
            )
            
        except Exception as e:
            logger.error(f"Error queuing random sample: {e}")
            return 0
    
    async def queue_relisted_properties(
        self,
        property_ids: List[str],
        session_id: str
    ) -> int:
        """
        Queue relisted properties for full scraping.
        
        Args:
            property_ids: List of property IDs to queue
            session_id: Session ID for tracking
            
        Returns:
            Number of properties queued
        """
        if not property_ids:
            return 0
        
        return await self._add_to_queue(
            property_ids=property_ids,
            priority=self.config.priority_relisted,
            queue_reason='relisted',
            session_id=session_id
        )
    
    async def queue_verification(
        self,
        property_ids: List[str],
        session_id: str
    ) -> int:
        """
        Queue properties for verification (general purpose).
        
        Args:
            property_ids: List of property IDs to queue
            session_id: Session ID for tracking
            
        Returns:
            Number of properties queued
        """
        if not property_ids:
            return 0
        
        return await self._add_to_queue(
            property_ids=property_ids,
            priority=self.config.priority_verification,
            queue_reason='verification',
            session_id=session_id
        )
    
    async def _add_to_queue(
        self,
        property_ids: List[str],
        priority: int,
        queue_reason: str,
        session_id: str,
        metadata_map: Optional[Dict[str, Dict]] = None
    ) -> int:
        """
        Internal method to add properties to the queue.
        
        Args:
            property_ids: List of property IDs
            priority: Priority level (1-5)
            queue_reason: Reason for queuing
            session_id: Session ID
            metadata_map: Optional metadata for each property
            
        Returns:
            Number of properties successfully queued
        """
        if not property_ids:
            return 0
        
        # Check queue size limit
        current_stats = await self.get_queue_stats()
        if current_stats.pending_count >= self.config.queue_max_pending:
            logger.warning(f"Queue at capacity ({current_stats.pending_count}), skipping new items")
            return 0
        
        try:
            # Get source URLs for properties in batches to avoid URL length limits
            source_urls = {}
            batch_size = 200
            
            # Query manifest in batches
            for i in range(0, len(property_ids), batch_size):
                batch_ids = property_ids[i:i + batch_size]
                try:
                    manifest_response = self.supabase.table('property_manifest').select(
                        'property_id, source_url'
                    ).in_('property_id', batch_ids).execute()
                    
                    for item in manifest_response.data:
                        source_urls[item['property_id']] = item.get('source_url', '')
                except Exception as e:
                    logger.warning(f"Error fetching manifest URLs batch {i}: {e}")
            
            # If not in manifest, try live table
            missing_ids = list(set(property_ids) - set(source_urls.keys()))
            if missing_ids:
                for i in range(0, len(missing_ids), batch_size):
                    batch_ids = missing_ids[i:i + batch_size]
                    try:
                        live_response = self.supabase.table('properties_live').select(
                            'property_id, source_url'
                        ).in_('property_id', batch_ids).execute()
                        
                        for item in live_response.data:
                            source_urls[item['property_id']] = item.get('source_url', '')
                    except Exception as e:
                        logger.warning(f"Error fetching live URLs batch {i}: {e}")
            
            # Build queue records
            now = datetime.utcnow().isoformat()
            records = []
            
            for prop_id in property_ids:
                source_url = source_urls.get(prop_id, '')
                if not source_url:
                    logger.debug(f"Skipping {prop_id}: no source URL")
                    continue
                
                metadata = metadata_map.get(prop_id, {}) if metadata_map else {}
                
                records.append({
                    'property_id': prop_id,
                    'source_url': source_url,
                    'priority': priority,
                    'queue_reason': queue_reason,
                    'session_id': session_id,
                    'status': 'pending',
                    'metadata': metadata,
                    'queued_at': now,
                    'created_at': now,
                    'updated_at': now
                })
            
            if not records:
                return 0
            
            # Insert with upsert to handle existing pending entries
            # The unique constraint on (property_id) WHERE status='pending' will handle this
            queued_count = 0
            batch_size = self.config.get_tier(1).batch_size
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    # Try to insert each record, skipping duplicates
                    for record in batch:
                        try:
                            self.supabase.table('scrape_queue').insert(record).execute()
                            queued_count += 1
                        except Exception as e:
                            if 'duplicate key' in str(e).lower() or '23505' in str(e):
                                logger.debug(f"Property {record['property_id']} already in queue")
                            else:
                                logger.warning(f"Error queuing {record['property_id']}: {e}")
                except Exception as e:
                    logger.error(f"Error in batch insert: {e}")
            
            logger.info(f"Queued {queued_count} properties with priority {priority} ({queue_reason})")
            return queued_count
            
        except Exception as e:
            logger.error(f"Error adding to queue: {e}")
            return 0
    
    async def claim_next_batch(
        self,
        batch_size: int,
        worker_id: str = "default"
    ) -> List[QueuedProperty]:
        """
        Claim the next batch of properties for processing.
        
        Args:
            batch_size: Number of properties to claim
            worker_id: Identifier for the worker claiming
            
        Returns:
            List of QueuedProperty objects
        """
        try:
            # Use Python-based approach for claiming (more reliable than RPC)
            now = datetime.utcnow().isoformat()
            
            # Step 1: Get pending items ordered by priority
            pending_response = self.supabase.table('scrape_queue').select(
                'id, property_id, source_url, priority, queue_reason'
            ).eq('status', 'pending').order(
                'priority'
            ).order('queued_at').limit(batch_size).execute()
            
            if not pending_response.data:
                return []
            
            # Step 2: Update them to in_progress
            item_ids = [item['id'] for item in pending_response.data]
            
            self.supabase.table('scrape_queue').update({
                'status': 'in_progress',
                'claimed_at': now,
                'claimed_by': worker_id,
                'updated_at': now
            }).in_('id', item_ids).execute()
            
            # Step 3: Build return list
            claimed = []
            for item in pending_response.data:
                claimed.append(QueuedProperty(
                    id=item['id'],
                    property_id=item['property_id'],
                    source_url=item['source_url'],
                    priority=item['priority'],
                    queue_reason=item['queue_reason']
                ))
            
            logger.info(f"Claimed {len(claimed)} items for worker {worker_id}")
            return claimed
            
        except Exception as e:
            logger.error(f"Error claiming queue items: {e}")
            return []
    
    async def mark_completed(
        self,
        queue_id: str,
        success: bool,
        error: Optional[str] = None
    ):
        """
        Mark a queue item as completed or failed.
        
        Args:
            queue_id: Queue entry UUID
            success: Whether scraping succeeded
            error: Error message if failed
        """
        try:
            now = datetime.utcnow().isoformat()
            
            update_data = {
                'completed_at': now,
                'status': 'completed' if success else 'failed',
                'updated_at': now
            }
            
            if error:
                update_data['last_error'] = error[:1000]  # Truncate long errors
            
            self.supabase.table('scrape_queue').update(
                update_data
            ).eq('id', queue_id).execute()
            
        except Exception as e:
            logger.error(f"Error marking queue item {queue_id}: {e}")
    
    async def mark_batch_completed(
        self,
        results: List[tuple]  # List of (queue_id, success, error)
    ):
        """
        Mark multiple queue items as completed or failed.
        
        Args:
            results: List of (queue_id, success, error_message) tuples
        """
        for queue_id, success, error in results:
            await self.mark_completed(queue_id, success, error)
    
    async def release_stale_claims(self, minutes: int = 30) -> int:
        """
        Release queue items that have been claimed but not completed.
        
        Args:
            minutes: Time threshold in minutes
            
        Returns:
            Number of items released
        """
        try:
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
            
            response = self.supabase.table('scrape_queue').update({
                'status': 'pending',
                'claimed_at': None,
                'claimed_by': None,
                'updated_at': datetime.utcnow().isoformat()
            }).eq('status', 'in_progress').lt(
                'claimed_at', cutoff.isoformat()
            ).execute()
            
            released_count = len(response.data) if response.data else 0
            
            if released_count > 0:
                logger.info(f"Released {released_count} stale queue claims")
            
            return released_count
            
        except Exception as e:
            logger.error(f"Error releasing stale claims: {e}")
            return 0
    
    async def get_queue_stats(self) -> QueueStats:
        """
        Get current queue statistics.
        
        Returns:
            QueueStats object with counts and distributions
        """
        try:
            response = self.supabase.rpc('get_queue_stats').execute()
            
            if response.data and len(response.data) > 0:
                data = response.data[0]
                return QueueStats(
                    pending_count=data.get('total_pending', 0),
                    in_progress_count=data.get('total_in_progress', 0),
                    completed_today=data.get('completed_today', 0),
                    failed_today=data.get('failed_today', 0),
                    by_priority=data.get('by_priority', {}),
                    by_reason=data.get('by_reason', {})
                )
            
            return QueueStats()
            
        except Exception as e:
            logger.error(f"Error getting queue stats: {e}")
            return QueueStats()
    
    async def cleanup_old_queue_entries(self, days: int = 7) -> int:
        """
        Delete old completed/cancelled queue entries.
        
        Args:
            days: Age threshold in days
            
        Returns:
            Number of entries deleted
        """
        try:
            response = self.supabase.rpc(
                'cleanup_old_queue_entries',
                {'p_days': days}
            ).execute()
            
            deleted_count = response.data if isinstance(response.data, int) else 0
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old queue entries")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up queue: {e}")
            return 0
    
    async def get_pending_by_reason(self, reason: str) -> int:
        """
        Get count of pending items for a specific reason.
        
        Args:
            reason: Queue reason to count
            
        Returns:
            Count of pending items
        """
        try:
            response = self.supabase.table('scrape_queue').select(
                'id', count='exact'
            ).eq('status', 'pending').eq('queue_reason', reason).execute()
            
            return response.count or 0
            
        except Exception as e:
            logger.error(f"Error counting pending by reason: {e}")
            return 0
    
    async def cancel_pending_by_reason(self, reason: str) -> int:
        """
        Cancel all pending items for a specific reason.
        
        Args:
            reason: Queue reason to cancel
            
        Returns:
            Number of items cancelled
        """
        try:
            response = self.supabase.table('scrape_queue').update({
                'status': 'cancelled',
                'completed_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }).eq('status', 'pending').eq('queue_reason', reason).execute()
            
            cancelled_count = len(response.data) if response.data else 0
            
            if cancelled_count > 0:
                logger.info(f"Cancelled {cancelled_count} pending items with reason '{reason}'")
            
            return cancelled_count
            
        except Exception as e:
            logger.error(f"Error cancelling pending items: {e}")
            return 0
    
    async def get_failed_items(self, limit: int = 100) -> List[Dict]:
        """
        Get recently failed queue items for retry analysis.
        
        Args:
            limit: Maximum items to return
            
        Returns:
            List of failed queue items
        """
        try:
            response = self.supabase.table('scrape_queue').select(
                'id, property_id, source_url, queue_reason, attempt_count, last_error, completed_at'
            ).eq('status', 'failed').order(
                'completed_at', desc=True
            ).limit(limit).execute()
            
            return response.data or []
            
        except Exception as e:
            logger.error(f"Error getting failed items: {e}")
            return []
    
    async def retry_failed_items(
        self,
        max_attempts: int = 3,
        limit: int = 100
    ) -> int:
        """
        Retry failed items that haven't exceeded max attempts.
        
        Args:
            max_attempts: Maximum retry attempts allowed
            limit: Maximum items to retry
            
        Returns:
            Number of items reset for retry
        """
        try:
            response = self.supabase.table('scrape_queue').update({
                'status': 'pending',
                'claimed_at': None,
                'claimed_by': None,
                'completed_at': None,
                'last_error': None,
                'updated_at': datetime.utcnow().isoformat()
            }).eq('status', 'failed').lt(
                'attempt_count', max_attempts
            ).limit(limit).execute()
            
            retry_count = len(response.data) if response.data else 0
            
            if retry_count > 0:
                logger.info(f"Reset {retry_count} failed items for retry")
            
            return retry_count
            
        except Exception as e:
            logger.error(f"Error retrying failed items: {e}")
            return 0

