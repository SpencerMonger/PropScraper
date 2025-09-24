"""
Data Synchronization Service for Property Data Management

This service handles the promotion of validated data from staging to live tables,
including data quality checks, validation, and performance monitoring.

Based on property_data_management_architecture.md
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from supabase import Client
import json
import time

from .change_detection_service import ChangeDetectionService, ChangeDetectionResult
from .data_quality_service import DataQualityService

logger = logging.getLogger(__name__)


@dataclass
class SyncMetrics:
    """Synchronization metrics and performance data"""
    total_scraped: int = 0
    new_properties: int = 0
    updated_properties: int = 0
    removed_properties: int = 0
    unchanged_properties: int = 0
    data_quality_score: float = 0.0
    completeness_rate: float = 0.0
    sync_duration_ms: int = 0
    staging_to_live_duration_ms: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


@dataclass
class SyncResult:
    """Result of synchronization operation"""
    success: bool
    metrics: SyncMetrics
    error_message: Optional[str] = None
    sync_metadata_id: Optional[str] = None


class DataSyncService:
    """
    Service for synchronizing data from staging to live tables
    """
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.change_detector = ChangeDetectionService(supabase_client)
        self.quality_service = DataQualityService(supabase_client)
        
    async def sync_session_data(self, session_id: str, validate_data: bool = True) -> SyncResult:
        """
        Main method to synchronize data from staging to live for a session
        
        Args:
            session_id: The scraping session ID to sync
            validate_data: Whether to run data validation before sync
            
        Returns:
            SyncResult with metrics and status
        """
        sync_start_time = time.time()
        metrics = SyncMetrics()
        
        try:
            logger.info(f"Starting data synchronization for session {session_id}")
            
            # Create sync metadata record
            sync_metadata_id = await self._create_sync_metadata(session_id)
            await self._update_sync_status(sync_metadata_id, 'running')
            
            # Step 1: Run change detection
            logger.info("Running change detection...")
            change_result = await self.change_detector.detect_changes(session_id)
            
            # Update metrics from change detection
            metrics.total_scraped = change_result.total_processed
            metrics.new_properties = len(change_result.new_properties)
            metrics.updated_properties = len(change_result.updated_properties)
            metrics.removed_properties = len(change_result.removed_properties)
            metrics.unchanged_properties = len(change_result.unchanged_properties)
            
            # Step 2: Data validation (if enabled)
            if validate_data:
                logger.info("Running data validation...")
                validation_result = await self.quality_service.validate_staging_data(session_id)
                metrics.data_quality_score = validation_result.overall_score
                metrics.completeness_rate = validation_result.completeness_rate
                
                # Check if data quality is acceptable
                if validation_result.overall_score < 0.7:  # 70% threshold
                    error_msg = f"Data quality score too low: {validation_result.overall_score:.2f}"
                    logger.warning(error_msg)
                    metrics.errors.append(error_msg)
                    
                    await self._update_sync_status(sync_metadata_id, 'failed', error_msg)
                    return SyncResult(False, metrics, error_msg, sync_metadata_id)
            
            # Step 3: Promote changes to live table
            staging_start_time = time.time()
            
            await self._promote_new_properties(change_result.new_properties, session_id)
            await self._promote_updated_properties(change_result.updated_properties, session_id)
            await self._handle_removed_properties(change_result.removed_properties)
            await self._update_unchanged_properties(change_result.unchanged_properties)
            
            staging_duration = int((time.time() - staging_start_time) * 1000)
            metrics.staging_to_live_duration_ms = staging_duration
            
            # Step 4: Save change records
            await self.change_detector.save_change_records(change_result.changes, session_id)
            
            # Step 5: Update search vectors and cleanup
            await self._refresh_search_vectors()
            await self._cleanup_staging_data(session_id)
            
            # Calculate final metrics
            sync_duration = int((time.time() - sync_start_time) * 1000)
            metrics.sync_duration_ms = sync_duration
            
            # Update sync metadata with final results
            await self._finalize_sync_metadata(sync_metadata_id, metrics, 'completed')
            
            logger.info(f"Data synchronization completed successfully for session {session_id}")
            logger.info(f"Metrics: {metrics.new_properties} new, {metrics.updated_properties} updated, "
                       f"{metrics.removed_properties} removed, {metrics.unchanged_properties} unchanged")
            
            return SyncResult(True, metrics, None, sync_metadata_id)
            
        except Exception as e:
            error_msg = f"Error during data synchronization: {str(e)}"
            logger.error(error_msg)
            metrics.errors.append(error_msg)
            
            # Update sync metadata with error
            if 'sync_metadata_id' in locals():
                await self._update_sync_status(sync_metadata_id, 'failed', error_msg)
            
            return SyncResult(False, metrics, error_msg, locals().get('sync_metadata_id'))

    async def _create_sync_metadata(self, session_id: str) -> str:
        """Create initial sync metadata record"""
        try:
            response = self.supabase.table('sync_metadata').insert({
                'session_id': session_id,
                'sync_status': 'pending'
            }).execute()
            
            return response.data[0]['id']
            
        except Exception as e:
            logger.error(f"Error creating sync metadata: {str(e)}")
            raise

    async def _update_sync_status(self, sync_metadata_id: str, status: str, error_summary: Optional[str] = None):
        """Update sync metadata status"""
        try:
            update_data = {'sync_status': status}
            if error_summary:
                update_data['error_summary'] = error_summary
            if status in ['completed', 'failed']:
                update_data['completed_at'] = datetime.utcnow().isoformat()
                
            self.supabase.table('sync_metadata').update(update_data).eq('id', sync_metadata_id).execute()
            
        except Exception as e:
            logger.error(f"Error updating sync status: {str(e)}")

    async def _finalize_sync_metadata(self, sync_metadata_id: str, metrics: SyncMetrics, status: str):
        """Update sync metadata with final metrics"""
        try:
            update_data = {
                'sync_status': status,
                'total_scraped': metrics.total_scraped,
                'new_properties': metrics.new_properties,
                'updated_properties': metrics.updated_properties,
                'removed_properties': metrics.removed_properties,
                'unchanged_properties': metrics.unchanged_properties,
                'data_quality_score': metrics.data_quality_score,
                'completeness_rate': metrics.completeness_rate,
                'sync_duration_ms': metrics.sync_duration_ms,
                'staging_to_live_duration_ms': metrics.staging_to_live_duration_ms,
                'completed_at': datetime.utcnow().isoformat()
            }
            
            if metrics.errors:
                update_data['error_summary'] = '; '.join(metrics.errors)
                
            self.supabase.table('sync_metadata').update(update_data).eq('id', sync_metadata_id).execute()
            
        except Exception as e:
            logger.error(f"Error finalizing sync metadata: {str(e)}")

    async def _promote_new_properties(self, property_ids: List[str], session_id: str):
        """
        Promote new properties from staging to live table
        
        Args:
            property_ids: List of new property IDs
            session_id: Session ID
        """
        if not property_ids:
            return
            
        try:
            logger.info(f"Promoting {len(property_ids)} new properties to live table")
            
            # Get staging data for new properties
            staging_response = self.supabase.table('property_scrapes_staging').select('*').eq('session_id', session_id).in_('property_id', property_ids).execute()
            
            # Prepare data for live table
            live_records = []
            for staging_item in staging_response.data:
                live_record = await self._prepare_live_record(staging_item, is_new=True)
                live_records.append(live_record)
            
            # Insert in batches
            batch_size = 50
            for i in range(0, len(live_records), batch_size):
                batch = live_records[i:i + batch_size]
                self.supabase.table('properties_live').insert(batch).execute()
                
            logger.info(f"Successfully promoted {len(live_records)} new properties")
            
        except Exception as e:
            logger.error(f"Error promoting new properties: {str(e)}")
            raise

    async def _promote_updated_properties(self, property_ids: List[str], session_id: str):
        """
        Update existing properties in live table with staging data
        
        Args:
            property_ids: List of updated property IDs
            session_id: Session ID
        """
        if not property_ids:
            return
            
        try:
            logger.info(f"Updating {len(property_ids)} properties in live table")
            
            # Get staging data for updated properties
            staging_response = self.supabase.table('property_scrapes_staging').select('*').eq('session_id', session_id).in_('property_id', property_ids).execute()
            staging_data = {item['property_id']: item for item in staging_response.data}
            
            # Update each property individually (for better error handling)
            for property_id in property_ids:
                if property_id in staging_data:
                    staging_item = staging_data[property_id]
                    live_record = await self._prepare_live_record(staging_item, is_new=False)
                    
                    # Update the live record
                    self.supabase.table('properties_live').update(live_record).eq('property_id', property_id).execute()
                    
            logger.info(f"Successfully updated {len(property_ids)} properties")
            
        except Exception as e:
            logger.error(f"Error updating properties: {str(e)}")
            raise

    async def _handle_removed_properties(self, property_ids: List[str]):
        """
        Handle properties that appear to be removed (mark as inactive)
        
        Args:
            property_ids: List of removed property IDs
        """
        if not property_ids:
            return
            
        try:
            logger.info(f"Marking {len(property_ids)} properties as inactive")
            
            # Mark properties as inactive rather than deleting them
            update_data = {
                'status': 'inactive',
                'last_updated_at': datetime.utcnow().isoformat()
            }
            
            self.supabase.table('properties_live').update(update_data).in_('property_id', property_ids).execute()
            
            logger.info(f"Successfully marked {len(property_ids)} properties as inactive")
            
        except Exception as e:
            logger.error(f"Error handling removed properties: {str(e)}")
            raise

    async def _update_unchanged_properties(self, property_ids: List[str]):
        """
        Update last_seen_at timestamp for unchanged properties
        
        Args:
            property_ids: List of unchanged property IDs
        """
        if not property_ids:
            return
            
        try:
            update_data = {
                'last_seen_at': datetime.utcnow().isoformat()
            }
            
            # Update in batches to avoid large queries
            batch_size = 100
            for i in range(0, len(property_ids), batch_size):
                batch = property_ids[i:i + batch_size]
                self.supabase.table('properties_live').update(update_data).in_('property_id', batch).execute()
                
            logger.info(f"Updated last_seen_at for {len(property_ids)} unchanged properties")
            
        except Exception as e:
            logger.error(f"Error updating unchanged properties: {str(e)}")

    async def _prepare_live_record(self, staging_item: Dict, is_new: bool = False) -> Dict:
        """
        Prepare a staging record for insertion/update in live table
        
        Args:
            staging_item: Staging table record
            is_new: Whether this is a new property
            
        Returns:
            Dictionary ready for live table
        """
        # Copy all relevant fields from staging
        live_record = {}
        
        # Basic property data
        fields_to_copy = [
            'property_id', 'title', 'description', 'property_type', 'operation_type',
            'address', 'neighborhood', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'gps_coordinates',
            'price', 'currency', 'price_per_m2', 'bedrooms', 'bathrooms', 'half_bathrooms', 'parking_spaces',
            'total_area_m2', 'covered_area_m2', 'lot_size_m2', 'floor_number', 'total_floors', 'age_years', 'construction_year',
            'features', 'amenities', 'main_image_url', 'image_urls', 'virtual_tour_url', 'video_url',
            'agent_name', 'agent_phone', 'agent_email', 'agency_name', 'message_url',
            'is_featured', 'is_premium', 'source_url', 'page_number', 'listing_date'
        ]
        
        for field in fields_to_copy:
            if field in staging_item:
                live_record[field] = staging_item[field]
        
        # Set timestamps
        now = datetime.utcnow().isoformat()
        live_record['scraped_at'] = staging_item.get('scraped_at', now)
        live_record['last_seen_at'] = now
        live_record['last_updated_at'] = now
        
        if is_new:
            live_record['first_seen_at'] = now
            live_record['status'] = 'active'
        
        # Calculate data completeness score
        live_record['data_completeness_score'] = await self._calculate_completeness_score(staging_item)
        
        return live_record

    async def _calculate_completeness_score(self, property_data: Dict) -> float:
        """
        Calculate data completeness score for a property
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            Completeness score between 0.0 and 1.0
        """
        # Define weights for different fields
        field_weights = {
            'title': 0.15,
            'description': 0.10,
            'price': 0.20,
            'property_type': 0.10,
            'operation_type': 0.05,
            'bedrooms': 0.05,
            'bathrooms': 0.05,
            'address': 0.10,
            'city': 0.05,
            'latitude': 0.05,
            'longitude': 0.05,
            'main_image_url': 0.05,
            'agent_name': 0.03,
            'agent_phone': 0.02
        }
        
        score = 0.0
        for field, weight in field_weights.items():
            value = property_data.get(field)
            if value is not None and str(value).strip():
                score += weight
                
        return min(1.0, score)

    async def _refresh_search_vectors(self):
        """Refresh search vectors and materialized views"""
        try:
            # Refresh materialized view for property stats
            self.supabase.rpc('refresh_property_stats').execute()
            logger.info("Refreshed materialized views")
            
        except Exception as e:
            logger.warning(f"Error refreshing search vectors: {str(e)}")

    async def _cleanup_staging_data(self, session_id: str, keep_days: int = 7):
        """
        Clean up old staging data
        
        Args:
            session_id: Current session ID to mark as processed
            keep_days: Number of days to keep staging data
        """
        try:
            # Mark current session staging data as processed
            self.supabase.table('property_scrapes_staging').update({'processing_status': 'processed'}).eq('session_id', session_id).execute()
            
            # Delete old staging data (older than keep_days)
            cutoff_date = datetime.utcnow() - timedelta(days=keep_days)
            
            # Find old sessions to clean up
            old_sessions_response = self.supabase.table('scraping_sessions').select('id').lt('created_at', cutoff_date.isoformat()).execute()
            old_session_ids = [session['id'] for session in old_sessions_response.data]
            
            if old_session_ids:
                # Delete old staging data
                self.supabase.table('property_scrapes_staging').delete().in_('session_id', old_session_ids).execute()
                logger.info(f"Cleaned up staging data for {len(old_session_ids)} old sessions")
                
        except Exception as e:
            logger.warning(f"Error during staging cleanup: {str(e)}")

    async def get_sync_metrics(self, session_id: str) -> Optional[Dict]:
        """
        Get synchronization metrics for a session
        
        Args:
            session_id: Session ID
            
        Returns:
            Dictionary with sync metrics or None if not found
        """
        try:
            response = self.supabase.table('sync_metadata').select('*').eq('session_id', session_id).execute()
            
            if response.data:
                return response.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Error fetching sync metrics: {str(e)}")
            return None

    async def get_recent_sync_summary(self, days: int = 7) -> Dict:
        """
        Get summary of recent synchronization operations
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            response = self.supabase.table('sync_metadata').select('*').gte('created_at', cutoff_date.isoformat()).execute()
            
            if not response.data:
                return {'total_syncs': 0}
            
            metrics = response.data
            
            summary = {
                'total_syncs': len(metrics),
                'successful_syncs': len([m for m in metrics if m['sync_status'] == 'completed']),
                'failed_syncs': len([m for m in metrics if m['sync_status'] == 'failed']),
                'total_properties_processed': sum(m.get('total_scraped', 0) for m in metrics),
                'total_new_properties': sum(m.get('new_properties', 0) for m in metrics),
                'total_updated_properties': sum(m.get('updated_properties', 0) for m in metrics),
                'total_removed_properties': sum(m.get('removed_properties', 0) for m in metrics),
                'average_quality_score': sum(m.get('data_quality_score', 0) for m in metrics if m.get('data_quality_score')) / len([m for m in metrics if m.get('data_quality_score')]) if metrics else 0,
                'average_sync_duration_ms': sum(m.get('sync_duration_ms', 0) for m in metrics if m.get('sync_duration_ms')) / len([m for m in metrics if m.get('sync_duration_ms')]) if metrics else 0
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting sync summary: {str(e)}")
            return {'error': str(e)} 