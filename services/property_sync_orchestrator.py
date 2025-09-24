"""
Property Synchronization Orchestrator

This service coordinates the complete data synchronization workflow,
implementing the daily sync process described in the architecture.

Based on property_data_management_architecture.md
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from supabase import Client
import json

from .change_detection_service import ChangeDetectionService
from .data_sync_service import DataSyncService, SyncResult
from .data_quality_service import DataQualityService

logger = logging.getLogger(__name__)


@dataclass
class WorkflowResult:
    """Complete workflow execution result"""
    success: bool
    session_id: str
    sync_result: Optional[SyncResult] = None
    quality_report: Optional[Dict] = None
    error_message: Optional[str] = None
    execution_time_ms: int = 0


class PropertySyncOrchestrator:
    """
    Main orchestrator for property data synchronization workflows
    """
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.change_detector = ChangeDetectionService(supabase_client)
        self.sync_service = DataSyncService(supabase_client)
        self.quality_service = DataQualityService(supabase_client)
        
    async def daily_sync_workflow(self, session_id: str, 
                                config: Optional[Dict] = None) -> WorkflowResult:
        """
        Execute the complete daily synchronization workflow
        
        Args:
            session_id: The scraping session ID to process
            config: Optional configuration overrides
            
        Returns:
            WorkflowResult with complete execution details
        """
        start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting daily sync workflow for session {session_id}")
            
            # Apply configuration defaults
            config = config or {}
            validate_data = config.get('validate_data', True)
            generate_report = config.get('generate_report', True)
            cleanup_staging = config.get('cleanup_staging', True)
            
            # Step 1: Validate session exists and is ready
            session_info = await self._validate_session(session_id)
            if not session_info:
                return WorkflowResult(
                    success=False,
                    session_id=session_id,
                    error_message="Session not found or not ready for sync"
                )
            
            # Step 2: Pre-sync validation (optional)
            quality_report = None
            if validate_data:
                logger.info("Running pre-sync data validation...")
                quality_report = await self.quality_service.generate_quality_report(session_id)
                
                # Check if quality is acceptable
                overall_quality = quality_report.get('quality_checks', {}).get('overall_quality', 0)
                if overall_quality < 0.6:  # 60% minimum threshold
                    logger.warning(f"Data quality too low for sync: {overall_quality:.2f}")
                    return WorkflowResult(
                        success=False,
                        session_id=session_id,
                        quality_report=quality_report,
                        error_message=f"Data quality below threshold: {overall_quality:.2f}"
                    )
            
            # Step 3: Execute data synchronization
            logger.info("Executing data synchronization...")
            sync_result = await self.sync_service.sync_session_data(session_id, validate_data)
            
            if not sync_result.success:
                return WorkflowResult(
                    success=False,
                    session_id=session_id,
                    sync_result=sync_result,
                    quality_report=quality_report,
                    error_message=sync_result.error_message
                )
            
            # Step 4: Post-sync operations
            await self._post_sync_operations(session_id, sync_result)
            
            # Step 5: Generate final report (optional)
            if generate_report and not quality_report:
                quality_report = await self.quality_service.generate_quality_report(session_id)
            
            # Step 6: Update session status
            await self._update_session_completion(session_id, sync_result)
            
            execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            logger.info(f"Daily sync workflow completed successfully for session {session_id}")
            logger.info(f"Execution time: {execution_time}ms, "
                       f"Properties: {sync_result.metrics.new_properties} new, "
                       f"{sync_result.metrics.updated_properties} updated, "
                       f"{sync_result.metrics.removed_properties} removed")
            
            return WorkflowResult(
                success=True,
                session_id=session_id,
                sync_result=sync_result,
                quality_report=quality_report,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            error_msg = f"Error in daily sync workflow: {str(e)}"
            logger.error(error_msg)
            execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            return WorkflowResult(
                success=False,
                session_id=session_id,
                error_message=error_msg,
                execution_time_ms=execution_time
            )

    async def _validate_session(self, session_id: str) -> Optional[Dict]:
        """
        Validate that session exists and is ready for sync
        
        Args:
            session_id: Session ID to validate
            
        Returns:
            Session info if valid, None otherwise
        """
        try:
            # Check if session exists
            session_response = self.supabase.table('scraping_sessions').select('*').eq('id', session_id).execute()
            
            if not session_response.data:
                logger.error(f"Session {session_id} not found")
                return None
            
            session_info = session_response.data[0]
            
            # Check if session is completed
            if session_info['status'] != 'completed':
                logger.error(f"Session {session_id} is not completed (status: {session_info['status']})")
                return None
            
            # Check if staging data exists
            staging_response = self.supabase.table('property_scrapes_staging').select('id').eq('session_id', session_id).limit(1).execute()
            
            if not staging_response.data:
                logger.error(f"No staging data found for session {session_id}")
                return None
            
            return session_info
            
        except Exception as e:
            logger.error(f"Error validating session {session_id}: {str(e)}")
            return None

    async def _post_sync_operations(self, session_id: str, sync_result: SyncResult):
        """
        Execute post-synchronization operations
        
        Args:
            session_id: Session ID
            sync_result: Synchronization result
        """
        try:
            # Refresh materialized views
            await self._refresh_materialized_views()
            
            # Invalidate caches (if using Redis or similar)
            await self._invalidate_caches(['property_search', 'property_stats'])
            
            # Send notifications (if configured)
            await self._send_sync_notifications(session_id, sync_result)
            
            logger.info("Post-sync operations completed")
            
        except Exception as e:
            logger.warning(f"Error in post-sync operations: {str(e)}")

    async def _refresh_materialized_views(self):
        """Refresh materialized views for analytics"""
        try:
            self.supabase.rpc('refresh_property_stats').execute()
            logger.info("Materialized views refreshed")
        except Exception as e:
            logger.warning(f"Error refreshing materialized views: {str(e)}")

    async def _invalidate_caches(self, cache_keys: List[str]):
        """
        Invalidate application caches
        
        Args:
            cache_keys: List of cache keys to invalidate
        """
        try:
            # This would integrate with your caching system (Redis, etc.)
            # For now, just log the operation
            logger.info(f"Cache invalidation requested for keys: {cache_keys}")
        except Exception as e:
            logger.warning(f"Error invalidating caches: {str(e)}")

    async def _send_sync_notifications(self, session_id: str, sync_result: SyncResult):
        """
        Send notifications about sync completion
        
        Args:
            session_id: Session ID
            sync_result: Synchronization result
        """
        try:
            # This would integrate with notification systems (email, Slack, etc.)
            notification_data = {
                'session_id': session_id,
                'success': sync_result.success,
                'new_properties': sync_result.metrics.new_properties,
                'updated_properties': sync_result.metrics.updated_properties,
                'removed_properties': sync_result.metrics.removed_properties,
                'quality_score': sync_result.metrics.data_quality_score,
                'sync_duration_ms': sync_result.metrics.sync_duration_ms
            }
            
            logger.info(f"Sync notification data: {notification_data}")
            
        except Exception as e:
            logger.warning(f"Error sending sync notifications: {str(e)}")

    async def _update_session_completion(self, session_id: str, sync_result: SyncResult):
        """
        Update session with sync completion information
        
        Args:
            session_id: Session ID
            sync_result: Synchronization result
        """
        try:
            update_data = {
                'properties_inserted': sync_result.metrics.new_properties,
                'properties_updated': sync_result.metrics.updated_properties,
                'completed_at': datetime.utcnow().isoformat()
            }
            
            self.supabase.table('scraping_sessions').update(update_data).eq('id', session_id).execute()
            
        except Exception as e:
            logger.warning(f"Error updating session completion: {str(e)}")

    async def batch_sync_workflow(self, session_ids: List[str], 
                                config: Optional[Dict] = None) -> List[WorkflowResult]:
        """
        Execute sync workflow for multiple sessions
        
        Args:
            session_ids: List of session IDs to process
            config: Optional configuration
            
        Returns:
            List of WorkflowResult objects
        """
        logger.info(f"Starting batch sync workflow for {len(session_ids)} sessions")
        
        # Process sessions in parallel (with concurrency limit)
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent syncs
        
        async def sync_with_semaphore(session_id: str) -> WorkflowResult:
            async with semaphore:
                return await self.daily_sync_workflow(session_id, config)
        
        results = await asyncio.gather(
            *[sync_with_semaphore(session_id) for session_id in session_ids],
            return_exceptions=True
        )
        
        # Handle any exceptions
        workflow_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                workflow_results.append(WorkflowResult(
                    success=False,
                    session_id=session_ids[i],
                    error_message=str(result)
                ))
            else:
                workflow_results.append(result)
        
        # Log batch summary
        successful = len([r for r in workflow_results if r.success])
        failed = len(workflow_results) - successful
        
        logger.info(f"Batch sync completed: {successful} successful, {failed} failed")
        
        return workflow_results

    async def get_pending_sessions(self, max_age_hours: int = 24) -> List[str]:
        """
        Get list of sessions that are ready for sync
        
        Args:
            max_age_hours: Maximum age of sessions to consider
            
        Returns:
            List of session IDs ready for sync
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
            
            # Find completed sessions that haven't been synced yet
            sessions_response = self.supabase.table('scraping_sessions').select('id').eq('status', 'completed').gte('completed_at', cutoff_time.isoformat()).execute()
            
            session_ids = [session['id'] for session in sessions_response.data]
            
            if not session_ids:
                return []
            
            # Filter out sessions that have already been synced
            synced_response = self.supabase.table('sync_metadata').select('session_id').in_('session_id', session_ids).eq('sync_status', 'completed').execute()
            
            synced_session_ids = {sync['session_id'] for sync in synced_response.data}
            
            pending_sessions = [sid for sid in session_ids if sid not in synced_session_ids]
            
            logger.info(f"Found {len(pending_sessions)} pending sessions for sync")
            
            return pending_sessions
            
        except Exception as e:
            logger.error(f"Error getting pending sessions: {str(e)}")
            return []

    async def cleanup_old_data(self, days_to_keep: int = 30) -> Dict:
        """
        Clean up old data based on retention policy
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Dictionary with cleanup statistics
        """
        try:
            logger.info(f"Starting data cleanup (keeping {days_to_keep} days)")
            
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            stats = {'cleaned_staging': 0, 'cleaned_changes': 0, 'cleaned_sync_metadata': 0}
            
            # Clean up old staging data
            old_sessions_response = self.supabase.table('scraping_sessions').select('id').lt('created_at', cutoff_date.isoformat()).execute()
            old_session_ids = [session['id'] for session in old_sessions_response.data]
            
            if old_session_ids:
                # Clean staging data
                staging_delete_response = self.supabase.table('property_scrapes_staging').delete().in_('session_id', old_session_ids).execute()
                stats['cleaned_staging'] = len(staging_delete_response.data) if staging_delete_response.data else 0
                
                # Clean change records
                changes_delete_response = self.supabase.table('property_changes').delete().in_('session_id', old_session_ids).execute()
                stats['cleaned_changes'] = len(changes_delete_response.data) if changes_delete_response.data else 0
                
                # Clean sync metadata
                sync_delete_response = self.supabase.table('sync_metadata').delete().in_('session_id', old_session_ids).execute()
                stats['cleaned_sync_metadata'] = len(sync_delete_response.data) if sync_delete_response.data else 0
            
            # Mark inactive properties as removed if they haven't been seen in a long time
            very_old_date = datetime.utcnow() - timedelta(days=days_to_keep * 2)
            inactive_update_response = self.supabase.table('properties_live').update({'status': 'removed'}).eq('status', 'inactive').lt('last_seen_at', very_old_date.isoformat()).execute()
            
            stats['marked_removed'] = len(inactive_update_response.data) if inactive_update_response.data else 0
            
            logger.info(f"Data cleanup completed: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error during data cleanup: {str(e)}")
            return {'error': str(e)}

    async def get_sync_dashboard_data(self, days: int = 7) -> Dict:
        """
        Get data for sync monitoring dashboard
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with dashboard data
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Get sync summary
            sync_summary = await self.sync_service.get_recent_sync_summary(days)
            
            # Get recent sessions
            sessions_response = self.supabase.table('scraping_sessions').select('*').gte('created_at', cutoff_date.isoformat()).order('created_at', desc=True).execute()
            
            # Get quality trends
            quality_response = self.supabase.table('sync_metadata').select('created_at, data_quality_score, completeness_rate').gte('created_at', cutoff_date.isoformat()).order('created_at', desc=True).execute()
            
            # Get property counts
            total_properties_response = self.supabase.table('properties_live').select('id', exact_count=True).eq('status', 'active').execute()
            
            dashboard_data = {
                'sync_summary': sync_summary,
                'recent_sessions': sessions_response.data[:10],  # Last 10 sessions
                'quality_trends': quality_response.data,
                'total_active_properties': total_properties_response.count,
                'last_updated': datetime.utcnow().isoformat()
            }
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Error getting dashboard data: {str(e)}")
            return {'error': str(e)}

    async def manual_sync_trigger(self, session_id: str, force: bool = False) -> WorkflowResult:
        """
        Manually trigger sync for a specific session
        
        Args:
            session_id: Session ID to sync
            force: Whether to force sync even if already completed
            
        Returns:
            WorkflowResult
        """
        try:
            # Check if already synced (unless forced)
            if not force:
                existing_sync = await self.sync_service.get_sync_metrics(session_id)
                if existing_sync and existing_sync.get('sync_status') == 'completed':
                    return WorkflowResult(
                        success=False,
                        session_id=session_id,
                        error_message="Session already synced (use force=True to override)"
                    )
            
            # Execute sync workflow
            result = await self.daily_sync_workflow(session_id)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in manual sync trigger: {str(e)}")
            return WorkflowResult(
                success=False,
                session_id=session_id,
                error_message=str(e)
            ) 