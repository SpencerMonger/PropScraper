"""
Scheduler Service for Hybrid 4-Tier Property Sync System

This service handles scheduling and execution of tiers via cron-like triggers,
managing the timing and coordination of the sync system.

Based on HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from supabase import Client

from config.tier_config import get_config, TierConfig, TierLevel
from .tier_orchestrator import TierOrchestrator, TierResult

logger = logging.getLogger(__name__)


@dataclass
class TierScheduleStatus:
    """Status of a tier's schedule"""
    tier_level: int
    tier_name: str
    last_run_at: Optional[datetime] = None
    last_run_success: bool = False
    next_run_at: Optional[datetime] = None
    is_due: bool = False
    is_running: bool = False


@dataclass
class ScheduleStatus:
    """Overall schedule status"""
    tiers: List[TierScheduleStatus] = field(default_factory=list)
    current_running_tier: Optional[int] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)


class SchedulerService:
    """
    Service for scheduling and executing tier syncs.
    
    Manages:
    - Checking if tiers are due to run
    - Calculating next run times
    - Running scheduled tiers in order
    - Preventing concurrent tier executions
    """
    
    def __init__(
        self,
        supabase_client: Client,
        config: Optional[TierConfig] = None,
        orchestrator: Optional[TierOrchestrator] = None
    ):
        self.supabase = supabase_client
        self.config = config or get_config()
        self.orchestrator = orchestrator or TierOrchestrator(supabase_client, self.config)
        
        self._running_tier: Optional[int] = None
        self._lock = asyncio.Lock()
    
    def set_orchestrator(self, orchestrator: TierOrchestrator):
        """Set the tier orchestrator instance"""
        self.orchestrator = orchestrator
    
    async def should_run_tier(self, tier_level: int) -> bool:
        """
        Check if a tier should run based on its schedule.
        
        Args:
            tier_level: Tier level to check (1-4)
            
        Returns:
            True if the tier should run
        """
        tier_settings = self.config.get_tier(tier_level)
        
        try:
            # Get last successful run
            last_run = await self.orchestrator.get_last_tier_run(tier_level)
            
            if not last_run:
                # Never run before - should run
                logger.info(f"Tier {tier_level} ({tier_settings.name}) has never run, should execute")
                return True
            
            # Parse last run time
            last_run_at = datetime.fromisoformat(
                last_run['started_at'].replace('Z', '+00:00')
            )
            
            # Calculate time since last run
            now = datetime.now(last_run_at.tzinfo) if last_run_at.tzinfo else datetime.utcnow()
            hours_since_last_run = (now - last_run_at).total_seconds() / 3600
            
            # Check if enough time has passed
            if hours_since_last_run >= tier_settings.frequency_hours:
                logger.info(f"Tier {tier_level} ({tier_settings.name}) is due: "
                           f"{hours_since_last_run:.1f}h since last run "
                           f"(threshold: {tier_settings.frequency_hours}h)")
                return True
            
            logger.debug(f"Tier {tier_level} ({tier_settings.name}) not due: "
                        f"{hours_since_last_run:.1f}h since last run "
                        f"(threshold: {tier_settings.frequency_hours}h)")
            return False
            
        except Exception as e:
            logger.error(f"Error checking tier {tier_level} schedule: {e}")
            return False
    
    async def get_next_scheduled_run(self, tier_level: int) -> Optional[datetime]:
        """
        Calculate the next scheduled run time for a tier.
        
        Args:
            tier_level: Tier level (1-4)
            
        Returns:
            Next scheduled datetime or None if never run
        """
        tier_settings = self.config.get_tier(tier_level)
        
        try:
            last_run = await self.orchestrator.get_last_tier_run(tier_level)
            
            if not last_run:
                # Never run - should run now
                return datetime.utcnow()
            
            # Parse last run time
            last_run_at = datetime.fromisoformat(
                last_run['started_at'].replace('Z', '+00:00')
            )
            
            # Calculate next run time
            next_run = last_run_at + timedelta(hours=tier_settings.frequency_hours)
            
            # If we're past the next run time, it should run now
            now = datetime.now(last_run_at.tzinfo) if last_run_at.tzinfo else datetime.utcnow()
            if next_run < now:
                return now
            
            return next_run.replace(tzinfo=None)  # Return as naive datetime for consistency
            
        except Exception as e:
            logger.error(f"Error calculating next run for tier {tier_level}: {e}")
            return None
    
    async def run_scheduled_tiers(self) -> List[TierResult]:
        """
        Run all tiers that are due according to their schedules.
        
        Tiers are run in order (1 → 2 → 3 → 4) to ensure proper ordering.
        Only one tier can run at a time.
        
        Returns:
            List of TierResult objects for each tier that ran
        """
        results = []
        
        async with self._lock:
            for tier_level in [1, 2, 3, 4]:
                # Check if tier should run
                if await self.should_run_tier(tier_level):
                    logger.info(f"Tier {tier_level} is due, executing...")
                    
                    try:
                        self._running_tier = tier_level
                        result = await self.orchestrator.run_tier(tier_level)
                        results.append(result)
                        
                        if not result.success:
                            logger.warning(f"Tier {tier_level} failed, continuing with next tier")
                        
                    except Exception as e:
                        logger.error(f"Error running tier {tier_level}: {e}")
                        results.append(TierResult(
                            tier_level=tier_level,
                            tier_name=self.config.get_tier(tier_level).name,
                            success=False,
                            errors=[str(e)]
                        ))
                    finally:
                        self._running_tier = None
        
        return results
    
    async def run_single_tier(self, tier_level: int, force: bool = False) -> TierResult:
        """
        Run a specific tier regardless of schedule.
        
        Args:
            tier_level: Tier level to run (1-4)
            force: If True, run even if another tier is running
            
        Returns:
            TierResult from the execution
        """
        tier_settings = self.config.get_tier(tier_level)
        
        if not force:
            async with self._lock:
                if self._running_tier:
                    logger.warning(f"Cannot run tier {tier_level}: tier {self._running_tier} is running")
                    return TierResult(
                        tier_level=tier_level,
                        tier_name=tier_settings.name,
                        success=False,
                        errors=[f"Tier {self._running_tier} is currently running"]
                    )
                
                self._running_tier = tier_level
        
        try:
            result = await self.orchestrator.run_tier(tier_level)
            return result
        finally:
            if not force:
                self._running_tier = None
    
    async def get_schedule_status(self) -> ScheduleStatus:
        """
        Get the complete schedule status for all tiers.
        
        Returns:
            ScheduleStatus with details for all tiers
        """
        status = ScheduleStatus(
            current_running_tier=self._running_tier,
            last_updated=datetime.utcnow()
        )
        
        for tier_level in [1, 2, 3, 4]:
            tier_settings = self.config.get_tier(tier_level)
            
            try:
                last_run = await self.orchestrator.get_last_tier_run(tier_level)
                next_run = await self.get_next_scheduled_run(tier_level)
                is_due = await self.should_run_tier(tier_level)
                
                tier_status = TierScheduleStatus(
                    tier_level=tier_level,
                    tier_name=tier_settings.name,
                    last_run_at=(
                        datetime.fromisoformat(last_run['started_at'].replace('Z', '+00:00'))
                        if last_run else None
                    ),
                    last_run_success=(
                        last_run.get('status') == 'completed' if last_run else False
                    ),
                    next_run_at=next_run,
                    is_due=is_due,
                    is_running=(self._running_tier == tier_level)
                )
                
                status.tiers.append(tier_status)
                
            except Exception as e:
                logger.error(f"Error getting status for tier {tier_level}: {e}")
                status.tiers.append(TierScheduleStatus(
                    tier_level=tier_level,
                    tier_name=tier_settings.name
                ))
        
        return status
    
    async def get_tier_history(
        self,
        tier_level: Optional[int] = None,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get recent execution history for tiers.
        
        Args:
            tier_level: Specific tier to filter (None = all tiers)
            limit: Maximum records to return
            
        Returns:
            List of sync run records
        """
        try:
            query = self.supabase.table('sync_runs').select('*')
            
            if tier_level:
                query = query.eq('tier_level', tier_level)
            
            response = query.order('started_at', desc=True).limit(limit).execute()
            
            return response.data or []
            
        except Exception as e:
            logger.error(f"Error getting tier history: {e}")
            return []
    
    async def get_sync_summary(self, days: int = 7) -> Dict:
        """
        Get a summary of sync activity over the specified period.
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            
            response = self.supabase.table('sync_runs').select('*').gte(
                'started_at', cutoff.isoformat()
            ).execute()
            
            runs = response.data or []
            
            summary = {
                'period_days': days,
                'total_runs': len(runs),
                'successful_runs': len([r for r in runs if r.get('status') == 'completed']),
                'failed_runs': len([r for r in runs if r.get('status') == 'failed']),
                'by_tier': {},
                'total_new_properties': sum(r.get('new_properties_found', 0) for r in runs),
                'total_price_changes': sum(r.get('price_changes_detected', 0) for r in runs),
                'total_removals': sum(r.get('removals_confirmed', 0) for r in runs),
                'total_scraped': sum(r.get('properties_scraped', 0) for r in runs),
                'average_duration_seconds': (
                    sum(r.get('execution_time_ms', 0) for r in runs) / len(runs) / 1000
                    if runs else 0
                )
            }
            
            # Break down by tier
            for tier_level in [1, 2, 3, 4]:
                tier_runs = [r for r in runs if r.get('tier_level') == tier_level]
                tier_settings = self.config.get_tier(tier_level)
                
                summary['by_tier'][tier_settings.name] = {
                    'runs': len(tier_runs),
                    'successful': len([r for r in tier_runs if r.get('status') == 'completed']),
                    'new_properties': sum(r.get('new_properties_found', 0) for r in tier_runs),
                    'price_changes': sum(r.get('price_changes_detected', 0) for r in tier_runs),
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting sync summary: {e}")
            return {}
    
    def is_tier_running(self) -> bool:
        """Check if any tier is currently running"""
        return self._running_tier is not None
    
    def get_running_tier(self) -> Optional[int]:
        """Get the currently running tier level, if any"""
        return self._running_tier
    
    async def cancel_current_run(self) -> bool:
        """
        Attempt to cancel the current running tier.
        
        Note: This is a soft cancel - the tier may complete its current operation.
        
        Returns:
            True if a tier was running and marked for cancellation
        """
        if self._running_tier is None:
            return False
        
        logger.warning(f"Cancellation requested for tier {self._running_tier}")
        
        # Update the sync run status to cancelled
        try:
            # Find the current running sync_run
            response = self.supabase.table('sync_runs').select('id').eq(
                'tier_level', self._running_tier
            ).eq('status', 'running').order(
                'started_at', desc=True
            ).limit(1).execute()
            
            if response.data:
                self.supabase.table('sync_runs').update({
                    'status': 'cancelled',
                    'completed_at': datetime.utcnow().isoformat(),
                    'error_summary': 'Cancelled by user request'
                }).eq('id', response.data[0]['id']).execute()
            
            self._running_tier = None
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling run: {e}")
            return False
    
    async def run_continuous(
        self,
        check_interval_seconds: int = 300,
        max_iterations: Optional[int] = None
    ):
        """
        Run the scheduler continuously, checking for due tiers.
        
        Args:
            check_interval_seconds: Seconds between schedule checks
            max_iterations: Maximum number of check iterations (None = infinite)
        """
        logger.info(f"Starting continuous scheduler (check interval: {check_interval_seconds}s)")
        
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            try:
                logger.debug("Checking for due tiers...")
                
                results = await self.run_scheduled_tiers()
                
                if results:
                    for result in results:
                        logger.info(f"Completed tier {result.tier_level} ({result.tier_name}): "
                                   f"{'success' if result.success else 'failed'}")
                
                # Wait before next check
                await asyncio.sleep(check_interval_seconds)
                
            except asyncio.CancelledError:
                logger.info("Scheduler cancelled, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(check_interval_seconds)
            
            iteration += 1
        
        logger.info("Scheduler stopped")

