"""
Property Data Management Services

This package implements the dual-table architecture with change detection
and data quality monitoring for property data management.

Includes the Hybrid 4-Tier Sync System for efficient property synchronization.

Based on property_data_management_architecture.md and HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

# Core services (existing)
from .change_detection_service import ChangeDetectionService, PropertyChange, ChangeDetectionResult
from .data_sync_service import DataSyncService, SyncMetrics, SyncResult
from .data_quality_service import DataQualityService, ValidationError, ValidationResult
from .property_sync_orchestrator import PropertySyncOrchestrator, WorkflowResult

# Hybrid 4-Tier Sync services (new)
from .manifest_scan_service import ManifestScanService, ManifestEntry, ManifestScanResult
from .property_diff_service import PropertyDiffService, PropertyPriceChange, PropertyRemovalCandidate, PropertyRemovalResult
from .scrape_queue_service import ScrapeQueueService, QueuedProperty, QueueStats
from .tier_orchestrator import TierOrchestrator, TierResult, QueueProcessResult
from .scheduler_service import SchedulerService, TierScheduleStatus, ScheduleStatus

__all__ = [
    # Core services
    'ChangeDetectionService',
    'PropertyChange', 
    'ChangeDetectionResult',
    'DataSyncService',
    'SyncMetrics',
    'SyncResult',
    'DataQualityService',
    'ValidationError',
    'ValidationResult',
    'PropertySyncOrchestrator',
    'WorkflowResult',
    
    # Hybrid 4-Tier Sync services
    'ManifestScanService',
    'ManifestEntry',
    'ManifestScanResult',
    'PropertyDiffService',
    'PropertyPriceChange',
    'PropertyRemovalCandidate',
    'PropertyRemovalResult',
    'ScrapeQueueService',
    'QueuedProperty',
    'QueueStats',
    'TierOrchestrator',
    'TierResult',
    'QueueProcessResult',
    'SchedulerService',
    'TierScheduleStatus',
    'ScheduleStatus',
]

__version__ = '2.0.0' 