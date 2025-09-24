"""
Property Data Management Services

This package implements the dual-table architecture with change detection
and data quality monitoring for property data management.

Based on property_data_management_architecture.md
"""

from .change_detection_service import ChangeDetectionService, PropertyChange, ChangeDetectionResult
from .data_sync_service import DataSyncService, SyncMetrics, SyncResult
from .data_quality_service import DataQualityService, ValidationError, ValidationResult
from .property_sync_orchestrator import PropertySyncOrchestrator, WorkflowResult

__all__ = [
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
    'WorkflowResult'
]

__version__ = '1.0.0' 