"""
Tier Configuration for Hybrid 4-Tier Property Sync System

This module contains all configuration settings for the tiered sync system.
Settings can be overridden via environment variables.

Tier Overview:
- Tier 1 (Hot Listings): Every 6 hours, first 10 pages, ~2 min
- Tier 2 (Daily Sync): Daily at midnight, first 100 pages + manifest, ~4-5 hours  
- Tier 3 (Weekly Deep): Weekly on Sunday, full manifest + removal detection, ~5-6 hours
- Tier 4 (Monthly Refresh): Monthly on 1st, targeted deep scrape of oldest data, ~8-12 hours
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import IntEnum


class TierLevel(IntEnum):
    """Tier levels for the sync system"""
    HOT_LISTINGS = 1
    DAILY_SYNC = 2
    WEEKLY_DEEP = 3
    MONTHLY_REFRESH = 4


@dataclass
class TierSettings:
    """Configuration for a single tier"""
    level: int
    name: str
    display_name: str
    frequency_hours: float
    pages_to_scan: int  # 0 = all pages
    description: str
    
    # Rate limiting
    delay_between_pages: float = 2.0  # seconds
    delay_between_details: float = 1.0  # seconds
    
    # Thresholds
    stale_days_threshold: int = 0  # Days before data is considered stale (0 = not applicable)
    random_sample_percent: float = 0.0  # Percentage for random sample verification
    
    # Error handling
    max_page_failures: int = 10  # Max failed pages before aborting
    max_error_percent: float = 10.0  # Max error rate before aborting
    retry_attempts: int = 3
    retry_delay: float = 5.0  # seconds
    
    # Processing limits
    max_queue_items: int = 10000  # Max items to process in one run
    batch_size: int = 50  # Batch size for database operations
    
    def __post_init__(self):
        """Validate settings after initialization"""
        assert 1 <= self.level <= 4, f"Invalid tier level: {self.level}"
        assert self.frequency_hours > 0, f"Frequency must be positive: {self.frequency_hours}"
        assert self.pages_to_scan >= 0, f"Pages must be non-negative: {self.pages_to_scan}"


# Default tier configurations
TIER_1_SETTINGS = TierSettings(
    level=1,
    name="hot_listings",
    display_name="Hot Listings",
    frequency_hours=float(os.getenv("TIER_1_FREQUENCY_HOURS", "6")),
    pages_to_scan=int(os.getenv("TIER_1_PAGES", "10")),
    description="Quick scan of newest listings every 6 hours",
    delay_between_pages=1.5,
    delay_between_details=0.5,
    max_queue_items=500,
)

TIER_2_SETTINGS = TierSettings(
    level=2,
    name="daily_sync",
    display_name="Daily Sync",
    frequency_hours=float(os.getenv("TIER_2_FREQUENCY_HOURS", "24")),
    pages_to_scan=int(os.getenv("TIER_2_PAGES", "100")),
    description="Daily sync of first 100 pages + full manifest check",
    delay_between_pages=2.0,
    delay_between_details=1.0,
    max_queue_items=5000,
)

TIER_3_SETTINGS = TierSettings(
    level=3,
    name="weekly_deep",
    display_name="Weekly Deep Scan",
    frequency_hours=float(os.getenv("TIER_3_FREQUENCY_DAYS", "7")) * 24,
    pages_to_scan=int(os.getenv("TIER_3_PAGES", "0")),  # 0 = all pages
    description="Weekly full manifest scan with removal detection",
    delay_between_pages=2.0,
    delay_between_details=1.0,
    stale_days_threshold=7,
    max_queue_items=10000,
)

TIER_4_SETTINGS = TierSettings(
    level=4,
    name="monthly_refresh",
    display_name="Monthly Refresh",
    frequency_hours=float(os.getenv("TIER_4_FREQUENCY_DAYS", "30")) * 24,
    pages_to_scan=0,  # Targeted, not page-based
    description="Monthly deep refresh of stale data + random validation",
    delay_between_pages=3.0,
    delay_between_details=1.5,
    stale_days_threshold=int(os.getenv("TIER_4_STALE_DAYS", "30")),
    random_sample_percent=10.0,
    max_queue_items=20000,
)

# Combined settings dictionary
TIER_SETTINGS: Dict[int, TierSettings] = {
    1: TIER_1_SETTINGS,
    2: TIER_2_SETTINGS,
    3: TIER_3_SETTINGS,
    4: TIER_4_SETTINGS,
}


@dataclass
class TierConfig:
    """Main configuration class for the tier sync system"""
    
    # Base URLs for scraping
    base_url: str = "https://www.pincali.com"
    
    # Listing sources with operation types
    listing_sources: List[Dict[str, str]] = field(default_factory=lambda: [
        {
            "name": "For Sale",
            "url": "https://www.pincali.com/en/properties/properties-for-sale",
            "operation_type": "sale"
        },
        {
            "name": "For Rent",
            "url": "https://www.pincali.com/en/properties/properties-for-rent",
            "operation_type": "rent"
        },
        {
            "name": "Foreclosure",
            "url": "https://www.pincali.com/en/properties/properties-for-foreclosure",
            "operation_type": "foreclosure"
        },
        {
            "name": "New Construction",
            "url": "https://www.pincali.com/en/properties/under-construction",
            "operation_type": "new_construction"
        }
    ])
    
    # Tier settings
    tiers: Dict[int, TierSettings] = field(default_factory=lambda: TIER_SETTINGS)
    
    # Global rate limiting
    global_delay_min: float = 1.0  # Minimum delay between any requests
    global_delay_max: float = 5.0  # Maximum delay for backoff
    
    # Browser/HTTP settings
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    request_timeout: int = 30  # seconds
    
    # Manifest settings
    manifest_price_change_threshold_percent: float = 1.0  # Flag if price changes > 1%
    manifest_price_change_threshold_absolute: float = 1000.0  # OR if changes > $1000
    
    # Removal detection settings
    min_missing_count_for_removal: int = 2  # Consecutive misses before flagging
    min_expected_properties_percent: float = 50.0  # Min % of expected properties before processing removals
    
    # Queue settings
    queue_max_pending: int = 10000  # Max pending items before blocking new additions
    queue_stale_claim_minutes: int = 30  # Release claimed items after this time
    queue_cleanup_days: int = 7  # Clean up completed/cancelled entries after this many days
    
    # Priority settings
    priority_new_property: int = 1
    priority_price_change: int = 2
    priority_relisted: int = 2
    priority_verification: int = 3
    priority_stale_data: int = 4
    priority_random_sample: int = 5
    
    # Logging
    log_file: str = "tier_sync.log"
    log_level: str = os.getenv("TIER_LOG_LEVEL", "INFO")
    
    # Concurrency
    max_concurrent_scrapers: int = 1  # For now, single scraper to be respectful
    
    def get_tier(self, level: int) -> TierSettings:
        """Get settings for a specific tier level"""
        if level not in self.tiers:
            raise ValueError(f"Invalid tier level: {level}. Must be 1-4.")
        return self.tiers[level]
    
    def get_tier_by_name(self, name: str) -> TierSettings:
        """Get settings for a tier by name"""
        for tier in self.tiers.values():
            if tier.name == name:
                return tier
        raise ValueError(f"Unknown tier name: {name}")
    
    @property
    def all_tier_names(self) -> List[str]:
        """Get all tier names"""
        return [tier.name for tier in self.tiers.values()]
    
    def should_flag_price_change(self, old_price: float, new_price: float) -> bool:
        """Determine if a price change should be flagged"""
        if old_price <= 0 or new_price <= 0:
            return old_price != new_price
        
        # Check absolute difference
        abs_diff = abs(new_price - old_price)
        if abs_diff > self.manifest_price_change_threshold_absolute:
            return True
        
        # Check percentage difference
        pct_diff = abs_diff / old_price * 100
        return pct_diff > self.manifest_price_change_threshold_percent
    
    def get_priority_for_reason(self, reason: str) -> int:
        """Get the priority level for a queue reason"""
        priority_map = {
            'new_property': self.priority_new_property,
            'price_change': self.priority_price_change,
            'relisted': self.priority_relisted,
            'verification': self.priority_verification,
            'stale_data': self.priority_stale_data,
            'random_sample': self.priority_random_sample,
        }
        return priority_map.get(reason, 3)  # Default to middle priority


# Global configuration instance
_config: Optional[TierConfig] = None


def get_config() -> TierConfig:
    """Get the global configuration instance"""
    global _config
    if _config is None:
        _config = TierConfig()
    return _config


def set_config(config: TierConfig) -> None:
    """Set the global configuration instance (useful for testing)"""
    global _config
    _config = config


# Convenience function to get tier settings
def get_tier_settings(level: int) -> TierSettings:
    """Get settings for a specific tier level"""
    return get_config().get_tier(level)

