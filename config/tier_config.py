"""
Tier Configuration for Hybrid 4-Tier Property Sync System

This module loads configuration from tier_config.yaml and provides
typed access to all tier and system settings.

Tier Overview:
- Tier 1 (Hot Listings): Every 6 hours, first 10 pages, ~2 min
- Tier 2 (Daily Sync): Daily at midnight, first 100 pages + manifest, ~4-5 hours  
- Tier 3 (Weekly Deep): Weekly on Sunday, full manifest + removal detection, ~5-6 hours
- Tier 4 (Monthly Refresh): Monthly on 1st, targeted deep scrape of oldest data, ~8-12 hours
"""

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
from enum import IntEnum

import yaml

logger = logging.getLogger(__name__)


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
    description: str = ""
    
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


def _load_yaml_config() -> Dict:
    """Load configuration from YAML file"""
    config_path = Path(__file__).parent / "tier_config.yaml"
    
    if not config_path.exists():
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return {}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
            return config or {}
    except Exception as e:
        logger.error(f"Error loading config from {config_path}: {e}")
        return {}


def _create_tier_settings(level: int, yaml_config: Dict) -> TierSettings:
    """Create TierSettings from YAML config with environment variable overrides"""
    tier_key = f"tier_{level}"
    tier_yaml = yaml_config.get('tiers', {}).get(tier_key, {})
    
    # Default values
    defaults = {
        1: {"name": "hot_listings", "display_name": "Hot Listings", "frequency_hours": 6, "pages_to_scan": 10},
        2: {"name": "daily_sync", "display_name": "Daily Sync", "frequency_hours": 24, "pages_to_scan": 100},
        3: {"name": "weekly_deep", "display_name": "Weekly Deep Scan", "frequency_hours": 168, "pages_to_scan": 0},
        4: {"name": "monthly_refresh", "display_name": "Monthly Refresh", "frequency_hours": 720, "pages_to_scan": 0},
    }
    
    default = defaults.get(level, defaults[1])
    
    # Environment variable overrides (for backwards compatibility)
    env_freq_key = f"TIER_{level}_FREQUENCY_HOURS" if level <= 2 else f"TIER_{level}_FREQUENCY_DAYS"
    env_pages_key = f"TIER_{level}_PAGES"
    
    if level <= 2:
        frequency = float(os.getenv(env_freq_key, tier_yaml.get('frequency_hours', default['frequency_hours'])))
    else:
        # For tiers 3 and 4, env var is in days
        env_days = os.getenv(env_freq_key)
        if env_days:
            frequency = float(env_days) * 24
        else:
            frequency = tier_yaml.get('frequency_hours', default['frequency_hours'])
    
    pages = int(os.getenv(env_pages_key, tier_yaml.get('pages_to_scan', default['pages_to_scan'])))
    
    return TierSettings(
        level=level,
        name=tier_yaml.get('name', default['name']),
        display_name=tier_yaml.get('display_name', default['display_name']),
        frequency_hours=frequency,
        pages_to_scan=pages,
        description=tier_yaml.get('description', ''),
        delay_between_pages=tier_yaml.get('delay_between_pages', 2.0),
        delay_between_details=tier_yaml.get('delay_between_details', 1.0),
        stale_days_threshold=tier_yaml.get('stale_days_threshold', 0),
        random_sample_percent=tier_yaml.get('random_sample_percent', 0.0),
        max_page_failures=tier_yaml.get('max_page_failures', 10),
        max_error_percent=tier_yaml.get('max_error_percent', 10.0),
        retry_attempts=tier_yaml.get('retry_attempts', 3),
        retry_delay=tier_yaml.get('retry_delay', 5.0),
        max_queue_items=tier_yaml.get('max_queue_items', 10000),
        batch_size=tier_yaml.get('batch_size', 50),
    )


@dataclass
class TierConfig:
    """Main configuration class for the tier sync system"""
    
    # Base URLs for scraping
    base_url: str = "https://www.pincali.com"
    
    # Listing sources with operation types
    listing_sources: List[Dict[str, str]] = field(default_factory=list)
    
    # Tier settings
    tiers: Dict[int, TierSettings] = field(default_factory=dict)
    
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
    log_level: str = "INFO"
    
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


def _build_config_from_yaml(yaml_config: Dict) -> TierConfig:
    """Build TierConfig from YAML configuration"""
    
    # Build tier settings
    tiers = {
        level: _create_tier_settings(level, yaml_config)
        for level in [1, 2, 3, 4]
    }
    
    # Build listing sources
    default_sources = [
        {"name": "For Sale", "url": "https://www.pincali.com/en/properties/properties-for-sale", "operation_type": "sale"},
        {"name": "For Rent", "url": "https://www.pincali.com/en/properties/properties-for-rent", "operation_type": "rent"},
        {"name": "Foreclosure", "url": "https://www.pincali.com/en/properties/properties-for-foreclosure", "operation_type": "foreclosure"},
        {"name": "New Construction", "url": "https://www.pincali.com/en/properties/under-construction", "operation_type": "new_construction"},
    ]
    listing_sources = yaml_config.get('listing_sources', default_sources)
    
    # Extract nested config sections
    rate_limiting = yaml_config.get('rate_limiting', {})
    browser = yaml_config.get('browser', {})
    manifest = yaml_config.get('manifest', {})
    removal = yaml_config.get('removal_detection', {})
    queue = yaml_config.get('queue', {})
    priorities = yaml_config.get('priorities', {})
    logging_config = yaml_config.get('logging', {})
    concurrency = yaml_config.get('concurrency', {})
    
    return TierConfig(
        base_url=yaml_config.get('base_url', "https://www.pincali.com"),
        listing_sources=listing_sources,
        tiers=tiers,
        
        # Rate limiting
        global_delay_min=rate_limiting.get('global_delay_min', 1.0),
        global_delay_max=rate_limiting.get('global_delay_max', 5.0),
        
        # Browser/HTTP
        user_agent=browser.get('user_agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
        request_timeout=browser.get('request_timeout', 30),
        
        # Manifest
        manifest_price_change_threshold_percent=manifest.get('price_change_threshold_percent', 1.0),
        manifest_price_change_threshold_absolute=manifest.get('price_change_threshold_absolute', 1000.0),
        
        # Removal detection
        min_missing_count_for_removal=removal.get('min_missing_count', 2),
        min_expected_properties_percent=removal.get('min_expected_percent', 50.0),
        
        # Queue
        queue_max_pending=queue.get('max_pending', 10000),
        queue_stale_claim_minutes=queue.get('stale_claim_minutes', 30),
        queue_cleanup_days=queue.get('cleanup_days', 7),
        
        # Priorities
        priority_new_property=priorities.get('new_property', 1),
        priority_price_change=priorities.get('price_change', 2),
        priority_relisted=priorities.get('relisted', 2),
        priority_verification=priorities.get('verification', 3),
        priority_stale_data=priorities.get('stale_data', 4),
        priority_random_sample=priorities.get('random_sample', 5),
        
        # Logging
        log_file=logging_config.get('log_file', "tier_sync.log"),
        log_level=os.getenv("TIER_LOG_LEVEL", logging_config.get('log_level', "INFO")),
        
        # Concurrency
        max_concurrent_scrapers=concurrency.get('max_concurrent_scrapers', 1),
    )


# Global configuration instance
_config: Optional[TierConfig] = None


def get_config(reload: bool = False) -> TierConfig:
    """
    Get the global configuration instance.
    
    Args:
        reload: If True, reload configuration from YAML file
        
    Returns:
        TierConfig instance
    """
    global _config
    if _config is None or reload:
        yaml_config = _load_yaml_config()
        _config = _build_config_from_yaml(yaml_config)
    return _config


def reload_config() -> TierConfig:
    """Force reload configuration from YAML file"""
    return get_config(reload=True)


def set_config(config: TierConfig) -> None:
    """Set the global configuration instance (useful for testing)"""
    global _config
    _config = config


# Convenience function to get tier settings
def get_tier_settings(level: int) -> TierSettings:
    """Get settings for a specific tier level"""
    return get_config().get_tier(level)
