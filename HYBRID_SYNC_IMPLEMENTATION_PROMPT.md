# Hybrid 4-Tier Property Sync System - Implementation Prompt

## Context & Background

You are implementing a scalable property synchronization system for a web scraper that targets Pincali.com (a Mexican real estate platform with 600k+ properties). The current system can only detect property changes through full scrapes, which takes ~28 days. This implementation creates a hybrid 4-tier approach that reduces sync time to hours while maintaining data accuracy.

### Current Architecture Summary

The existing system uses:
- **Staging Table** (`property_scrapes_staging`): Raw scraped data per session
- **Live Table** (`properties_live`): Validated production data
- **Session Tracking** (`scraping_sessions`): Tracks scrape progress and status
- **Services Layer**: `PropertySyncOrchestrator`, `ChangeDetectionService`, `DataSyncService`, `DataQualityService`

Key existing files:
- `enhanced_property_scraper.py` - Main scraper with Playwright for detail pages
- `pincali_scraper.py` - Original HTTP-based scraper
- `services/change_detection_service.py` - Compares staging vs live
- `services/data_sync_service.py` - Promotes data from staging to live
- `services/property_sync_orchestrator.py` - Coordinates sync workflows

### The Problem Being Solved

1. Full scrapes take 28+ days (600k properties × ~4 seconds each)
2. Cannot detect removed/sold properties without full re-scrape
3. New listings may be days old before appearing in database
4. No way to prioritize which properties need updating

---

## Implementation Requirements

### Overview: 4-Tier Hybrid Sync Strategy

| Tier | Name | Frequency | Scope | Duration | Purpose |
|------|------|-----------|-------|----------|---------|
| 1 | Hot Listings | Every 6 hours | First 10 pages | ~2 min | Catch brand new listings immediately |
| 2 | Daily Sync | Daily (midnight) | First 100 pages + full manifest | ~4-5 hours | New listings + existence verification |
| 3 | Weekly Deep Scan | Weekly (Sunday) | Full manifest + removal detection | ~5-6 hours | Confirm removals, update stale data |
| 4 | Monthly Refresh | Monthly (1st) | Targeted deep scrape of oldest data | ~8-12 hours | Data freshness, validation |

---

## Database Schema Changes

### 1. Modify `properties_live` Table

Add the following columns to the existing `properties_live` table:

```
last_manifest_seen_at    TIMESTAMP    -- When property was last seen in a manifest scan
consecutive_missing_count INTEGER     -- How many consecutive scans the property was missing
listing_status           VARCHAR(50)  -- 'active', 'likely_removed', 'confirmed_removed', 'sold', 'relisted'
last_full_scrape_at      TIMESTAMP    -- When the detail page was last fully scraped
data_staleness_days      INTEGER      -- Computed: days since last_full_scrape_at (can be generated column)
scrape_priority          INTEGER      -- 1=highest priority, 5=lowest (for queue system)
price_at_last_manifest   DECIMAL      -- Price captured during manifest scan (for change detection)
```

### 2. Create `property_manifest` Table

New table for fast manifest tracking (lightweight records):

```
id                    UUID PRIMARY KEY
property_id           VARCHAR UNIQUE NOT NULL  -- Same format as properties_live.property_id
source_url            TEXT NOT NULL
listing_page_price    DECIMAL                  -- Price visible on listing page
listing_page_title    VARCHAR(500)             -- Title visible on listing page  
first_seen_at         TIMESTAMP DEFAULT NOW()
last_seen_at          TIMESTAMP DEFAULT NOW()
seen_in_session_id    UUID                     -- Last session that saw this property
is_new                BOOLEAN DEFAULT TRUE     -- Not yet in properties_live
needs_full_scrape     BOOLEAN DEFAULT TRUE     -- Flagged for detail page scraping
price_changed         BOOLEAN DEFAULT FALSE    -- Price differs from properties_live
```

### 3. Create `scrape_queue` Table

Priority queue for targeted scraping:

```
id                    UUID PRIMARY KEY
property_id           VARCHAR NOT NULL
source_url            TEXT NOT NULL
priority              INTEGER NOT NULL         -- 1=highest, 5=lowest
queue_reason          VARCHAR(100)             -- 'new_property', 'price_change', 'stale_data', 'verification', 'random_sample'
queued_at             TIMESTAMP DEFAULT NOW()
claimed_at            TIMESTAMP                -- When a worker picked it up
completed_at          TIMESTAMP                -- When scraping finished
status                VARCHAR(50) DEFAULT 'pending'  -- 'pending', 'in_progress', 'completed', 'failed', 'cancelled'
attempt_count         INTEGER DEFAULT 0
last_error            TEXT
session_id            UUID                     -- Which scraping session processed this
```

### 4. Create `sync_runs` Table

Track each tier execution:

```
id                    UUID PRIMARY KEY
tier_level            INTEGER NOT NULL         -- 1, 2, 3, or 4
tier_name             VARCHAR(50)              -- 'hot_listings', 'daily_sync', 'weekly_deep', 'monthly_refresh'
started_at            TIMESTAMP DEFAULT NOW()
completed_at          TIMESTAMP
status                VARCHAR(50) DEFAULT 'running'  -- 'running', 'completed', 'failed', 'cancelled'
pages_scanned         INTEGER DEFAULT 0
properties_in_manifest INTEGER DEFAULT 0
new_properties_found  INTEGER DEFAULT 0
price_changes_detected INTEGER DEFAULT 0
removals_detected     INTEGER DEFAULT 0
properties_queued     INTEGER DEFAULT 0
properties_scraped    INTEGER DEFAULT 0
error_count           INTEGER DEFAULT 0
error_summary         TEXT
execution_time_ms     INTEGER
```

### 5. Database Indexes

Create these indexes for performance:

```
-- Fast manifest lookups
CREATE INDEX idx_manifest_property_id ON property_manifest(property_id);
CREATE INDEX idx_manifest_needs_scrape ON property_manifest(needs_full_scrape) WHERE needs_full_scrape = TRUE;
CREATE INDEX idx_manifest_is_new ON property_manifest(is_new) WHERE is_new = TRUE;

-- Queue processing
CREATE INDEX idx_queue_pending ON scrape_queue(priority, queued_at) WHERE status = 'pending';
CREATE INDEX idx_queue_property ON scrape_queue(property_id);

-- Live table optimizations
CREATE INDEX idx_live_manifest_check ON properties_live(property_id, last_manifest_seen_at);
CREATE INDEX idx_live_listing_status ON properties_live(listing_status);
CREATE INDEX idx_live_stale ON properties_live(last_full_scrape_at) WHERE listing_status = 'active';
CREATE INDEX idx_live_priority ON properties_live(scrape_priority) WHERE listing_status = 'active';
```

---

## New Services to Create

### 1. `ManifestScanService` (services/manifest_scan_service.py)

**Purpose**: Fast scanning of listing pages to collect property IDs, URLs, and visible prices WITHOUT visiting detail pages.

**Key Methods**:

- `run_manifest_scan(max_pages: int, session_id: str) -> ManifestScanResult`
  - Scrapes listing pages only (no detail pages)
  - Extracts: property_id, source_url, visible_price, visible_title
  - Uses HTTP requests (not Playwright) for speed
  - Rate limiting: 1-2 seconds between pages
  - Returns count of properties found, pages scanned, duration

- `parse_listing_page_for_manifest(html: str, page_url: str) -> List[ManifestEntry]`
  - Extracts minimal data from listing page HTML
  - Uses same CSS selectors as existing scraper (`li.property__component`)
  - Returns list of ManifestEntry dataclass objects

- `upsert_manifest_entries(entries: List[ManifestEntry], session_id: str)`
  - Batch upsert to property_manifest table
  - Updates last_seen_at for existing entries
  - Marks new entries with is_new=TRUE

**Dataclasses**:
```
@dataclass
class ManifestEntry:
    property_id: str
    source_url: str
    listing_page_price: Optional[float]
    listing_page_title: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

@dataclass  
class ManifestScanResult:
    pages_scanned: int
    properties_found: int
    new_properties: int
    price_changes: int
    duration_seconds: float
    errors: List[str]
```

### 2. `PropertyDiffService` (services/property_diff_service.py)

**Purpose**: Compare manifest data against live database to detect new, changed, and removed properties.

**Key Methods**:

- `detect_new_properties(session_id: str) -> List[str]`
  - Query: property_manifest WHERE is_new = TRUE
  - Returns list of property_ids that need full scraping

- `detect_price_changes(session_id: str) -> List[PropertyPriceChange]`
  - Compare property_manifest.listing_page_price vs properties_live.price
  - Threshold: Flag if difference > 1% OR > $1000 (whichever is smaller)
  - Returns list with old_price, new_price, percent_change

- `detect_removed_properties(current_session_id: str) -> List[PropertyRemovalCandidate]`
  - Find properties in properties_live WHERE:
    - listing_status = 'active'
    - property_id NOT IN (SELECT property_id FROM property_manifest WHERE seen_in_session_id = current_session_id)
  - Increment consecutive_missing_count
  - Return candidates with missing_count >= 2

- `confirm_removals(candidates: List[str]) -> List[PropertyRemovalResult]`
  - For each candidate, attempt HTTP HEAD request to source_url
  - If 404 or redirect to search page → confirmed removed
  - If 200 → still exists (manifest scan missed it, reset counter)
  - Returns confirmation results

- `update_removal_status(property_id: str, confirmed: bool)`
  - If confirmed: Set listing_status = 'confirmed_removed' or 'sold'
  - If not confirmed: Reset consecutive_missing_count, keep active

**Dataclasses**:
```
@dataclass
class PropertyPriceChange:
    property_id: str
    old_price: float
    new_price: float
    percent_change: float
    
@dataclass
class PropertyRemovalCandidate:
    property_id: str
    source_url: str
    last_seen_at: datetime
    consecutive_missing_count: int
    
@dataclass
class PropertyRemovalResult:
    property_id: str
    confirmed_removed: bool
    http_status: Optional[int]
    redirect_url: Optional[str]
```

### 3. `ScrapeQueueService` (services/scrape_queue_service.py)

**Purpose**: Manage the priority queue of properties that need full detail page scraping.

**Key Methods**:

- `queue_new_properties(property_ids: List[str], session_id: str)`
  - Add to scrape_queue with priority=1 (highest)
  - queue_reason = 'new_property'

- `queue_price_changes(changes: List[PropertyPriceChange], session_id: str)`
  - Add to scrape_queue with priority=2
  - queue_reason = 'price_change'

- `queue_stale_properties(days_threshold: int, limit: int, session_id: str)`
  - Query properties_live WHERE last_full_scrape_at < (NOW - days_threshold)
  - Add to queue with priority=4
  - queue_reason = 'stale_data'

- `queue_random_sample(sample_size: int, session_id: str)`
  - Random sample of active properties for health check
  - Add to queue with priority=5
  - queue_reason = 'random_sample'

- `claim_next_batch(batch_size: int, worker_id: str) -> List[QueuedProperty]`
  - SELECT ... WHERE status = 'pending' ORDER BY priority, queued_at LIMIT batch_size FOR UPDATE SKIP LOCKED
  - Update claimed_at, status = 'in_progress'
  - Returns list of properties to scrape

- `mark_completed(property_id: str, success: bool, error: Optional[str])`
  - Update status = 'completed' or 'failed'
  - Set completed_at, last_error if applicable

- `get_queue_stats() -> QueueStats`
  - Returns counts by status and priority

- `cleanup_old_queue_entries(days: int)`
  - Delete completed/cancelled entries older than N days

**Dataclasses**:
```
@dataclass
class QueuedProperty:
    id: str
    property_id: str
    source_url: str
    priority: int
    queue_reason: str
    
@dataclass
class QueueStats:
    pending_count: int
    in_progress_count: int
    completed_today: int
    failed_today: int
    by_priority: Dict[int, int]
    by_reason: Dict[str, int]
```

### 4. `TierOrchestrator` (services/tier_orchestrator.py)

**Purpose**: Coordinate the execution of each tier's workflow.

**Key Methods**:

- `run_tier_1_hot_listings() -> TierResult`
  - Create sync_runs record with tier_level=1
  - Run manifest scan for pages 1-10
  - Detect new properties only
  - Queue new properties for immediate scraping
  - Process queue (scrape detail pages)
  - Update sync_runs with results

- `run_tier_2_daily_sync() -> TierResult`
  - Create sync_runs record with tier_level=2
  - Run manifest scan for pages 1-100
  - Detect new properties
  - Detect price changes
  - Update consecutive_missing_count for properties not seen
  - Queue new + price_changed properties
  - Process queue
  - Update manifest is_new flags
  - Update sync_runs

- `run_tier_3_weekly_deep() -> TierResult`
  - Create sync_runs record with tier_level=3
  - Run FULL manifest scan (all pages, ~8600)
  - Detect new properties
  - Detect price changes
  - Detect removal candidates (missing 2+ consecutive)
  - Confirm removals via HTTP checks
  - Queue new + price_changed + stale (7+ days)
  - Process queue
  - Update removal statuses
  - Clean up manifest entries for confirmed removals
  - Update sync_runs

- `run_tier_4_monthly_refresh() -> TierResult`
  - Create sync_runs record with tier_level=4
  - Identify properties with last_full_scrape_at > 30 days
  - Queue all stale properties (batch by priority)
  - Run random sample verification (10% of active)
  - Process queue with extended rate limiting
  - Generate data quality report
  - Update sync_runs

- `process_scrape_queue(max_items: int, rate_limit_seconds: float) -> QueueProcessResult`
  - Claim batch from queue
  - For each: call existing detail page scraper
  - Mark completed/failed
  - Update properties_live.last_full_scrape_at
  - Return stats

**Dataclasses**:
```
@dataclass
class TierResult:
    tier_level: int
    tier_name: str
    success: bool
    pages_scanned: int
    new_properties: int
    price_changes: int
    removals_confirmed: int
    properties_scraped: int
    duration_seconds: float
    errors: List[str]

@dataclass
class QueueProcessResult:
    processed: int
    succeeded: int
    failed: int
    duration_seconds: float
```

### 5. `SchedulerService` (services/scheduler_service.py)

**Purpose**: Handle scheduling and execution of tiers via cron-like triggers.

**Key Methods**:

- `should_run_tier(tier_level: int) -> bool`
  - Check last run time from sync_runs
  - Apply frequency rules (6h, 24h, 7d, 30d)
  - Return whether tier should execute

- `get_next_scheduled_run(tier_level: int) -> datetime`
  - Calculate next run time based on frequency

- `run_scheduled_tiers()`
  - Check each tier, run if due
  - Enforce tier ordering (don't run tier 3 if tier 2 running)
  - Log all executions

- `get_schedule_status() -> ScheduleStatus`
  - Last run time per tier
  - Next run time per tier
  - Current running tier (if any)

**Configuration** (via environment or config file):
```
TIER_1_FREQUENCY_HOURS=6
TIER_2_FREQUENCY_HOURS=24
TIER_3_FREQUENCY_DAYS=7
TIER_4_FREQUENCY_DAYS=30
TIER_1_PAGES=10
TIER_2_PAGES=100
TIER_3_PAGES=0  # 0 = all pages
TIER_4_STALE_DAYS=30
```

---

## Modification to Existing Files

### 1. `enhanced_property_scraper.py`

Add a new method that can scrape a single property by URL (for queue processing):

- `scrape_single_property(source_url: str) -> Dict`
  - Load cookies
  - Scrape detail page with Playwright
  - Extract all property data
  - Return cleaned property dict
  - Do NOT save to database (caller handles that)

Modify existing code to be callable as a library, not just CLI.

### 2. `services/property_sync_orchestrator.py`

Add integration points for the new tier system:

- Add method `run_hybrid_sync(tier_level: int)` that delegates to TierOrchestrator
- Modify `daily_sync_workflow` to optionally use new tier system
- Add fallback to legacy full-sync if tier system encounters issues

### 3. `services/change_detection_service.py`

Add methods to support manifest-based detection:

- `detect_new_from_manifest(session_id: str) -> List[str]`
- `detect_price_changes_from_manifest(session_id: str) -> List[PropertyPriceChange]`

These complement existing methods but use manifest table instead of staging.

---

## CLI Interface

Create `tier_sync_cli.py` with the following commands:

```
python tier_sync_cli.py tier1              # Run Tier 1 (hot listings)
python tier_sync_cli.py tier2              # Run Tier 2 (daily sync)
python tier_sync_cli.py tier3              # Run Tier 3 (weekly deep)
python tier_sync_cli.py tier4              # Run Tier 4 (monthly refresh)
python tier_sync_cli.py auto               # Run whichever tiers are due
python tier_sync_cli.py status             # Show schedule status
python tier_sync_cli.py queue-stats        # Show queue statistics
python tier_sync_cli.py manifest-stats     # Show manifest statistics
python tier_sync_cli.py process-queue 100  # Process 100 items from queue
```

---

## Edge Cases & Error Handling

### 1. Manifest Scan Failures
- If a page fails to load, log error and continue to next page
- If >10% of pages fail, abort scan and mark sync_run as failed
- Retry failed pages up to 3 times with exponential backoff

### 2. Property URL Changes
- Some properties may get new URLs (slug changes)
- Detection: Same lat/long + similar price + same address = likely same property
- Create linking table if needed to track URL migrations

### 3. Temporary Site Outages
- If manifest scan finds 0 properties, do NOT mark everything as removed
- Require minimum threshold (e.g., 50% of expected properties) before processing removals
- Alert if property count drops significantly between scans

### 4. Rate Limiting / Blocking
- Implement exponential backoff on 429 responses
- Rotate user agents
- If blocked, pause all tiers and alert
- Track success rate per tier and auto-pause if <90%

### 5. Duplicate Properties
- Some properties may appear on multiple pages (featured listings)
- Use property_id as unique key, upsert behavior
- Track all pages where property appears (for debugging)

### 6. Price Format Variations
- Handle: MXN, USD, EUR currencies
- Handle: "From $X" (development projects)
- Handle: "Contact for price" (null price)
- Flag unusual price changes (>50%) for manual review

### 7. Relisted Properties
- Property was confirmed_removed but now appears in manifest
- Set listing_status = 'relisted'
- Queue for full scrape
- Consider as "new" for reporting purposes

---

## Testing Requirements

### Unit Tests

1. **ManifestScanService**
   - Test HTML parsing with sample listing page HTML
   - Test price extraction with various formats
   - Test batch upsert logic

2. **PropertyDiffService**
   - Test new property detection with mock data
   - Test price change detection with edge cases (small changes, currencies)
   - Test removal detection logic with various missing counts

3. **ScrapeQueueService**
   - Test priority ordering
   - Test claim/release logic
   - Test concurrent claim handling

4. **TierOrchestrator**
   - Test each tier workflow with mocked services
   - Test error handling and rollback

### Integration Tests

1. **End-to-End Tier 1**
   - Mock HTTP responses for 10 pages
   - Verify manifest population
   - Verify queue population
   - Verify scrape execution

2. **Removal Detection Flow**
   - Seed database with test properties
   - Run manifest scan that excludes some
   - Verify consecutive_missing_count increments
   - Verify HTTP verification works
   - Verify status updates

### Load Tests

1. **Manifest Scan Performance**
   - Target: 100 pages in <5 minutes
   - Target: 1000 pages in <45 minutes

2. **Queue Processing Performance**
   - Target: 50 properties/minute with Playwright
   - Target: 200 properties/minute with HTTP-only

---

## Monitoring & Observability

### Metrics to Track

1. **Per Tier Run**
   - Duration
   - Pages scanned
   - Properties found
   - New/changed/removed counts
   - Error count
   - Success rate

2. **Queue Health**
   - Pending count over time
   - Processing rate
   - Failure rate by reason
   - Average time in queue

3. **Data Freshness**
   - Distribution of last_full_scrape_at ages
   - % of properties scraped in last 7/14/30 days
   - Stale property count trend

4. **Removal Detection**
   - Properties marked likely_removed
   - Properties confirmed removed per week
   - False positive rate (marked removed but still exists)

### Alerts

- Tier hasn't run in expected window
- Queue backlog exceeds threshold
- Error rate exceeds 10%
- Property count drops >5% in single scan
- Manifest scan finds <50% expected properties

---

## Migration Plan

### Phase 1: Database Setup
1. Run schema migrations to add new columns and tables
2. Backfill last_manifest_seen_at = last_seen_at for existing properties
3. Backfill last_full_scrape_at = scraped_at for existing properties
4. Set all listing_status = 'active' for existing properties

### Phase 2: Service Implementation
1. Implement ManifestScanService (can test independently)
2. Implement PropertyDiffService
3. Implement ScrapeQueueService
4. Implement TierOrchestrator
5. Implement SchedulerService

### Phase 3: Testing & Validation
1. Run Tier 1 in dry-run mode (log only, no database writes)
2. Validate manifest data matches expectations
3. Run Tier 2 in dry-run mode
4. Validate diff detection accuracy

### Phase 4: Gradual Rollout
1. Enable Tier 1 in production (low risk - 10 pages)
2. Monitor for 1 week
3. Enable Tier 2 in production
4. Monitor for 1 week
5. Enable Tier 3 in production
6. Enable Tier 4 after first month

---

## File Structure

```
services/
├── __init__.py                      # Export new services
├── manifest_scan_service.py         # NEW
├── property_diff_service.py         # NEW
├── scrape_queue_service.py          # NEW
├── tier_orchestrator.py             # NEW
├── scheduler_service.py             # NEW
├── change_detection_service.py      # MODIFY
├── data_sync_service.py             # (existing)
├── data_quality_service.py          # (existing)
└── property_sync_orchestrator.py    # MODIFY

tier_sync_cli.py                     # NEW - CLI interface
config/
└── tier_config.py                   # NEW - Tier configuration

tests/
├── test_manifest_scan_service.py    # NEW
├── test_property_diff_service.py    # NEW
├── test_scrape_queue_service.py     # NEW
├── test_tier_orchestrator.py        # NEW
└── test_integration_tiers.py        # NEW

dbschema/
└── hybrid_sync_migration.sql        # NEW - Schema changes
```

---

## Success Criteria

1. **Tier 1** completes in <5 minutes, runs 4x daily
2. **Tier 2** completes in <5 hours, captures 95%+ of new listings within 24 hours
3. **Tier 3** completes in <8 hours, removal detection accuracy >95%
4. **Tier 4** ensures no property data is >60 days stale
5. **Queue backlog** never exceeds 10,000 pending items
6. **System can scale** to 1M+ properties without architectural changes

---

## Notes for Implementation

- Use async/await throughout for consistency with existing codebase
- Follow existing logging patterns (file + console handlers)
- Use existing Supabase client patterns from `enhanced_property_scraper.py`
- Respect existing rate limiting conventions (3s between pages, 1s between detail requests)
- All new services should be independently testable with dependency injection
- Use dataclasses for structured data, matching existing patterns in services/

The implementation should integrate seamlessly with the existing staging/live table architecture while adding the new manifest-based differential sync capabilities.

