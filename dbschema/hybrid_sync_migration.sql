-- ============================================================================
-- Hybrid 4-Tier Property Sync System - Database Migration
-- ============================================================================
-- This migration adds the tables and columns needed for the hybrid sync system
-- that reduces full scrape time from ~28 days to hours while maintaining accuracy.
--
-- Run this migration in Supabase SQL Editor
-- Generated: 2024-12-29
-- ============================================================================

-- ============================================================================
-- PART 1: Modify properties_live Table - Add new tracking columns
-- ============================================================================

-- Add new columns to properties_live for manifest-based tracking
-- Note: data_staleness_days is computed via a helper function instead of a generated column
-- because PostgreSQL requires generated columns to use immutable expressions (NOW() is not immutable)
ALTER TABLE properties_live 
ADD COLUMN IF NOT EXISTS last_manifest_seen_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS consecutive_missing_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS listing_status VARCHAR(50) DEFAULT 'active',
ADD COLUMN IF NOT EXISTS last_full_scrape_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS scrape_priority INTEGER DEFAULT 3,
ADD COLUMN IF NOT EXISTS price_at_last_manifest NUMERIC;

-- Add constraint for listing_status valid values
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_listing_status'
    ) THEN
        ALTER TABLE properties_live 
        ADD CONSTRAINT chk_listing_status 
        CHECK (listing_status IN ('active', 'likely_removed', 'confirmed_removed', 'sold', 'relisted'));
    END IF;
END $$;

-- Add constraint for scrape_priority valid values
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_scrape_priority'
    ) THEN
        ALTER TABLE properties_live 
        ADD CONSTRAINT chk_scrape_priority 
        CHECK (scrape_priority BETWEEN 1 AND 5);
    END IF;
END $$;

-- Create indexes for new columns
CREATE INDEX IF NOT EXISTS idx_live_manifest_check 
ON properties_live(property_id, last_manifest_seen_at);

CREATE INDEX IF NOT EXISTS idx_live_listing_status 
ON properties_live(listing_status);

CREATE INDEX IF NOT EXISTS idx_live_stale 
ON properties_live(last_full_scrape_at) 
WHERE listing_status = 'active';

CREATE INDEX IF NOT EXISTS idx_live_priority 
ON properties_live(scrape_priority) 
WHERE listing_status = 'active';

CREATE INDEX IF NOT EXISTS idx_live_missing_count 
ON properties_live(consecutive_missing_count) 
WHERE listing_status = 'active' AND consecutive_missing_count > 0;

-- ============================================================================
-- PART 2: Create property_manifest Table
-- ============================================================================
-- Lightweight table for fast manifest tracking (minimal data per property)

CREATE TABLE IF NOT EXISTS property_manifest (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id VARCHAR(255) UNIQUE NOT NULL,
    source_url TEXT NOT NULL,
    listing_page_price NUMERIC,
    listing_page_title VARCHAR(500),
    latitude NUMERIC,
    longitude NUMERIC,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    seen_in_session_id UUID REFERENCES scraping_sessions(id),
    is_new BOOLEAN DEFAULT TRUE,
    needs_full_scrape BOOLEAN DEFAULT TRUE,
    price_changed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add comment for documentation
COMMENT ON TABLE property_manifest IS 
'Lightweight manifest of all known properties for fast existence checking and price change detection';

COMMENT ON COLUMN property_manifest.is_new IS 
'True if property is not yet in properties_live table';

COMMENT ON COLUMN property_manifest.needs_full_scrape IS 
'True if property needs detail page scraping (new or has changes)';

COMMENT ON COLUMN property_manifest.price_changed IS 
'True if listing_page_price differs from properties_live.price';

-- Create indexes for manifest operations
CREATE INDEX IF NOT EXISTS idx_manifest_property_id 
ON property_manifest(property_id);

CREATE INDEX IF NOT EXISTS idx_manifest_needs_scrape 
ON property_manifest(needs_full_scrape) 
WHERE needs_full_scrape = TRUE;

CREATE INDEX IF NOT EXISTS idx_manifest_is_new 
ON property_manifest(is_new) 
WHERE is_new = TRUE;

CREATE INDEX IF NOT EXISTS idx_manifest_last_seen 
ON property_manifest(last_seen_at);

CREATE INDEX IF NOT EXISTS idx_manifest_session 
ON property_manifest(seen_in_session_id);

CREATE INDEX IF NOT EXISTS idx_manifest_price_changed 
ON property_manifest(price_changed) 
WHERE price_changed = TRUE;

-- ============================================================================
-- PART 3: Create scrape_queue Table
-- ============================================================================
-- Priority queue for targeted scraping of specific properties

CREATE TABLE IF NOT EXISTS scrape_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id VARCHAR(255) NOT NULL,
    source_url TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 3,
    queue_reason VARCHAR(100) NOT NULL,
    queued_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    claimed_at TIMESTAMP WITH TIME ZONE,
    claimed_by VARCHAR(100),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'pending',
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    last_error TEXT,
    session_id UUID REFERENCES scraping_sessions(id),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add comment for documentation
COMMENT ON TABLE scrape_queue IS 
'Priority queue of properties needing full detail page scraping';

COMMENT ON COLUMN scrape_queue.priority IS 
'1=highest (new properties), 2=price changes, 3=normal, 4=stale data, 5=random sample';

COMMENT ON COLUMN scrape_queue.queue_reason IS 
'Reason for queuing: new_property, price_change, stale_data, verification, random_sample';

COMMENT ON COLUMN scrape_queue.status IS 
'Status: pending, in_progress, completed, failed, cancelled';

-- Add constraints
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_queue_priority'
    ) THEN
        ALTER TABLE scrape_queue 
        ADD CONSTRAINT chk_queue_priority 
        CHECK (priority BETWEEN 1 AND 5);
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_queue_status'
    ) THEN
        ALTER TABLE scrape_queue 
        ADD CONSTRAINT chk_queue_status 
        CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'cancelled'));
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_queue_reason'
    ) THEN
        ALTER TABLE scrape_queue 
        ADD CONSTRAINT chk_queue_reason 
        CHECK (queue_reason IN ('new_property', 'price_change', 'stale_data', 'verification', 'random_sample', 'relisted'));
    END IF;
END $$;

-- Create indexes for queue operations
CREATE INDEX IF NOT EXISTS idx_queue_pending 
ON scrape_queue(priority, queued_at) 
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_queue_property 
ON scrape_queue(property_id);

CREATE INDEX IF NOT EXISTS idx_queue_status 
ON scrape_queue(status);

CREATE INDEX IF NOT EXISTS idx_queue_session 
ON scrape_queue(session_id);

CREATE INDEX IF NOT EXISTS idx_queue_claimed 
ON scrape_queue(claimed_at) 
WHERE status = 'in_progress';

-- Prevent duplicate pending entries for same property
CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_unique_pending 
ON scrape_queue(property_id) 
WHERE status = 'pending';

-- ============================================================================
-- PART 4: Create sync_runs Table
-- ============================================================================
-- Track each tier execution and its results

CREATE TABLE IF NOT EXISTS sync_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tier_level INTEGER NOT NULL,
    tier_name VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'running',
    pages_scanned INTEGER DEFAULT 0,
    properties_in_manifest INTEGER DEFAULT 0,
    new_properties_found INTEGER DEFAULT 0,
    price_changes_detected INTEGER DEFAULT 0,
    removals_detected INTEGER DEFAULT 0,
    removals_confirmed INTEGER DEFAULT 0,
    properties_queued INTEGER DEFAULT 0,
    properties_scraped INTEGER DEFAULT 0,
    properties_updated INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    error_summary TEXT,
    execution_time_ms INTEGER,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add comment for documentation
COMMENT ON TABLE sync_runs IS 
'Tracks execution of each tier in the hybrid sync system';

COMMENT ON COLUMN sync_runs.tier_level IS 
'Tier level: 1=hot_listings (6h), 2=daily_sync (24h), 3=weekly_deep (7d), 4=monthly_refresh (30d)';

COMMENT ON COLUMN sync_runs.tier_name IS 
'Human-readable tier name: hot_listings, daily_sync, weekly_deep, monthly_refresh';

-- Add constraints
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_sync_run_tier'
    ) THEN
        ALTER TABLE sync_runs 
        ADD CONSTRAINT chk_sync_run_tier 
        CHECK (tier_level BETWEEN 1 AND 4);
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_sync_run_status'
    ) THEN
        ALTER TABLE sync_runs 
        ADD CONSTRAINT chk_sync_run_status 
        CHECK (status IN ('running', 'completed', 'failed', 'cancelled'));
    END IF;
END $$;

DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'chk_sync_run_tier_name'
    ) THEN
        ALTER TABLE sync_runs 
        ADD CONSTRAINT chk_sync_run_tier_name 
        CHECK (tier_name IN ('hot_listings', 'daily_sync', 'weekly_deep', 'monthly_refresh'));
    END IF;
END $$;

-- Create indexes for sync_runs
CREATE INDEX IF NOT EXISTS idx_sync_runs_tier 
ON sync_runs(tier_level, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_runs_status 
ON sync_runs(status);

CREATE INDEX IF NOT EXISTS idx_sync_runs_started 
ON sync_runs(started_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_runs_tier_status 
ON sync_runs(tier_level, status, started_at DESC);

-- ============================================================================
-- PART 5: Helper Functions
-- ============================================================================

-- Function to get the last successful run for a tier
CREATE OR REPLACE FUNCTION get_last_tier_run(p_tier_level INTEGER)
RETURNS TABLE (
    id UUID,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50),
    execution_time_ms INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        sr.id,
        sr.started_at,
        sr.completed_at,
        sr.status,
        sr.execution_time_ms
    FROM sync_runs sr
    WHERE sr.tier_level = p_tier_level
    AND sr.status = 'completed'
    ORDER BY sr.started_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to claim queue items (for concurrent workers)
CREATE OR REPLACE FUNCTION claim_queue_items(
    p_batch_size INTEGER,
    p_worker_id VARCHAR(100)
)
RETURNS TABLE (
    id UUID,
    property_id VARCHAR(255),
    source_url TEXT,
    priority INTEGER,
    queue_reason VARCHAR(100)
) AS $$
DECLARE
    claimed_ids UUID[];
BEGIN
    -- Select and lock items atomically
    WITH claimed AS (
        SELECT sq.id
        FROM scrape_queue sq
        WHERE sq.status = 'pending'
        ORDER BY sq.priority, sq.queued_at
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE scrape_queue sq
    SET 
        status = 'in_progress',
        claimed_at = NOW(),
        claimed_by = p_worker_id,
        attempt_count = sq.attempt_count + 1,
        updated_at = NOW()
    FROM claimed
    WHERE sq.id = claimed.id
    RETURNING sq.id INTO claimed_ids;
    
    -- Return the claimed items
    RETURN QUERY
    SELECT 
        sq.id,
        sq.property_id,
        sq.source_url,
        sq.priority,
        sq.queue_reason
    FROM scrape_queue sq
    WHERE sq.id = ANY(claimed_ids);
END;
$$ LANGUAGE plpgsql;

-- Function to get queue statistics
CREATE OR REPLACE FUNCTION get_queue_stats()
RETURNS TABLE (
    total_pending BIGINT,
    total_in_progress BIGINT,
    completed_today BIGINT,
    failed_today BIGINT,
    by_priority JSONB,
    by_reason JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH stats AS (
        SELECT 
            COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
            COUNT(*) FILTER (WHERE status = 'in_progress') as in_progress_count,
            COUNT(*) FILTER (WHERE status = 'completed' AND completed_at >= CURRENT_DATE) as completed_today_count,
            COUNT(*) FILTER (WHERE status = 'failed' AND completed_at >= CURRENT_DATE) as failed_today_count
        FROM scrape_queue
    ),
    priority_stats AS (
        SELECT jsonb_object_agg(priority::text, cnt) as priority_json
        FROM (
            SELECT priority, COUNT(*) as cnt 
            FROM scrape_queue 
            WHERE status = 'pending'
            GROUP BY priority
        ) p
    ),
    reason_stats AS (
        SELECT jsonb_object_agg(queue_reason, cnt) as reason_json
        FROM (
            SELECT queue_reason, COUNT(*) as cnt 
            FROM scrape_queue 
            WHERE status = 'pending'
            GROUP BY queue_reason
        ) r
    )
    SELECT 
        s.pending_count,
        s.in_progress_count,
        s.completed_today_count,
        s.failed_today_count,
        COALESCE(p.priority_json, '{}'),
        COALESCE(r.reason_json, '{}')
    FROM stats s
    CROSS JOIN priority_stats p
    CROSS JOIN reason_stats r;
END;
$$ LANGUAGE plpgsql;

-- Function to get manifest statistics
CREATE OR REPLACE FUNCTION get_manifest_stats()
RETURNS TABLE (
    total_properties BIGINT,
    new_properties BIGINT,
    needs_scrape BIGINT,
    price_changed BIGINT,
    avg_days_since_seen NUMERIC,
    oldest_seen_days INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_properties,
        COUNT(*) FILTER (WHERE is_new = TRUE) as new_properties,
        COUNT(*) FILTER (WHERE needs_full_scrape = TRUE) as needs_scrape,
        COUNT(*) FILTER (WHERE price_changed = TRUE) as price_changed,
        ROUND(AVG(EXTRACT(DAY FROM (NOW() - last_seen_at))), 2) as avg_days_since_seen,
        MAX(EXTRACT(DAY FROM (NOW() - last_seen_at)))::INTEGER as oldest_seen_days
    FROM property_manifest;
END;
$$ LANGUAGE plpgsql;

-- Function to increment consecutive_missing_count for properties not seen
CREATE OR REPLACE FUNCTION increment_missing_count(p_session_id UUID)
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE properties_live pl
    SET 
        consecutive_missing_count = consecutive_missing_count + 1,
        updated_at = NOW()
    WHERE pl.listing_status = 'active'
    AND pl.property_id NOT IN (
        SELECT pm.property_id 
        FROM property_manifest pm 
        WHERE pm.seen_in_session_id = p_session_id
    );
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Function to reset consecutive_missing_count for properties seen
CREATE OR REPLACE FUNCTION reset_missing_count(p_session_id UUID)
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE properties_live pl
    SET 
        consecutive_missing_count = 0,
        last_manifest_seen_at = NOW(),
        updated_at = NOW()
    FROM property_manifest pm
    WHERE pl.property_id = pm.property_id
    AND pm.seen_in_session_id = p_session_id
    AND pl.consecutive_missing_count > 0;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup old queue entries
CREATE OR REPLACE FUNCTION cleanup_old_queue_entries(p_days INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM scrape_queue
    WHERE status IN ('completed', 'cancelled')
    AND completed_at < NOW() - (p_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate data staleness days for a property
-- Use this instead of a generated column since NOW() is not immutable
CREATE OR REPLACE FUNCTION get_data_staleness_days(p_last_full_scrape_at TIMESTAMP WITH TIME ZONE)
RETURNS INTEGER AS $$
BEGIN
    IF p_last_full_scrape_at IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN EXTRACT(DAY FROM (NOW() - p_last_full_scrape_at))::INTEGER;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to get stale properties that need refresh
CREATE OR REPLACE FUNCTION get_stale_properties(p_stale_days INTEGER DEFAULT 14, p_limit INTEGER DEFAULT 1000)
RETURNS TABLE (
    property_id VARCHAR(255),
    source_url TEXT,
    last_full_scrape_at TIMESTAMP WITH TIME ZONE,
    staleness_days INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pl.property_id,
        pl.source_url,
        pl.last_full_scrape_at,
        EXTRACT(DAY FROM (NOW() - pl.last_full_scrape_at))::INTEGER as staleness_days
    FROM properties_live pl
    WHERE pl.listing_status = 'active'
    AND (
        pl.last_full_scrape_at IS NULL 
        OR pl.last_full_scrape_at < NOW() - (p_stale_days || ' days')::INTERVAL
    )
    ORDER BY pl.last_full_scrape_at NULLS FIRST
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- PART 6: Triggers for automatic timestamp updates
-- ============================================================================

-- Trigger function for updated_at
CREATE OR REPLACE FUNCTION update_manifest_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for property_manifest
DROP TRIGGER IF EXISTS trigger_manifest_updated_at ON property_manifest;
CREATE TRIGGER trigger_manifest_updated_at
    BEFORE UPDATE ON property_manifest
    FOR EACH ROW
    EXECUTE FUNCTION update_manifest_updated_at();

-- Trigger for scrape_queue
DROP TRIGGER IF EXISTS trigger_queue_updated_at ON scrape_queue;
CREATE TRIGGER trigger_queue_updated_at
    BEFORE UPDATE ON scrape_queue
    FOR EACH ROW
    EXECUTE FUNCTION update_manifest_updated_at();

-- ============================================================================
-- PART 7: Backfill existing data
-- ============================================================================
-- Run this AFTER creating the tables to populate initial values

-- Backfill last_manifest_seen_at from last_seen_at
UPDATE properties_live
SET last_manifest_seen_at = COALESCE(last_seen_at, last_updated_at, created_at)
WHERE last_manifest_seen_at IS NULL;

-- Backfill last_full_scrape_at from scraped_at
UPDATE properties_live
SET last_full_scrape_at = COALESCE(scraped_at, last_updated_at, created_at)
WHERE last_full_scrape_at IS NULL;

-- Ensure all active properties have listing_status set
UPDATE properties_live
SET listing_status = 'active'
WHERE listing_status IS NULL
AND status = 'active';

-- Set listing_status to confirmed_removed for inactive/removed properties
UPDATE properties_live
SET listing_status = 'confirmed_removed'
WHERE listing_status IS NULL
AND status IN ('inactive', 'removed');

-- ============================================================================
-- PART 8: Row Level Security (if using Supabase RLS)
-- ============================================================================
-- Uncomment if you need RLS policies

/*
-- Enable RLS on new tables
ALTER TABLE property_manifest ENABLE ROW LEVEL SECURITY;
ALTER TABLE scrape_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_runs ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "Service role has full access to property_manifest"
ON property_manifest FOR ALL
USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to scrape_queue"
ON scrape_queue FOR ALL
USING (auth.role() = 'service_role');

CREATE POLICY "Service role has full access to sync_runs"
ON sync_runs FOR ALL
USING (auth.role() = 'service_role');

-- Allow authenticated users to read
CREATE POLICY "Authenticated users can read property_manifest"
ON property_manifest FOR SELECT
USING (auth.role() = 'authenticated');

CREATE POLICY "Authenticated users can read sync_runs"
ON sync_runs FOR SELECT
USING (auth.role() = 'authenticated');
*/

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================
-- Run these after migration to verify everything is set up correctly

-- Check properties_live new columns
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'properties_live'
AND column_name IN (
    'last_manifest_seen_at', 
    'consecutive_missing_count', 
    'listing_status',
    'last_full_scrape_at', 
    'scrape_priority',
    'price_at_last_manifest'
)
ORDER BY ordinal_position;

-- Check new tables exist
SELECT table_name 
FROM information_schema.tables
WHERE table_name IN ('property_manifest', 'scrape_queue', 'sync_runs')
ORDER BY table_name;

-- Check indexes
SELECT indexname, tablename
FROM pg_indexes
WHERE tablename IN ('property_manifest', 'scrape_queue', 'sync_runs', 'properties_live')
AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;

-- Check functions
SELECT routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'public'
AND routine_name IN (
    'get_last_tier_run',
    'claim_queue_items',
    'get_queue_stats',
    'get_manifest_stats',
    'increment_missing_count',
    'reset_missing_count',
    'cleanup_old_queue_entries',
    'get_data_staleness_days',
    'get_stale_properties'
)
ORDER BY routine_name;

-- ============================================================================
-- END OF MIGRATION
-- ============================================================================

