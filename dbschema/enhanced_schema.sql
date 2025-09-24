-- Enhanced Property Data Management Schema
-- Implements dual-table strategy with change detection and data quality monitoring
-- Based on property_data_management_architecture.md

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =====================================================
-- 1. STAGING TABLE: property_scrapes_staging
-- =====================================================
-- Purpose: Temporary storage for each scraping session's raw data

CREATE TABLE IF NOT EXISTS property_scrapes_staging (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES scraping_sessions(id) ON DELETE CASCADE,
    
    -- Original property data (mirrors pulled_properties structure)
    property_id VARCHAR(255) NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    property_type VARCHAR(100),
    operation_type VARCHAR(50),
    
    -- Location data
    address TEXT,
    neighborhood VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    gps_coordinates VARCHAR(255),
    
    -- Property details
    price DECIMAL(15, 2),
    currency VARCHAR(10) DEFAULT 'MXN',
    price_per_m2 DECIMAL(10, 2),
    bedrooms INTEGER,
    bathrooms INTEGER,
    half_bathrooms INTEGER,
    parking_spaces INTEGER,
    
    -- Area measurements
    total_area_m2 DECIMAL(10, 2),
    covered_area_m2 DECIMAL(10, 2),
    lot_size_m2 DECIMAL(10, 2),
    
    -- Property characteristics
    floor_number INTEGER,
    total_floors INTEGER,
    age_years INTEGER,
    construction_year INTEGER,
    
    -- Features and amenities
    features JSONB,
    amenities JSONB,
    
    -- Media
    main_image_url TEXT,
    image_urls JSONB,
    virtual_tour_url TEXT,
    video_url TEXT,
    
    -- Agent/Contact information
    agent_name VARCHAR(255),
    agent_phone VARCHAR(50),
    agent_email VARCHAR(255),
    agency_name VARCHAR(255),
    message_url TEXT,
    
    -- Property status
    status VARCHAR(50) DEFAULT 'active',
    is_featured BOOLEAN DEFAULT false,
    is_premium BOOLEAN DEFAULT false,
    
    -- Metadata
    source_url TEXT NOT NULL,
    page_number INTEGER,
    listing_date DATE,
    
    -- Staging-specific metadata
    scraped_at TIMESTAMPTZ DEFAULT now(),
    processing_status VARCHAR(50) DEFAULT 'pending', -- pending, processed, error
    change_type VARCHAR(50), -- new, updated, unchanged, removed
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT now(),
    
    -- Ensure unique property per session
    CONSTRAINT unique_property_per_session UNIQUE (session_id, property_id)
);

-- =====================================================
-- 2. PRODUCTION TABLE: properties_live (Enhanced from pulled_properties)
-- =====================================================
-- Purpose: Clean, validated data for frontend consumption

CREATE TABLE IF NOT EXISTS properties_live (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    
    -- Original property data
    property_id VARCHAR(255) UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    property_type VARCHAR(100),
    operation_type VARCHAR(50),
    
    -- Location data
    address TEXT,
    neighborhood VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    gps_coordinates VARCHAR(255),
    
    -- Property details
    price DECIMAL(15, 2),
    currency VARCHAR(10) DEFAULT 'MXN',
    price_per_m2 DECIMAL(10, 2),
    bedrooms INTEGER,
    bathrooms INTEGER,
    half_bathrooms INTEGER,
    parking_spaces INTEGER,
    
    -- Area measurements
    total_area_m2 DECIMAL(10, 2),
    covered_area_m2 DECIMAL(10, 2),
    lot_size_m2 DECIMAL(10, 2),
    
    -- Property characteristics
    floor_number INTEGER,
    total_floors INTEGER,
    age_years INTEGER,
    construction_year INTEGER,
    
    -- Features and amenities
    features JSONB,
    amenities JSONB,
    
    -- Media
    main_image_url TEXT,
    image_urls JSONB,
    virtual_tour_url TEXT,
    video_url TEXT,
    
    -- Agent/Contact information
    agent_name VARCHAR(255),
    agent_phone VARCHAR(50),
    agent_email VARCHAR(255),
    agency_name VARCHAR(255),
    message_url TEXT,
    
    -- Property status
    is_featured BOOLEAN DEFAULT false,
    is_premium BOOLEAN DEFAULT false,
    
    -- Metadata
    source_url TEXT NOT NULL,
    page_number INTEGER,
    listing_date DATE,
    scraped_at TIMESTAMPTZ DEFAULT now(),
    
    -- Enhanced metadata for production table
    first_seen_at TIMESTAMPTZ DEFAULT now(),
    last_seen_at TIMESTAMPTZ DEFAULT now(),
    last_updated_at TIMESTAMPTZ DEFAULT now(),
    status VARCHAR(50) DEFAULT 'active', -- active, inactive, removed
    
    -- Data quality indicators
    data_completeness_score NUMERIC(3,2) DEFAULT 0.00, -- 0.00 to 1.00
    verification_status VARCHAR(50) DEFAULT 'unverified', -- unverified, verified, flagged
    
    -- Performance optimization
    search_vector TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('spanish', COALESCE(title, '') || ' ' || COALESCE(description, '') || ' ' || 
                   COALESCE(neighborhood, '') || ' ' || COALESCE(city, ''))
    ) STORED
);

-- =====================================================
-- 3. PROPERTY CHANGE HISTORY: property_changes
-- =====================================================
-- Purpose: Track all property modifications and lifecycle events

CREATE TABLE IF NOT EXISTS property_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id VARCHAR(255) NOT NULL,
    session_id UUID REFERENCES scraping_sessions(id),
    
    change_type VARCHAR(50) NOT NULL, -- created, updated, removed, reactivated
    field_name VARCHAR(100), -- specific field that changed (for updates)
    old_value JSONB, -- previous value
    new_value JSONB, -- new value
    
    confidence_score NUMERIC(3,2) DEFAULT 1.00, -- how confident we are in this change
    change_reason TEXT, -- why this change was detected
    
    created_at TIMESTAMPTZ DEFAULT now()
);

-- =====================================================
-- 4. DATA SYNCHRONIZATION METADATA: sync_metadata
-- =====================================================
-- Purpose: Track synchronization state and performance metrics

CREATE TABLE IF NOT EXISTS sync_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES scraping_sessions(id),
    
    -- Synchronization metrics
    total_scraped INTEGER DEFAULT 0,
    new_properties INTEGER DEFAULT 0,
    updated_properties INTEGER DEFAULT 0,
    removed_properties INTEGER DEFAULT 0,
    unchanged_properties INTEGER DEFAULT 0,
    
    -- Quality metrics
    data_quality_score NUMERIC(3,2),
    completeness_rate NUMERIC(3,2),
    
    -- Performance metrics
    sync_duration_ms INTEGER,
    staging_to_live_duration_ms INTEGER,
    
    -- Status tracking
    sync_status VARCHAR(50) DEFAULT 'pending', -- pending, running, completed, failed
    error_summary TEXT,
    
    created_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- =====================================================
-- 5. VALIDATION RULES: validation_rules
-- =====================================================
-- Purpose: Automated data validation rules

CREATE TABLE IF NOT EXISTS validation_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_name VARCHAR(100) NOT NULL UNIQUE,
    field_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL, -- range, pattern, required, custom
    rule_config JSONB NOT NULL,
    severity VARCHAR(20) DEFAULT 'warning', -- error, warning, info
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- =====================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- =====================================================

-- Staging table indexes
CREATE INDEX IF NOT EXISTS idx_staging_session_status 
    ON property_scrapes_staging (session_id, processing_status);
CREATE INDEX IF NOT EXISTS idx_staging_property_id 
    ON property_scrapes_staging (property_id);
CREATE INDEX IF NOT EXISTS idx_staging_change_type 
    ON property_scrapes_staging (change_type);
CREATE INDEX IF NOT EXISTS idx_staging_scraped_at 
    ON property_scrapes_staging (scraped_at);

-- Production table indexes (enhanced from original)
CREATE INDEX IF NOT EXISTS idx_properties_live_property_type 
    ON properties_live(property_type);
CREATE INDEX IF NOT EXISTS idx_properties_live_operation_type 
    ON properties_live(operation_type);
CREATE INDEX IF NOT EXISTS idx_properties_live_city 
    ON properties_live(city);
CREATE INDEX IF NOT EXISTS idx_properties_live_neighborhood 
    ON properties_live(neighborhood);
CREATE INDEX IF NOT EXISTS idx_properties_live_price 
    ON properties_live(price);
CREATE INDEX IF NOT EXISTS idx_properties_live_bedrooms 
    ON properties_live(bedrooms);
CREATE INDEX IF NOT EXISTS idx_properties_live_status 
    ON properties_live(status);
CREATE INDEX IF NOT EXISTS idx_properties_live_last_updated 
    ON properties_live(last_updated_at);
CREATE INDEX IF NOT EXISTS idx_properties_live_search 
    ON properties_live USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_properties_live_amenities 
    ON properties_live USING GIN(amenities);
CREATE INDEX IF NOT EXISTS idx_properties_live_location 
    ON properties_live(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Partial indexes for common queries
CREATE INDEX IF NOT EXISTS idx_properties_active 
    ON properties_live (last_updated_at) 
    WHERE status = 'active';

-- Change tracking indexes
CREATE INDEX IF NOT EXISTS idx_property_changes_property_id 
    ON property_changes (property_id);
CREATE INDEX IF NOT EXISTS idx_property_changes_session_id 
    ON property_changes (session_id);
CREATE INDEX IF NOT EXISTS idx_property_changes_type 
    ON property_changes (change_type);
CREATE INDEX IF NOT EXISTS idx_property_changes_created_at 
    ON property_changes (created_at);

-- Sync metadata indexes
CREATE INDEX IF NOT EXISTS idx_sync_metadata_session_id 
    ON sync_metadata (session_id);
CREATE INDEX IF NOT EXISTS idx_sync_metadata_status 
    ON sync_metadata (sync_status);
CREATE INDEX IF NOT EXISTS idx_sync_metadata_created_at 
    ON sync_metadata (created_at);

-- =====================================================
-- TRIGGERS AND FUNCTIONS
-- =====================================================

-- Function to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for properties_live
CREATE TRIGGER update_properties_live_updated_at 
    BEFORE UPDATE ON properties_live 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Function for real-time change notifications
CREATE OR REPLACE FUNCTION notify_property_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM pg_notify('property_changes', 
            json_build_object('type', 'created', 'id', NEW.id, 'property_id', NEW.property_id)::text);
    ELSIF TG_OP = 'UPDATE' THEN
        PERFORM pg_notify('property_changes', 
            json_build_object('type', 'updated', 'id', NEW.id, 'property_id', NEW.property_id)::text);
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('property_changes', 
            json_build_object('type', 'deleted', 'id', OLD.id, 'property_id', OLD.property_id)::text);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Trigger for real-time notifications
CREATE TRIGGER property_changes_trigger
    AFTER INSERT OR UPDATE OR DELETE ON properties_live
    FOR EACH ROW EXECUTE FUNCTION notify_property_changes();

-- =====================================================
-- MATERIALIZED VIEWS FOR ANALYTICS
-- =====================================================

-- Property summary statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS property_stats AS
SELECT 
    property_type,
    operation_type,
    city,
    COUNT(*) as total_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(total_area_m2) as avg_area,
    COUNT(*) FILTER (WHERE status = 'active') as active_count
FROM properties_live
GROUP BY property_type, operation_type, city;

-- Create unique index for concurrent refresh
CREATE UNIQUE INDEX IF NOT EXISTS property_stats_unique_idx 
    ON property_stats (property_type, operation_type, city);

-- Function to refresh property stats
CREATE OR REPLACE FUNCTION refresh_property_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY property_stats;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- ROW LEVEL SECURITY (RLS) POLICIES
-- =====================================================

-- Enable RLS on new tables
ALTER TABLE property_scrapes_staging ENABLE ROW LEVEL SECURITY;
ALTER TABLE properties_live ENABLE ROW LEVEL SECURITY;
ALTER TABLE property_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE validation_rules ENABLE ROW LEVEL SECURITY;

-- Staging table policies
CREATE POLICY "Enable read access for staging" ON property_scrapes_staging FOR SELECT USING (true);
CREATE POLICY "Enable insert access for staging" ON property_scrapes_staging FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for staging" ON property_scrapes_staging FOR UPDATE USING (true);

-- Production table policies
CREATE POLICY "Enable read access for properties_live" ON properties_live FOR SELECT USING (true);
CREATE POLICY "Enable insert access for properties_live" ON properties_live FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for properties_live" ON properties_live FOR UPDATE USING (true);

-- Change tracking policies
CREATE POLICY "Enable read access for property_changes" ON property_changes FOR SELECT USING (true);
CREATE POLICY "Enable insert access for property_changes" ON property_changes FOR INSERT WITH CHECK (true);

-- Sync metadata policies
CREATE POLICY "Enable read access for sync_metadata" ON sync_metadata FOR SELECT USING (true);
CREATE POLICY "Enable insert access for sync_metadata" ON sync_metadata FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for sync_metadata" ON sync_metadata FOR UPDATE USING (true);

-- Validation rules policies
CREATE POLICY "Enable read access for validation_rules" ON validation_rules FOR SELECT USING (true);
CREATE POLICY "Enable insert access for validation_rules" ON validation_rules FOR INSERT WITH CHECK (true);
CREATE POLICY "Enable update access for validation_rules" ON validation_rules FOR UPDATE USING (true);

-- Additional index for validation rules
CREATE INDEX IF NOT EXISTS idx_validation_rules_rule_name 
    ON validation_rules (rule_name);
CREATE INDEX IF NOT EXISTS idx_validation_rules_field_name 
    ON validation_rules (field_name);

-- =====================================================
-- SAMPLE VALIDATION RULES
-- =====================================================

INSERT INTO validation_rules (rule_name, field_name, rule_type, rule_config, severity) VALUES
('price_range', 'price', 'range', '{"min": 1000, "max": 50000000}', 'warning'),
('valid_email', 'agent_email', 'pattern', '{"regex": "^[^@]+@[^@]+\\.[^@]+$"}', 'warning'),
('required_title', 'title', 'required', '{}', 'error'),
('bedrooms_range', 'bedrooms', 'range', '{"min": 0, "max": 20}', 'warning'),
('bathrooms_range', 'bathrooms', 'range', '{"min": 0, "max": 10}', 'warning'),
('valid_coordinates', 'latitude', 'range', '{"min": -90, "max": 90}', 'warning'),
('valid_longitude', 'longitude', 'range', '{"min": -180, "max": 180}', 'warning')
ON CONFLICT (rule_name) DO NOTHING;

-- =====================================================
-- MIGRATION HELPER FUNCTIONS
-- =====================================================

-- Function to migrate data from pulled_properties to properties_live
CREATE OR REPLACE FUNCTION migrate_pulled_properties_to_live()
RETURNS INTEGER AS $$
DECLARE
    migrated_count INTEGER := 0;
BEGIN
    -- Check if pulled_properties table exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'pulled_properties') THEN
        INSERT INTO properties_live (
            property_id, title, description, property_type, operation_type,
            address, neighborhood, city, state, postal_code, latitude, longitude, gps_coordinates,
            price, currency, price_per_m2, bedrooms, bathrooms, half_bathrooms, parking_spaces,
            total_area_m2, covered_area_m2, lot_size_m2, floor_number, total_floors, age_years, construction_year,
            features, amenities, main_image_url, image_urls, virtual_tour_url, video_url,
            agent_name, agent_phone, agent_email, agency_name, message_url,
            is_featured, is_premium, source_url, page_number, listing_date, scraped_at,
            created_at, updated_at, first_seen_at, last_seen_at, last_updated_at
        )
        SELECT 
            property_id, title, description, property_type, operation_type,
            address, neighborhood, city, state, postal_code, latitude, longitude, gps_coordinates,
            price, currency, price_per_m2, bedrooms, bathrooms, half_bathrooms, parking_spaces,
            total_area_m2, covered_area_m2, lot_size_m2, floor_number, total_floors, age_years, construction_year,
            features, amenities, main_image_url, image_urls, virtual_tour_url, video_url,
            agent_name, agent_phone, agent_email, agency_name, message_url,
            is_featured, is_premium, source_url, page_number, listing_date, scraped_at,
            created_at, updated_at, created_at, updated_at, updated_at
        FROM pulled_properties
        WHERE property_id IS NOT NULL
        ON CONFLICT (property_id) DO NOTHING;
        
        GET DIAGNOSTICS migrated_count = ROW_COUNT;
    ELSE
        migrated_count := 0;
    END IF;
    
    RETURN migrated_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================

COMMENT ON TABLE property_scrapes_staging IS 'Temporary storage for raw scraped property data before processing';
COMMENT ON TABLE properties_live IS 'Clean, validated property data for frontend consumption';
COMMENT ON TABLE property_changes IS 'Audit trail of all property modifications and lifecycle events';
COMMENT ON TABLE sync_metadata IS 'Synchronization metrics and performance tracking';
COMMENT ON TABLE validation_rules IS 'Configurable data validation rules';

COMMENT ON COLUMN properties_live.data_completeness_score IS 'Score from 0.00 to 1.00 indicating data completeness';
COMMENT ON COLUMN properties_live.verification_status IS 'Data verification status: unverified, verified, flagged';
COMMENT ON COLUMN property_changes.confidence_score IS 'Confidence level (0.00-1.00) in the detected change';
COMMENT ON COLUMN sync_metadata.sync_duration_ms IS 'Total synchronization time in milliseconds';

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Enhanced property data management schema created successfully!';
    RAISE NOTICE 'Tables created: property_scrapes_staging, properties_live, property_changes, sync_metadata, validation_rules';
    RAISE NOTICE 'Run SELECT migrate_pulled_properties_to_live(); to migrate existing data';
END $$; 