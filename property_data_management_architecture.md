# Property Data Management Architecture

## Overview

This document outlines a comprehensive architecture for managing scraped property data that addresses synchronization challenges, performance optimization, and data integrity while maintaining real-time availability for frontend applications.

## Current State Analysis

### Existing Architecture Strengths
- **Single Source Table**: `pulled_properties` serves as the primary data store
- **Session Tracking**: `scraping_sessions` provides audit trails and progress monitoring
- **Error Handling**: `scraping_errors` enables debugging and retry logic
- **Rich Data Model**: Comprehensive property attributes with JSONB flexibility

### Identified Challenges
1. **Stale Data Detection**: No mechanism to identify removed listings
2. **Frontend Performance**: Direct queries against scraping table during updates
3. **Data Consistency**: Risk of serving incomplete data during scraping operations
4. **Historical Tracking**: Limited ability to track property lifecycle changes

## Proposed Multi-Table Architecture

### Core Concept: Dual-Table Strategy with Change Detection

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Staging Table  │───▶│  Production     │───▶│   Frontend      │
│  (Raw Scrapes)  │    │     Table       │    │     APIs        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│ Change Detection│    │   Audit Log     │
│    Service      │    │     Table       │
└─────────────────┘    └─────────────────┘
```

## Enhanced Database Schema

### 1. Staging Table: `property_scrapes_staging`

**Purpose**: Temporary storage for each scraping session's raw data

```sql
CREATE TABLE property_scrapes_staging (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES scraping_sessions(id),
    
    -- Original property data (mirrors pulled_properties structure)
    property_id VARCHAR UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    -- ... all existing pulled_properties columns ...
    
    -- Staging-specific metadata
    scraped_at TIMESTAMPTZ DEFAULT now(),
    processing_status VARCHAR DEFAULT 'pending', -- pending, processed, error
    change_type VARCHAR, -- new, updated, unchanged, removed
    
    CONSTRAINT property_scrapes_staging_session_fkey 
        FOREIGN KEY (session_id) REFERENCES scraping_sessions(id)
);
```

### 2. Production Table: `properties_live` (Renamed from `pulled_properties`)

**Purpose**: Clean, validated data for frontend consumption

```sql
CREATE TABLE properties_live (
    -- Existing pulled_properties structure with additions:
    
    -- Enhanced metadata
    first_seen_at TIMESTAMPTZ DEFAULT now(),
    last_seen_at TIMESTAMPTZ DEFAULT now(),
    last_updated_at TIMESTAMPTZ DEFAULT now(),
    status VARCHAR DEFAULT 'active', -- active, inactive, removed
    
    -- Data quality indicators
    data_completeness_score NUMERIC(3,2), -- 0.00 to 1.00
    verification_status VARCHAR DEFAULT 'unverified', -- unverified, verified, flagged
    
    -- Performance optimization
    search_vector TSVECTOR, -- Enhanced full-text search
    location_point GEOMETRY(POINT, 4326), -- PostGIS for spatial queries
    
    CONSTRAINT properties_live_pkey PRIMARY KEY (id)
);
```

### 3. Property Change History: `property_changes`

**Purpose**: Track all property modifications and lifecycle events

```sql
CREATE TABLE property_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id VARCHAR NOT NULL,
    session_id UUID REFERENCES scraping_sessions(id),
    
    change_type VARCHAR NOT NULL, -- created, updated, removed, reactivated
    field_name VARCHAR, -- specific field that changed (for updates)
    old_value JSONB, -- previous value
    new_value JSONB, -- new value
    
    confidence_score NUMERIC(3,2), -- how confident we are in this change
    change_reason TEXT, -- why this change was detected
    
    created_at TIMESTAMPTZ DEFAULT now(),
    
    INDEX idx_property_changes_property_id (property_id),
    INDEX idx_property_changes_session_id (session_id),
    INDEX idx_property_changes_type (change_type)
);
```

### 4. Data Synchronization Metadata: `sync_metadata`

**Purpose**: Track synchronization state and performance metrics

```sql
CREATE TABLE sync_metadata (
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
    sync_status VARCHAR DEFAULT 'pending', -- pending, running, completed, failed
    error_summary TEXT,
    
    created_at TIMESTAMPTZ DEFAULT now(),
    completed_at TIMESTAMPTZ
);
```

## Data Flow Architecture

### 1. Scraping Phase

```
Scraper → property_scrapes_staging
    ├── Raw data insertion with session_id
    ├── No validation or cleaning at this stage
    └── Fast bulk inserts for performance
```

### 2. Change Detection Phase

```
Change Detection Service:
    ├── Compare staging vs live data
    ├── Identify: new, updated, unchanged, removed
    ├── Calculate confidence scores
    ├── Log changes to property_changes table
    └── Mark staging records with change_type
```

### 3. Data Promotion Phase

```
Data Promotion Service:
    ├── Validate and clean staging data
    ├── Apply business rules and quality checks
    ├── Promote approved changes to properties_live
    ├── Update metadata and search vectors
    └── Archive or cleanup staging data
```

### 4. Frontend Serving

```
Frontend APIs → properties_live (only)
    ├── Optimized indexes and search vectors
    ├── No interference from scraping operations
    ├── Consistent data availability
    └── Enhanced query performance
```

## Change Detection Logic

### 1. Property Lifecycle States

```
NEW → ACTIVE → UPDATED → INACTIVE → REMOVED
  ↑      ↓         ↑         ↓         ↓
  └── REACTIVATED ←┘    ARCHIVED ← EXPIRED
```

### 2. Detection Algorithms

#### **New Property Detection**
```sql
-- Properties in staging but not in live
SELECT s.* FROM property_scrapes_staging s
LEFT JOIN properties_live l ON s.property_id = l.property_id
WHERE l.property_id IS NULL AND s.session_id = ?
```

#### **Updated Property Detection**
```sql
-- Compare key fields for changes
WITH changed_properties AS (
    SELECT s.property_id,
           s.title != l.title AS title_changed,
           s.price != l.price AS price_changed,
           s.description != l.description AS desc_changed,
           -- ... other field comparisons
    FROM property_scrapes_staging s
    JOIN properties_live l ON s.property_id = l.property_id
    WHERE s.session_id = ?
)
SELECT * FROM changed_properties 
WHERE title_changed OR price_changed OR desc_changed -- OR other changes
```

#### **Removed Property Detection**
```sql
-- Properties in live but not in recent scraping sessions
SELECT l.* FROM properties_live l
WHERE l.property_id NOT IN (
    SELECT DISTINCT property_id 
    FROM property_scrapes_staging 
    WHERE session_id IN (
        SELECT id FROM scraping_sessions 
        WHERE created_at > NOW() - INTERVAL '7 days'
        AND status = 'completed'
    )
)
AND l.status = 'active'
AND l.last_seen_at < NOW() - INTERVAL '3 days'
```

### 3. Confidence Scoring

```python
def calculate_change_confidence(old_data, new_data, change_type):
    """
    Calculate confidence score (0.0 to 1.0) for detected changes
    """
    confidence_factors = {
        'price_change': 0.9,  # Price changes are highly reliable
        'title_change': 0.8,  # Title changes are usually significant
        'description_change': 0.6,  # Descriptions might have minor updates
        'image_change': 0.7,  # Image changes indicate real updates
        'contact_change': 0.85, # Agent/contact changes are significant
    }
    
    # Combine factors based on change magnitude and type
    # Return weighted confidence score
```

## Advanced Features

### 1. Smart Deduplication

```sql
-- Identify potential duplicates across different property_ids
WITH similarity_scores AS (
    SELECT 
        p1.id as prop1_id,
        p2.id as prop2_id,
        similarity(p1.title, p2.title) as title_sim,
        similarity(p1.address, p2.address) as address_sim,
        ST_Distance(p1.location_point, p2.location_point) as distance_m
    FROM properties_live p1
    CROSS JOIN properties_live p2
    WHERE p1.id != p2.id
)
SELECT * FROM similarity_scores
WHERE title_sim > 0.8 AND address_sim > 0.7 AND distance_m < 100
```

### 2. Data Quality Scoring

```python
def calculate_data_quality_score(property_data):
    """
    Assess data completeness and quality
    """
    quality_factors = {
        'has_price': 0.2,
        'has_description': 0.15,
        'has_images': 0.15,
        'has_location': 0.2,
        'has_contact_info': 0.1,
        'has_property_details': 0.1,
        'description_length': 0.05,  # Longer descriptions = better quality
        'image_count': 0.05,  # More images = better quality
    }
    # Calculate weighted score
```

### 3. Automated Data Validation

```sql
-- Create validation rules
CREATE TABLE validation_rules (
    id UUID PRIMARY KEY,
    rule_name VARCHAR NOT NULL,
    field_name VARCHAR NOT NULL,
    rule_type VARCHAR NOT NULL, -- range, pattern, required, custom
    rule_config JSONB NOT NULL,
    severity VARCHAR DEFAULT 'warning', -- error, warning, info
    is_active BOOLEAN DEFAULT true
);

-- Example rules
INSERT INTO validation_rules (rule_name, field_name, rule_type, rule_config) VALUES
('price_range', 'price', 'range', '{"min": 1000, "max": 50000000}'),
('valid_email', 'agent_email', 'pattern', '{"regex": "^[^@]+@[^@]+\\.[^@]+$"}'),
('required_title', 'title', 'required', '{}'),
('bedrooms_range', 'bedrooms', 'range', '{"min": 0, "max": 20}');
```

## Performance Optimization Strategies

### 1. Database Indexing Strategy

```sql
-- Performance-critical indexes
CREATE INDEX CONCURRENTLY idx_properties_live_location 
    ON properties_live USING GIST (location_point);

CREATE INDEX CONCURRENTLY idx_properties_live_search 
    ON properties_live USING GIN (search_vector);

CREATE INDEX CONCURRENTLY idx_properties_live_filters 
    ON properties_live (property_type, operation_type, price, bedrooms);

CREATE INDEX CONCURRENTLY idx_staging_session 
    ON property_scrapes_staging (session_id, processing_status);

-- Partial indexes for common queries
CREATE INDEX CONCURRENTLY idx_properties_active 
    ON properties_live (last_updated_at) 
    WHERE status = 'active';
```

### 2. Materialized Views for Complex Queries

```sql
-- Property summary statistics
CREATE MATERIALIZED VIEW property_stats AS
SELECT 
    property_type,
    operation_type,
    city,
    COUNT(*) as total_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(total_area_m2) as avg_area
FROM properties_live
WHERE status = 'active'
GROUP BY property_type, operation_type, city;

-- Refresh strategy
CREATE OR REPLACE FUNCTION refresh_property_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY property_stats;
END;
$$ LANGUAGE plpgsql;
```

### 3. Caching Strategy

```python
# Redis caching layers
CACHE_STRATEGIES = {
    'property_search': {
        'ttl': 300,  # 5 minutes
        'key_pattern': 'search:{filters_hash}',
        'invalidate_on': ['property_update', 'property_create']
    },
    'property_stats': {
        'ttl': 1800,  # 30 minutes
        'key_pattern': 'stats:{city}:{type}',
        'invalidate_on': ['daily_refresh']
    },
    'featured_properties': {
        'ttl': 600,  # 10 minutes
        'key_pattern': 'featured:{region}',
        'invalidate_on': ['featured_update']
    }
}
```

## Operational Procedures

### 1. Daily Synchronization Workflow

```python
async def daily_sync_workflow():
    """
    Complete daily synchronization process
    """
    # 1. Start scraping session
    session = await start_scraping_session()
    
    # 2. Scrape data to staging
    await scrape_to_staging(session.id)
    
    # 3. Run change detection
    changes = await detect_changes(session.id)
    
    # 4. Apply business rules and validation
    validated_changes = await validate_changes(changes)
    
    # 5. Promote changes to live table
    await promote_to_live(validated_changes)
    
    # 6. Update search indexes and cache
    await refresh_search_indexes()
    await invalidate_caches(['property_search', 'property_stats'])
    
    # 7. Generate sync report
    await generate_sync_report(session.id)
    
    # 8. Cleanup staging data
    await cleanup_staging(session.id)
```

### 2. Real-time Change Notifications

```sql
-- PostgreSQL triggers for real-time notifications
CREATE OR REPLACE FUNCTION notify_property_changes()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM pg_notify('property_changes', 
            json_build_object('type', 'created', 'id', NEW.id)::text);
    ELSIF TG_OP = 'UPDATE' THEN
        PERFORM pg_notify('property_changes', 
            json_build_object('type', 'updated', 'id', NEW.id)::text);
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM pg_notify('property_changes', 
            json_build_object('type', 'deleted', 'id', OLD.id)::text);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER property_changes_trigger
    AFTER INSERT OR UPDATE OR DELETE ON properties_live
    FOR EACH ROW EXECUTE FUNCTION notify_property_changes();
```

### 3. Data Quality Monitoring

```python
class DataQualityMonitor:
    """
    Monitor and alert on data quality issues
    """
    
    def __init__(self):
        self.quality_thresholds = {
            'completeness_rate': 0.85,
            'duplicate_rate': 0.02,
            'error_rate': 0.05,
            'freshness_hours': 24
        }
    
    async def run_quality_checks(self, session_id):
        """
        Run comprehensive data quality checks
        """
        checks = [
            self.check_data_completeness(),
            self.check_duplicate_rate(),
            self.check_price_anomalies(),
            self.check_data_freshness(),
            self.check_geographic_distribution()
        ]
        
        results = await asyncio.gather(*checks)
        await self.generate_quality_report(session_id, results)
        await self.send_alerts_if_needed(results)
```

## Migration Strategy

### Phase 1: Parallel Implementation (Weeks 1-2)
1. Create new staging and change tracking tables
2. Modify scraper to write to both old and new tables
3. Implement change detection service
4. Test synchronization logic with small datasets

### Phase 2: Frontend Migration (Weeks 3-4)
1. Update frontend APIs to use new properties_live table
2. Implement caching and performance optimizations
3. Add monitoring and alerting systems
4. Run parallel systems to validate data consistency

### Phase 3: Full Cutover (Week 5)
1. Switch all systems to new architecture
2. Remove old direct-write patterns
3. Implement automated quality monitoring
4. Optimize based on production performance data

### Phase 4: Advanced Features (Weeks 6-8)
1. Add machine learning for change detection
2. Implement predictive data quality scoring
3. Add advanced deduplication algorithms
4. Create comprehensive analytics dashboard

## Benefits of This Architecture

### 1. **Data Consistency**
- Frontend always serves stable, validated data
- Scraping operations don't interfere with user queries
- Atomic updates prevent partial data states

### 2. **Performance Optimization**
- Dedicated indexes and caching for frontend queries
- Bulk operations optimized for staging table
- Materialized views for complex analytics

### 3. **Change Tracking**
- Complete audit trail of all property modifications
- Ability to rollback changes if needed
- Historical analysis of market trends

### 4. **Scalability**
- Horizontal scaling possible with read replicas
- Staging operations can be distributed
- Caching reduces database load

### 5. **Data Quality**
- Automated validation and quality scoring
- Early detection of scraping issues
- Business rule enforcement before production

### 6. **Operational Excellence**
- Comprehensive monitoring and alerting
- Automated synchronization workflows
- Clear separation of concerns

This architecture provides a robust foundation for managing scraped property data while ensuring optimal frontend performance and data integrity. The dual-table approach with intelligent change detection addresses your core requirements while providing room for future enhancements and scaling. 