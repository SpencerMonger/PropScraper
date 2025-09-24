# PropScraper Architecture Documentation

## Overview

PropScraper is a Python-based web scraping system designed to extract property listings from real estate websites and store them in a Supabase database. Currently supports Pincali.com (Mexico) as the primary target and Inmuebles24.com (Mexico) for legacy support. The system is built with modern async/await patterns, comprehensive error handling, captcha solving capabilities, and robust data persistence.

## System Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Targets   │    │ Enhanced Scraper│    │   Staging Table │
│  (Pincali.com   │◄──►│    System       │◄──►│property_scrapes_│
│ Inmuebles24.com)│    │                 │    │    staging      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                         │
                              ▼                         ▼
                    ┌─────────────────┐    ┌─────────────────┐
                    │ SolveCaptcha    │    │Services Layer   │
                    │     API         │    │• Change Detection│
                    └─────────────────┘    │• Data Quality   │
                                          │• Sync Service   │
                                          │• Orchestrator   │
                                          └─────────────────┘
                                                    │
                                                    ▼
                                          ┌─────────────────┐
                                          │   Live Table    │
                                          │ properties_live │
                                          │  (Production)   │
                                          └─────────────────┘
```

### Core Components

1. **Enhanced Pincali Scraper** (`enhanced_property_scraper.py`) - Primary scraper with staging architecture
2. **Pincali Scraper Engine** (`pincali_scraper.py`) - Original scraper (legacy)
3. **Services Architecture** (`services/` directory) - Data management and sync services
4. **Inmuebles24 Scraper Engine** (`inmuebles24_scraper.py`) - Legacy scraper
5. **Captcha Solver** (`captcha_solver.py`)
6. **Database Schema** (Auto-generated documentation available)
7. **Debug Tools** (`debug_pincali_scraper.py`, `test_setup.py`)
8. **Testing Framework** (`tests/` directory)
9. **Configuration Management** (`.env` files)

## Component Architecture

### 1. Enhanced Pincali Scraper (`EnhancedPincaliScraper`)

The enhanced scraping engine implements a dual-table architecture with staging and live tables:

#### Key Components:
- **Session Management**: Tracks scraping progress and handles interruptions
- **Data Extraction**: Parses property listings using proven CSS selectors from working scraper
- **Staging Table Storage**: Saves raw scraped data to `property_scrapes_staging` table
- **Services Integration**: Uses services architecture for data validation and sync
- **Error Handling**: Comprehensive logging and error recovery
- **Auto-Sync Workflow**: Automatically promotes validated data to live table

#### Dual-Table Architecture:
1. **Scraping Phase**: Raw data → `property_scrapes_staging` table
2. **Validation Phase**: Data quality checks and validation
3. **Change Detection**: Compare staging vs live data to identify new/updated/removed properties
4. **Sync Phase**: Promote validated changes to `properties_live` table

#### Architecture Pattern:
```python
class EnhancedPincaliScraper:
    def __init__():
        # Initialize browser config, database client, services orchestrator
        self.orchestrator = PropertySyncOrchestrator(supabase)
    
    async def scrape_all_pages(auto_sync=True):
        # Main orchestration method
        # Creates session → Scrapes pages → Saves to staging → Optionally runs sync
        
        # Scraping phase
        for page in pages:
            properties = await self.scrape_property_list_page(page)
            for prop in properties:
                cleaned_data = self.extract_property_details(prop)
                await self.save_property_to_staging(cleaned_data)
        
        # Auto-sync phase (if enabled)
        if auto_sync:
            workflow_result = await self.orchestrator.daily_sync_workflow(session_id)
    
    async def save_property_to_staging():
        # Save raw data to staging table for later processing
```

#### Technology Stack:
- **Crawl4AI**: Modern async web crawling with Playwright backend
- **Supabase**: PostgreSQL database with real-time capabilities
- **BeautifulSoup**: HTML parsing and data extraction
- **AsyncIO**: Asynchronous execution for better performance
- **Services Architecture**: Modular data management services

### 2. Services Architecture (`services/` directory)

The system implements a comprehensive services layer for data management:

#### Core Services:

##### PropertySyncOrchestrator (`property_sync_orchestrator.py`)
- **Purpose**: Coordinates the complete data synchronization workflow
- **Key Methods**:
  - `daily_sync_workflow()`: Main orchestration method
  - Validates session data, runs quality checks, executes sync
- **Integration**: Used by enhanced scraper for auto-sync functionality

##### ChangeDetectionService (`change_detection_service.py`)
- **Purpose**: Detects differences between staging and live data
- **Key Methods**:
  - `detect_changes()`: Identifies new, updated, and removed properties
  - `_detect_new_properties()`: Finds properties in staging but not in live
  - `_detect_updated_properties()`: Identifies changed properties
- **Intelligence**: Uses confidence scoring and field-level change detection

##### DataSyncService (`data_sync_service.py`)
- **Purpose**: Promotes validated data from staging to live tables
- **Key Methods**:
  - `sync_session_data()`: Main sync execution
  - `_promote_new_properties()`: Inserts new properties to live table
  - `_promote_updated_properties()`: Updates existing properties
- **Features**: Batch processing, error handling, performance tracking

##### DataQualityService (`data_quality_service.py`)
- **Purpose**: Validates data quality before promotion to live table
- **Key Methods**:
  - `validate_staging_data()`: Comprehensive data validation
  - `generate_quality_report()`: Detailed quality assessment
- **Rules**: Configurable validation rules stored in database

#### Services Workflow:
```
1. Enhanced Scraper → Staging Table
2. PropertySyncOrchestrator.daily_sync_workflow()
   ├── DataQualityService.validate_staging_data()
   ├── ChangeDetectionService.detect_changes()
   └── DataSyncService.sync_session_data()
       ├── Promote new properties
       ├── Update changed properties  
       └── Handle removed properties
```

### 2. Captcha Solver Integration

#### Purpose:
Handles anti-bot measures including Cloudflare Turnstile, reCAPTCHA v2/v3, and hCaptcha challenges.

#### Architecture:
```python
class CaptchaSolver:
    # Direct integration with SolveCaptcha API
    async def solve_cloudflare_turnstile()
    async def solve_recaptcha_v2()

class CrawlerWithCaptchaSolver:
    # Enhanced crawler with automatic captcha detection
    async def crawl_with_captcha_handling()
    def _has_captcha_challenge()
    def _extract_captcha_info()
    async def _solve_captcha()
    async def _retry_with_captcha_solution()
```

#### Features:
- **Automatic Detection**: Scans HTML for captcha elements
- **Multi-Type Support**: Handles various captcha types
- **JavaScript Injection**: Automatically submits solved captchas
- **Retry Logic**: Multiple attempts with exponential backoff

### 3. Database Architecture

#### Schema Design:
The system uses a PostgreSQL database through Supabase with a dual-table architecture and supporting tables:

##### Core Data Tables:

###### `property_scrapes_staging` Table:
```sql
- id (UUID, Primary Key)
- session_id (UUID) - Links to scraping session
- property_id (VARCHAR) - Unique identifier from source site
- Basic Info: title, description, property_type, operation_type
- Location: address, neighborhood, city, state, coordinates
- Details: price, currency, bedrooms, bathrooms, area measurements
- Features: JSON fields for amenities and characteristics
- Media: image URLs, virtual tours, videos
- Agent Info: contact details and agency information
- Metadata: source URLs, scraping timestamps, page numbers
- Raw Data: Preserves original scraped data for quality analysis
```

###### `properties_live` Table:
```sql
- id (UUID, Primary Key)
- property_id (VARCHAR) - Unique identifier from source site
- Same structure as staging table but for validated, live data
- Additional fields: data_quality_score, last_validated_at
- Optimized indexes for production queries
- Full-text search capabilities for Spanish content
```

##### Session & Monitoring Tables:

###### `scraping_sessions` Table:
```sql
- id (UUID, Primary Key)
- status (VARCHAR) - running, completed, failed, paused
- source_site (VARCHAR) - pincali.com, inmuebles24.com, etc.
- Performance metrics: pages_scraped, properties_found, properties_inserted
- Timing data: started_at, completed_at, duration
- Error tracking: error_count, last_error
```

###### `scraping_errors` Table:
```sql
- id (UUID, Primary Key)
- session_id (UUID) - Links to scraping session
- error_type (VARCHAR) - categorized error types
- error_message (TEXT) - detailed error information
- url (TEXT) - URL where error occurred
- created_at (TIMESTAMP)
```

##### Services Support Tables:

###### `data_quality_rules` Table:
```sql
- id (UUID, Primary Key)
- rule_name (VARCHAR) - human-readable rule name
- rule_type (VARCHAR) - validation, transformation, etc.
- field_name (VARCHAR) - target field for rule
- rule_config (JSONB) - rule parameters and thresholds
- is_active (BOOLEAN) - enable/disable rules
```

###### `sync_logs` Table:
```sql
- id (UUID, Primary Key)
- session_id (UUID) - Links to scraping session
- sync_type (VARCHAR) - daily_sync, manual_sync, etc.
- properties_processed (INTEGER)
- properties_promoted (INTEGER)
- sync_status (VARCHAR) - completed, failed, partial
- execution_time (INTERVAL)
- created_at (TIMESTAMP)
```

#### Database Features:
- **Indexes**: Optimized for common queries (location, price, property type)
- **Full-Text Search**: Spanish-language search capabilities
- **Row Level Security**: Configurable access controls
- **Triggers**: Automatic timestamp updates
- **JSON Fields**: Flexible storage for variable property features

### 4. Testing Framework

#### Test Categories:

##### Unit Tests:
- **Captcha Solver Tests** (`test_captcha_solver.py`): Validation of captcha detection and solving
- **Setup Tests** (`test_setup.py`): Dependency and configuration validation
- **Component Tests**: Individual function and method testing

##### Integration Tests:
- **Debug Scraper** (`debug_scraper.py`): Connectivity and basic functionality tests
- **Minimal Working Examples**: Simplified versions for troubleshooting
- **Anti-Captcha Integration**: End-to-end captcha solving tests

##### System Tests:
- **Timeout Safety** (`timeout_safe_scraper.py`): Performance and reliability testing
- **Simple Scraper** (`simple_scraper.py`): Basic functionality validation

## Data Flow Architecture

### 1. Enhanced Scraping Pipeline (Dual-Table Architecture)

```
Start Session → Get Total Pages → For Each Page:
    ├── Fetch Page (with captcha handling)
    ├── Extract Properties (CSS selectors)
    ├── Clean Data (normalize, validate)
    ├── Save to Staging Table (property_scrapes_staging)
    ├── Update Progress
    └── Rate Limit Delay

Optional Auto-Sync Workflow:
    ├── Data Quality Validation
    ├── Change Detection (staging vs live)
    ├── Promote New Properties → Live Table
    ├── Update Changed Properties → Live Table
    └── Generate Sync Report
```

### 2. Manual Sync Pipeline

```
PropertySyncOrchestrator.daily_sync_workflow(session_id):
    ├── Validate Session Data
    ├── Run Data Quality Checks
    ├── Detect Changes (new, updated, removed)
    ├── Batch Promote Properties to Live Table
    ├── Update Sync Logs
    └── Generate Performance Report
```

### 3. Error Handling Flow

```
Error Occurs →
    ├── Log to Database (scraping_errors table)
    ├── Update Session Status
    ├── Continue with Next Item
    └── Report in Final Summary

Service Errors →
    ├── Log to sync_logs table
    ├── Mark sync as failed/partial
    ├── Preserve staging data for retry
    └── Generate error report for debugging
```

### 4. Captcha Handling Flow

```
Page Load → Captcha Detected? →
    ├── Yes: Extract Captcha Info → Solve via API → Inject Solution → Retry
    └── No: Continue Normal Processing
```

## Configuration Management

### Environment Variables:
- **SUPABASE_URL**: Database connection URL
- **SUPABASE_ANON_KEY**: Database authentication key
- **SOLVECAPTCHA_API_KEY**: Captcha solving service key
- **MAX_PAGES**: Scraping scope configuration
- **DELAY_BETWEEN_PAGES**: Rate limiting configuration

### Browser Configuration:
```python
BrowserConfig(
    headless=True,
    java_script_enabled=True,
    user_agent="Mozilla/5.0...",
    browser_type="chromium"
)
```

### Extraction Schema:
CSS selector-based configuration for property data extraction with fallback selectors for different page layouts.

## Enhanced Data Parsing Architecture (v1.3)

### Intelligent Data Extraction Pipeline

The scraper implements a sophisticated multi-stage parsing pipeline that transforms raw HTML into structured, categorized property data:

#### Stage 1: Raw Data Extraction
- **CSS Selectors**: Multi-tier selector strategy with fallbacks
- **HTML Parsing**: BeautifulSoup with lxml backend for robust parsing
- **Data Collection**: Extracts raw text, URLs, and attributes from both listing and detail pages

#### Stage 2: Intelligent Processing
- **Property Type Detection**: Keyword-based classification (house/apartment/lot) from title/description
- **Agent/Agency Separation**: Heuristic parsing to separate agent names from agency names
- **Temporal Parsing**: Converts "Published X ago" text to actual dates
- **Location Parsing**: Splits location strings into address components
- **Amenities Categorization**: Maps raw amenities to structured categories

#### Stage 3: Data Normalization
- **Text Cleaning**: Removes prefixes like "Published by", normalizes whitespace
- **Currency Detection**: Multi-currency support with automatic detection
- **Unit Conversion**: Area measurements with m² parsing
- **Contact Extraction**: Regex-based phone and email extraction

### Enhanced Parsing Methods

#### Property Type Classification
```python
def extract_property_type_from_text(text: str) -> str:
    # Keyword mapping for Spanish/English terms
    type_mappings = {
        'house': ['house', 'casa', 'home', 'villa', 'chalet'],
        'apartment': ['apartment', 'departamento', 'depto', 'condo'],
        'lot': ['lot', 'lote', 'terreno', 'land', 'plot']
    }
    # Returns classified type with fallback to 'house'
```

#### Agent/Agency Intelligence
```python
def extract_agent_and_agency_info(agent_text: str) -> Dict:
    # Removes "Published by" prefixes
    # Uses heuristics to separate person names from company names
    # Handles multiple formats: "Agent\nAgency" or "Agency\nAgent"
    # Extracts contact information with regex patterns
```

#### Temporal Data Processing
```python
def extract_listing_date(published_text: str, scraped_at: str) -> str:
    # Parses: "Published 2 hours ago", "Published a minute ago"
    # Supports: minutes, hours, days, weeks, months, years
    # Calculates actual date by subtracting from scraped timestamp
```

#### Structured Amenities System
```python
def structure_amenities(amenities_data: Dict) -> Dict:
    # Categories mirror website structure:
    # - exterior: parking, garden, balcony, terrace
    # - general: elevator, security, furnished, kitchen
    # - policies: pets allowed, smoking rules
    # - recreation: pool, gym, tennis court, games room
    # Multi-language keyword matching (Spanish/English)
```

### Database Schema Integration

#### New Schema Features (v1.3)
```sql
-- Enhanced columns for parsing improvements
gps_coordinates VARCHAR(255)  -- Combined "lat,lng" format
message_url TEXT             -- Send message button URL
amenities JSONB             -- Structured categories instead of boolean flags

-- Structured amenities example:
{
  "exterior": ["covered_parking", "garden"],
  "general": ["elevator", "24_hour_security"],
  "policies": ["pets_allowed"],
  "recreation": ["pool", "gym"]
}
```

#### Migration Strategy
- **Backward Compatible**: Preserves existing data during schema updates
- **Gradual Migration**: Optional removal of deprecated boolean columns
- **Data Transformation**: Converts old boolean flags to new structured format

### Quality Assurance

#### Parsing Validation
- **Unit Tests**: Comprehensive test suite for all parsing methods
- **Edge Case Handling**: Graceful handling of malformed data
- **Fallback Mechanisms**: Default values for missing or invalid data
- **Error Logging**: Detailed logging for debugging parsing issues

#### Data Integrity
- **Input Sanitization**: Cleaning and validation of extracted text
- **Type Validation**: Ensures data types match database schema
- **Duplicate Detection**: Property ID generation prevents duplicates
- **Consistency Checks**: Validates relationships between fields

## Performance Characteristics

### Scalability Features:
- **Async Processing**: Non-blocking I/O operations
- **Session Persistence**: Resume capability for large scraping jobs
- **Rate Limiting**: Configurable delays to respect target site resources
- **Batch Processing**: Efficient database operations with upserts

### Resource Management:
- **Memory Efficiency**: Streaming data processing
- **Connection Pooling**: Reused database connections
- **Browser Resource Management**: Proper cleanup of browser instances
- **Error Recovery**: Graceful handling of failures without stopping entire process

## Security Considerations

### Anti-Detection Measures:
- **Realistic Browser Headers**: Proper User-Agent strings
- **Rate Limiting**: Respectful request timing
- **Session Management**: Proper cleanup and resource management
- **Error Handling**: Avoid patterns that might trigger additional blocking

### Data Security:
- **Environment Variables**: Secure credential management
- **Row Level Security**: Database access controls
- **Input Validation**: Data sanitization and validation
- **SQL Injection Prevention**: Parameterized queries through Supabase client

## Monitoring and Observability

### Logging Architecture:
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inmuebles24_scraper.log'),
        logging.StreamHandler()
    ]
)
```

### Metrics Tracking:
- **Session Progress**: Pages scraped, properties found/inserted/updated
- **Error Rates**: Failed requests and categorized errors
- **Performance Metrics**: Timing data and resource usage
- **Database Growth**: Property count and data volume tracking

### Debugging Tools:
- **Debug Scraper**: Connectivity and basic functionality testing
- **Verbose Logging**: Detailed execution traces
- **Error Categorization**: Structured error reporting
- **Test Suite**: Comprehensive validation tools

## Deployment Architecture

### Dependencies:
```
Python 3.8+ → Core Runtime
Crawl4AI → Web Crawling Engine
Playwright → Browser Automation
Supabase → Database Client
SolveCaptcha → Anti-Captcha Service
BeautifulSoup → HTML Parsing
```

### Infrastructure Requirements:
- **Python Environment**: Virtual environment with dependencies
- **Browser Binaries**: Playwright browser installation
- **Network Access**: HTTPS connectivity to target sites and APIs
- **Database Access**: Supabase connection with appropriate permissions

### Configuration Files:
- **requirements.txt**: Python dependencies
- **schema.sql**: Database schema definition
- **.env**: Environment configuration
- **env_example.txt**: Configuration template

## Extension Points

### Customization Options:
1. **Target URLs**: Modify `target_url` for different property types
2. **CSS Selectors**: Update `property_schema` for site changes
3. **Data Fields**: Extend database schema for additional property attributes
4. **Processing Logic**: Custom data cleaning and validation rules
5. **Captcha Services**: Alternative captcha solving providers

### Integration Capabilities:
- **Multiple Sites**: Extend for other real estate platforms
- **Data Export**: Additional output formats (CSV, JSON, XML)
- **Notification Systems**: Email/Slack alerts for completion/errors
- **Scheduling**: Cron job integration for automated runs
- **Analytics**: Data analysis and reporting capabilities

## Best Practices Implementation

### Code Quality:
- **Type Hints**: Comprehensive type annotations
- **Error Handling**: Graceful failure handling
- **Documentation**: Inline comments and docstrings
- **Modular Design**: Separation of concerns
- **Configuration Management**: Externalized configuration

### Operational Excellence:
- **Logging**: Comprehensive logging at appropriate levels
- **Monitoring**: Progress tracking and error reporting
- **Testing**: Automated testing framework
- **Documentation**: User guides and technical documentation
- **Version Control**: Git-based version management

## Version History & Evolution

### v2.0 (Dual-Table Architecture with Services Layer)
- **Dual-Table Architecture**: Staging table (`property_scrapes_staging`) and live table (`properties_live`)
- **Services Architecture**: Modular data management with PropertySyncOrchestrator, ChangeDetectionService, DataSyncService, and DataQualityService
- **Enhanced Scraper**: `enhanced_property_scraper.py` with auto-sync capabilities
- **Change Detection**: Intelligent detection of new, updated, and removed properties
- **Data Quality Validation**: Configurable validation rules with quality scoring
- **Session-Based Sync**: Track and sync specific scraping sessions
- **Batch Processing**: Efficient bulk operations for large datasets
- **Comprehensive Logging**: Detailed sync logs and performance tracking
- **Error Recovery**: Robust error handling with retry capabilities

### v1.3 (Enhanced Parsing Architecture)
- **Intelligent Property Type Detection**: Automatic classification from text content
- **Agent/Agency Separation**: Smart parsing of contact information
- **Structured Amenities System**: Categorized amenities mirroring website structure
- **Temporal Data Processing**: Listing date calculation from relative timestamps
- **Enhanced Location Handling**: GPS coordinates with reverse geocoding support
- **Message URL Extraction**: Contact button link scraping
- **Multi-language Support**: Spanish/English keyword recognition
- **Improved Data Quality**: Advanced text cleaning and normalization

### v1.2 (Schema & Session Tracking)
- Database schema optimization with proper indexing
- Session tracking and progress monitoring
- Error logging and recovery mechanisms

### v1.1 (Core Functionality)
- Basic property data extraction
- Supabase database integration
- Rate limiting and anti-bot measures

### v1.0 (Initial Release)
- Foundation web scraping architecture
- Crawl4AI integration with Playwright backend

This architecture provides a robust, scalable, and maintainable foundation for web scraping operations while respecting target site resources and handling common challenges like anti-bot measures. The v1.3 enhancements significantly improve data quality and structure, making the scraped data more useful for analysis and application development. 