# PropScraper Architecture Documentation

## Overview

PropScraper is a Python-based web scraping system designed to extract property listings from real estate websites and store them in a Supabase database. Currently supports Pincali.com (Mexico) as the primary target and Inmuebles24.com (Mexico) for legacy support. The system is built with modern async/await patterns, comprehensive error handling, captcha solving capabilities, and robust data persistence.

## System Architecture

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Targets   │    │   PropScraper   │    │   Supabase DB   │
│  (Pincali.com   │◄──►│     System      │◄──►│   (PostgreSQL)  │
│ Inmuebles24.com)│    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │ SolveCaptcha    │
                    │     API         │
                    └─────────────────┘
```

### Core Components

1. **Pincali Scraper Engine** (`pincali_scraper.py`) - Primary scraper
2. **Inmuebles24 Scraper Engine** (`inmuebles24_scraper.py`) - Legacy scraper
3. **Captcha Solver** (`captcha_solver.py`)
4. **Database Schema** (`schema.sql`)
5. **Debug Tools** (`debug_pincali_scraper.py`, `test_setup.py`)
6. **Testing Framework** (`tests/` directory)
7. **Configuration Management** (`.env` files)

## Component Architecture

### 1. Main Scraper Engine (`Inmuebles24Scraper`)

The core scraping engine is implemented as a single class with the following responsibilities:

#### Key Components:
- **Session Management**: Tracks scraping progress and handles interruptions
- **Data Extraction**: Parses property listings using CSS selectors
- **Data Cleaning**: Normalizes and validates extracted data
- **Database Operations**: Handles CRUD operations with Supabase
- **Error Handling**: Comprehensive logging and error recovery

#### Architecture Pattern:
```python
class Inmuebles24Scraper:
    def __init__():
        # Initialize browser config, database client, CSS selectors
    
    async def scrape_all_pages():
        # Main orchestration method
        # Creates session → Scrapes pages → Saves data → Updates progress
    
    async def scrape_property_list_page():
        # Individual page scraping logic
    
    def extract_property_details():
        # Data transformation and cleaning
    
    async def save_property():
        # Database persistence with upsert logic
```

#### Technology Stack:
- **Crawl4AI**: Modern async web crawling with Playwright backend
- **Supabase**: PostgreSQL database with real-time capabilities
- **BeautifulSoup**: HTML parsing and data extraction
- **AsyncIO**: Asynchronous execution for better performance

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
The system uses a PostgreSQL database through Supabase with three main tables:

##### `pulled_properties` Table:
```sql
- id (UUID, Primary Key)
- Basic Info: title, description, property_type, operation_type
- Location: address, neighborhood, city, state, coordinates
- Details: price, currency, bedrooms, bathrooms, area measurements
- Features: JSON fields for amenities and characteristics
- Media: image URLs, virtual tours, videos
- Agent Info: contact details and agency information
- Metadata: source URLs, scraping timestamps, page numbers
- Search: Full-text search vector for Spanish content
```

##### `scraping_sessions` Table:
```sql
- Session tracking and progress monitoring
- Status management (running, completed, failed, paused)
- Performance metrics (pages scraped, properties found/inserted/updated)
- Error tracking and timing data
```

##### `scraping_errors` Table:
```sql
- Detailed error logging with URL and session context
- Error categorization and retry tracking
- Debugging information for failed operations
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

### 1. Scraping Pipeline

```
Start Session → Get Total Pages → For Each Page:
    ├── Fetch Page (with captcha handling)
    ├── Extract Properties (CSS selectors)
    ├── Clean Data (normalize, validate)
    ├── Save to Database (upsert logic)
    ├── Update Progress
    └── Rate Limit Delay
```

### 2. Error Handling Flow

```
Error Occurs →
    ├── Log to Database (scraping_errors table)
    ├── Update Session Status
    ├── Continue with Next Item
    └── Report in Final Summary
```

### 3. Captcha Handling Flow

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

This architecture provides a robust, scalable, and maintainable foundation for web scraping operations while respecting target site resources and handling common challenges like anti-bot measures. 