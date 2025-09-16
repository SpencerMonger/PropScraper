# Inmuebles24.com Property Scraper

A Python script that uses Crawl4AI to scrape property listings from inmuebles24.com and store the data in a Supabase database.

## Features

- **Comprehensive Data Extraction**: Scrapes all property metadata including price, location, specifications, images, and contact information
- **Database Storage**: Stores data in Supabase with proper schema and indexing
- **Session Tracking**: Tracks scraping progress and handles errors gracefully
- **Rate Limiting**: Respects website resources with configurable delays
- **Pagination Support**: Automatically handles multiple pages of listings
- **Error Handling**: Logs errors and failed URLs for debugging
- **Resume Capability**: Can resume interrupted scraping sessions

## Prerequisites

- Python 3.8 or higher
- Supabase account and project
- Git (for cloning the repository)

## Installation

1. **Clone or download the project files**:
   ```bash
   # If using git
   git clone <your-repo-url>
   cd propscraper
   
   # Or download the files directly to your project directory
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Playwright browsers** (required by Crawl4AI):
   ```bash
   playwright install
   ```

## Database Setup

1. **Create a Supabase project** at [supabase.com](https://supabase.com)

2. **Run the SQL schema** in your Supabase SQL editor:
   - Open your Supabase project dashboard
   - Go to the SQL Editor
   - Copy and paste the contents of `schema.sql`
   - Execute the SQL to create all necessary tables

3. **Get your Supabase credentials**:
   - Go to Project Settings > API
   - Copy your Project URL and anon public key

## Configuration

1. **Create environment file**:
   ```bash
   # Copy the example file
   cp env_example.txt .env
   ```

2. **Edit the .env file** with your Supabase credentials:
   ```env
   SUPABASE_URL=your_supabase_project_url_here
   SUPABASE_ANON_KEY=your_supabase_anon_key_here
   ```

## Usage

### Basic Usage

Run the scraper with default settings (10 pages):

```bash
python inmuebles24_scraper.py
```

### Advanced Usage

You can customize the scraping parameters by editing the `main()` function in `inmuebles24_scraper.py`:

```python
async def main():
    scraper = Inmuebles24Scraper()
    
    # Customize these parameters
    MAX_PAGES = 50      # Number of pages to scrape
    START_PAGE = 1      # Starting page number
    
    await scraper.scrape_all_pages(max_pages=MAX_PAGES, start_page=START_PAGE)
```

### Different Property Types

To scrape different property types, modify the `target_url` in the `Inmuebles24Scraper` class:

```python
# For apartments/condos
self.target_url = "https://www.inmuebles24.com/departamentos-en-venta.html"

# For rentals
self.target_url = "https://www.inmuebles24.com/casas-en-renta.html"

# For commercial properties
self.target_url = "https://www.inmuebles24.com/oficinas-en-venta.html"
```

## Database Schema

The scraper creates three main tables:

### `pulled_properties` table
Stores all property data including:
- Basic info (title, description, type, operation)
- Location data (address, city, coordinates)
- Property details (price, bedrooms, bathrooms, area)
- Features and amenities (JSON fields)
- Media URLs (images, videos, virtual tours)
- Agent contact information
- Metadata (scraping timestamps, source URLs)

### `scraping_sessions` table
Tracks scraping progress:
- Session information and status
- Progress counters
- Error tracking
- Timing data

### `scraping_errors` table
Logs errors and failed URLs for debugging

## Monitoring and Logging

The scraper provides comprehensive logging:

- **Console output**: Real-time progress updates
- **Log file**: Detailed logs saved to `inmuebles24_scraper.log`
- **Database tracking**: Session progress stored in Supabase

### Log levels:
- `INFO`: General progress information
- `ERROR`: Errors and exceptions
- `DEBUG`: Detailed debugging information

## Handling Captchas and Rate Limiting

The scraper includes comprehensive captcha-solving capabilities and anti-bot measures:

### Automatic Captcha Solving
1. **SolveCaptcha Integration**: Uses professional API service to solve captchas
2. **Supported Types**: Cloudflare Turnstile, reCAPTCHA v2/v3, hCaptcha
3. **Automatic Detection**: Detects captchas in page content automatically
4. **Retry Logic**: Attempts multiple solutions if first attempt fails

### Anti-Bot Measures
1. **Realistic browser headers**: Uses proper User-Agent strings
2. **Rate limiting**: 2-second delays between pages (configurable)
3. **Error handling**: Graceful handling of failed requests
4. **Session tracking**: Ability to resume interrupted sessions

### Captcha Setup
1. Sign up for [SolveCaptcha](https://solvecaptcha.com)
2. Get your API key from the dashboard
3. Add `SOLVECAPTCHA_API_KEY=your_key_here` to your `.env` file
4. The scraper will automatically handle captchas when encountered

If you still encounter issues:
- Increase the delay between pages
- Run smaller batches (fewer pages at a time)
- Consider using residential proxies (advanced)

## Data Fields Extracted

The scraper extracts the following property data:

- **Basic Information**:
  - Title and description
  - Property type (casa, departamento, etc.)
  - Operation type (venta, renta)

- **Location**:
  - Full address
  - Neighborhood
  - City and state
  - Coordinates (when available)

- **Property Details**:
  - Price and currency
  - Bedrooms and bathrooms
  - Total area and covered area
  - Parking spaces
  - Floor information

- **Features**:
  - Pool, garden, elevator
  - Security features
  - Pet-friendly status
  - Furnished status

- **Media**:
  - Property images
  - Virtual tour links
  - Video URLs

- **Contact Information**:
  - Agent name and contact details
  - Agency information

## Troubleshooting

### Common Issues

1. **"Failed to create scraping session"**:
   - Check your Supabase credentials
   - Ensure the database schema is properly created
   - Verify network connectivity

2. **"No properties found"**:
   - The website structure may have changed
   - Update the CSS selectors in `property_schema`
   - Check if the target URL is accessible

3. **Captcha or blocking**:
   - Increase delays between requests
   - Reduce the number of concurrent requests
   - Use different User-Agent strings

4. **Memory issues**:
   - Process smaller batches
   - Restart the scraper periodically
   - Monitor system resources

### Testing Captcha Solver

Test your captcha solver setup:

```bash
python tests/test_captcha_solver.py
```

This will validate:
- API key configuration
- Captcha detection algorithms
- Integration with Crawl4AI

### Debugging

Enable debug logging by modifying the logging configuration:

```python
logging.basicConfig(level=logging.DEBUG)
```

Check the database for error logs:
```sql
SELECT * FROM scraping_errors ORDER BY created_at DESC LIMIT 10;
```

## Legal and Ethical Considerations

- **Respect robots.txt**: Check the website's robots.txt file
- **Rate limiting**: Don't overwhelm the server with requests
- **Terms of service**: Review the website's terms of service
- **Data usage**: Use scraped data responsibly and legally
- **Attribution**: Consider crediting the data source

## Contributing

To contribute to this project:

1. Test the scraper with different property types
2. Improve CSS selectors for better data extraction
3. Add support for additional filters and search parameters
4. Enhance error handling and recovery mechanisms
5. Add data validation and cleaning functions

## License

This project is for educational and research purposes. Please ensure compliance with applicable laws and website terms of service.

## Support

For issues and questions:

1. Check the logs in `inmuebles24_scraper.log`
2. Review the error entries in the database
3. Verify your environment configuration
4. Test with a smaller number of pages first

## Version History

- **v1.0**: Initial release with basic scraping functionality
- **v1.1**: Added session tracking and error logging
- **v1.2**: Improved data extraction and database schema #   P r o p S c r a p e r  
 