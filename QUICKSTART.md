# PropScraper Quick Start Guide

Get up and running with the Pincali.com property scraper in minutes!

## Prerequisites

- Python 3.8 or higher
- Supabase account (free tier works fine)

## 1. Setup Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install

# Create environment file
cp env_example.txt .env
```

## 2. Configure Supabase

1. Create a new project at [supabase.com](https://supabase.com)
2. Go to Settings → API to get your credentials
3. Edit `.env` file:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_ANON_KEY=your-anon-key-here
   ```

## 3. Setup Database

1. Open your Supabase project dashboard
2. Go to SQL Editor
3. Copy and paste the contents of `schema.sql`
4. Click "Run" to create the tables

## 4. Test Your Setup

```bash
# Verify everything is working
python test_setup.py

# Test the scraper (opens browser window)
python debug_pincali_scraper.py
```

## 5. Start Scraping

```bash
# Run the main scraper
python pincali_scraper.py
```

The scraper will:
- Start with 10 pages by default
- Show progress in the console
- Save data to your Supabase database
- Handle errors gracefully

## 6. View Your Data

Go to your Supabase dashboard → Table Editor → `pulled_properties` to see the scraped data.

## Customization

Edit `pincali_scraper.py` to change:
- `MAX_PAGES = 50` - Number of pages to scrape
- `START_PAGE = 1` - Starting page number

## Troubleshooting

### Common Issues:

1. **"Module not found" errors**
   ```bash
   pip install -r requirements.txt
   playwright install
   ```

2. **Supabase connection errors**
   - Check your `.env` file has correct credentials
   - Verify your Supabase project is active

3. **No properties found**
   - Run `debug_pincali_scraper.py` to test connectivity
   - Check the generated `pincali_debug.html` file

4. **Browser issues**
   ```bash
   playwright install chromium
   ```

### Getting Help

- Check the logs in `pincali_scraper.log`
- Review the database tables `scraping_sessions` and `scraping_errors`
- Read the full documentation in `README.md`

## What's Scraped

The scraper extracts:
- Property titles and descriptions
- Prices and currency
- Location information
- Property details (bedrooms, bathrooms, area)
- Images and contact information
- Features and amenities
- All metadata for comprehensive analysis

Happy scraping! 🏠✨ 