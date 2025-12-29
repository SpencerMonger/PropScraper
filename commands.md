# PropScraper CLI Commands Reference

## Quick Reference

| Command | Purpose | Typical Use |
|---------|---------|-------------|
| `python tier_sync_cli.py status` | Check tier schedule status | Daily monitoring |
| `python tier_sync_cli.py run-tier 2` | Run daily sync | Manual daily sync |
| `python tier_sync_cli.py process-queue` | Scrape queued properties | After tier runs |
| `python enhanced_property_scraper.py --all-sources` | Full scrape (legacy) | Initial data load |

---

## Hybrid 4-Tier Sync CLI (`tier_sync_cli.py`)

### Check Status

```powershell
# View schedule status for all tiers
python tier_sync_cli.py status
```

Shows:
- Last run time for each tier
- Next scheduled run
- Current status (Running, Due, OK, Failed)

---

### Run Tiers

```powershell
# Run a specific tier (1-4)
python tier_sync_cli.py run-tier 1    # Hot Listings (every 4h)
python tier_sync_cli.py run-tier 2    # Daily Sync (every 24h)
python tier_sync_cli.py run-tier 3    # Weekly Deep Scan
python tier_sync_cli.py run-tier 4    # Monthly Refresh

# Force run even if another tier is running
python tier_sync_cli.py run-tier 2 --force

# Run all tiers that are due according to schedule
python tier_sync_cli.py run-scheduled
```

**Tier Summary:**

| Tier | Name | Frequency | What It Does |
|------|------|-----------|--------------|
| 1 | Hot Listings | 4 hours | Scan first 10 pages for new listings |
| 2 | Daily Sync | 24 hours | Full manifest scan (100 pages/source) |
| 3 | Weekly Deep | 7 days | Refresh stale property data |
| 4 | Monthly | 30 days | Random sample verification |

---

### Process Scrape Queue

```powershell
# Process pending items in the queue (default: 50 items)
python tier_sync_cli.py process-queue

# Process a specific number of items
python tier_sync_cli.py process-queue --batch-size 100

# Adjust rate limiting (default: 2 seconds between requests)
python tier_sync_cli.py process-queue --batch-size 50 --rate-limit 3.0
```

This command:
- Takes items from `scrape_queue` table
- Visits each property's detail page
- Extracts full data (description, amenities, phone numbers)
- Updates `properties_live` table

---

### View Queue Statistics

```powershell
python tier_sync_cli.py queue-stats
```

Shows:
- Total queued items
- Pending vs processing
- Breakdown by priority and reason

---

### View History

```powershell
# Show last 10 sync runs
python tier_sync_cli.py history

# Show last 20 runs
python tier_sync_cli.py history --limit 20

# Filter by tier
python tier_sync_cli.py history --tier 2 --limit 10
```

---

### View Summary

```powershell
# Show 7-day summary
python tier_sync_cli.py summary

# Show 14-day summary
python tier_sync_cli.py summary --days 14
```

Shows:
- Total runs and success rate
- New properties found
- Price changes detected
- Breakdown by tier

---

### Run as Daemon

```powershell
# Run continuously, check every 5 minutes (default)
python tier_sync_cli.py daemon

# Custom check interval (10 minutes)
python tier_sync_cli.py daemon --interval 600
```

Runs continuously and automatically executes tiers when they're due.

---

## Legacy Full Scraper (`enhanced_property_scraper.py`)

### Full Scrape Commands

```powershell
# Scrape all 4 sources (sale, rent, foreclosure, new_construction)
python enhanced_property_scraper.py --all-sources

# Scrape all pages from all sources
python enhanced_property_scraper.py --all-sources --all

# Scrape limited pages per source
python enhanced_property_scraper.py --all-sources --pages 10

# Scrape specific sources only
python enhanced_property_scraper.py --sources sale rent
python enhanced_property_scraper.py --sources foreclosure new_construction

# Skip auto-sync (just populate staging table)
python enhanced_property_scraper.py --all-sources --pages 5 --no-sync
```

### Source Types

| Source | Operation Type | URL Path |
|--------|----------------|----------|
| For Sale | `sale` | `/properties/properties-for-sale` |
| For Rent | `rent` | `/properties/properties-for-rent` |
| Foreclosure | `foreclosure` | `/properties/properties-for-foreclosure` |
| New Construction | `new_construction` | `/properties/under-construction` |

---

## Cookie Management (`get_cookies.py`)

```powershell
# Open browser for manual login, then save cookies
python get_cookies.py
```

Required for extracting phone numbers (authenticated scraping).

---

## Typical Workflows

### Initial Setup (First Time)

```powershell
# 1. Save login cookies for phone extraction
python get_cookies.py

# 2. Run full scrape to populate database
python enhanced_property_scraper.py --all-sources --all
```

### Daily Operations

```powershell
# Option A: Run tier 2 manually
python tier_sync_cli.py run-tier 2
python tier_sync_cli.py process-queue --batch-size 100

# Option B: Run as daemon (automatic)
python tier_sync_cli.py daemon
```

### Check System Health

```powershell
python tier_sync_cli.py status
python tier_sync_cli.py queue-stats
python tier_sync_cli.py summary --days 7
```

---

## Environment Variables

Required in `.env` file:

```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
```

Optional:
```
MAX_PAGES=50
DELAY_BETWEEN_PAGES=2
LOG_LEVEL=INFO
SOLVECAPTCHA_API_KEY=your-key  # For captcha solving
```

