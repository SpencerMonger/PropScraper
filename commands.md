# PropScraper CLI Commands Reference

## Quick Reference

| Command | Purpose | Typical Use |
|---------|---------|-------------|
| `python tier_sync_cli.py run-scheduled` | Run due tiers + process queue | **Main daily command** |
| `python tier_sync_cli.py status` | Check tier schedule status | Daily monitoring |
| `python tier_sync_cli.py run-tier 2` | Run specific tier | Manual trigger |
| `python tier_sync_cli.py process-queue` | Process queue only | Catch up on backlog |
| `python enhanced_property_scraper.py --all-sources` | Full scrape (legacy) | Initial data load |

---

## Hybrid 4-Tier Sync CLI (`tier_sync_cli.py`)

### Main Command: Run Scheduled Tiers

```powershell
# Run all due tiers with full queue processing (RECOMMENDED)
python tier_sync_cli.py run-scheduled
```

This is the **main command** for daily operations. It:
1. Checks which tiers are due based on their schedules
2. Runs manifest scanning for each due tier
3. Queues new/changed properties
4. **Processes the queue** (scrapes property details with Playwright)

---

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

### Run Specific Tier

```powershell
# Run a specific tier (1-4) with full queue processing
python tier_sync_cli.py run-tier 1    # Hot Listings (every 6h)
python tier_sync_cli.py run-tier 2    # Daily Sync (every 24h)
python tier_sync_cli.py run-tier 3    # Weekly Deep Scan
python tier_sync_cli.py run-tier 4    # Monthly Refresh

# Force run even if another tier is running
python tier_sync_cli.py run-tier 2 --force
```

**Tier Summary:**

| Tier | Name | Frequency | What It Does |
|------|------|-----------|--------------|
| 1 | Hot Listings | 6 hours | Scan first 10 pages for new listings |
| 2 | Daily Sync | 24 hours | Full manifest scan (100 pages/source) |
| 3 | Weekly Deep | 7 days | Refresh stale property data |
| 4 | Monthly | 30 days | Random sample verification |

---

### Process Queue Only

```powershell
# Process pending items in the queue (default: 50 items)
python tier_sync_cli.py process-queue

# Process a specific number of items
python tier_sync_cli.py process-queue --batch-size 100

# Adjust rate limiting (default: 2 seconds between requests)
python tier_sync_cli.py process-queue --batch-size 50 --rate-limit 3.0
```

Use this to:
- Catch up on a backlog after interruption
- Process more items than a tier normally handles
- Run with custom rate limiting

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
Includes full queue processing.

---

## Configuration

### YAML Config File (`config/tier_config.yaml`)

All tier parameters can be adjusted in `config/tier_config.yaml`:

```yaml
tiers:
  tier_1:
    frequency_hours: 6        # How often to run
    pages_to_scan: 10         # Pages per source
    delay_between_pages: 1.5  # Rate limiting
    max_queue_items: 500      # Max items per run
  
  tier_2:
    frequency_hours: 24
    pages_to_scan: 100
    # ... etc
```

**Key settings you might want to adjust:**

| Setting | Location | Default | Description |
|---------|----------|---------|-------------|
| `frequency_hours` | `tiers.tier_N` | varies | How often tier runs |
| `pages_to_scan` | `tiers.tier_N` | varies | Pages to scan per source |
| `delay_between_pages` | `tiers.tier_N` | 1.5-3.0 | Seconds between page requests |
| `delay_between_details` | `tiers.tier_N` | 0.5-1.5 | Seconds between detail scrapes |
| `max_queue_items` | `tiers.tier_N` | varies | Max items to process per run |

Changes take effect on the next run (no restart required).

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
# 1. Install dependencies (including pyyaml)
pip install -r requirements.txt

# 2. Save login cookies for phone extraction
python get_cookies.py

# 3. Run full scrape to populate database
python enhanced_property_scraper.py --all-sources --all
```

### Daily Operations (Recommended)

```powershell
# Single command: runs due tiers + processes queue
python tier_sync_cli.py run-scheduled
```

### Manual Tier Run

```powershell
# Run a specific tier manually
python tier_sync_cli.py run-tier 2
```

### Automated Operation

```powershell
# Run as daemon (automatic scheduling)
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

Optional (override YAML config):
```
TIER_1_FREQUENCY_HOURS=6
TIER_1_PAGES=10
TIER_2_FREQUENCY_HOURS=24
TIER_2_PAGES=100
TIER_3_FREQUENCY_DAYS=7
TIER_4_FREQUENCY_DAYS=30
TIER_LOG_LEVEL=INFO
SOLVECAPTCHA_API_KEY=your-key  # For captcha solving
```
