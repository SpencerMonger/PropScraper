#!/usr/bin/env python3
"""
CLI Interface for Hybrid 4-Tier Property Sync System

Provides commands for:
- Running individual tiers
- Running scheduled tiers
- Viewing schedule status
- Monitoring queue statistics
- Viewing sync history

Usage:
    python tier_sync_cli.py status
    python tier_sync_cli.py run-tier 1
    python tier_sync_cli.py run-scheduled
    python tier_sync_cli.py queue-stats
    python tier_sync_cli.py history
    python tier_sync_cli.py summary

Based on HYBRID_SYNC_IMPLEMENTATION_PROMPT.md
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client, Client

from config.tier_config import get_config, TierLevel
from services.manifest_scan_service import ManifestScanService
from services.property_diff_service import PropertyDiffService
from services.scrape_queue_service import ScrapeQueueService
from services.tier_orchestrator import TierOrchestrator
from services.scheduler_service import SchedulerService

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('tier_sync.log')
    ]
)
logger = logging.getLogger(__name__)


def get_supabase_client() -> Client:
    """Create and return a Supabase client."""
    url = os.getenv('SUPABASE_URL')
    # Support both SUPABASE_KEY and SUPABASE_ANON_KEY for compatibility
    key = os.getenv('SUPABASE_KEY') or os.getenv('SUPABASE_ANON_KEY')
    
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_ANON_KEY environment variables required")
        sys.exit(1)
    
    return create_client(url, key)


def create_services(supabase: Client, include_scraper: bool = False):
    """
    Create all required service instances.
    
    Args:
        supabase: Supabase client instance
        include_scraper: If True, include the EnhancedPincaliScraper for queue processing
    """
    config = get_config()
    
    manifest_service = ManifestScanService(supabase, config)
    diff_service = PropertyDiffService(supabase, config)
    queue_service = ScrapeQueueService(supabase, config)
    
    # Create scraper if needed for queue processing
    scraper = None
    if include_scraper:
        from enhanced_property_scraper import EnhancedPincaliScraper
        scraper = EnhancedPincaliScraper()
        logger.info("Scraper initialized for queue processing")
    
    orchestrator = TierOrchestrator(supabase, config, scraper)
    scheduler = SchedulerService(supabase, config, orchestrator)
    
    return {
        'config': config,
        'manifest': manifest_service,
        'diff': diff_service,
        'queue': queue_service,
        'orchestrator': orchestrator,
        'scheduler': scheduler,
        'scraper': scraper
    }


async def cmd_status(args):
    """Show current schedule status for all tiers."""
    print("\n" + "=" * 60)
    print("TIER SYNC SCHEDULE STATUS")
    print("=" * 60)
    
    supabase = get_supabase_client()
    services = create_services(supabase)
    scheduler = services['scheduler']
    config = services['config']
    
    status = await scheduler.get_schedule_status()
    
    print(f"\nLast Updated: {status.last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    if status.current_running_tier:
        print(f"\n⚡ CURRENTLY RUNNING: Tier {status.current_running_tier}")
    
    print("\n" + "-" * 60)
    print(f"{'Tier':<20} {'Last Run':<20} {'Next Run':<15} {'Status':<10}")
    print("-" * 60)
    
    for tier in status.tiers:
        last_run = tier.last_run_at.strftime('%Y-%m-%d %H:%M') if tier.last_run_at else 'Never'
        next_run = tier.next_run_at.strftime('%Y-%m-%d %H:%M') if tier.next_run_at else 'Now'
        
        if tier.is_running:
            tier_status = '🔄 Running'
        elif tier.is_due:
            tier_status = '⏰ Due'
        elif tier.last_run_success:
            tier_status = '✅ OK'
        else:
            tier_status = '❌ Failed' if tier.last_run_at else '⏳ Pending'
        
        tier_settings = config.get_tier(tier.tier_level)
        print(f"T{tier.tier_level}: {tier_settings.name:<16} {last_run:<20} {next_run:<15} {tier_status:<10}")
    
    print("-" * 60)
    
    # Show tier frequency info
    print("\nTier Frequencies:")
    for level in [1, 2, 3, 4]:
        tier = config.get_tier(level)
        print(f"  T{level} ({tier.name}): Every {tier.frequency_hours}h")
    
    print()


async def cmd_run_tier(args):
    """Run a specific tier with full queue processing."""
    tier_level = args.tier
    
    if tier_level not in [1, 2, 3, 4]:
        print(f"Error: Invalid tier level {tier_level}. Must be 1-4.")
        sys.exit(1)
    
    supabase = get_supabase_client()
    services = create_services(supabase, include_scraper=True)
    scheduler = services['scheduler']
    config = services['config']
    
    tier_settings = config.get_tier(tier_level)
    
    print(f"\n{'=' * 60}")
    print(f"RUNNING TIER {tier_level}: {tier_settings.name} (with queue processing)")
    print(f"{'=' * 60}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Initializing scraper for queue processing...")
    print()
    
    try:
        result = await scheduler.run_single_tier(tier_level, force=args.force)
        
        print("\n" + "-" * 60)
        print("RESULTS:")
        print("-" * 60)
        print(f"Status: {'✅ Success' if result.success else '❌ Failed'}")
        print(f"Duration: {result.duration_seconds:.1f} seconds")
        print(f"Pages Scanned: {result.pages_scanned}")
        print(f"New Properties: {result.new_properties}")
        print(f"Price Changes: {result.price_changes}")
        print(f"Removals Confirmed: {result.removals_confirmed}")
        print(f"Properties Queued: {result.properties_queued}")
        print(f"Properties Scraped: {result.properties_scraped}")
        
        if result.errors:
            print("\nErrors:")
            for error in result.errors:
                print(f"  - {error}")
        
        print()
        
    except Exception as e:
        print(f"\nError running tier {tier_level}: {e}")
        logger.exception("Tier execution error")
        sys.exit(1)


async def cmd_run_scheduled(args):
    """Run all scheduled (due) tiers with full queue processing."""
    print("\n" + "=" * 60)
    print("RUNNING SCHEDULED TIERS (with queue processing)")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Initializing scraper for queue processing...")
    
    supabase = get_supabase_client()
    services = create_services(supabase, include_scraper=True)
    scheduler = services['scheduler']
    
    try:
        results = await scheduler.run_scheduled_tiers()
        
        if not results:
            print("\nNo tiers were due to run.")
        else:
            print(f"\nCompleted {len(results)} tier(s):")
            print("-" * 60)
            
            for result in results:
                status = '✅' if result.success else '❌'
                print(f"{status} Tier {result.tier_level} ({result.tier_name}): "
                      f"{result.duration_seconds:.1f}s, "
                      f"{result.new_properties} new, "
                      f"{result.price_changes} price changes, "
                      f"{result.properties_scraped} scraped")
        
        print()
        
    except Exception as e:
        print(f"\nError running scheduled tiers: {e}")
        logger.exception("Scheduled run error")
        sys.exit(1)


async def cmd_queue_stats(args):
    """Show scrape queue statistics."""
    print("\n" + "=" * 60)
    print("SCRAPE QUEUE STATISTICS")
    print("=" * 60)
    
    supabase = get_supabase_client()
    services = create_services(supabase)
    queue_service = services['queue']
    
    try:
        stats = await queue_service.get_queue_stats()
        
        print(f"\nTotal Queued: {stats.total_queued}")
        print(f"Total Pending: {stats.total_pending}")
        print(f"Total Processing: {stats.total_processing}")
        print(f"Completed Today: {stats.completed_today}")
        print(f"Failed Today: {stats.failed_today}")
        
        if stats.by_priority:
            print("\nBy Priority:")
            for priority, count in sorted(stats.by_priority.items(), reverse=True):
                print(f"  Priority {priority}: {count}")
        
        if stats.by_reason:
            print("\nBy Queue Reason:")
            for reason, count in stats.by_reason.items():
                print(f"  {reason}: {count}")
        
        print()
        
    except Exception as e:
        print(f"\nError getting queue stats: {e}")
        logger.exception("Queue stats error")
        sys.exit(1)


async def cmd_history(args):
    """Show sync run history."""
    print("\n" + "=" * 60)
    print("SYNC RUN HISTORY")
    print("=" * 60)
    
    supabase = get_supabase_client()
    services = create_services(supabase)
    scheduler = services['scheduler']
    
    tier_filter = args.tier if hasattr(args, 'tier') and args.tier else None
    limit = args.limit if hasattr(args, 'limit') and args.limit else 10
    
    try:
        history = await scheduler.get_tier_history(tier_filter, limit)
        
        if not history:
            print("\nNo sync runs found.")
        else:
            print(f"\nShowing {len(history)} most recent runs:")
            print("-" * 90)
            print(f"{'Tier':<10} {'Started':<20} {'Status':<12} {'Duration':<10} {'New':<8} {'Changes':<10}")
            print("-" * 90)
            
            for run in history:
                tier = f"T{run.get('tier_level', '?')}"
                started = run.get('started_at', '')[:19].replace('T', ' ')
                status = run.get('status', 'unknown')
                duration_ms = run.get('execution_time_ms', 0) or 0
                duration = f"{duration_ms / 1000:.1f}s" if duration_ms else '-'
                new_props = run.get('new_properties_found', 0) or 0
                changes = run.get('price_changes_detected', 0) or 0
                
                status_icon = {
                    'completed': '✅',
                    'failed': '❌',
                    'running': '🔄',
                    'cancelled': '⏹️'
                }.get(status, '❓')
                
                print(f"{tier:<10} {started:<20} {status_icon} {status:<10} {duration:<10} {new_props:<8} {changes:<10}")
        
        print("-" * 90)
        print()
        
    except Exception as e:
        print(f"\nError getting history: {e}")
        logger.exception("History error")
        sys.exit(1)


async def cmd_summary(args):
    """Show sync summary statistics."""
    days = args.days if hasattr(args, 'days') and args.days else 7
    
    print("\n" + "=" * 60)
    print(f"SYNC SUMMARY (Last {days} days)")
    print("=" * 60)
    
    supabase = get_supabase_client()
    services = create_services(supabase)
    scheduler = services['scheduler']
    
    try:
        summary = await scheduler.get_sync_summary(days)
        
        if not summary:
            print("\nNo data available.")
        else:
            print(f"\nTotal Runs: {summary.get('total_runs', 0)}")
            print(f"  Successful: {summary.get('successful_runs', 0)}")
            print(f"  Failed: {summary.get('failed_runs', 0)}")
            print(f"\nTotal New Properties: {summary.get('total_new_properties', 0):,}")
            print(f"Total Price Changes: {summary.get('total_price_changes', 0):,}")
            print(f"Total Removals: {summary.get('total_removals', 0):,}")
            print(f"Total Scraped: {summary.get('total_scraped', 0):,}")
            print(f"\nAvg Duration: {summary.get('average_duration_seconds', 0):.1f} seconds")
            
            if summary.get('by_tier'):
                print("\nBy Tier:")
                print("-" * 50)
                for tier_name, tier_stats in summary['by_tier'].items():
                    print(f"  {tier_name}:")
                    print(f"    Runs: {tier_stats.get('runs', 0)} "
                          f"({tier_stats.get('successful', 0)} successful)")
                    print(f"    New: {tier_stats.get('new_properties', 0):,}, "
                          f"Changes: {tier_stats.get('price_changes', 0):,}")
        
        print()
        
    except Exception as e:
        print(f"\nError getting summary: {e}")
        logger.exception("Summary error")
        sys.exit(1)


async def cmd_process_queue(args):
    """Process pending items in the scrape queue."""
    batch_size = args.batch_size if hasattr(args, 'batch_size') and args.batch_size else 50
    rate_limit = args.rate_limit if hasattr(args, 'rate_limit') and args.rate_limit else 2.0
    
    print("\n" + "=" * 60)
    print(f"PROCESSING SCRAPE QUEUE (Batch Size: {batch_size})")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Rate Limit: {rate_limit}s between requests")
    print("Initializing scraper...")
    
    supabase = get_supabase_client()
    services = create_services(supabase, include_scraper=True)
    orchestrator = services['orchestrator']
    config = services['config']
    
    try:
        # Create a scraping session for this queue processing run
        session_response = supabase.table('scraping_sessions').insert({
            'session_name': f"Queue Processing {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            'base_url': config.base_url,
            'status': 'running'
        }).execute()
        
        session_id = session_response.data[0]['id']
        print(f"Session ID: {session_id}")
        print(f"Scraper: Initialized")
        
        result = await orchestrator.process_scrape_queue(batch_size, rate_limit, session_id)
        
        # Update session status
        supabase.table('scraping_sessions').update({
            'status': 'completed',
            'completed_at': datetime.now().isoformat()
        }).eq('id', session_id).execute()
        
        print("\n" + "-" * 60)
        print("RESULTS:")
        print("-" * 60)
        print(f"Duration: {result.duration_seconds:.1f} seconds")
        print(f"Processed: {result.processed}")
        print(f"Succeeded: {result.succeeded}")
        print(f"Failed: {result.failed}")
        
        # Get remaining queue count
        queue_stats = await orchestrator.queue_service.get_queue_stats()
        print(f"Remaining in Queue: {queue_stats.pending_count}")
        
        print()
        
    except Exception as e:
        print(f"\nError processing queue: {e}")
        logger.exception("Queue processing error")
        sys.exit(1)


async def cmd_run_daemon(args):
    """Run the scheduler as a continuous daemon with full queue processing."""
    interval = args.interval if hasattr(args, 'interval') and args.interval else 300
    
    print("\n" + "=" * 60)
    print("STARTING TIER SYNC DAEMON (with queue processing)")
    print("=" * 60)
    print(f"Check Interval: {interval} seconds")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Initializing scraper for queue processing...")
    print("\nPress Ctrl+C to stop...")
    print()
    
    supabase = get_supabase_client()
    services = create_services(supabase, include_scraper=True)
    scheduler = services['scheduler']
    
    try:
        await scheduler.run_continuous(check_interval_seconds=interval)
    except KeyboardInterrupt:
        print("\n\nDaemon stopped by user.")
    except Exception as e:
        print(f"\nDaemon error: {e}")
        logger.exception("Daemon error")
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description='Hybrid 4-Tier Property Sync System CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s status                  Show schedule status for all tiers
  %(prog)s run-tier 1              Run Tier 1 (Hot Listings)
  %(prog)s run-tier 2 --force      Force run Tier 2 even if another is running
  %(prog)s run-scheduled           Run all tiers that are due
  %(prog)s queue-stats             Show scrape queue statistics
  %(prog)s process-queue           Process pending queue items
  %(prog)s history --limit 20      Show last 20 sync runs
  %(prog)s summary --days 14       Show 14-day summary
  %(prog)s daemon --interval 600   Run as daemon, check every 10 minutes
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show schedule status for all tiers')
    status_parser.set_defaults(func=cmd_status)
    
    # Run tier command
    run_tier_parser = subparsers.add_parser('run-tier', help='Run a specific tier')
    run_tier_parser.add_argument('tier', type=int, choices=[1, 2, 3, 4], help='Tier level to run')
    run_tier_parser.add_argument('--force', action='store_true', help='Force run even if another tier is running')
    run_tier_parser.set_defaults(func=cmd_run_tier)
    
    # Run scheduled command
    run_scheduled_parser = subparsers.add_parser('run-scheduled', help='Run all scheduled (due) tiers')
    run_scheduled_parser.set_defaults(func=cmd_run_scheduled)
    
    # Queue stats command
    queue_stats_parser = subparsers.add_parser('queue-stats', help='Show scrape queue statistics')
    queue_stats_parser.set_defaults(func=cmd_queue_stats)
    
    # Process queue command
    process_queue_parser = subparsers.add_parser('process-queue', help='Process pending queue items')
    process_queue_parser.add_argument('--batch-size', type=int, default=50, help='Number of items to process')
    process_queue_parser.add_argument('--rate-limit', type=float, default=2.0, help='Seconds between requests (default: 2.0)')
    process_queue_parser.set_defaults(func=cmd_process_queue)
    
    # History command
    history_parser = subparsers.add_parser('history', help='Show sync run history')
    history_parser.add_argument('--tier', type=int, choices=[1, 2, 3, 4], help='Filter by tier level')
    history_parser.add_argument('--limit', type=int, default=10, help='Number of records to show')
    history_parser.set_defaults(func=cmd_history)
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Show sync summary statistics')
    summary_parser.add_argument('--days', type=int, default=7, help='Number of days to include')
    summary_parser.set_defaults(func=cmd_summary)
    
    # Daemon command
    daemon_parser = subparsers.add_parser('daemon', help='Run as continuous daemon')
    daemon_parser.add_argument('--interval', type=int, default=300, help='Check interval in seconds')
    daemon_parser.set_defaults(func=cmd_run_daemon)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(0)
    
    # Run the async command
    asyncio.run(args.func(args))


if __name__ == '__main__':
    main()

