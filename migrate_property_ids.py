#!/usr/bin/env python3
"""
Migrate Property IDs to New Hash-Based Format

This script updates existing property_id values in the database to use
the new centralized hash-based ID format.

This is a NON-DESTRUCTIVE migration - it updates IDs in place rather than
deleting data. Old IDs are logged for reference.

Usage:
    python migrate_property_ids.py                  # Dry run (preview only)
    python migrate_property_ids.py --execute       # Actually perform migration
    python migrate_property_ids.py --execute --table properties_live  # Specific table
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from supabase import create_client, Client

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config.property_id import generate_property_id

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Please set SUPABASE_URL and SUPABASE_ANON_KEY environment variables")
    
    return create_client(supabase_url, supabase_key)


def get_properties_to_migrate(
    supabase: Client,
    table_name: str,
    batch_size: int = 500
) -> List[Dict]:
    """
    Fetch all properties with their current IDs and source URLs.
    
    Args:
        supabase: Supabase client
        table_name: Table to query
        batch_size: Number of records per batch
        
    Returns:
        List of property records
    """
    all_properties = []
    offset = 0
    
    while True:
        try:
            response = supabase.table(table_name).select(
                "id, property_id, source_url"
            ).range(offset, offset + batch_size - 1).execute()
            
            if not response.data:
                break
            
            all_properties.extend(response.data)
            offset += batch_size
            
            if len(response.data) < batch_size:
                break
                
        except Exception as e:
            logger.error(f"Error fetching properties at offset {offset}: {e}")
            break
    
    return all_properties


def is_pincali_slug_format(property_id: str) -> bool:
    """
    Check if property_id is in the OLD Pincali slug format (not hash format).
    
    Old format: pincali_property-name-slug (variable length, contains dashes/words)
    New format: pincali_a3f8b2c1d4e5f6a7 (exactly 16 hex chars after prefix)
    
    Returns True if it's the old slug format that needs migration.
    """
    if not property_id or not property_id.startswith("pincali_"):
        return False
    
    suffix = property_id[8:]  # Everything after "pincali_"
    
    # New hash format is exactly 16 hex characters
    if len(suffix) == 16:
        try:
            int(suffix, 16)  # Try to parse as hex
            return False  # It's already in hash format
        except ValueError:
            pass  # Not hex, might be a slug
    
    # If suffix contains letters that aren't valid hex, or is not 16 chars, it's a slug
    return True


def calculate_migrations(properties: List[Dict]) -> Tuple[List[Dict], Dict[str, int]]:
    """
    Calculate which properties need migration.
    Only migrates Pincali properties in the old slug format.
    Skips EasyBroker properties (eb_*) and already-migrated properties.
    
    Args:
        properties: List of property records
        
    Returns:
        Tuple of (migrations_needed, stats)
    """
    migrations = []
    stats = {
        "total": len(properties),
        "needs_migration": 0,
        "already_correct": 0,
        "no_source_url": 0,
        "no_property_id": 0,
        "skipped_non_pincali": 0
    }
    
    for prop in properties:
        record_id = prop.get("id")
        current_id = prop.get("property_id")
        source_url = prop.get("source_url")
        
        if not source_url:
            stats["no_source_url"] += 1
            continue
        
        if not current_id:
            stats["no_property_id"] += 1
            continue
        
        # Skip non-Pincali properties (e.g., EasyBroker properties starting with eb_)
        if not current_id.startswith("pincali_"):
            stats["skipped_non_pincali"] += 1
            continue
        
        # Check if already in new hash format
        if not is_pincali_slug_format(current_id):
            stats["already_correct"] += 1
            continue
        
        # Calculate what the new ID should be
        new_id = generate_property_id(source_url)
        
        if current_id == new_id:
            stats["already_correct"] += 1
        else:
            migrations.append({
                "id": record_id,
                "old_property_id": current_id,
                "new_property_id": new_id,
                "source_url": source_url
            })
            stats["needs_migration"] += 1
    
    return migrations, stats


def update_related_tables(
    supabase: Client,
    old_property_id: str,
    new_property_id: str
) -> Tuple[int, int]:
    """
    Update related tables that have foreign key references to properties_live.property_id.
    
    Args:
        supabase: Supabase client
        old_property_id: The old property ID being replaced
        new_property_id: The new property ID to use
        
    Returns:
        Tuple of (updated_count, error_count)
    """
    updated = 0
    errors = 0
    
    # Tables that reference properties_live.property_id
    related_tables = [
        ("easybroker_property_mappings", "property_id"),
        ("playlist_properties", "property_id"),
        ("agent_interactions", "property_id"),
    ]
    
    for table_name, column_name in related_tables:
        try:
            # Update the foreign key reference
            response = supabase.table(table_name).update({
                column_name: new_property_id
            }).eq(column_name, old_property_id).execute()
            
            if response.data:
                updated += len(response.data)
                
        except Exception as e:
            # Silently handle - table might not exist or have no matching records
            error_msg = str(e)
            if "does not exist" not in error_msg and "404" not in error_msg:
                logger.debug(f"Could not update {table_name}: {e}")
            errors += 1
    
    return updated, errors


def execute_migrations(
    supabase: Client,
    table_name: str,
    migrations: List[Dict],
    batch_size: int = 50
) -> Tuple[int, int]:
    """
    Execute the property ID migrations with cascading updates to related tables.
    
    Args:
        supabase: Supabase client
        table_name: Table to update
        migrations: List of migration records
        batch_size: Updates per batch
        
    Returns:
        Tuple of (success_count, failure_count)
    """
    success_count = 0
    failure_count = 0
    related_updated = 0
    
    logger.info(f"Executing {len(migrations)} migrations...")
    
    for i, migration in enumerate(migrations):
        try:
            old_id = migration["old_property_id"]
            new_id = migration["new_property_id"]
            
            # Step 1: Update related tables FIRST (before updating the main table)
            if table_name == "properties_live" and old_id:
                rel_updated, _ = update_related_tables(supabase, old_id, new_id)
                related_updated += rel_updated
            
            # Step 2: Update the main table
            supabase.table(table_name).update({
                "property_id": new_id,
                "updated_at": datetime.utcnow().isoformat()
            }).eq("id", migration["id"]).execute()
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"Failed to update {migration['id']}: {e}")
            failure_count += 1
        
        # Progress logging
        if (i + 1) % 100 == 0:
            logger.info(f"Progress: {i + 1}/{len(migrations)} ({success_count} success, {failure_count} failed, {related_updated} related updates)")
    
    if related_updated > 0:
        logger.info(f"Updated {related_updated} related records in child tables")
    
    return success_count, failure_count


def main():
    parser = argparse.ArgumentParser(
        description='Migrate property IDs to new hash-based format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_property_ids.py                    # Preview migration (dry run)
  python migrate_property_ids.py --execute         # Execute migration on all tables
  python migrate_property_ids.py --execute --table properties_live  # Specific table only
  python migrate_property_ids.py --sample 10       # Show 10 sample migrations
        """
    )
    
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually execute the migration (default is dry run)'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        choices=['properties_live', 'property_manifest', 'pulled_properties', 'all'],
        default='all',
        help='Which table(s) to migrate (default: all)'
    )
    
    parser.add_argument(
        '--sample',
        type=int,
        default=5,
        help='Number of sample migrations to show (default: 5)'
    )
    
    args = parser.parse_args()
    
    try:
        supabase = get_supabase_client()
        logger.info("Connected to Supabase")
        
        # Determine tables to process
        if args.table == 'all':
            tables = ['properties_live', 'property_manifest', 'pulled_properties']
        else:
            tables = [args.table]
        
        total_migrations = 0
        total_success = 0
        total_failed = 0
        
        for table_name in tables:
            print(f"\n{'=' * 60}")
            print(f"TABLE: {table_name}")
            print(f"{'=' * 60}")
            
            # Fetch properties
            logger.info(f"Fetching properties from {table_name}...")
            properties = get_properties_to_migrate(supabase, table_name)
            
            if not properties:
                print(f"  No properties found in {table_name}")
                continue
            
            # Calculate migrations
            migrations, stats = calculate_migrations(properties)
            
            # Print stats
            print(f"\nStats for {table_name}:")
            print(f"  Total records:      {stats['total']:,}")
            print(f"  Already correct:    {stats['already_correct']:,}")
            print(f"  Needs migration:    {stats['needs_migration']:,}")
            print(f"  Skipped (non-pincali): {stats.get('skipped_non_pincali', 0):,}")
            print(f"  No source_url:      {stats['no_source_url']:,}")
            print(f"  No property_id:     {stats['no_property_id']:,}")
            
            if not migrations:
                print(f"\n  ✓ No migrations needed for {table_name}")
                continue
            
            # Show sample migrations
            print(f"\nSample migrations (showing {min(args.sample, len(migrations))}):")
            for m in migrations[:args.sample]:
                print(f"  OLD: {m['old_property_id']}")
                print(f"  NEW: {m['new_property_id']}")
                print(f"  URL: {m['source_url'][:60]}...")
                print()
            
            if len(migrations) > args.sample:
                print(f"  ... and {len(migrations) - args.sample} more")
            
            total_migrations += len(migrations)
            
            # Execute if requested
            if args.execute:
                print(f"\nExecuting migration for {table_name}...")
                success, failed = execute_migrations(
                    supabase, table_name, migrations
                )
                total_success += success
                total_failed += failed
                print(f"  Completed: {success} success, {failed} failed")
            else:
                print(f"\n  [DRY RUN] Would migrate {len(migrations)} records")
        
        # Summary
        print(f"\n{'=' * 60}")
        print("MIGRATION SUMMARY")
        print(f"{'=' * 60}")
        print(f"Total records needing migration: {total_migrations:,}")
        
        if args.execute:
            print(f"Successfully migrated: {total_success:,}")
            print(f"Failed: {total_failed:,}")
        else:
            print("\n[DRY RUN] No changes made. Use --execute to perform migration.")
        
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

