#!/usr/bin/env python3
"""
Drop Properties Live Table Data

This script clears all data from the properties_live table.
Use this when you need to reset the production property data,
such as after changing the property_id generation logic.

WARNING: This is a destructive operation. All property data will be deleted.

Usage:
    python drop_live_table.py
    python drop_live_table.py --confirm  # Skip confirmation prompt
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from supabase import create_client, Client

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


def get_table_count(supabase: Client, table_name: str) -> int:
    """Get the count of rows in a table"""
    try:
        response = supabase.table(table_name).select("*", count="exact", head=True).execute()
        return response.count or 0
    except Exception as e:
        logger.error(f"Error getting count for {table_name}: {e}")
        return -1


def drop_table_data(supabase: Client, table_name: str) -> bool:
    """
    Delete all rows from a table using batch deletion.
    
    Args:
        supabase: Supabase client
        table_name: Name of the table to clear
        
    Returns:
        True if successful, False otherwise
    """
    try:
        batch_size = 1000
        total_deleted = 0
        max_iterations = 100  # Safety limit
        
        for iteration in range(max_iterations):
            # Fetch a batch of IDs
            response = supabase.table(table_name).select("id").limit(batch_size).execute()
            
            if not response.data:
                # No more rows to delete
                break
            
            # Extract IDs from the batch
            ids_to_delete = [row["id"] for row in response.data]
            
            # Delete this batch using IN clause
            supabase.table(table_name).delete().in_("id", ids_to_delete).execute()
            
            total_deleted += len(ids_to_delete)
            
            if len(ids_to_delete) < batch_size:
                # Last batch was smaller, we're done
                break
            
            # Progress indicator for large tables
            if (iteration + 1) % 5 == 0:
                print(f"    ... deleted {total_deleted:,} rows so far")
        
        logger.info(f"Deleted {total_deleted:,} rows from {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error deleting data from {table_name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Clear all data from the properties_live table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python drop_live_table.py           # Clear with confirmation prompt
  python drop_live_table.py --confirm # Clear without confirmation
  python drop_live_table.py --dry-run # Show what would be deleted
        """
    )
    
    parser.add_argument(
        '--confirm',
        action='store_true',
        help='Skip confirmation prompt'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    parser.add_argument(
        '--include-manifest',
        action='store_true',
        help='Also clear the property_manifest table'
    )
    
    parser.add_argument(
        '--include-queue',
        action='store_true',
        help='Also clear the scrape_queue table'
    )
    
    args = parser.parse_args()
    
    try:
        supabase = get_supabase_client()
        logger.info("Connected to Supabase")
        
        # Tables to clear
        tables = ['properties_live']
        
        if args.include_manifest:
            tables.append('property_manifest')
        
        if args.include_queue:
            tables.append('scrape_queue')
        
        # Get current counts
        counts = {}
        total_rows = 0
        
        print("\n" + "=" * 60)
        print("CURRENT TABLE STATUS")
        print("=" * 60)
        
        for table in tables:
            count = get_table_count(supabase, table)
            counts[table] = count
            total_rows += count
            print(f"  {table}: {count:,} rows")
        
        print("=" * 60)
        print(f"Total rows to delete: {total_rows:,}")
        print("=" * 60 + "\n")
        
        if total_rows == 0:
            print("Tables are already empty. Nothing to delete.")
            return
        
        if args.dry_run:
            print("[DRY RUN] Would delete the above rows. No changes made.")
            return
        
        # Confirmation
        if not args.confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE all data from the above tables!")
            print("⚠️  This action cannot be undone.\n")
            
            user_input = input("Type 'DELETE' to confirm: ")
            
            if user_input != "DELETE":
                print("Aborted. No data was deleted.")
                return
        
        # Perform deletion
        print("\nDeleting data...")
        
        success_count = 0
        for table in tables:
            print(f"  Clearing {table}...", end=" ")
            
            if counts[table] == 0:
                print("already empty")
                continue
            
            if drop_table_data(supabase, table):
                # Verify deletion
                new_count = get_table_count(supabase, table)
                if new_count == 0:
                    print(f"✓ deleted {counts[table]:,} rows")
                    success_count += 1
                else:
                    print(f"⚠ {new_count:,} rows remaining")
            else:
                print("✗ failed")
        
        # Summary
        print("\n" + "=" * 60)
        print("DELETION COMPLETE")
        print("=" * 60)
        print(f"  Tables cleared: {success_count}/{len(tables)}")
        print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Verify final counts
        print("\nFinal table status:")
        for table in tables:
            count = get_table_count(supabase, table)
            status = "✓ empty" if count == 0 else f"⚠ {count:,} rows remaining"
            print(f"  {table}: {status}")
        
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

