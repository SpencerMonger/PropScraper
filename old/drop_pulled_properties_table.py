#!/usr/bin/env python3
"""
Standalone script to drop the 'pulled_properties' table from Supabase database.
This script will permanently delete the table and all its data.

Usage:
    python drop_pulled_properties_table.py

Requirements:
    - .env file with SUPABASE_URL and SUPABASE_ANON_KEY
    - supabase-py package installed
"""

import os
import sys
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

def load_environment_variables() -> tuple[Optional[str], Optional[str]]:
    """Load environment variables from .env file."""
    load_dotenv()
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    
    return supabase_url, supabase_key

def validate_credentials(supabase_url: Optional[str], supabase_key: Optional[str]) -> bool:
    """Validate that required credentials are present."""
    if not supabase_url:
        print("❌ Error: SUPABASE_URL environment variable is not set")
        return False
    
    if not supabase_key:
        print("❌ Error: SUPABASE_ANON_KEY environment variable is not set")
        return False
    
    return True

def confirm_table_clear() -> bool:
    """Ask user for confirmation before clearing the table."""
    print("⚠️  WARNING: This will permanently delete ALL data from the 'pulled_properties' table!")
    print("   The table structure will remain intact, but all records will be deleted.")
    print("   This action cannot be undone.")
    print()
    
    while True:
        response = input("Are you sure you want to proceed? Type 'yes' to confirm: ").strip().lower()
        
        if response == 'yes':
            return True
        elif response in ['no', 'n', '']:
            return False
        else:
            print("Please type 'yes' to confirm or press Enter to cancel.")

def drop_pulled_properties_table(supabase_client: Client) -> bool:
    """Drop the pulled_properties table by deleting all data first."""
    try:
        # First, check how many records exist
        print("🔍 Checking current table state...")
        count_result = supabase_client.table('pulled_properties').select("id", count="exact").execute()
        total_count = count_result.count
        print(f"   Found {total_count} records in pulled_properties table")
        
        if total_count == 0:
            print("✅ Table is already empty - no action needed")
            return True
        
        print(f"🗑️  Attempting to delete {total_count} records from pulled_properties table...")
        
        # Try multiple delete approaches to handle different scenarios
        deleted_count = 0
        
        # Approach 1: Delete where id is not empty (should match all records)
        try:
            result = supabase_client.table('pulled_properties').delete().neq('id', '').execute()
            deleted_count = len(result.data) if result.data else 0
            print(f"   Approach 1: Deleted {deleted_count} records")
        except Exception as e:
            print(f"   Approach 1 failed: {e}")
        
        # If first approach didn't work, try approach 2
        if deleted_count == 0:
            try:
                # Approach 2: Delete where created_at is not null
                result = supabase_client.table('pulled_properties').delete().not_.is_('created_at', 'null').execute()
                deleted_count = len(result.data) if result.data else 0
                print(f"   Approach 2: Deleted {deleted_count} records")
            except Exception as e:
                print(f"   Approach 2 failed: {e}")
        
        # If still no success, try approach 3
        if deleted_count == 0:
            try:
                # Approach 3: Get all IDs and delete them explicitly
                all_records = supabase_client.table('pulled_properties').select('id').execute()
                if all_records.data:
                    ids_to_delete = [record['id'] for record in all_records.data]
                    print(f"   Approach 3: Found {len(ids_to_delete)} IDs to delete")
                    
                    # Delete in batches to avoid overwhelming the API
                    batch_size = 100
                    total_deleted = 0
                    
                    for i in range(0, len(ids_to_delete), batch_size):
                        batch_ids = ids_to_delete[i:i + batch_size]
                        result = supabase_client.table('pulled_properties').delete().in_('id', batch_ids).execute()
                        batch_deleted = len(result.data) if result.data else 0
                        total_deleted += batch_deleted
                        print(f"   Deleted batch {i//batch_size + 1}: {batch_deleted} records")
                    
                    deleted_count = total_deleted
            except Exception as e:
                print(f"   Approach 3 failed: {e}")
        
        # Verify the deletion
        verify_result = supabase_client.table('pulled_properties').select("id", count="exact").execute()
        remaining_count = verify_result.count
        
        if remaining_count == 0:
            print("✅ Successfully cleared 'pulled_properties' table")
            print(f"   - Deleted {deleted_count} records from the table")
            print("   - Table structure remains intact for fresh data")
            print("   - All existing data has been removed")
            return True
        else:
            print(f"⚠️  Deletion blocked: {deleted_count} records deleted, {remaining_count} still remain")
            print("   This is due to Row Level Security (RLS) policies preventing bulk deletion")
            print("\n🔧 SOLUTION: Run the SQL directly in your Supabase dashboard:")
            print("   1. Go to your Supabase project dashboard")
            print("   2. Navigate to SQL Editor")
            print("   3. Run this command:")
            print("      DELETE FROM pulled_properties;")
            print("   4. Or use the provided 'clear_table_sql.sql' file")
            print("\n📁 I've created 'clear_table_sql.sql' with the exact commands you need")
            return False
        
    except Exception as error:
        print(f"❌ Error clearing table: {error}")
        print("\n💡 Alternative approaches:")
        print("   1. Run this SQL directly in your Supabase SQL editor:")
        print("      DELETE FROM pulled_properties;")
        print("   2. Or to completely drop the table:")
        print("      DROP TABLE IF EXISTS pulled_properties CASCADE;")
        print("   3. Check if your Supabase project has RLS policies that prevent deletion")
        print("   4. Verify your API key has DELETE permissions")
        return False

def main():
    """Main function to orchestrate the table clearing process."""
    print("🚀 PropScraper - Clear 'pulled_properties' Table Script")
    print("=" * 57)
    print()
    
    # Load environment variables
    supabase_url, supabase_key = load_environment_variables()
    
    # Validate credentials
    if not validate_credentials(supabase_url, supabase_key):
        print("\n💡 Make sure you have a .env file with the following variables:")
        print("   SUPABASE_URL=your_supabase_project_url")
        print("   SUPABASE_ANON_KEY=your_supabase_anon_key")
        sys.exit(1)
    
    # Confirm with user
    if not confirm_table_clear():
        print("❌ Operation cancelled by user")
        sys.exit(0)
    
    try:
        # Create Supabase client
        print("🔗 Connecting to Supabase...")
        supabase_client: Client = create_client(supabase_url, supabase_key)
        print("✅ Successfully connected to Supabase")
        print()
        
        # Clear the table
        success = drop_pulled_properties_table(supabase_client)
        
        if success:
            print("\n🎉 Table clearing completed successfully!")
            print("\n📝 Next steps:")
            print("   - The table structure is intact and ready for fresh data")
            print("   - You can now run your scraper to populate with new data")
            print("   - If you need to modify the schema, use the Supabase SQL editor")
        else:
            print("\n❌ Table clearing failed. Please check the error messages above.")
            sys.exit(1)
            
    except Exception as error:
        print(f"❌ Failed to connect to Supabase: {error}")
        print("\n💡 Please check your credentials and network connection")
        sys.exit(1)

if __name__ == "__main__":
    main() 