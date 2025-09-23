#!/usr/bin/env python3
"""
Check database for existing property records
"""

import os
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

def check_database():
    """Check the current state of the database"""
    
    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    
    if not supabase_url or not supabase_key:
        print("❌ Missing Supabase credentials")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    
    print("🔍 Checking database state...")
    print("=" * 50)
    
    try:
        # Get total count
        result = supabase.table("pulled_properties").select("id", count="exact").execute()
        total_count = result.count
        print(f"Total properties in database: {total_count}")
        
        # Get properties with NULL property_id
        null_result = supabase.table("pulled_properties").select("id,property_id,title,source_url").is_("property_id", "null").execute()
        null_count = len(null_result.data)
        print(f"Properties with NULL property_id: {null_count}")
        
        # Get properties with empty property_id
        empty_result = supabase.table("pulled_properties").select("id,property_id,title,source_url").eq("property_id", "").execute()
        empty_count = len(empty_result.data)
        print(f"Properties with empty property_id: {empty_count}")
        
        # Get sample of existing property_ids
        sample_result = supabase.table("pulled_properties").select("property_id,title,source_url").limit(10).execute()
        
        print(f"\nSample of existing records:")
        print("-" * 30)
        for record in sample_result.data:
            property_id = record.get("property_id", "NULL")
            title = record.get("title", "No title")[:50]
            source_url = record.get("source_url", "No URL")
            print(f"ID: {property_id}")
            print(f"Title: {title}")
            print(f"URL: {source_url}")
            print()
        
        # Check for duplicate property_ids
        dup_result = supabase.rpc("get_duplicate_property_ids").execute()
        if hasattr(dup_result, 'data') and dup_result.data:
            print(f"Found duplicate property_ids: {len(dup_result.data)}")
            for dup in dup_result.data:
                print(f"  Duplicate: {dup}")
        else:
            print("No duplicate property_ids found (or RPC not available)")
            
    except Exception as e:
        print(f"❌ Database check failed: {e}")

if __name__ == "__main__":
    check_database() 