#!/usr/bin/env python3
"""
Fix database property_id issues
"""

import os
import sys
from dotenv import load_dotenv
from supabase import create_client

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.property_id import generate_property_id

# Load environment variables
load_dotenv()

# Note: generate_property_id is now imported from config.property_id
# This ensures consistent ID generation across all services

def fix_database():
    """Fix the database property_id issues"""
    
    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    
    if not supabase_url or not supabase_key:
        print("ERROR: Missing Supabase credentials")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    
    print("Fixing database property_id issues...")
    print("=" * 50)
    
    try:
        # Get properties with empty property_id
        empty_result = supabase.table("pulled_properties").select("id,property_id,title,source_url").eq("property_id", "").execute()
        empty_count = len(empty_result.data)
        print(f"Found {empty_count} properties with empty property_id")
        
        # Fix each property with empty property_id
        for record in empty_result.data:
            record_id = record['id']
            source_url = record['source_url']
            title = record['title']
            
            print(f"\nFixing record:")
            print(f"  ID: {record_id}")
            print(f"  Title: {title[:50]}...")
            print(f"  URL: {source_url}")
            
            # Generate new property_id
            new_property_id = generate_property_id(source_url)
            print(f"  New property_id: {new_property_id}")
            
            # Update the record
            update_result = supabase.table("pulled_properties").update({"property_id": new_property_id}).eq("id", record_id).execute()
            
            if update_result.data:
                print(f"  ✅ Updated successfully")
            else:
                print(f"  ❌ Update failed")
        
        print(f"\n✅ Database fix completed!")
        
        # Test property_id generation with some sample URLs
        print(f"\nTesting property_id generation:")
        print("-" * 30)
        
        test_urls = [
            "https://www.pincali.com/en/home/ofrezco-departamento-en-renta-san-jeronimo-san-jeronimo",
            "https://www.pincali.com/en/home/departamento-601-en-blue-hills-en-ubicacion-privilegiada",
            "https://www.pincali.com/en/home/la-ribera-luxury-lot-across-from-four-seasons-la-ribera-bcs",
            ""  # Test empty URL
        ]
        
        for url in test_urls:
            prop_id = generate_property_id(url)
            print(f"URL: {url[:60]}...")
            print(f"ID:  {prop_id}")
            print()
            
    except Exception as e:
        print(f"❌ Database fix failed: {e}")

if __name__ == "__main__":
    fix_database() 