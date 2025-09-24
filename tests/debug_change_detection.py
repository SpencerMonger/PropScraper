#!/usr/bin/env python3
"""
Debug script to test change detection logic
"""

import os
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

def test_change_detection():
    # Use the actual session ID from the logs
    session_id = '2d837c5c-328c-4ebc-b333-5718dca63e54'
    
    print(f"Testing change detection for session: {session_id}")
    
    # Get staging data for this session
    staging_response = supabase.table('property_scrapes_staging').select('property_id').eq('session_id', session_id).execute()
    live_response = supabase.table('properties_live').select('property_id').execute()
    
    staging_ids = set([item['property_id'] for item in staging_response.data])
    live_ids = set([item['property_id'] for item in live_response.data])
    
    new_ids = list(staging_ids - live_ids)
    
    print(f"Staging IDs count: {len(staging_ids)}")
    print(f"Live IDs count: {len(live_ids)}")
    print(f"New IDs count: {len(new_ids)}")
    
    if len(staging_ids) > 0:
        print(f"First 3 staging IDs: {list(staging_ids)[:3]}")
    
    if len(live_ids) > 0:
        print(f"First 3 live IDs: {list(live_ids)[:3]}")
    else:
        print("Live table is empty")
    
    if len(new_ids) > 0:
        print(f"First 3 new IDs: {new_ids[:3]}")
        print("✅ These should be detected as NEW properties")
    else:
        print("❌ No new properties detected - this is the problem!")
    
    # Test what the change detection service is actually doing
    print("\n" + "="*50)
    print("Testing actual change detection service...")
    
    from services.change_detection_service import ChangeDetectionService
    
    change_detector = ChangeDetectionService(supabase)
    
    # Test the _detect_new_properties method directly
    import asyncio
    
    async def test_detect_new():
        try:
            new_properties = await change_detector._detect_new_properties(session_id)
            print(f"Change detector found {len(new_properties)} new properties")
            if new_properties:
                print(f"First 3: {new_properties[:3]}")
            return new_properties
        except Exception as e:
            print(f"Error in change detector: {e}")
            return []
    
    new_properties = asyncio.run(test_detect_new())
    
    if len(new_properties) != len(new_ids):
        print(f"❌ MISMATCH: Manual calculation found {len(new_ids)} new, but service found {len(new_properties)}")
    else:
        print(f"✅ MATCH: Both methods found {len(new_properties)} new properties")

if __name__ == "__main__":
    test_change_detection() 