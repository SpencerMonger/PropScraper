#!/usr/bin/env python3
"""
Test script to run sync manually
"""

import asyncio
import os
from dotenv import load_dotenv
from supabase import create_client
from services import PropertySyncOrchestrator

# Load environment variables
load_dotenv()

async def test_sync():
    # Initialize Supabase client
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))
    
    # Use the latest session ID from the scraper
    session_id = '19f7b190-cd1a-4856-8c5b-07f4145cb536'
    
    print(f"Testing sync for session: {session_id}")
    
    # Check counts before sync
    live_count_before = supabase.table('properties_live').select('id', count='exact').execute().count
    staging_count = supabase.table('property_scrapes_staging').select('id', count='exact').eq('session_id', session_id).execute().count
    
    print(f"Before sync - Live: {live_count_before}, Staging (this session): {staging_count}")
    
    # Run sync
    orchestrator = PropertySyncOrchestrator(supabase)
    
    try:
        workflow_result = await orchestrator.daily_sync_workflow(session_id)
        
        if workflow_result.success:
            print("✅ Sync completed successfully!")
            
            # Check counts after sync
            live_count_after = supabase.table('properties_live').select('id', count='exact').execute().count
            print(f"After sync - Live: {live_count_after}")
            
            if workflow_result.sync_result:
                metrics = workflow_result.sync_result.metrics
                print(f"✅ New properties: {metrics.new_properties}")
                print(f"✅ Updated properties: {metrics.updated_properties}")
                print(f"✅ Removed properties: {metrics.removed_properties}")
                print(f"✅ Data quality score: {metrics.data_quality_score:.2f}")
                
                expected_live_count = live_count_before + metrics.new_properties
                if live_count_after == expected_live_count:
                    print(f"✅ Live table count is correct: {live_count_after}")
                else:
                    print(f"❌ Live table count mismatch: expected {expected_live_count}, got {live_count_after}")
            else:
                print("❌ No sync result available")
        else:
            print(f"❌ Sync failed: {workflow_result.error_message}")
            
    except Exception as e:
        print(f"❌ Sync error: {e}")

if __name__ == "__main__":
    asyncio.run(test_sync()) 