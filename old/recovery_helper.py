#!/usr/bin/env python3
"""
Recovery Helper for Pincali Scraper
Checks the last scraped page and suggests how to resume
"""

import os
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

def check_last_session():
    """Check the last scraping session and suggest recovery options"""
    
    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    
    if not supabase_url or not supabase_key:
        print("❌ Missing SUPABASE_URL or SUPABASE_ANON_KEY environment variables")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    
    try:
        # Get the most recent session
        result = supabase.table("scraping_sessions").select("*").order("created_at", desc=True).limit(1).execute()
        
        if not result.data:
            print("❌ No scraping sessions found")
            return
        
        session = result.data[0]
        print(f"📊 LAST SCRAPING SESSION ANALYSIS")
        print(f"=" * 50)
        print(f"Session ID: {session['id']}")
        print(f"Session Name: {session['session_name']}")
        print(f"Status: {session['status']}")
        print(f"Created: {session['created_at']}")
        print(f"Updated: {session.get('updated_at', 'N/A')}")
        
        pages_scraped = session.get('pages_scraped', 0)
        last_page = session.get('last_page_completed', 0)
        total_pages = session.get('total_pages', 100)
        
        print(f"\n📈 PROGRESS:")
        print(f"Pages Scraped: {pages_scraped}")
        print(f"Last Page Completed: {last_page}")
        print(f"Total Pages Planned: {total_pages}")
        
        if pages_scraped > 0:
            progress_pct = (pages_scraped / total_pages) * 100
            print(f"Progress: {progress_pct:.1f}%")
        
        # Check if session was interrupted
        if session['status'] in ['running', 'paused']:
            print(f"\n⚠️  SESSION WAS INTERRUPTED")
            next_page = last_page + 1 if last_page > 0 else pages_scraped + 1
            remaining_pages = total_pages - pages_scraped
            
            print(f"\n🔄 RECOVERY SUGGESTIONS:")
            print(f"To resume from page {next_page}:")
            print(f"python pincali_scraper.py --resume --start {next_page} --pages {remaining_pages}")
            
            print(f"\nTo resume with a smaller batch (safer):")
            safe_batch = min(10, remaining_pages)
            print(f"python pincali_scraper.py --resume --start {next_page} --pages {safe_batch}")
            
        elif session['status'] == 'completed':
            print(f"\n✅ SESSION COMPLETED SUCCESSFULLY")
            
        elif session['status'] == 'failed':
            print(f"\n❌ SESSION FAILED")
            error_msg = session.get('error_message', 'Unknown error')
            print(f"Error: {error_msg}")
            
            # Suggest restart
            next_page = max(1, last_page)
            print(f"\n🔄 RECOVERY SUGGESTIONS:")
            print(f"To restart from page {next_page}:")
            print(f"python pincali_scraper.py --start {next_page} --pages {total_pages - next_page + 1}")
        
        # Check recent errors
        print(f"\n🔍 CHECKING RECENT ERRORS...")
        error_result = supabase.table("scraping_errors").select("*").eq("session_id", session['id']).order("created_at", desc=True).limit(5).execute()
        
        if error_result.data:
            print(f"Found {len(error_result.data)} recent errors:")
            for error in error_result.data:
                print(f"  - {error['error_type']}: {error['error_message'][:100]}...")
        else:
            print("No recent errors found")
            
        # Check property counts
        print(f"\n📊 DATABASE STATS:")
        count_result = supabase.table("pulled_properties").select("id", count="exact").execute()
        total_properties = count_result.count
        print(f"Total Properties in Database: {total_properties}")
        
        # Properties from this session
        if session.get('properties_inserted', 0) > 0 or session.get('properties_updated', 0) > 0:
            inserted = session.get('properties_inserted', 0)
            updated = session.get('properties_updated', 0)
            print(f"This Session - Inserted: {inserted}, Updated: {updated}")
        
    except Exception as e:
        print(f"❌ Error checking session: {e}")

if __name__ == "__main__":
    print(f"🔧 Pincali Scraper Recovery Helper")
    print(f"Checking last session at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    check_last_session() 