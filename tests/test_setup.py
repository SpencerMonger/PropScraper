#!/usr/bin/env python3
"""
Test script to validate the setup and dependencies for the Inmuebles24 scraper
Run this before executing the main scraper to ensure everything is configured correctly
"""

import sys
import os
import asyncio
from datetime import datetime

def test_imports():
    """Test if all required packages are installed"""
    print("Testing imports...")
    
    try:
        import crawl4ai
        print("✓ Crawl4AI imported successfully")
    except ImportError as e:
        print(f"✗ Crawl4AI import failed: {e}")
        return False
    
    try:
        from supabase import create_client
        print("✓ Supabase client imported successfully")
    except ImportError as e:
        print(f"✗ Supabase import failed: {e}")
        return False
    
    try:
        from dotenv import load_dotenv
        print("✓ Python-dotenv imported successfully")
    except ImportError as e:
        print(f"✗ Python-dotenv import failed: {e}")
        return False
    
    try:
        import requests
        print("✓ Requests imported successfully")
    except ImportError as e:
        print(f"✗ Requests import failed: {e}")
        return False
    
    try:
        from bs4 import BeautifulSoup
        print("✓ BeautifulSoup imported successfully")
    except ImportError as e:
        print(f"✗ BeautifulSoup import failed: {e}")
        return False
    
    return True

def test_environment():
    """Test environment variables"""
    print("\nTesting environment variables...")
    
    from dotenv import load_dotenv
    load_dotenv()
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_ANON_KEY")
    
    if not supabase_url:
        print("✗ SUPABASE_URL not found in environment variables")
        return False
    else:
        print(f"✓ SUPABASE_URL found: {supabase_url[:30]}...")
    
    if not supabase_key:
        print("✗ SUPABASE_ANON_KEY not found in environment variables")
        return False
    else:
        print(f"✓ SUPABASE_ANON_KEY found: {supabase_key[:30]}...")
    
    return True

async def test_crawl4ai():
    """Test Crawl4AI basic functionality"""
    print("\nTesting Crawl4AI...")
    
    try:
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
        
        browser_config = BrowserConfig(headless=True, verbose=False)
        crawler_config = CrawlerRunConfig(page_timeout=10000)
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            result = await crawler.arun("https://httpbin.org/html", config=crawler_config)
            
            if result.success:
                print("✓ Crawl4AI basic test successful")
                return True
            else:
                print(f"✗ Crawl4AI test failed: {result.error_message}")
                return False
    
    except Exception as e:
        print(f"✗ Crawl4AI test failed with exception: {e}")
        return False

def test_supabase_connection():
    """Test Supabase connection"""
    print("\nTesting Supabase connection...")
    
    try:
        from dotenv import load_dotenv
        from supabase import create_client
        
        load_dotenv()
        
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")
        
        if not supabase_url or not supabase_key:
            print("✗ Missing Supabase credentials")
            return False
        
        supabase = create_client(supabase_url, supabase_key)
        
        # Test a simple query - this will fail if credentials are wrong
        try:
            # Try to query a system table that should always exist
            result = supabase.rpc('version').execute()
            print("✓ Supabase connection successful")
            return True
        except Exception as e:
            # If the above fails, try a different approach
            try:
                # Try to access the pulled_properties table (will fail if schema not created)
                result = supabase.table("pulled_properties").select("count", count="exact").limit(1).execute()
                print("✓ Supabase connection and schema validation successful")
                return True
            except Exception as schema_error:
                print(f"✗ Supabase schema error (run schema.sql first): {schema_error}")
                return False
    
    except Exception as e:
        print(f"✗ Supabase connection failed: {e}")
        return False

def test_target_website():
    """Test if target website is accessible"""
    print("\nTesting target website accessibility...")
    
    try:
        import requests
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get("https://www.inmuebles24.com/casas-en-venta.html", 
                              headers=headers, timeout=10)
        
        if response.status_code == 200:
            print(f"✓ Target website accessible (Status: {response.status_code})")
            return True
        else:
            print(f"✗ Target website returned status: {response.status_code}")
            return False
    
    except Exception as e:
        print(f"✗ Target website test failed: {e}")
        return False

async def main():
    """Run all tests"""
    print("=" * 60)
    print("INMUEBLES24 SCRAPER SETUP VALIDATION")
    print("=" * 60)
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests = [
        ("Package Imports", test_imports),
        ("Environment Variables", test_environment),
        ("Crawl4AI Functionality", test_crawl4ai),
        ("Supabase Connection", test_supabase_connection),
        ("Target Website Access", test_target_website),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name:<30} {status}")
        if result:
            passed += 1
    
    print(f"\nTests passed: {passed}/{len(results)}")
    
    if passed == len(results):
        print("\n🎉 All tests passed! You're ready to run the scraper.")
        print("\nNext steps:")
        print("1. Run: python inmuebles24_scraper.py")
        print("2. Monitor the logs for progress")
        print("3. Check your Supabase database for results")
    else:
        print("\n❌ Some tests failed. Please fix the issues before running the scraper.")
        print("\nCommon fixes:")
        print("1. Install missing packages: pip install -r requirements.txt")
        print("2. Install Playwright: playwright install")
        print("3. Create .env file with your Supabase credentials")
        print("4. Run schema.sql in your Supabase SQL editor")
    
    return passed == len(results)

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1) 