"""
Manual login helper - opens browser so you can log in, then saves cookies

Usage:
    python get_cookies.py
"""

import asyncio
from playwright.async_api import async_playwright
import json

async def get_cookies():
    """Open browser for manual login and save cookies"""
    
    print("\n" + "="*80)
    print("MANUAL LOGIN - COOKIE EXTRACTOR")
    print("="*80)
    print("\nThis will open a browser window.")
    print("Please log in manually, then press ENTER in this terminal.")
    print("="*80 + "\n")
    
    login_url = "https://www.pincali.com/en/account/authentication/new"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        
        await page.goto(login_url)
        
        print("✅ Browser opened. Please log in...")
        print("\nPress ENTER after you've successfully logged in...")
        input()
        
        # Get cookies
        cookies = await context.cookies()
        
        # Save to file
        with open('pincali_cookies.json', 'w') as f:
            json.dump(cookies, f, indent=2)
        
        print(f"\n✅ Saved {len(cookies)} cookies to pincali_cookies.json")
        
        # Show session cookie
        session_cookie = next((c for c in cookies if 'session' in c['name'].lower()), None)
        if session_cookie:
            print(f"✅ Found session cookie: {session_cookie['name']}")
        
        await browser.close()
        
    print("\n✅ Done! You can now run test_with_cookies.py")
    print("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(get_cookies())

