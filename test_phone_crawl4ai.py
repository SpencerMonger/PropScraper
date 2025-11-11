"""
Test phone number extraction using Crawl4AI with login via hooks

Usage:
    python test_phone_crawl4ai.py
"""

import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup
import re
import json

async def test_phone_extraction():
    """Test phone extraction with Crawl4AI after login"""
    
    print("\n" + "="*80)
    print("PHONE EXTRACTION TEST - USING CRAWL4AI")
    print("="*80)
    
    property_url = "https://www.pincali.com/en/home/casa-en-renta-en-quintas-del-rey-quintas-del-rey-ii"
    login_url = "https://www.pincali.com/en/account/authentication/new"
    
    # Login credentials
    email = "spenc1924@gmail.com"
    password = "pooph3ad1!"
    
    print(f"\nProperty URL: {property_url}")
    print(f"Login URL: {login_url}\n")
    
    try:
        # Configure Crawl4AI browser with anti-detection
        browser_config = BrowserConfig(
            headless=True,
            verbose=True,
            extra_args=[
                '--disable-blink-features=AutomationControlled',
            ],
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        print("Step 1: Logging in via Crawl4AI...")
        
        # JavaScript to handle login
        login_js = f"""
        (async () => {{
            console.log('Starting login process...');
            
            // Click "Continue with email"
            const emailButton = document.querySelector('a[data-email-button]');
            if (emailButton) {{
                emailButton.click();
                await new Promise(r => setTimeout(r, 1000));
            }}
            
            // Fill email
            const emailInput = document.querySelector('input#authentication_email');
            if (emailInput) {{
                emailInput.value = '{email}';
                const continueBtn = document.querySelector('input[type="submit"][value="Continue"]');
                if (continueBtn) {{
                    continueBtn.click();
                    await new Promise(r => setTimeout(r, 2000));
                }}
            }}
            
            // Fill password
            const passwordInput = document.querySelector('input[name*="password"]');
            if (passwordInput) {{
                passwordInput.value = '{password}';
                const loginBtn = document.querySelector('input[type="submit"], button[type="submit"]');
                if (loginBtn) {{
                    loginBtn.click();
                    await new Promise(r => setTimeout(r, 5000));
                }}
            }}
            
            console.log('Login sequence completed');
        }})();
        """
        
        # Crawl login page first to establish session
        login_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            js_code=[login_js],
            wait_for="networkidle",
            delay_before_return_html=8.0,  # Wait for login to complete
            session_id="pincali_session"  # Use session to maintain cookies
        )
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            print("Executing login...")
            login_result = await crawler.arun(
                url=login_url,
                config=login_config
            )
            
            # Save login result
            with open('login_result.html', 'w', encoding='utf-8') as f:
                f.write(login_result.html)
            print("✅ Login page processed, saved to login_result.html")
            
            # Now crawl the property page using the same session
            print("\nStep 2: Crawling property page with session...")
            
            property_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                wait_for="networkidle",
                delay_before_return_html=5.0,  # Wait for dynamic content
                session_id="pincali_session",  # Reuse session with login cookies
                js_only=True  # Don't create new page, use existing session
            )
            
            property_result = await crawler.arun(
                url=property_url,
                config=property_config
            )
            
            if not property_result.success:
                print(f"❌ Failed to crawl property page: {property_result.error_message}")
                return
            
            print("✅ Property page crawled successfully")
            
            # Save HTML
            with open('debug_page_crawl.html', 'w', encoding='utf-8') as f:
                f.write(property_result.html)
            print("✅ Saved HTML to debug_page_crawl.html")
            
            # Parse and search for phone numbers
            print("\nStep 3: Searching for phone numbers...")
            soup = BeautifulSoup(property_result.html, 'html.parser')
            
            phone_numbers = set()
            
            # Check publisher-phones div
            phones_divs = soup.select('.publisher-phones')
            print(f"Found {len(phones_divs)} publisher-phones divs")
            
            for div in phones_divs:
                text = div.get_text(strip=True)
                if text:
                    print(f"✅ Found phones in div: {text}")
                    phone_numbers.add(text)
                else:
                    print(f"⚠️ Empty publisher-phones div: {div}")
            
            # Search for phone patterns anywhere in the page
            phone_pattern = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
            all_text = soup.get_text()
            found_phones = re.findall(phone_pattern, all_text)
            
            for phone in found_phones:
                cleaned = phone.strip()
                if len(cleaned) >= 10:
                    phone_numbers.add(cleaned)
            
            # Clean up session
            await crawler.kill_session("pincali_session")
            
            # Results
            if phone_numbers:
                print(f"\n✅ SUCCESS! Found {len(phone_numbers)} phone number(s):")
                for phone in phone_numbers:
                    print(f"  📞 {phone}")
            else:
                print("\n⚠️ No phone numbers found")
                print("\nDebugging info:")
                print(f"  - HTML length: {len(property_result.html)}")
                print(f"  - Markdown length: {len(property_result.markdown.raw_markdown)}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(test_phone_extraction())
