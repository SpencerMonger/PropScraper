#!/usr/bin/env python3
"""
Simple test to extract phone numbers from Pincali listings with Playwright.

Usage:
    python test_phone_simple.py
"""

import asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup
import re

async def test_phone_extraction():
    """Test phone extraction by clicking message button"""
    
    print("\n" + "="*80)
    print("PHONE EXTRACTION TEST - USING PLAYWRIGHT")
    print("="*80)
    
    property_url = "https://www.pincali.com/en/home/casa-en-renta-en-quintas-del-rey-quintas-del-rey-ii"
    login_url = "https://www.pincali.com/en/account/authentication/new"
    
    # Login credentials
    email = "spenc1924@gmail.com"
    password = "pooph3ad1!"
    
    print(f"\nProperty URL: {property_url}")
    print(f"Login URL: {login_url}\n")
    
    try:
        print("Step 1: Starting Playwright...")
        async with async_playwright() as p:
            print("Step 2: Launching browser with stealth settings...")
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )
            
            # Create context with realistic settings
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            page = await context.new_page()
            
            # Hide webdriver property
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """)
            
            print("Step 3: Logging in (multi-step process)...")
            await page.goto(login_url, wait_until='networkidle', timeout=30000)
            print("✅ Login page loaded")
            
            # Step 3a: Click "Continue with email"
            print("\nStep 3a: Clicking 'Continue with email'...")
            await page.click('a[data-email-button]')
            await page.wait_for_timeout(1000)
            print("✅ Email form should now be visible")
            
            # Step 3b: Fill in email and submit
            print("\nStep 3b: Entering email...")
            await page.wait_for_selector('input#authentication_email', state='visible', timeout=5000)
            await page.fill('input#authentication_email', email)
            print(f"✅ Filled email: {email}")
            
            print("Clicking 'Continue' button...")
            await page.click('input[type="submit"][value="Continue"]')
            await page.wait_for_timeout(2000)
            print("✅ Submitted email")
            
            # Step 3c: Fill in password on the next page
            print("\nStep 3c: Entering password...")
            await page.wait_for_selector('input[name*="password"]', state='visible', timeout=10000)
            password_input = await page.query_selector('input[name*="password"]')
            await password_input.fill(password)
            print("✅ Filled password")
            
            print("Clicking 'Log in' button...")
            await page.click('input[type="submit"], button[type="submit"]')
            
            # Wait for login to complete - check for redirect or account indicator
            print("Waiting for login to complete...")
            await page.wait_for_timeout(5000)
            
            # Verify we're logged in by checking current URL or page elements
            current_url = page.url
            print(f"Current URL after login: {current_url}")
            
            # Save screenshot and HTML after login
            await page.screenshot(path='after_login.png')
            print("✅ Saved screenshot to after_login.png")
            
            # Check cookies
            cookies = await context.cookies()
            session_cookies = [c for c in cookies if 'session' in c['name'].lower() or 'auth' in c['name'].lower()]
            print(f"Found {len(session_cookies)} session/auth cookies")
            for cookie in session_cookies:
                print(f"  - {cookie['name']}: {cookie['value'][:20]}...")
            
            await page.wait_for_timeout(2000)
            print("✅ Logged in!\n")
            
            print("Step 4: Navigating to property page...")
            await page.goto(property_url, wait_until='networkidle', timeout=30000)
            print("✅ Property page loaded!\n")
            
            print("Step 5: Scrolling down to load all content...")
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(2000)
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
            await page.wait_for_timeout(2000)
            print("✅ Scrolled\n")
            
            print("Step 6: Waiting for phone numbers to load dynamically...")
            # Phone numbers might load via AJAX after page load
            await page.wait_for_timeout(5000)
            
            # Check if publisher-phones div has content
            phones_div = await page.query_selector('.publisher-phones')
            if phones_div:
                phones_text = await phones_div.inner_text()
                if phones_text.strip():
                    print(f"✅ Phone numbers found: {phones_text}")
                else:
                    print("⚠️ Publisher-phones div is empty")
                    
                    # Try hovering over the contact section to trigger reveal
                    print("Trying to hover over publisher info...")
                    publisher_info = await page.query_selector('.publisher-info')
                    if publisher_info:
                        await publisher_info.hover()
                        await page.wait_for_timeout(2000)
                        phones_text = await phones_div.inner_text()
                        if phones_text.strip():
                            print(f"✅ Phone numbers appeared after hover: {phones_text}")
                        else:
                            print("⚠️ Still no phone numbers after hover")
            else:
                print("⚠️ Publisher-phones div not found")
            
            await page.wait_for_timeout(1000)
            
            print("\nStep 7: Getting page HTML...")
            html = await page.content()
            
            # Save HTML for debugging
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print("✅ Saved HTML to debug_page.html for inspection\n")
            
            await context.close()
            await browser.close()
            
            print("Step 8: Parsing HTML...")
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract agent info from publisher-container
            print("\nLooking for publisher container...")
            publisher_container = soup.select_one('.publisher-container')
            
            if publisher_container:
                print("✅ Found publisher-container!\n")
                print("Full HTML of publisher container:")
                print(publisher_container.prettify()[:2000])
                print("\n" + "="*80 + "\n")
            else:
                print("❌ publisher-container not found")
                print("\nLet me check what classes ARE in the HTML...")
                all_divs = soup.find_all('div', class_=True)
                classes = set()
                for div in all_divs[:50]:  # Check first 50 divs
                    classes.update(div.get('class', []))
                print(f"Found classes: {sorted(list(classes))[:30]}\n")
            
            # Look for phone numbers
            print("Searching for phone numbers...")
            phone_numbers = set()
            
            # Method 1: Look in publisher container
            print("  Checking publisher-container...")
            if publisher_container:
                text = publisher_container.get_text()
                print(f"  Text content: {text[:500]}")
                
                # Extract all phone-like patterns
                patterns = [
                    r'\+\d{12,13}',  # +526865231719
                    r'\+\d{1,3}\s?\d{3}\s?\d{3}\s?\d{4}',
                    r'\d{10,13}'
                ]
                
                for pattern in patterns:
                    found = re.findall(pattern, text)
                    for phone in found:
                        phone_numbers.add(phone.strip())
                        print(f"    ✅ Found: {phone}")
            
            # Method 2: Try other selectors
            print("\n  Checking other selectors...")
            for selector in ['.publisher-info', '.sidebar-content', '[class*="phone"]', 'a[href^="tel:"]']:
                elements = soup.select(selector)
                print(f"    Found {len(elements)} elements with selector: {selector}")
                for elem in elements:
                    text = elem.get_text(strip=True)
                    href = elem.get('href', '')
                    
                    # DEBUG: Show what's in the element
                    print(f"      Element text: '{text}'")
                    print(f"      Element href: '{href}'")
                    print(f"      Element HTML: {str(elem)[:200]}")
                    
                    if href.startswith('tel:'):
                        phone = href.replace('tel:', '').strip()
                        phone_numbers.add(phone)
                        print(f"      ✅ Found tel: link -> {phone}")
                    
                    # Extract numbers from text
                    found = re.findall(r'\+?\d[\d\s\-\(\)]{9,}', text)
                    for phone in found:
                        phone_numbers.add(phone.strip())
                        print(f"      ✅ Found in text -> {phone}")
            
            # Method 2: WhatsApp links
            print("  Checking WhatsApp links...")
            whatsapp_links = soup.select('a[href*="whatsapp"], a[href*="wa.me"]')
            print(f"    Found {len(whatsapp_links)} WhatsApp links")
            for link in whatsapp_links:
                href = link.get('href', '')
                print(f"      WhatsApp href: {href}")
                
                # Try multiple patterns
                patterns = [
                    r'(?:whatsapp\.com|wa\.me)/(\+?\d+)',
                    r'phone=(\+?\d+)',
                    r'(\+\d{10,15})',
                    r'(\d{10,15})'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, href)
                    if match:
                        phone = match.group(1)
                        phone_numbers.add(phone)
                        print(f"      ✅ Extracted from WhatsApp URL -> {phone}")
                        break
            
            # Display results
            print("\n" + "-"*80)
            if phone_numbers:
                print(f"✅ SUCCESS! Found {len(phone_numbers)} phone number(s):")
                for phone in phone_numbers:
                    print(f"   📞 {phone}")
            else:
                print("⚠️  WARNING: No phone numbers found")
                print("\nTrying to find publisher section for debugging...")
                publisher_section = soup.select_one('[class*="publisher"]')
                if publisher_section:
                    print("Publisher section HTML (first 500 chars):")
                    print(publisher_section.prettify()[:500])
                else:
                    print("No publisher section found in HTML")
            print("-"*80)
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_phone_extraction())
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80 + "\n")

