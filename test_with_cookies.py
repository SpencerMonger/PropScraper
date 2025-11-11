"""
Test phone extraction using saved cookies with Playwright

Usage:
    1. First run: python get_cookies.py (to save cookies)
    2. Then run: python test_with_cookies.py
"""

import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json
import os
import re

async def test_with_cookies():
    """Test phone extraction using saved cookies"""
    
    print("\n" + "="*80)
    print("PHONE EXTRACTION TEST - USING SAVED COOKIES")
    print("="*80)
    
    # Check for cookies file
    if not os.path.exists('pincali_cookies.json'):
        print("\n❌ ERROR: pincali_cookies.json not found!")
        print("Please run 'python get_cookies.py' first to save your login cookies.\n")
        return
    
    # Load cookies
    with open('pincali_cookies.json', 'r') as f:
        cookies = json.load(f)
    
    print(f"\n✅ Loaded {len(cookies)} cookies from file")
    
    # Test with a property that has phone numbers
    # Original test property (might not have phones): casa-en-renta-en-quintas-del-rey-quintas-del-rey-ii
    # Property from screenshot with phones: casa-en-pueblo-telchac-puerto
    property_url = "https://www.pincali.com/en/home/casa-en-renta-en-quintas-del-rey-quintas-del-rey-ii"
    
    print(f"Property URL: {property_url}")
    print("NOTE: This property might not have phone numbers listed.\n")
    
    try:
        print("Step 1: Launching browser with saved cookies...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            # Add cookies to context
            await context.add_cookies(cookies)
            print(f"✅ Injected {len(cookies)} cookies")
            
            page = await context.new_page()
            
            # Monitor ALL network requests to find the phone data
            all_requests = []
            
            async def log_request(request):
                url = request.url
                # Only log API/XHR requests, not images/css/js files
                if any(ext in url for ext in ['.jpg', '.png', '.css', '.woff', '.svg', '.gif']):
                    return
                all_requests.append(url)
            
            page.on("request", log_request)
            
            # Navigate to property page
            print("\nStep 2: Loading property page...")
            await page.goto(property_url, wait_until='networkidle', timeout=30000)
            print("✅ Page loaded")
            
            # Screenshot #1: Right after page load
            await page.screenshot(path='screenshot_1_page_loaded.png', full_page=True)
            print("📸 Screenshot 1: Page just loaded")
            
            # Wait 2 seconds
            print("\nWaiting 2 seconds for JavaScript to execute...")
            await page.wait_for_timeout(2000)
            
            # Screenshot #2: After 2 seconds
            await page.screenshot(path='screenshot_2_after_2sec.png', full_page=True)
            print("📸 Screenshot 2: After 2 seconds")
            
            # CLICK THE SEND MESSAGE BUTTON!
            print("\nStep 3: Clicking 'Send message' button...")
            try:
                # Find and click the button
                send_button = await page.query_selector('input[type="submit"][value="Send message"]')
                if send_button:
                    await send_button.click()
                    print("✅ Clicked 'Send message' button")
                    
                    # Wait for phone numbers to appear
                    await page.wait_for_timeout(2000)
                    
                    # Screenshot #3: After clicking
                    await page.screenshot(path='screenshot_3_after_click.png', full_page=True)
                    print("📸 Screenshot 3: After clicking Send message")
                else:
                    print("❌ Send message button not found!")
            except Exception as e:
                print(f"❌ Error clicking button: {e}")
            
            # Check for phone numbers NOW
            phones_text = await page.evaluate('''() => {
                const divs = document.querySelectorAll('.publisher-phones');
                let text = '';
                divs.forEach(div => {
                    console.log('Phone div HTML:', div.innerHTML);
                    text += div.textContent;
                });
                return text.trim();
            }''')
            
            print(f"Phone div content: '{phones_text}'")
            
            if phones_text:
                print(f"✅ PHONE NUMBERS FOUND: {phones_text}")
            else:
                print("⚠️ Phone div is EMPTY")
                
                # Check if there's ANY text in the publisher container
                publisher_text = await page.evaluate('''() => {
                    const pub = document.querySelector('.publisher-container');
                    return pub ? pub.textContent.trim() : 'NOT FOUND';
                }''')
                print(f"Publisher container text: {publisher_text[:200]}")
                
                # Screenshot the specific contact section
                contact_section = await page.query_selector('.publisher-info, .publisher-container')
                if contact_section:
                    await contact_section.screenshot(path='screenshot_4_contact_section.png')
                    print("📸 Screenshot 4: Contact section closeup")
                
            print(f"\n📡 Captured {len(all_requests)} network requests")
            # Show requests that might contain phone data
            property_id = property_url.split('/')[-1]
            relevant = [r for r in all_requests if property_id in r or 'message' in r or 'casa-en' in r]
            if relevant:
                print("Relevant requests:")
                for r in relevant[:10]:
                    print(f"  - {r}")
            
            # Get final HTML
            html = await page.content()
            
            # Save HTML
            with open('debug_page_cookies.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print("✅ Saved HTML to debug_page_cookies.html")
            
            # Parse and verify login
            print("\nStep 3: Verifying login status...")
            soup = BeautifulSoup(html, 'html.parser')
            
            # Check if logged in
            user_info = soup.select_one('#current_user_info')
            if user_info:
                user_email = user_info.get('data-email', '')
                user_name = user_info.get('data-name', '')
                print(f"✅ Logged in as: {user_name} ({user_email})")
            else:
                print("❌ NOT LOGGED IN - Login failed or cookies expired")
                print("Please run get_cookies.py again")
                return
            
            # Extract agent and agency info
            print("\nStep 4: Extracting agent and agency info...")
            
            agent_name = None
            agency_name = None
            phone_numbers = set()
            
            # Get agent and agency names
            publisher_name = soup.select_one('.publisher-name')
            if publisher_name:
                agent_name = publisher_name.get_text(strip=True)
                print(f"✅ Agent: {agent_name}")
            
            publisher_org = soup.select_one('.publisher-organization-name')
            if publisher_org:
                agency_name = publisher_org.get_text(strip=True)
                print(f"✅ Agency: {agency_name}")
            
            # Extract phone numbers
            
            # Check publisher-phones divs
            phones_divs = soup.select('.publisher-phones')
            print(f"Found {len(phones_divs)} publisher-phones divs")
            
            for div in phones_divs:
                # Find all phone links
                phone_links = div.select('a[href^="tel:"]')
                for link in phone_links:
                    phone = link.get('href', '').replace('tel:', '').strip()
                    if phone:
                        phone_numbers.add(phone)
                        print(f"✅ Found phone: {phone}")
            
            # Search for phone patterns in entire HTML
            phone_pattern = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
            found_phones = re.findall(phone_pattern, html)
            
            for phone in found_phones:
                cleaned = phone.strip()
                if len(cleaned) >= 10:
                    # Filter out common false positives (dimensions, years, etc)
                    if not any(x in cleaned for x in ['1920', '1080', '2024', '2025']):
                        phone_numbers.add(cleaned)
            
            await browser.close()
            
            # Results
            print("\n" + "="*80)
            print("EXTRACTION RESULTS")
            print("="*80)
            
            if agent_name:
                print(f"👤 Agent Name: {agent_name}")
            else:
                print("⚠️  Agent Name: Not found")
                
            if agency_name:
                print(f"🏢 Agency Name: {agency_name}")
            else:
                print("⚠️  Agency Name: Not found")
            
            if phone_numbers:
                print(f"\n📞 Phone Numbers ({len(phone_numbers)} found):")
                for phone in sorted(phone_numbers):
                    print(f"   • {phone}")
                    
                print("\n✅ SUCCESS! All contact info extracted!")
            else:
                print("\n⚠️ No phone numbers found")
                print("\nNote: The 'Send message' button was clicked but no phones appeared.")
                print("This property might not have phone numbers configured.")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(test_with_cookies())
