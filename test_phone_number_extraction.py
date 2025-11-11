#!/usr/bin/env python3
"""
Standalone test file to extract phone numbers from Pincali property listings.
The phone numbers are hidden behind a "Send message" button interaction.

This test file will:
1. Navigate to a property detail page
2. Click the "Send message" button to reveal contact info
3. Extract agent and agency phone numbers
4. Test the logic before integrating into main scraper

Usage:
    python test_phone_number_extraction.py <property_url>
    
Example:
    python test_phone_number_extraction.py https://www.pincali.com/en/home/eb-ur0278
"""

import asyncio
import logging
import sys
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class PincaliPhoneExtractor:
    """Test class to extract phone numbers from Pincali property pages"""
    
    def __init__(self):
        self.browser_config = BrowserConfig(
            headless=True,  # Run without visible browser
            verbose=True,
            java_script_enabled=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            browser_type="chromium"
        )
    
    async def extract_phone_numbers_with_click(self, property_url: str) -> Dict:
        """
        Extract phone numbers by clicking the message/contact button
        
        Returns:
            Dict with agent_name, agency_name, agent_phone, agency_phone
        """
        logger.info(f"Extracting phone numbers from: {property_url}")
        
        result = {
            "agent_name": None,
            "agency_name": None,
            "agent_phone": None,
            "agency_phone": None,
            "all_phones": []
        }
        
        try:
            # Configure crawler with JavaScript execution
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                wait_for="body",
                page_timeout=60000,
                delay_before_return_html=3000,  # Wait 3 seconds after page load
                js_code=[
                    # JavaScript to click WhatsApp button and reveal phone numbers (avoiding email links)
                    """
                    // Wait for page to be fully loaded
                    await new Promise(r => setTimeout(r, 2000));
                    
                    console.log('Searching for WhatsApp/phone buttons...');
                    
                    // Find WhatsApp buttons, avoiding email links
                    const allButtons = document.querySelectorAll('a, button');
                    const messageButtons = [];
                    
                    for (const btn of allButtons) {
                        const text = btn.textContent.toLowerCase();
                        const href = btn.href ? btn.href.toLowerCase() : '';
                        
                        // SKIP email links
                        if (href.includes('mailto:')) {
                            console.log('Skipping email link:', text);
                            continue;
                        }
                        
                        // Look for WhatsApp or phone buttons
                        if (href.includes('whatsapp') || href.includes('wa.me') ||
                            text.includes('whatsapp') || text.includes('enviar whatsapp')) {
                            console.log('Found WhatsApp button:', text);
                            messageButtons.push(btn);
                        }
                    }
                    
                    console.log('Found WhatsApp buttons:', messageButtons.length);
                    
                    // Click the first available WhatsApp button
                    if (messageButtons.length > 0) {
                        console.log('Clicking WhatsApp button...');
                        messageButtons[0].click();
                        
                        // Wait for phone numbers to appear
                        await new Promise(r => setTimeout(r, 3000));
                        
                        // Try to find phone number elements
                        const phoneElements = document.querySelectorAll(
                            '.publisher-phones, .agent-phone, .agency-phone, ' +
                            '[class*="phone"], [class*="telefono"], a[href^="tel:"]'
                        );
                        
                        console.log('Found phone elements after click:', phoneElements.length);
                        
                        // Mark phone elements for easier extraction
                        phoneElements.forEach((el, idx) => {
                            el.setAttribute('data-phone-revealed', 'true');
                            console.log('Phone element', idx, ':', el.textContent);
                        });
                    } else {
                        console.log('No WhatsApp button found, phone numbers might already be visible');
                    }
                    
                    // Wait a bit more to ensure everything is loaded
                    await new Promise(r => setTimeout(r, 2000));
                    """
                ]
            )
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                crawl_result = await crawler.arun(property_url, config=crawler_config)
                
                if not crawl_result.success:
                    logger.error(f"Failed to crawl page: {crawl_result.error_message}")
                    return result
                
                logger.info("Page loaded and JavaScript executed")
                
                # Parse the HTML after JavaScript execution
                soup = BeautifulSoup(crawl_result.html, 'html.parser')
                
                # Extract agent/agency names
                publisher_name_elem = soup.select_one('.publisher-name')
                if publisher_name_elem:
                    result["agent_name"] = publisher_name_elem.get_text(strip=True)
                    logger.info(f"Found agent name: {result['agent_name']}")
                
                publisher_org_elem = soup.select_one('.publisher-organization-name')
                if publisher_org_elem:
                    result["agency_name"] = publisher_org_elem.get_text(strip=True)
                    logger.info(f"Found agency name: {result['agency_name']}")
                
                # Extract phone numbers - try multiple selectors
                phone_selectors = [
                    '.publisher-phones',
                    '.agent-phone',
                    '.agency-phone',
                    '[class*="phone"]',
                    '[class*="telefono"]',
                    '[data-phone-revealed="true"]',
                    'a[href^="tel:"]',
                    'a[href*="whatsapp"]'
                ]
                
                found_phones = set()
                
                for selector in phone_selectors:
                    phone_elements = soup.select(selector)
                    for elem in phone_elements:
                        # Get text content
                        phone_text = elem.get_text(strip=True)
                        
                        # Also check href attribute for tel: links
                        if elem.name == 'a':
                            href = elem.get('href', '')
                            if 'tel:' in href:
                                phone_text = href.replace('tel:', '').strip()
                            elif 'whatsapp.com' in href or 'wa.me' in href:
                                # Extract phone from WhatsApp URL
                                phone_match = re.search(r'(?:whatsapp\.com|wa\.me)/(\+?\d+)', href)
                                if phone_match:
                                    phone_text = phone_match.group(1)
                        
                        # Extract phone number using regex
                        phone_numbers = self.extract_phone_from_text(phone_text)
                        for phone in phone_numbers:
                            if phone:
                                found_phones.add(phone)
                                logger.info(f"Found phone number: {phone} (from selector: {selector})")
                
                # Convert to list and assign
                result["all_phones"] = list(found_phones)
                
                # Try to assign specific roles
                if len(result["all_phones"]) >= 1:
                    result["agent_phone"] = result["all_phones"][0]
                if len(result["all_phones"]) >= 2:
                    result["agency_phone"] = result["all_phones"][1]
                
                # Additional extraction: look for phone numbers in publisher section
                publisher_section = soup.select_one('.listing__publisher, .publisher-info, [class*="publisher"]')
                if publisher_section:
                    text_content = publisher_section.get_text()
                    additional_phones = self.extract_phone_from_text(text_content)
                    for phone in additional_phones:
                        if phone and phone not in found_phones:
                            found_phones.add(phone)
                            result["all_phones"].append(phone)
                            logger.info(f"Found additional phone in publisher section: {phone}")
                
                # Print the entire publisher section HTML for debugging
                if publisher_section:
                    logger.debug("Publisher section HTML:")
                    logger.debug(publisher_section.prettify()[:1000])
                
                return result
                
        except Exception as e:
            logger.error(f"Error extracting phone numbers: {e}", exc_info=True)
            return result
    
    def extract_phone_from_text(self, text: str) -> List[str]:
        """
        Extract phone numbers from text using regex patterns
        Supports various formats including international numbers
        """
        if not text:
            return []
        
        phone_numbers = []
        
        # Common phone number patterns
        patterns = [
            # International format: +52 686 523 1719 or +526865231719
            r'\+\d{1,3}\s*\d{3,4}\s*\d{3,4}\s*\d{3,4}',
            r'\+\d{10,15}',
            
            # Mexican format with parentheses: (686) 523-1719
            r'\(\d{3}\)\s*\d{3}[-\s]?\d{4}',
            
            # Standard format: 686-523-1719 or 686 523 1719
            r'\d{3}[-\s]?\d{3}[-\s]?\d{4}',
            
            # 10+ digit numbers
            r'\b\d{10,15}\b',
            
            # Format with dots: 686.523.1719
            r'\d{3}\.\d{3}\.\d{4}'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Clean the phone number
                cleaned = re.sub(r'[^\d+]', '', match)
                
                # Only keep if it has at least 10 digits
                if len(re.sub(r'\D', '', cleaned)) >= 10:
                    phone_numbers.append(match.strip())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_phones = []
        for phone in phone_numbers:
            if phone not in seen:
                seen.add(phone)
                unique_phones.append(phone)
        
        return unique_phones
    
    async def test_with_fallback_methods(self, property_url: str) -> Dict:
        """
        Test multiple methods to extract phone numbers
        """
        logger.info("=" * 80)
        logger.info("Testing phone number extraction with multiple methods")
        logger.info("=" * 80)
        
        # Method 1: With button click and JavaScript
        logger.info("\n--- Method 1: JavaScript Click Method ---")
        result1 = await self.extract_phone_numbers_with_click(property_url)
        logger.info(f"Result: {result1}")
        
        # Method 2: Simple HTTP request (for comparison)
        logger.info("\n--- Method 2: Simple HTTP Method (for comparison) ---")
        result2 = await self.extract_phone_numbers_http(property_url)
        logger.info(f"Result: {result2}")
        
        # Compare results
        logger.info("\n" + "=" * 80)
        logger.info("COMPARISON:")
        logger.info("=" * 80)
        logger.info(f"Method 1 (JS Click) found: {len(result1['all_phones'])} phone numbers")
        logger.info(f"Method 2 (HTTP) found: {len(result2['all_phones'])} phone numbers")
        
        if len(result1['all_phones']) > len(result2['all_phones']):
            logger.info("✅ JavaScript click method found more phone numbers!")
            return result1
        else:
            logger.info("⚠️  Both methods found similar results")
            return result1 if result1['all_phones'] else result2
    
    async def extract_phone_numbers_http(self, property_url: str) -> Dict:
        """
        Extract phone numbers using simple HTTP request (no JavaScript)
        This is the fallback method for comparison
        """
        import requests
        
        result = {
            "agent_name": None,
            "agency_name": None,
            "agent_phone": None,
            "agency_phone": None,
            "all_phones": []
        }
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            }
            
            response = requests.get(property_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract agent/agency names
                publisher_name_elem = soup.select_one('.publisher-name')
                if publisher_name_elem:
                    result["agent_name"] = publisher_name_elem.get_text(strip=True)
                
                publisher_org_elem = soup.select_one('.publisher-organization-name')
                if publisher_org_elem:
                    result["agency_name"] = publisher_org_elem.get_text(strip=True)
                
                # Extract phone numbers
                phone_elements = soup.select('.publisher-phones, [class*="phone"], a[href^="tel:"]')
                
                found_phones = set()
                for elem in phone_elements:
                    phone_text = elem.get_text(strip=True)
                    if elem.name == 'a':
                        href = elem.get('href', '')
                        if 'tel:' in href:
                            phone_text = href.replace('tel:', '').strip()
                    
                    phones = self.extract_phone_from_text(phone_text)
                    found_phones.update(phones)
                
                result["all_phones"] = list(found_phones)
                
                if len(result["all_phones"]) >= 1:
                    result["agent_phone"] = result["all_phones"][0]
                if len(result["all_phones"]) >= 2:
                    result["agency_phone"] = result["all_phones"][1]
            
            return result
            
        except Exception as e:
            logger.error(f"HTTP method error: {e}")
            return result


async def main():
    """Main function to test phone extraction"""
    
    # Check if URL is provided
    if len(sys.argv) < 2:
        print("\n❌ Please provide a Pincali property URL")
        print("\nUsage:")
        print("    python test_phone_number_extraction.py <property_url>")
        print("\nExample:")
        print("    python test_phone_number_extraction.py https://www.pincali.com/en/home/eb-ur0278")
        print("    python test_phone_number_extraction.py https://www.pincali.com/en/home/eb-ur0282")
        sys.exit(1)
    
    property_url = sys.argv[1]
    
    # Validate URL
    if "pincali.com" not in property_url:
        print("\n❌ URL must be from pincali.com")
        sys.exit(1)
    
    # Create extractor and test
    extractor = PincaliPhoneExtractor()
    
    # Run comprehensive test
    result = await extractor.test_with_fallback_methods(property_url)
    
    # Print final results
    print("\n" + "=" * 80)
    print("FINAL RESULTS:")
    print("=" * 80)
    print(f"Agent Name: {result['agent_name']}")
    print(f"Agency Name: {result['agency_name']}")
    print(f"Agent Phone: {result['agent_phone']}")
    print(f"Agency Phone: {result['agency_phone']}")
    print(f"All Phones Found: {result['all_phones']}")
    print("=" * 80)
    
    if result['all_phones']:
        print("\n✅ SUCCESS: Phone numbers extracted!")
    else:
        print("\n⚠️  WARNING: No phone numbers found. They might be hidden behind authentication or CAPTCHA.")
        print("\nNext steps to debug:")
        print("1. Check if the page requires login")
        print("2. Check if there's a CAPTCHA")
        print("3. Look at the browser window to see what's displayed")
        print("4. Try a different property URL")


if __name__ == "__main__":
    asyncio.run(main())

