#!/usr/bin/env python3
"""
Debug script for Pincali.com scraper
Tests connectivity and basic functionality with better error handling and feedback
"""

import asyncio
import json
import logging
import signal
import sys
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PincaliDebugger:
    def __init__(self):
        self.base_url = "https://www.pincali.com"
        self.target_url = "https://www.pincali.com/en/properties/residential-listings-for-sale-or-rent"
        
        # More conservative browser config for debugging
        self.browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            java_script_enabled=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            browser_type="chromium",
            # Add extra options to avoid detection
            extra_args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-plugins",
                "--disable-images",
                "--disable-javascript",  # Try without JS first
            ]
        )
        
        # Track if we should stop
        self.should_stop = False
    
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        logger.info("🛑 Received interrupt signal, stopping...")
        self.should_stop = True
    
    async def test_basic_connectivity(self):
        """Test basic connectivity to Pincali.com with timeout"""
        logger.info("🌐 Testing basic connectivity to Pincali.com...")
        logger.info("   This may take 30-60 seconds, please wait...")
        
        try:
            # Very aggressive timeout settings for testing
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                wait_for="body",
                page_timeout=15000,  # Even shorter timeout
                delay_before_return_html=1000,  # Much shorter delay
                verbose=False,
                # Don't wait for JavaScript since we disabled it
                js_code=None,
                wait_for_images=False
            )
            
            logger.info("🚀 Launching browser and navigating to Pincali...")
            
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                if self.should_stop:
                    return None
                
                # Add timeout wrapper with fallback
                try:
                    result = await asyncio.wait_for(
                        crawler.arun(self.target_url, config=crawler_config),
                        timeout=25.0  # Shorter timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning("⚠️ Browser method timed out, trying fallback...")
                    # Fallback to simple HTTP request since that worked
                    return await self.fallback_http_method()
                except Exception as e:
                    logger.warning(f"⚠️ Browser method failed: {e}")
                    logger.info("🔄 Trying HTTP fallback...")
                    return await self.fallback_http_method()
                
                if result.success:
                    logger.info("✅ Successfully connected to Pincali.com!")
                    logger.info(f"📄 Page title: {result.metadata.get('title', 'N/A')}")
                    logger.info(f"📏 HTML length: {len(result.html):,} characters")
                    
                    # Quick check for content
                    if "pincali" in result.html.lower():
                        logger.info("✅ Pincali content detected")
                    else:
                        logger.warning("⚠️ Pincali content not clearly detected")
                    
                    return result
                else:
                    logger.error(f"❌ Failed to connect: {result.error_message}")
                    return None
        
        except Exception as e:
            logger.error(f"❌ Connection error: {e}")
            return None
    
    async def fallback_http_method(self):
        """Fallback method using HTTP requests when browser fails"""
        logger.info("🔄 Using HTTP fallback method...")
        
        try:
            import requests
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.pincali.com',
            }
            
            response = requests.get(self.target_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                logger.info("✅ HTTP fallback successful!")
                
                # Create a mock result object that matches what crawler returns
                class MockResult:
                    def __init__(self, html, success=True):
                        self.html = html
                        self.success = success
                        self.metadata = {'title': 'Pincali Properties (HTTP)'}
                        self.error_message = None
                
                return MockResult(response.text)
            else:
                logger.error(f"❌ HTTP fallback failed: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ HTTP fallback error: {e}")
            return None
    
    async def quick_structure_analysis(self, html_content: str):
        """Quick analysis of page structure"""
        logger.info("🔍 Performing quick structure analysis...")
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Basic page info
        title = soup.find('title')
        if title:
            logger.info(f"📋 Page title: {title.get_text().strip()}")
        
        # Look for key elements
        elements_to_check = [
            ('Property listings', ['div[class*="property"]', 'div[class*="listing"]', 'article']),
            ('Price elements', ['.price', '[class*="price"]', '.amount']),
            ('Navigation/Pagination', ['.pagination', '[class*="page"]', 'nav']),
            ('Images', ['img']),
            ('Links', ['a[href]'])
        ]
        
        for element_type, selectors in elements_to_check:
            found = False
            total_count = 0
            
            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    found = True
                    total_count += len(elements)
            
            if found:
                logger.info(f"✅ {element_type}: Found {total_count} elements")
            else:
                logger.info(f"❌ {element_type}: Not found")
    
    async def save_debug_files(self, html_content: str):
        """Save debug files for manual inspection"""
        try:
            import os
            
            # Create outputs directory if it doesn't exist
            output_dir = 'outputs'
            os.makedirs(output_dir, exist_ok=True)
            
            # Save full HTML
            full_html_path = os.path.join(output_dir, 'pincali_debug_full.html')
            with open(full_html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"💾 Saved full HTML to: {full_html_path}")
            
            # Save a cleaned version
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove scripts and styles for cleaner viewing
            for script in soup(["script", "style"]):
                script.decompose()
            
            clean_html_path = os.path.join(output_dir, 'pincali_debug_clean.html')
            with open(clean_html_path, 'w', encoding='utf-8') as f:
                f.write(soup.prettify())
            logger.info(f"💾 Saved clean HTML to: {clean_html_path}")
            
            # Analyze the actual HTML structure more deeply
            soup_full = BeautifulSoup(html_content, 'html.parser')
            
            # Create a detailed analysis file
            analysis_path = os.path.join(output_dir, 'pincali_structure_analysis.txt')
            with open(analysis_path, 'w', encoding='utf-8') as f:
                f.write("PINCALI DETAILED STRUCTURE ANALYSIS\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"URL: {self.target_url}\n")
                f.write(f"HTML Length: {len(html_content):,} characters\n")
                f.write(f"Title: {soup_full.find('title').get_text().strip() if soup_full.find('title') else 'N/A'}\n\n")
                
                # Analyze property containers in detail
                f.write("PROPERTY CONTAINER ANALYSIS\n")
                f.write("-" * 40 + "\n")
                
                # Try different selectors and show what we find
                selectors_to_test = [
                    ('Articles', 'article'),
                    ('Divs with property class', 'div[class*="property"]'),
                    ('Divs with listing class', 'div[class*="listing"]'),
                    ('Divs with card class', 'div[class*="card"]'),
                    ('Property components (Pincali)', 'li.property__component'),
                ]
                
                for name, selector in selectors_to_test:
                    elements = soup_full.select(selector)
                    f.write(f"{name} ({selector}): {len(elements)} found\n")
                    
                    if elements:
                        # Show first element's structure
                        first_elem = elements[0]
                        f.write(f"  Sample classes: {first_elem.get('class', [])}\n")
                        f.write(f"  Sample attributes: {list(first_elem.attrs.keys())}\n")
                        f.write(f"  Sample text (first 100 chars): {first_elem.get_text()[:100]}...\n")
                        f.write("\n")
                
                # Look for specific property data patterns
                f.write("\nPROPERTY DATA PATTERNS\n")
                f.write("-" * 40 + "\n")
                
                # Price patterns
                price_patterns = [
                    ('.price', 'Standard .price class'),
                    ('[class*="price"]', 'Any class containing "price"'),
                    ('[data-testid*="price"]', 'Data testid with price'),
                    ('span:contains("$")', 'Spans containing $'),
                    ('div:contains("$")', 'Divs containing $'),
                ]
                
                for selector, desc in price_patterns:
                    try:
                        if ':contains(' in selector:
                            # Skip CSS :contains for now as BeautifulSoup doesn't support it
                            continue
                        elements = soup_full.select(selector)
                        f.write(f"{desc}: {len(elements)} found\n")
                        if elements:
                            sample_prices = [elem.get_text().strip() for elem in elements[:3]]
                            f.write(f"  Sample prices: {sample_prices}\n")
                    except Exception as e:
                        f.write(f"{desc}: Error - {e}\n")
                
                f.write(f"\nTotal elements with '$' in text: {len([elem for elem in soup_full.find_all(text=True) if '$' in str(elem)])}\n")
                
            logger.info(f"💾 Saved detailed analysis to: {analysis_path}")
            
            # Create a simple summary
            summary_path = os.path.join(output_dir, 'pincali_debug_summary.txt')
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("PINCALI DEBUG SUMMARY\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"URL: {self.target_url}\n")
                f.write(f"HTML Length: {len(html_content):,} characters\n")
                f.write(f"Title: {soup_full.find('title').get_text().strip() if soup_full.find('title') else 'N/A'}\n\n")
                
                # Count key elements
                property_divs = len(soup_full.select('div[class*="property"], div[class*="listing"], article'))
                price_elements = len(soup_full.select('.price, [class*="price"], .amount'))
                images = len(soup_full.select('img'))
                links = len(soup_full.select('a[href]'))
                
                f.write(f"Potential property containers: {property_divs}\n")
                f.write(f"Price elements: {price_elements}\n")
                f.write(f"Images: {images}\n")
                f.write(f"Links: {links}\n")
            
            logger.info(f"💾 Saved summary to: {summary_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save debug files: {e}")
    
    async def run_debug_test(self):
        """Run a focused debug test"""
        logger.info("🚀 Starting Pincali.com debug test...")
        logger.info("⏱️ This test will timeout after 60 seconds total")
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        
        try:
            # Test connectivity with timeout
            start_time = asyncio.get_event_loop().time()
            result = await self.test_basic_connectivity()
            
            if self.should_stop:
                logger.info("🛑 Test interrupted by user")
                return
            
            if not result:
                logger.error("❌ Cannot proceed - connectivity test failed")
                logger.info("\n🔧 Troubleshooting suggestions:")
                logger.info("   1. Check your internet connection")
                logger.info("   2. Try running with headless=False to see browser")
                logger.info("   3. Check if Pincali.com is accessible in your browser")
                logger.info("   4. Consider using a VPN if the site is geo-blocked")
                return
            
            elapsed = asyncio.get_event_loop().time() - start_time
            logger.info(f"⏱️ Connectivity test completed in {elapsed:.1f} seconds")
            
            # Quick analysis
            await self.quick_structure_analysis(result.html)
            
            # Save debug files
            await self.save_debug_files(result.html)
            
            logger.info("\n✅ Debug test completed successfully!")
            logger.info("📁 Check these files for detailed analysis:")
            logger.info("   - pincali_debug_full.html (complete page)")
            logger.info("   - pincali_debug_clean.html (cleaned version)")
            logger.info("   - pincali_debug_summary.txt (quick summary)")
            
        except Exception as e:
            logger.error(f"❌ Debug test failed: {e}")
            logger.info("\n🔧 Try these solutions:")
            logger.info("   1. Restart the script")
            logger.info("   2. Check your internet connection")
            logger.info("   3. Update dependencies: pip install -r requirements.txt")

async def main():
    """Main function with better error handling"""
    debugger = PincaliDebugger()
    
    try:
        await asyncio.wait_for(debugger.run_debug_test(), timeout=90.0)
    except asyncio.TimeoutError:
        logger.error("❌ Debug test timed out after 90 seconds")
        logger.info("💡 The website might be slow or blocking requests")
    except KeyboardInterrupt:
        logger.info("🛑 Debug test interrupted by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    print("🔧 Pincali Debug Tool")
    print("=" * 40)
    print("This tool will:")
    print("• Test connectivity to Pincali.com")
    print("• Analyze the page structure")
    print("• Save debug files for inspection")
    print("• Timeout after 90 seconds")
    print("\nPress Ctrl+C to stop at any time")
    print("=" * 40)
    
    asyncio.run(main()) 