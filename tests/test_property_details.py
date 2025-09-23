#!/usr/bin/env python3
"""
Test script to verify property detail page scraping
"""

import asyncio
import json
import os
from pincali_scraper import PincaliScraper

async def test_property_detail_scraping():
    """Test scraping a single property detail page"""
    
    # Test URL from the screenshot
    test_url = "https://www.pincali.com/en/home/estrena-departamento-en-corregidora-piramides"
    
    print("🧪 Testing Property Detail Scraping")
    print("=" * 50)
    print(f"Test URL: {test_url}")
    print()
    
    try:
        scraper = PincaliScraper()
        
        print("📡 Fetching property details...")
        details = await scraper.scrape_property_details(test_url)
        
        if details:
            print(f"✅ Successfully extracted {len(details)} detail fields!")
            print("\n📋 Extracted Details:")
            print("-" * 30)
            
            for key, value in details.items():
                if isinstance(value, list):
                    print(f"{key}: {len(value)} items")
                    if key == "image_urls":
                        print(f"  Sample images: {value[:3]}")
                    elif key == "features":
                        print(f"  Sample features: {value[:5]}")
                else:
                    # Truncate long text for display
                    display_value = str(value)
                    if len(display_value) > 100:
                        display_value = display_value[:100] + "..."
                    print(f"{key}: {display_value}")
            
            # Save to outputs folder for inspection
            os.makedirs('outputs', exist_ok=True)
            
            with open('outputs/test_property_details.json', 'w', encoding='utf-8') as f:
                json.dump(details, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Saved detailed results to: outputs/test_property_details.json")
            
        else:
            print("❌ No details extracted - check the selectors")
            
    except Exception as e:
        print(f"❌ Error during testing: {e}")

async def test_listing_plus_details():
    """Test the complete flow: listing page + detail pages"""
    print("\n" + "=" * 50)
    print("🧪 Testing Complete Flow (Listing + Details)")
    print("=" * 50)
    
    try:
        scraper = PincaliScraper()
        
        # Test with just the first page
        print("📡 Scraping first page of listings...")
        properties = await scraper.scrape_property_list_page(scraper.target_url, 1)
        
        if properties:
            print(f"✅ Found {len(properties)} properties on listing page")
            
            # Test detail scraping for first 3 properties
            print(f"\n🔍 Testing detail scraping for first 3 properties...")
            
            for i, prop in enumerate(properties[:3]):
                if prop.get('link'):
                    print(f"\nProperty {i+1}: {prop.get('title', 'Unknown')}")
                    detail_url = scraper.base_url + prop['link'] if not prop['link'].startswith('http') else prop['link']
                    
                    details = await scraper.scrape_property_details(detail_url)
                    
                    if details:
                        print(f"  ✅ Extracted {len(details)} additional details")
                        
                        # Show some key details
                        if details.get('description'):
                            desc_preview = details['description'][:100] + "..." if len(details['description']) > 100 else details['description']
                            print(f"  📝 Description: {desc_preview}")
                        
                        if details.get('image_urls'):
                            print(f"  🖼️  Images: {len(details['image_urls'])} found")
                        
                        if details.get('features'):
                            print(f"  🏠 Features: {len(details['features'])} found")
                            
                    else:
                        print(f"  ❌ No additional details extracted")
                    
                    # Small delay between requests
                    await asyncio.sleep(1)
                else:
                    print(f"Property {i+1}: No link found")
                    
        else:
            print("❌ No properties found on listing page")
            
    except Exception as e:
        print(f"❌ Error during complete flow test: {e}")

async def main():
    """Run all tests"""
    await test_property_detail_scraping()
    await test_listing_plus_details()
    
    print("\n" + "=" * 50)
    print("✅ Testing completed!")
    print("📁 Check outputs/ folder for detailed results")

if __name__ == "__main__":
    asyncio.run(main()) 