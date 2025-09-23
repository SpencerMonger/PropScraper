#!/usr/bin/env python3
"""
Simple connectivity test for Pincali.com
Uses basic HTTP requests instead of browser automation
"""

import requests
import time
from urllib.parse import urlparse

def test_basic_http_connectivity():
    """Test basic HTTP connectivity to Pincali.com"""
    print("🌐 Testing basic HTTP connectivity to Pincali.com...")
    
    url = "https://www.pincali.com"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        print(f"📡 Making HTTP request to {url}...")
        start_time = time.time()
        
        response = requests.get(url, headers=headers, timeout=30)
        
        elapsed = time.time() - start_time
        
        print(f"✅ Response received in {elapsed:.2f} seconds")
        print(f"📊 Status Code: {response.status_code}")
        print(f"📏 Content Length: {len(response.content):,} bytes")
        print(f"🔧 Content Type: {response.headers.get('content-type', 'Unknown')}")
        
        if response.status_code == 200:
            print("✅ Website is accessible!")
            
            # Check if content looks like Pincali
            content_lower = response.text.lower()
            if 'pincali' in content_lower:
                print("✅ Pincali content detected")
            else:
                print("⚠️  Pincali content not clearly detected")
                
            # Look for property-related keywords
            property_keywords = ['property', 'properties', 'real estate', 'casa', 'apartment']
            found_keywords = [kw for kw in property_keywords if kw in content_lower]
            if found_keywords:
                print(f"✅ Property-related keywords found: {', '.join(found_keywords)}")
            else:
                print("⚠️  No property-related keywords detected")
                
            return True
            
        elif response.status_code == 403:
            print("❌ Access forbidden (403) - Website might be blocking automated requests")
            return False
        elif response.status_code == 404:
            print("❌ Page not found (404)")
            return False
        else:
            print(f"⚠️  Unexpected status code: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out after 30 seconds")
        return False
    except requests.exceptions.ConnectionError:
        print("❌ Connection error - check your internet connection")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False

def test_target_url():
    """Test the specific target URL for property listings"""
    print("\n🎯 Testing target URL for property listings...")
    
    url = "https://www.pincali.com/en/properties/residential-listings-for-sale-or-rent"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.pincali.com',
    }
    
    try:
        print(f"📡 Making request to property listings page...")
        start_time = time.time()
        
        response = requests.get(url, headers=headers, timeout=30)
        
        elapsed = time.time() - start_time
        
        print(f"✅ Response received in {elapsed:.2f} seconds")
        print(f"📊 Status Code: {response.status_code}")
        print(f"📏 Content Length: {len(response.content):,} bytes")
        
        if response.status_code == 200:
            print("✅ Property listings page is accessible!")
            
            # Look for property listing indicators
            content_lower = response.text.lower()
            listing_indicators = ['property', 'listing', 'for sale', 'for rent', 'bedrooms', 'price']
            found_indicators = [ind for ind in listing_indicators if ind in content_lower]
            
            if found_indicators:
                print(f"✅ Property listing indicators found: {', '.join(found_indicators)}")
            else:
                print("⚠️  No clear property listing indicators found")
                
            return True
        else:
            print(f"❌ Failed to access property listings page: {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to access target URL: {e}")
        return False

def main():
    """Run simple connectivity tests"""
    print("🔧 Simple Pincali Connectivity Test")
    print("=" * 50)
    print("This test uses basic HTTP requests (no browser)")
    print("to check if Pincali.com is accessible.\n")
    
    # Test basic connectivity
    basic_success = test_basic_http_connectivity()
    
    if basic_success:
        # Test target URL
        target_success = test_target_url()
        
        print("\n" + "=" * 50)
        if target_success:
            print("✅ All connectivity tests passed!")
            print("\n🎯 Next steps:")
            print("   1. Run: python debug_pincali_scraper.py")
            print("   2. If that works, run: python pincali_scraper.py")
        else:
            print("⚠️  Basic connectivity works, but property page has issues")
            print("\n💡 The website might:")
            print("   - Require JavaScript to load content")
            print("   - Use dynamic loading for property listings")
            print("   - Have different URL structure")
    else:
        print("\n" + "=" * 50)
        print("❌ Basic connectivity failed")
        print("\n🔧 Troubleshooting:")
        print("   1. Check your internet connection")
        print("   2. Try accessing Pincali.com in your browser")
        print("   3. Check if you need a VPN")
        print("   4. Verify the website is not down")

if __name__ == "__main__":
    main() 