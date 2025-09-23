#!/usr/bin/env python3
"""
Test script to verify PropScraper setup and dependencies
"""

import sys
import os
from dotenv import load_dotenv

def test_python_version():
    """Test Python version compatibility"""
    print("🐍 Testing Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.8+")
        return False

def test_dependencies():
    """Test required dependencies"""
    print("\n📦 Testing dependencies...")
    
    dependencies = [
        ('crawl4ai', 'AsyncWebCrawler'),
        ('supabase', 'create_client'),
        ('bs4', 'BeautifulSoup'),
        ('dotenv', 'load_dotenv'),
        ('requests', 'get'),
    ]
    
    all_good = True
    
    for package, item in dependencies:
        try:
            module = __import__(package)
            if hasattr(module, item):
                print(f"✅ {package} - OK")
            else:
                print(f"❌ {package} - Missing {item}")
                all_good = False
        except ImportError:
            print(f"❌ {package} - Not installed")
            all_good = False
    
    return all_good

def test_environment_variables():
    """Test environment variables"""
    print("\n🔐 Testing environment variables...")
    
    load_dotenv()
    
    required_vars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY']
    all_good = True
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var} - Set")
        else:
            print(f"❌ {var} - Missing")
            all_good = False
    
    return all_good

def test_file_structure():
    """Test required files exist"""
    print("\n📁 Testing file structure...")
    
    required_files = [
        'pincali_scraper.py',
        'inmuebles24_scraper.py',
        'debug_pincali_scraper.py',
        'schema.sql',
        'requirements.txt',
        '.env'
    ]
    
    all_good = True
    
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file} - Found")
        else:
            print(f"❌ {file} - Missing")
            if file == '.env':
                print("   💡 Create .env file from env_example.txt")
            all_good = False
    
    return all_good

def main():
    """Run all tests"""
    print("🚀 PropScraper Setup Test\n")
    
    tests = [
        test_python_version,
        test_dependencies,
        test_environment_variables,
        test_file_structure
    ]
    
    results = []
    
    for test in tests:
        results.append(test())
    
    print("\n" + "="*50)
    
    if all(results):
        print("✅ All tests passed! PropScraper is ready to use.")
        print("\n🎯 Next steps:")
        print("   1. Run: python debug_pincali_scraper.py")
        print("   2. If debug works, run: python pincali_scraper.py")
    else:
        print("❌ Some tests failed. Please fix the issues above.")
        print("\n🔧 Common fixes:")
        print("   - Install dependencies: pip install -r requirements.txt")
        print("   - Install Playwright: playwright install")
        print("   - Create .env file from env_example.txt")
        print("   - Set up Supabase project and get credentials")
    
    return all(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 