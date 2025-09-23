#!/usr/bin/env python3
"""
Test script for captcha solver integration
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))

from old.captcha_solver import CaptchaSolver, CrawlerWithCaptchaSolver
from crawl4ai import BrowserConfig, CrawlerRunConfig

# Enable detailed logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_captcha_solver_initialization():
    """Test if captcha solver can be initialized properly"""
    logger.info("🔍 Testing CaptchaSolver initialization...")
    
    try:
        # Test without API key (should fail)
        try:
            solver = CaptchaSolver()
            logger.error("❌ Should have failed without API key")
            return False
        except ValueError as e:
            logger.info(f"✅ Correctly failed without API key: {e}")
        
        # Test with dummy API key
        solver = CaptchaSolver(api_key="dummy_key_for_testing")
        logger.info("✅ CaptchaSolver initialized with dummy key")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during initialization test: {e}")
        return False

async def test_captcha_detection():
    """Test captcha detection in HTML content"""
    logger.info("🔍 Testing captcha detection...")
    
    try:
        # Initialize with dummy solver
        solver = CaptchaSolver(api_key="dummy_key")
        crawler_with_captcha = CrawlerWithCaptchaSolver(solver)
        
        # Test HTML with Cloudflare Turnstile
        cloudflare_html = '''
        <html>
        <body>
            <div class="cf-turnstile" data-sitekey="0x4AAAAAABBBBCCCDDDeeee"></div>
            <p>Checking your browser before accessing the website.</p>
        </body>
        </html>
        '''
        
        # Test HTML with reCAPTCHA
        recaptcha_html = '''
        <html>
        <body>
            <div class="g-recaptcha" data-sitekey="6LdyC2cUAAAAACGuDKpXeDorzUDWDstqtVS5V3-Z"></div>
            <p>Please verify you are human</p>
        </body>
        </html>
        '''
        
        # Test HTML without captcha
        normal_html = '''
        <html>
        <body>
            <h1>Welcome to our website</h1>
            <p>This is normal content without any captcha.</p>
        </body>
        </html>
        '''
        
        # Test captcha detection
        tests = [
            (cloudflare_html, True, "Cloudflare Turnstile"),
            (recaptcha_html, True, "reCAPTCHA"),
            (normal_html, False, "Normal content")
        ]
        
        all_passed = True
        for html, should_detect, description in tests:
            detected = crawler_with_captcha._has_captcha_challenge(html)
            if detected == should_detect:
                logger.info(f"✅ {description}: {'Detected' if detected else 'Not detected'} (correct)")
            else:
                logger.error(f"❌ {description}: Expected {should_detect}, got {detected}")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        logger.error(f"❌ Error during captcha detection test: {e}")
        return False

async def test_captcha_info_extraction():
    """Test captcha information extraction from HTML"""
    logger.info("🔍 Testing captcha info extraction...")
    
    try:
        solver = CaptchaSolver(api_key="dummy_key")
        crawler_with_captcha = CrawlerWithCaptchaSolver(solver)
        
        # Test Cloudflare Turnstile extraction
        cloudflare_html = '''
        <div class="cf-turnstile" data-sitekey="0x4AAAAAABBBBCCCDDDeeee" data-theme="light"></div>
        '''
        
        # Test reCAPTCHA extraction
        recaptcha_html = '''
        <div class="g-recaptcha" data-sitekey="6LdyC2cUAAAAACGuDKpXeDorzUDWDstqtVS5V3-Z"></div>
        '''
        
        test_url = "https://example.com/test"
        
        # Test Cloudflare extraction
        cf_info = crawler_with_captcha._extract_captcha_info(cloudflare_html, test_url)
        if cf_info and cf_info['type'] == 'turnstile' and cf_info['site_key'] == '0x4AAAAAABBBBCCCDDDeeee':
            logger.info("✅ Cloudflare Turnstile info extracted correctly")
        else:
            logger.error(f"❌ Cloudflare extraction failed: {cf_info}")
            return False
        
        # Test reCAPTCHA extraction
        rc_info = crawler_with_captcha._extract_captcha_info(recaptcha_html, test_url)
        if rc_info and rc_info['type'] == 'recaptcha_v2' and rc_info['site_key'] == '6LdyC2cUAAAAACGuDKpXeDorzUDWDstqtVS5V3-Z':
            logger.info("✅ reCAPTCHA info extracted correctly")
        else:
            logger.error(f"❌ reCAPTCHA extraction failed: {rc_info}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during info extraction test: {e}")
        return False

async def test_basic_crawler_integration():
    """Test basic crawler integration without actual captcha solving"""
    logger.info("🔍 Testing basic crawler integration...")
    
    try:
        # Only test if we have a real API key
        api_key = os.getenv('SOLVECAPTCHA_API_KEY')
        if not api_key or api_key == 'your_solvecaptcha_api_key_here':
            logger.info("⚠️  No real API key found, skipping integration test")
            return True
        
        solver = CaptchaSolver(api_key=api_key)
        browser_config = BrowserConfig(
            headless=True,
            browser_type="chromium",
            verbose=True
        )
        
        crawler_with_captcha = CrawlerWithCaptchaSolver(solver, browser_config)
        
        # Test with a simple page (should work without captcha)
        test_url = "https://httpbin.org/html"
        logger.info(f"Testing basic crawl with: {test_url}")
        
        result = await crawler_with_captcha.crawl_with_captcha_handling(
            test_url,
            max_captcha_attempts=1
        )
        
        if result and result.success:
            logger.info("✅ Basic crawler integration test passed")
            return True
        else:
            logger.error("❌ Basic crawler integration test failed")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error during integration test: {e}")
        return False

async def main():
    """Run all captcha solver tests"""
    logger.info("=" * 60)
    logger.info("🧪 CAPTCHA SOLVER TESTS")
    logger.info("=" * 60)
    
    tests = [
        ("Solver Initialization", test_captcha_solver_initialization),
        ("Captcha Detection", test_captcha_detection),
        ("Info Extraction", test_captcha_info_extraction),
        ("Basic Integration", test_basic_crawler_integration),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running: {test_name}")
        logger.info("-" * 40)
        
        try:
            success = await test_func()
            results[test_name] = success
            status = "✅ PASSED" if success else "❌ FAILED"
            logger.info(f"{status} - {test_name}")
            
        except Exception as e:
            results[test_name] = False
            logger.error(f"❌ FAILED - {test_name}: {e}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST RESULTS SUMMARY")
    logger.info("=" * 60)
    
    passed_count = sum(results.values())
    total_count = len(results)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status} {test_name}")
    
    logger.info(f"\nOverall: {passed_count}/{total_count} tests passed")
    
    # Recommendations
    logger.info("\n🔍 SETUP INSTRUCTIONS:")
    logger.info("1. Sign up for SolveCaptcha at https://solvecaptcha.com")
    logger.info("2. Get your API key from the dashboard")
    logger.info("3. Add SOLVECAPTCHA_API_KEY=your_key_here to your .env file")
    logger.info("4. Install dependencies: pip install solvecaptcha-python")
    
    if passed_count == total_count:
        logger.info("\n🎉 All tests passed! Captcha solver is ready to use.")
    else:
        logger.info(f"\n⚠️  {total_count - passed_count} test(s) failed. Check the logs above.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"�� Fatal error: {e}") 