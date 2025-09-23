#!/usr/bin/env python3
"""
Test script for new parsing methods in Pincali scraper
Tests the enhanced data extraction features
"""

import sys
import os
from datetime import datetime

# Add the parent directory to Python path so we can import the scraper
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pincali_scraper import PincaliScraper

def test_property_type_extraction():
    """Test property type extraction from text"""
    scraper = PincaliScraper()
    
    test_cases = [
        ("Beautiful house in downtown area", "house"),
        ("Modern apartment with great view", "apartment"),
        ("Spacious departamento near metro", "apartment"),
        ("Large lot for sale", "lot"),
        ("Villa with pool", "house"),
        ("Condo in luxury building", "apartment"),
        ("Terreno comercial", "lot"),
        ("Casa familiar", "house"),
    ]
    
    print("Testing property type extraction:")
    for text, expected in test_cases:
        result = scraper.extract_property_type_from_text(text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{text}' -> '{result}' (expected: '{expected}')")
    print()

def test_listing_date_extraction():
    """Test listing date extraction from published text"""
    scraper = PincaliScraper()
    
    scraped_at = datetime.now().isoformat()
    
    test_cases = [
        "Published 2 hours ago",
        "Published 1 day ago", 
        "Published 3 weeks ago",
        "Published a minute ago",
        "Published an hour ago",
        "Published 5 days ago",
        "Published 1 month ago",
        "Published 2 years ago",
        " Published an hour ago ",  # Test with extra whitespace
        "published 30 minutes ago",  # Test lowercase
        "Published a day ago",
        "Published a week ago",
    ]
    
    print("Testing listing date extraction:")
    for published_text in test_cases:
        result = scraper.extract_listing_date(published_text, scraped_at)
        status = "✓" if result else "✗"
        print(f"  {status} '{published_text}' -> {result}")
    print()

def test_agent_agency_parsing():
    """Test agent and agency name parsing"""
    scraper = PincaliScraper()
    
    test_cases = [
        "Published by: John Smith\nABC Real Estate\nphone: 555-1234",
        "Contact: Maria Garcia\nInvermax Bienes Raices\nemail: maria@invermax.com",
        "Lucia López\nInvermax Bienes Raices",
        "Published by Angelica Espinosa de los Monteros\nAmir Cherit Real Estate",
    ]
    
    print("Testing agent and agency parsing:")
    for agent_text in test_cases:
        result = scraper.extract_agent_and_agency_info(agent_text)
        print(f"  Input: {repr(agent_text)}")
        print(f"  Agent: {result.get('agent_name', 'Not found')}")
        print(f"  Agency: {result.get('agency_name', 'Not found')}")
        print(f"  Phone: {result.get('agent_phone', 'Not found')}")
        print(f"  Email: {result.get('agent_email', 'Not found')}")
        print()

def test_amenities_structuring():
    """Test amenities structuring"""
    scraper = PincaliScraper()
    
    test_amenities = {
        "amenities": [
            "Pool",
            "Covered parking",
            "Elevator",
            "Garden",
            "Gym",
            "Pets allowed",
            "Tennis court",
            "24 hour security",
            "Furnished",
            "Laundry room"
        ]
    }
    
    print("Testing amenities structuring:")
    result = scraper.structure_amenities(test_amenities)
    
    for category, items in result.items():
        print(f"  {category.title()}:")
        for item in items:
            print(f"    - {item}")
    print()

def test_location_extraction():
    """Test enhanced location extraction with postal codes and state parsing"""
    scraper = PincaliScraper()
    
    test_cases = [
        {
            "input": "House in Ciudad Caucel, Mérida, Yucatán 97314",
            "expected": {
                "address": "House in Ciudad Caucel",
                "neighborhood": "Mérida", 
                "state": "Yucatán",
                "postal_code": "97314"
            }
        },
        {
            "input": "Apartment in Valle Dorado, Tlalnepantla de Baz, Estado de México",
            "expected": {
                "address": "Apartment in Valle Dorado",
                "neighborhood": "Tlalnepantla de Baz",
                "state": "Estado de México"
            }
        },
        {
            "input": "Bosque Real, Huixquilucan, Estado de México 52774",
            "expected": {
                "address": "Bosque Real",
                "neighborhood": "Huixquilucan",
                "state": "Estado de México",
                "postal_code": "52774"
            }
        },
        {
            "input": "Condominium in Bosque Esmeralda, Atizapán de Zaragoza 52930",
            "expected": {
                "address": "Condominium in Bosque Esmeralda",
                "neighborhood": "Atizapán de Zaragoza",
                "postal_code": "52930"
            }
        }
    ]
    
    print("Testing enhanced location extraction:")
    for test_case in test_cases:
        result = scraper.extract_location(test_case["input"])
        print(f"  Input: {test_case['input']}")
        print(f"  Result: {result}")
        
        # Check each expected field
        for field, expected_value in test_case["expected"].items():
            actual_value = result.get(field)
            status = "✓" if actual_value == expected_value else "✗"
            print(f"    {status} {field}: '{actual_value}' (expected: '{expected_value}')")
        print()

def test_description_cleaning():
    """Test description cleaning to remove 'Description' prefix"""
    scraper = PincaliScraper()
    
    test_cases = [
        ("Description: Beautiful house with garden", "Beautiful house with garden"),
        ("Description Beautiful apartment", "Beautiful apartment"),
        ("DESCRIPTION: Modern villa with pool", "Modern villa with pool"),
        ("description: Spacious lot for sale", "Spacious lot for sale"),
        ("Beautiful house without prefix", "Beautiful house without prefix"),
        ("Description:Luxury condo", "Luxury condo"),
        ("", ""),
    ]
    
    print("Testing description cleaning:")
    for input_text, expected in test_cases:
        result = scraper.clean_text(input_text)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{input_text}' -> '{result}' (expected: '{expected}')")
    print()

def test_gps_coordinates():
    """Test GPS coordinates handling"""
    scraper = PincaliScraper()
    
    test_data = {
        "latitude": "19.2545",
        "longitude": "-99.1727"
    }
    
    print("Testing GPS coordinates:")
    cleaned_data = scraper.extract_property_details(test_data)
    
    if 'latitude' in cleaned_data and 'longitude' in cleaned_data:
        print(f"  ✓ Latitude: {cleaned_data['latitude']}")
        print(f"  ✓ Longitude: {cleaned_data['longitude']}")
        print(f"  ✓ GPS String: {cleaned_data.get('gps_coordinates', 'Not found')}")
    else:
        print("  ✗ GPS coordinates not extracted properly")
    print()

def main():
    """Run all tests"""
    print("=" * 60)
    print("TESTING NEW PINCALI SCRAPER PARSING METHODS")
    print("=" * 60)
    print()
    
    try:
        test_property_type_extraction()
        test_listing_date_extraction()
        test_agent_agency_parsing()
        test_amenities_structuring()
        test_location_extraction()
        test_description_cleaning()
        test_gps_coordinates()
        
        print("=" * 60)
        print("All tests completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 