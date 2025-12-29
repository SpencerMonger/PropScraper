"""
Centralized Property ID Generation

This module provides a single, deterministic method for generating property IDs
from source URLs. All scrapers and services should use this function to ensure
consistency across the entire system.

The ID is generated using an MD5 hash of the normalized URL path, ensuring:
1. Deterministic: Same URL always produces the same ID
2. Fixed length: Always 16 characters (plus prefix)
3. URL-independent: Works regardless of URL length or format changes
"""

import hashlib
import re
from urllib.parse import urlparse, urlunparse


def normalize_url(source_url: str) -> str:
    """
    Normalize a URL for consistent hashing.
    
    Normalization steps:
    1. Parse the URL
    2. Remove query parameters
    3. Remove fragment
    4. Remove trailing slashes
    5. Convert to lowercase
    6. Keep only the path portion for hashing
    
    Args:
        source_url: The full property URL
        
    Returns:
        Normalized URL path string for hashing
    """
    if not source_url:
        return ""
    
    try:
        parsed = urlparse(source_url)
        
        # Normalize the path: lowercase, strip trailing slashes
        path = parsed.path.lower().rstrip('/')
        
        # Remove common prefixes that don't affect uniqueness
        # e.g., /en/home/ is just routing, the property slug is what matters
        # But we keep it for safety in case different languages have different properties
        
        # Reconstruct with just scheme, netloc, and normalized path
        normalized = urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            '',  # params
            '',  # query - removed for consistency
            ''   # fragment - removed
        ))
        
        return normalized
        
    except Exception:
        # Fallback to basic normalization
        return source_url.lower().rstrip('/').split('?')[0].split('#')[0]


def generate_property_id(source_url: str) -> str:
    """
    Generate a deterministic property ID from a source URL.
    
    This is the SINGLE SOURCE OF TRUTH for property ID generation.
    All scrapers, manifest scanners, and sync services should use this function.
    
    Format: pincali_{md5_hash[:16]}
    
    Examples:
        https://www.pincali.com/en/home/beautiful-house-cancun -> pincali_a3f8b2c1d4e5f6a7
        https://www.pincali.com/en/home/apartment-playa -> pincali_b7c9d1e2f3a4b5c6
    
    Args:
        source_url: The full URL to the property detail page
        
    Returns:
        A deterministic property ID string (pincali_{16-char-hash})
    """
    if not source_url:
        # Fallback for empty URLs - should not happen in practice
        return f"pincali_{hashlib.md5('empty'.encode()).hexdigest()[:16]}"
    
    # Normalize the URL for consistent hashing
    normalized = normalize_url(source_url)
    
    # Generate MD5 hash of the normalized URL
    url_hash = hashlib.md5(normalized.encode()).hexdigest()[:16]
    
    return f"pincali_{url_hash}"


def extract_source_url_from_path(base_url: str, path: str) -> str:
    """
    Construct a full source URL from a base URL and path.
    
    Args:
        base_url: The base URL (e.g., https://www.pincali.com)
        path: The path (e.g., /en/home/property-name)
        
    Returns:
        Full URL string
    """
    from urllib.parse import urljoin
    return urljoin(base_url, path)


# For backwards compatibility and explicit imports
__all__ = ['generate_property_id', 'normalize_url', 'extract_source_url_from_path']

