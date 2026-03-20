"""
sites/books_toscrape/transforms.py — Pure normalization functions.

Handles price cleanup, rating conversion, stock count extraction,
URL resolution, and deterministic ID generation.
"""

import hashlib
import re
from typing import Optional
from urllib.parse import urljoin

from sites.books_toscrape.config import RATING_MAP


def parse_price(raw: str) -> Optional[float]:
    """
    Extract numeric price from raw string.

    '£51.77' → 51.77
    'Â£51.77' → 51.77
    '' → None
    """
    if not raw:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw.strip())
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_price_str(raw: str) -> str:
    """
    Extract numeric price as a clean string.

    '£51.77' → '51.77'
    '' → ''
    """
    val = parse_price(raw)
    return f"{val:.2f}" if val is not None else ""


def parse_rating(class_str: str) -> Optional[int]:
    """
    Convert star-rating CSS class to integer 1-5.

    'star-rating Three' → 3
    'star-rating One' → 1
    '' → None
    """
    if not class_str:
        return None
    parts = class_str.lower().split()
    for part in parts:
        if part in RATING_MAP:
            return RATING_MAP[part]
    return None


def rating_text_from_class(class_str: str) -> str:
    """
    Extract the rating word from the CSS class.

    'star-rating Three' → 'Three'
    """
    if not class_str:
        return ""
    parts = class_str.split()
    for part in parts:
        if part.lower() in RATING_MAP:
            return part
    return ""


def parse_stock_count(availability_text: str) -> Optional[int]:
    """
    Extract integer stock count from availability text.

    'In stock (22 available)' → 22
    'In stock' → None
    'Out of stock' → 0
    """
    if not availability_text:
        return None
    text = availability_text.strip().lower()
    if "out of stock" in text:
        return 0
    m = re.search(r"\((\d+)\s+available\)", availability_text)
    if m:
        return int(m.group(1))
    if "in stock" in text:
        return None  # in stock but count unknown
    return None


def resolve_url(base: str, relative: str) -> str:
    """
    Resolve a relative URL against a base URL.

    resolve_url('https://books.toscrape.com/catalogue/page-2.html', '../the-book/index.html')
    → 'https://books.toscrape.com/catalogue/the-book/index.html'
    """
    if not relative:
        return ""
    if relative.startswith(("http://", "https://")):
        return relative
    return urljoin(base, relative)


def make_book_id(upc: str, product_url: str) -> str:
    """
    Generate a deterministic book ID.

    Prefers UPC as the stable ID. Falls back to SHA1 of product URL.
    """
    if upc and upc.strip():
        return upc.strip()
    if product_url:
        return hashlib.sha1(product_url.encode()).hexdigest()[:16]
    return ""
