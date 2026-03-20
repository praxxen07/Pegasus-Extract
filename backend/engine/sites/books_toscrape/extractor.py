"""
sites/books_toscrape/extractor.py — Page-level extraction for Books to Scrape.

Since the site is static HTML, extraction is simple:
navigate → page.content() → delegate to parser.py.
No JS evaluation or tab clicking needed.
"""

import logging
from typing import List, Optional

from playwright.async_api import Page

from sites.books_toscrape.parser import parse_detail_page, parse_listing_page

logger = logging.getLogger("extraction-engine")


async def extract_book_detail(page: Page, url: str) -> Optional[dict]:
    """
    Extract a single book's full record from its detail page.

    Returns the book record dict or None on failure.
    """
    try:
        html = await page.content()
    except Exception as e:
        logger.warning("Failed to get page content for %s: %s", url, e)
        return None

    record = parse_detail_page(html, url)

    if not record.get("title"):
        logger.warning("No title found on %s", url)
        return None

    return record


async def discover_books_from_listing(
    page: Page,
    start_url: str,
    max_pages: int = 50,
) -> List[dict]:
    """
    Crawl listing pages starting from start_url, following pagination.

    Returns a list of book stub dicts with detail_url, title, price, etc.
    """
    from core.browser import BrowserManager

    all_books = []
    current_url = start_url
    page_num = 0

    while current_url and page_num < max_pages:
        page_num += 1
        logger.info("  Discovery page %d: %s", page_num, current_url)

        await BrowserManager.navigate_static(page, current_url, settle_ms=800)
        html = await page.content()

        books, next_url = parse_listing_page(html, current_url)
        all_books.extend(books)

        logger.info("    Found %d books (total: %d)", len(books), len(all_books))
        current_url = next_url

    return all_books
