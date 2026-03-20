"""
sites/books_toscrape/parser.py — HTML parsing for Books to Scrape.

BeautifulSoup-based extraction for listing pages and detail pages.
Since the site is static HTML, no JS evaluation needed.
"""

import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from sites.books_toscrape.config import (
    LISTING_CONTAINER, LISTING_TITLE_LINK, LISTING_PRICE,
    LISTING_AVAILABILITY, LISTING_RATING, LISTING_IMAGE,
    LISTING_NEXT_PAGE, DETAIL_TITLE, DETAIL_PRICE,
    DETAIL_AVAILABILITY, DETAIL_RATING, DETAIL_IMAGE,
    DETAIL_DESCRIPTION, DETAIL_TABLE, DETAIL_BREADCRUMB,
    TABLE_KEY_MAP,
)
from sites.books_toscrape.transforms import (
    parse_price_str, parse_rating, rating_text_from_class,
    parse_stock_count, resolve_url, make_book_id,
)

logger = logging.getLogger("extraction-engine")


def _text(tag: Optional[Tag]) -> str:
    """Get stripped text from a tag, or empty string."""
    if tag is None:
        return ""
    return (tag.get_text(strip=True) or "").strip()


def _classes_str(tag: Optional[Tag]) -> str:
    """Get the full class attribute string from a tag."""
    if tag is None:
        return ""
    classes = tag.get("class", [])
    return " ".join(classes) if isinstance(classes, list) else str(classes)


def parse_listing_page(html: str, page_url: str) -> Tuple[List[dict], Optional[str]]:
    """
    Parse a listing/catalogue page.

    Returns:
        (book_stubs, next_page_url) where each stub has:
        title, detail_url, price, availability_text, rating_text, rating_value, image_url
    """
    soup = BeautifulSoup(html, "html.parser")
    books = []

    for article in soup.select(LISTING_CONTAINER):
        stub: dict = {}

        # Title + detail link
        title_link = article.select_one(LISTING_TITLE_LINK)
        if title_link:
            stub["title"] = title_link.get("title", "") or _text(title_link)
            href = title_link.get("href", "")
            stub["detail_url"] = resolve_url(page_url, href)

        # Price
        price_el = article.select_one(LISTING_PRICE)
        stub["price_gbp"] = parse_price_str(_text(price_el))

        # Availability
        avail_el = article.select_one(LISTING_AVAILABILITY)
        stub["availability_text"] = _text(avail_el)

        # Rating
        rating_el = article.select_one(LISTING_RATING)
        cls = _classes_str(rating_el)
        stub["rating_text"] = rating_text_from_class(cls)
        stub["rating_value"] = parse_rating(cls)

        # Image
        img_el = article.select_one(LISTING_IMAGE)
        if img_el:
            stub["image_url"] = resolve_url(page_url, img_el.get("src", ""))

        if stub.get("detail_url"):
            books.append(stub)

    # Next page
    next_link = soup.select_one(LISTING_NEXT_PAGE)
    next_url = None
    if next_link:
        href = next_link.get("href", "")
        if href:
            next_url = resolve_url(page_url, href)

    return books, next_url


def parse_detail_page(html: str, page_url: str) -> dict:
    """
    Parse a book detail page into a full record.

    Returns a dict with all 20 target fields populated where possible.
    """
    soup = BeautifulSoup(html, "html.parser")
    record: dict = {"product_page_url": page_url}

    # Title
    title_el = soup.select_one(DETAIL_TITLE)
    record["title"] = _text(title_el)

    # Price (main)
    price_el = soup.select_one(DETAIL_PRICE)
    record["price_gbp"] = parse_price_str(_text(price_el))

    # Availability
    avail_el = soup.select_one(DETAIL_AVAILABILITY)
    avail_text = _text(avail_el)
    record["availability_text"] = avail_text
    stock = parse_stock_count(avail_text)
    record["stock_count"] = str(stock) if stock is not None else ""

    # Rating
    rating_el = soup.select_one(DETAIL_RATING)
    cls = _classes_str(rating_el)
    record["rating_text"] = rating_text_from_class(cls)
    val = parse_rating(cls)
    record["rating_value"] = str(val) if val is not None else ""

    # Image
    img_el = soup.select_one(DETAIL_IMAGE)
    if img_el:
        record["image_url"] = resolve_url(page_url, img_el.get("src", ""))
    else:
        record["image_url"] = ""

    # Description
    desc_el = soup.select_one(DETAIL_DESCRIPTION)
    record["description"] = _text(desc_el)

    # Breadcrumb
    bc_el = soup.select_one(DETAIL_BREADCRUMB)
    if bc_el:
        crumbs = [_text(li) for li in bc_el.select("li")]
        record["breadcrumb"] = " > ".join(crumbs)
        # Category is the second-to-last breadcrumb (last is the book title)
        if len(crumbs) >= 2:
            record["category"] = crumbs[-2]
        else:
            record["category"] = ""
    else:
        record["breadcrumb"] = ""
        record["category"] = ""

    # Product information table
    table = soup.select_one(DETAIL_TABLE)
    if table:
        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if th and td:
                key = _text(th)
                value = _text(td)
                field_name = TABLE_KEY_MAP.get(key)
                if field_name:
                    if field_name.endswith("_gbp"):
                        record[field_name] = parse_price_str(value)
                    elif field_name == "number_of_reviews":
                        record[field_name] = value
                    elif field_name == "availability_detail":
                        # More detailed availability from table
                        if not record.get("stock_count"):
                            stock = parse_stock_count(value)
                            record["stock_count"] = str(stock) if stock is not None else ""
                    else:
                        record[field_name] = value

    # Generate book_id (UPC preferred, URL hash fallback)
    record["book_id"] = make_book_id(record.get("upc", ""), page_url)

    # Defaults for missing optional fields
    for fld in ["catalogue_page_url", "upc", "product_type",
                "price_excl_tax_gbp", "price_incl_tax_gbp", "tax_gbp",
                "number_of_reviews", "description", "image_url",
                "category", "breadcrumb", "scraped_at"]:
        record.setdefault(fld, "")

    return record
