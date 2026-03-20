"""
sites/books_toscrape/config.py — Constants and selectors for Books to Scrape.

All site-specific selectors, URL patterns, CSV schema, and field lists live here.
"""

# ── Base URLs ──────────────────────────────────────────────────────────────
BASE_URL = "https://books.toscrape.com"
CATALOGUE_URL = f"{BASE_URL}/catalogue/"

# ── Listing page selectors ─────────────────────────────────────────────────
LISTING_CONTAINER = "article.product_pod"
LISTING_TITLE_LINK = "h3 > a"
LISTING_PRICE = "p.price_color"
LISTING_AVAILABILITY = "p.availability"  # also "p.instock.availability"
LISTING_RATING = "p.star-rating"
LISTING_IMAGE = "div.image_container img"
LISTING_NEXT_PAGE = "li.next > a"

# ── Detail page selectors ──────────────────────────────────────────────────
DETAIL_TITLE = "div.product_main > h1"
DETAIL_PRICE = "p.price_color"
DETAIL_AVAILABILITY = "p.availability"
DETAIL_RATING = "p.star-rating"
DETAIL_IMAGE = "#product_gallery img"
DETAIL_DESCRIPTION = "#product_description ~ p"
DETAIL_TABLE = "table.table-striped"
DETAIL_BREADCRUMB = "ul.breadcrumb"

# ── Rating class mapping ──────────────────────────────────────────────────
RATING_MAP = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
}

# ── Product info table expected keys ──────────────────────────────────────
TABLE_KEY_MAP = {
    "UPC": "upc",
    "Product Type": "product_type",
    "Price (excl. tax)": "price_excl_tax_gbp",
    "Price (incl. tax)": "price_incl_tax_gbp",
    "Tax": "tax_gbp",
    "Availability": "availability_detail",
    "Number of reviews": "number_of_reviews",
}

# ── CSV schema (20 columns) ───────────────────────────────────────────────
CSV_COLUMNS = [
    "book_id",
    "product_page_url",
    "catalogue_page_url",
    "title",
    "price_gbp",
    "availability_text",
    "stock_count",
    "rating_text",
    "rating_value",
    "upc",
    "product_type",
    "price_excl_tax_gbp",
    "price_incl_tax_gbp",
    "tax_gbp",
    "number_of_reviews",
    "category",
    "description",
    "image_url",
    "breadcrumb",
    "scraped_at",
]

# ── Validation ─────────────────────────────────────────────────────────────
REQUIRED_FIELDS = ["book_id", "product_page_url", "title"]
FLAGGABLE_FIELDS = ["description", "rating_value", "stock_count", "image_url"]
