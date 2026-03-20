"""
sites/books_toscrape/crawler.py — SiteAdapter implementation for Books to Scrape.

Implements the full SiteAdapter interface:
- get_input_records: load pilot URLs or run seed discovery
- extract_page: navigate detail page → parse → return ExtractResult
- to_csv_rows: 1:1 (one book = one row)
- validate: required fields, duplicates, price/rating checks
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from playwright.async_api import Page

from core.browser import BrowserManager
from core.config import RunConfig
from core.models import ExtractResult, PageStatus
from core.validators import ValidationReport, Validator
from sites.base import SiteAdapter
from sites.books_toscrape.config import (
    BASE_URL, CSV_COLUMNS, REQUIRED_FIELDS, FLAGGABLE_FIELDS,
)
from sites.books_toscrape.extractor import extract_book_detail
from sites.books_toscrape.transforms import parse_price

logger = logging.getLogger("extraction-engine")


class BooksSiteAdapter(SiteAdapter):
    """
    Books to Scrape SiteAdapter implementation.

    Supports two modes via input file:
    - Pilot: load a JSON list of book detail URLs
    - Discovery: seed URLs → crawl listings → collect detail URLs
    """

    def __init__(self, input_path: Optional[Path] = None):
        self._input_path = input_path

    # ── SiteAdapter interface ──────────────────────────────────────────────

    @property
    def site_name(self) -> str:
        return "Books to Scrape"

    @property
    def url_field(self) -> str:
        return "product_page_url"

    @property
    def label_field(self) -> str:
        return "title"

    def get_input_records(self, config: RunConfig) -> List[dict]:
        """Load input records from pilot JSON file."""
        path = self._input_path or config.input_path
        if not path or not path.exists():
            logger.warning("No input file found at %s", path)
            return []

        with open(path, encoding="utf-8") as f:
            records = json.load(f)

        if config.limit:
            records = records[:config.limit]

        logger.info("Loaded %d input records from %s", len(records), path)
        return records

    async def extract_page(
        self, page: Page, record: dict, config: RunConfig
    ) -> ExtractResult:
        """
        Extract a single book's data from its detail page.

        Returns ExtractResult with one record (1:1 book-to-record).
        """
        url = record.get("product_page_url", record.get("url", ""))
        if not url:
            return ExtractResult(
                url="", status=PageStatus.FAILED, reason="No URL in record"
            )

        # Navigate
        try:
            await BrowserManager.navigate_static(
                page, url,
                nav_timeout_ms=config.nav_timeout_ms,
                settle_ms=min(config.settle_ms, 800),  # static site needs less settle
            )
        except Exception as e:
            return ExtractResult(
                url=url, status=PageStatus.FAILED,
                reason=f"Navigation failed: {e}",
            )

        # Extract
        book = await extract_book_detail(page, url)

        if not book:
            return ExtractResult(
                url=url, status=PageStatus.FAILED,
                reason="Empty record — no title or meaningful data",
            )

        # Stamp extraction time
        book["scraped_at"] = datetime.now(timezone.utc).isoformat()

        # Carry forward catalogue_page_url from input if present
        if record.get("catalogue_page_url"):
            book["catalogue_page_url"] = record["catalogue_page_url"]

        # Check for flaggable issues
        missing_flags = [f for f in FLAGGABLE_FIELDS if not book.get(f)]
        if missing_flags:
            return ExtractResult(
                url=url, status=PageStatus.OK,
                records=[book],
                reason="",
                meta={"flagged_missing": missing_flags},
            )

        return ExtractResult(url=url, status=PageStatus.OK, records=[book])

    def to_csv_rows(self, record: dict) -> List[dict]:
        """Convert a book record to CSV rows (1:1 — one book = one row)."""
        row = {}
        for col in CSV_COLUMNS:
            val = record.get(col, "")
            row[col] = str(val) if val is not None else ""
        return [row]

    def csv_columns(self) -> List[str]:
        return CSV_COLUMNS

    def validate(
        self, records: List[dict], input_records: List[dict]
    ) -> ValidationReport:
        """Run validation checks on extracted book records."""
        report = ValidationReport()

        # Required fields
        req_report = Validator.check_required_fields(
            records, REQUIRED_FIELDS, record_type="book"
        )
        for k, v in req_report.issues.items():
            report.add_issue(k, v)

        # Duplicate book_id
        dup_report = Validator.detect_duplicates(
            records, id_field="book_id", label="book_id"
        )
        for k, v in dup_report.issues.items():
            report.add_issue(k, v)

        # Duplicate product_page_url
        dup_url_report = Validator.detect_duplicates(
            records, id_field="product_page_url", label="product_url"
        )
        for k, v in dup_url_report.issues.items():
            report.add_issue(k, v)

        # Price sanity
        for rec in records:
            price = parse_price(rec.get("price_gbp", ""))
            if price is not None and (price < 0 or price > 10000):
                report.add_issue("price_out_of_range")

        # Rating sanity
        for rec in records:
            rv = rec.get("rating_value", "")
            if rv and rv.isdigit():
                v = int(rv)
                if v < 1 or v > 5:
                    report.add_issue("rating_out_of_range")

        return report
