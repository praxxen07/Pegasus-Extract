"""
sites/autoevolution/parser.py — HTML/DOM parsing helpers for Autoevolution.

BeautifulSoup-based fallback parsers for when JS-based extraction fails.
Also provides utility text normalization functions.

Extracted from extract_l4_fixed.py parse_engine_tables_from_html() and helpers.
"""

import hashlib
import re
from typing import List, Optional, Tuple
from urllib.parse import urlsplit

from bs4 import BeautifulSoup


# ── Text normalization ─────────────────────────────────────────────────────

def normalize_space(text: str) -> str:
    """Collapse whitespace, strip NBSP and zero-width chars."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\u00a0", " ").replace("\u200b", "")).strip()


def clean(x) -> str:
    """Normalize a value to a stripped string."""
    return "" if x is None else str(x).strip()


def sha1_text(text: str) -> str:
    """SHA-1 hash of text, used for generating stable IDs."""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def make_id(*parts) -> str:
    """Generate a SHA-1 ID from concatenated parts."""
    combined = "|".join(str(p) for p in parts)
    return sha1_text(combined)


def extract_anchor_from_url(url: str) -> str:
    """Extract the fragment/anchor from a URL."""
    frag = urlsplit(url).fragment
    return frag.strip() if frag else ""


# ── Spec table parsing (BeautifulSoup fallback) ───────────────────────────

def parse_engine_tables_from_html(html: str) -> Tuple[List[str], List[dict]]:
    """
    Parse engine spec tables from raw HTML using BeautifulSoup.

    This is the fallback when JS-based extraction doesn't work.
    Returns (anchors, spec_sections).
    """
    if not html:
        return [], []

    soup = BeautifulSoup(html, "lxml")
    anchors = []
    spec_sections = []

    # Find engine anchors
    for a in soup.find_all("a", id=True):
        aid = a.get("id", "")
        if aid.startswith("aeng_"):
            anchors.append(aid)

    # Find spec tables
    for table in soup.find_all("table", class_="techdata"):
        current_section = None
        current_items = []

        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            # Section header: single cell or colspan
            if len(cells) == 1 or (cells[0].get("colspan") and cells[0]["colspan"] != "1"):
                text = normalize_space(cells[0].get_text())
                if text and _is_section_header(text):
                    if current_section and current_items:
                        spec_sections.append({
                            "section_name": current_section,
                            "items": current_items,
                        })
                    current_section = _clean_section_name(text)
                    current_items = []
                continue

            # Data row: label + value
            if len(cells) >= 2:
                label = normalize_space(cells[0].get_text()).rstrip(":")
                value = normalize_space(cells[1].get_text())
                if label and value:
                    if _is_section_header(label):
                        if current_section and current_items:
                            spec_sections.append({
                                "section_name": current_section,
                                "items": current_items,
                            })
                        current_section = _clean_section_name(label)
                        current_items = []
                    else:
                        if not current_section:
                            current_section = "SPECS"
                        current_items.append({"label": label, "value": value})

        if current_section and current_items:
            spec_sections.append({
                "section_name": current_section,
                "items": current_items,
            })

    return anchors, spec_sections


def _is_section_header(text: str) -> bool:
    """Check if text is a spec section header."""
    from sites.autoevolution.config import SECTION_KEYWORDS

    upper = re.sub(r"\s*[-–]\s+.*$", "", text).strip().upper()
    if upper in SECTION_KEYWORDS:
        return True
    if upper.startswith("ENGINE SPECS"):
        return True
    if upper.endswith(" SPECS"):
        return True
    return False


def _clean_section_name(text: str) -> str:
    """Clean a section header name."""
    return re.sub(r"\s*[-–]\s+.*$", "", text).strip()
