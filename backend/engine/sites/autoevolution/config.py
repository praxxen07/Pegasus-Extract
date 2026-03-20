"""
sites/autoevolution/config.py — Autoevolution site-specific configuration.

All selectors, URL patterns, anchor regex, section keywords, and CSV column
definitions specific to autoevolution.com live here.
"""

import re
from typing import List


# ── Domain ─────────────────────────────────────────────────────────────────

DOMAIN = "www.autoevolution.com"
BASE_URL = f"https://{DOMAIN}"

# ── Anchor patterns ────────────────────────────────────────────────────────

ANCHOR_PREFIX = "aeng_"
ANCHOR_RE = re.compile(r"^aeng_[a-z0-9-]+-\d+-hp$|^aeng_[a-z0-9-]+$")

# ── Expected engine count on page ──────────────────────────────────────────

EXPECTED_COUNT_RE = re.compile(
    r"Available\s+(?:with|in)\s+(\d+)\s+engine",
    re.IGNORECASE,
)

# ── Year-only label detection ──────────────────────────────────────────────

YEAR_ONLY_RE = re.compile(r"^\d{4}$")
YEAR_RANGE_RE = re.compile(r"^\d{4}-\d{4}$")

# ── Spec section keywords ─────────────────────────────────────────────────

SECTION_KEYWORDS = {
    "ENGINE SPECS", "PERFORMANCE SPECS", "TRANSMISSION SPECS",
    "DRIVETRAIN SPECS", "BRAKES SPECS", "TIRES SPECS", "DIMENSIONS",
    "WEIGHT SPECS", "FUEL ECONOMY", "FUEL ECONOMY (NEDC)",
    "FUEL ECONOMY (WLTP)", "POWER SYSTEM SPECS", "BATTERY SPECS",
    "CHARGING SPECS", "SUSPENSION SPECS", "STEERING SPECS",
    "EMISSION SPECS", "CO2 EMISSIONS", "AERODYNAMICS",
}

# ── CSS selectors for sidebar engine list ──────────────────────────────────

SIDEBAR_ENGINE_LI = 'li[id^="li_eng_"]'
SIDEBAR_ENGINE_LINK = "a"

# ── CSV flat schema ────────────────────────────────────────────────────────

CSV_COLUMNS: List[str] = [
    "spec_id",
    "engine_id",
    "engine_anchor_id",
    "engine_specs_url",
    "engine_url",
    "model_year_id",
    "model_id",
    "brand_id",
    "brand_name",
    "model_name",
    "model_year_url",
    "model_year_label",
    "engine_name",
    "section_name",
    "spec_label",
    "spec_value",
    "position",
]

# ── Required fields for validation ─────────────────────────────────────────

REQUIRED_ENGINE_FIELDS = [
    "engine_id",
    "brand_id",
    "model_id",
    "model_year_id",
    "model_year_url",
    "model_year_label",
    "engine_name",
    "engine_anchor_id",
    "engine_specs_url",
    "engine_url",
    "spec_sections",
]

REQUIRED_CSV_FIELDS = [
    "spec_id",
    "engine_id",
    "model_year_id",
    "model_id",
    "brand_id",
    "model_year_url",
    "model_year_label",
    "engine_name",
    "section_name",
    "spec_label",
    "spec_value",
]


# ── Helpers ────────────────────────────────────────────────────────────────

def valid_anchor(anchor: str) -> bool:
    """Check if an anchor looks like a real engine anchor."""
    if not anchor:
        return False
    return anchor.startswith(ANCHOR_PREFIX) and len(anchor) > len(ANCHOR_PREFIX) + 5


def anchor_prefix_for_url(url: str) -> str:
    """
    Derive the expected anchor prefix for a model-year URL.

    e.g. .../cars/maserati-grancabrio-trofeo-2024.html
         -> "aeng_maserati-grancabrio-trofeo-2024-"
    """
    from urllib.parse import urlsplit
    path = urlsplit(url).path
    slug = path.rstrip("/").split("/")[-1].replace(".html", "")
    return f"aeng_{slug}-"


def filter_anchors_for_url(anchors: List[str], url: str) -> List[str]:
    """Keep only anchors belonging to this model-year page."""
    prefix = anchor_prefix_for_url(url)
    return [a for a in anchors if a.startswith(prefix)]


def is_generic_label(label: str) -> bool:
    """Check if a model-year label is just a bare year or year-range."""
    label = label.strip() if label else ""
    return bool(YEAR_ONLY_RE.fullmatch(label) or YEAR_RANGE_RE.fullmatch(label))
