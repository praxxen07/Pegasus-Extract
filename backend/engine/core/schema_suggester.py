"""
core/schema_suggester.py — Propose entity schema from a SiteProfile.

Takes a listing-page profile and an optional detail-page profile,
then suggests entity name, fields, CSV columns, and validation rules.

Generic V2 capability — no site-specific logic.
"""

import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from core.site_profiler import SiteProfile

logger = logging.getLogger("extraction-engine")


@dataclass
class SchemaField:
    """A proposed field for the entity schema."""
    name: str
    source: str = ""  # selector, table_row, breadcrumb, computed
    required: bool = False
    type_hint: str = "str"  # str, int, float, url, datetime
    notes: str = ""


@dataclass
class SchemaSuggestion:
    """Proposed entity schema derived from site profiles."""
    entity_name: str = "record"
    record_id_candidate: str = ""
    required_fields: List[str] = field(default_factory=list)
    optional_fields: List[str] = field(default_factory=list)
    all_fields: List[Dict[str, Any]] = field(default_factory=list)
    csv_columns: List[str] = field(default_factory=list)
    validation_rules: List[Dict[str, str]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def suggest_schema(
    listing_profile: SiteProfile,
    detail_profile: Optional[SiteProfile] = None,
    entity_name: str = "record",
) -> SchemaSuggestion:
    """
    Propose an entity schema from analyzed site profiles.

    Uses detected fields, page structure, and heuristics to suggest
    required/optional fields, CSV columns, and validation rules.
    """
    suggestion = SchemaSuggestion(entity_name=entity_name)

    # Always include core fields
    core_fields = [
        SchemaField("record_id", "computed", required=True, notes="Deterministic hash or unique key"),
        SchemaField("product_page_url", "link_href", required=True, type_hint="url"),
        SchemaField("title", "heading/link_text", required=True),
    ]

    detected_fields: List[SchemaField] = []

    # Infer from candidate fields
    for cf in listing_profile.candidate_fields:
        name = cf.get("name", "")
        if "price" in name:
            detected_fields.append(SchemaField("price", cf.get("selector", ""), type_hint="float"))
        elif "rating" in name:
            detected_fields.append(SchemaField("rating_value", cf.get("selector", ""), type_hint="int",
                                               notes="Numeric 1-5 from class/text"))
        elif "availability" in name:
            detected_fields.append(SchemaField("availability_text", cf.get("selector", "")))
            detected_fields.append(SchemaField("stock_count", "parsed from availability", type_hint="int",
                                               notes="Parse integer from availability text"))
        elif "image" in name:
            detected_fields.append(SchemaField("image_url", cf.get("selector", ""), type_hint="url"))

    # Infer from detail profile
    if detail_profile:
        if detail_profile.total_tables > 0:
            detected_fields.append(SchemaField("product_info_table", "table rows", notes="Key-value pairs from product table"))
        if detail_profile.breadcrumb:
            detected_fields.append(SchemaField("category", "breadcrumb", notes="Category from breadcrumb trail"))
            detected_fields.append(SchemaField("breadcrumb", "breadcrumb", notes="Full breadcrumb path"))
        # Description
        detected_fields.append(SchemaField("description", "paragraph text", notes="Product description if present"))

    # ID candidate
    if detail_profile and detail_profile.total_tables > 0:
        suggestion.record_id_candidate = "UPC or product_page_url hash"
        suggestion.notes.append("Detail page has tables — look for UPC/SKU as natural ID")
    else:
        suggestion.record_id_candidate = "product_page_url hash"

    # Dedup field names
    seen = set()
    all_fields = []
    for f in core_fields + detected_fields:
        if f.name not in seen:
            seen.add(f.name)
            all_fields.append(f)

    suggestion.required_fields = [f.name for f in all_fields if f.required]
    suggestion.optional_fields = [f.name for f in all_fields if not f.required]
    suggestion.all_fields = [asdict(f) for f in all_fields]
    suggestion.csv_columns = [f.name for f in all_fields]

    # Validation rules
    for f in all_fields:
        if f.required:
            suggestion.validation_rules.append({"field": f.name, "rule": "required_non_empty"})
        if f.type_hint == "int":
            suggestion.validation_rules.append({"field": f.name, "rule": "integer_when_present"})
        if f.type_hint == "float":
            suggestion.validation_rules.append({"field": f.name, "rule": "numeric_when_present"})
        if f.type_hint == "url":
            suggestion.validation_rules.append({"field": f.name, "rule": "valid_url_when_present"})

    # Pagination note
    if listing_profile.pagination.get("next_href"):
        suggestion.notes.append("Listing has pagination — implement next-page following for discovery")
    if not listing_profile.is_js_heavy:
        suggestion.notes.append("Static site — BeautifulSoup extraction recommended over JS evaluation")

    logger.info("Schema suggestion: entity=%s, %d required, %d optional fields",
                entity_name, len(suggestion.required_fields), len(suggestion.optional_fields))
    return suggestion
