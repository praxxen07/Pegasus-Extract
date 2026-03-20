"""
core/adapter_spec.py — Generate adapter specification artifacts.

Takes a SiteProfile + SchemaSuggestion and produces:
1. A machine-readable profile JSON (already done by SiteProfile.save)
2. A human-readable adapter spec markdown for implementation

Generic V2 capability — no site-specific logic.
"""

import json
import logging
from pathlib import Path
from typing import Optional

from core.site_profiler import SiteProfile
from core.schema_suggester import SchemaSuggestion

logger = logging.getLogger("extraction-engine")


def generate_adapter_spec(
    listing_profile: SiteProfile,
    detail_profile: Optional[SiteProfile],
    schema: SchemaSuggestion,
    output_path: Path,
    site_slug: str = "site",
) -> Path:
    """
    Generate a markdown adapter specification from profiles and schema.

    This spec tells the coding layer exactly what to build.
    """
    lines = [
        f"# Adapter Specification: {site_slug}",
        "",
        f"Generated from analysis of: `{listing_profile.url}`",
        "",
        "## Site Overview",
        "",
        f"- **Page type**: {listing_profile.page_type}",
        f"- **JS-heavy**: {'Yes' if listing_profile.is_js_heavy else 'No'}",
        f"- **Total links**: {listing_profile.total_links}",
        f"- **Total tables**: {listing_profile.total_tables}",
        f"- **Confidence**: {listing_profile.confidence:.0%}",
        "",
    ]

    # Discovery
    lines += [
        "## Discovery Strategy",
        "",
        f"- **Start URL**: `{listing_profile.url}`",
    ]
    if listing_profile.pagination.get("next_href"):
        lines.append(f"- **Pagination**: Follow `{listing_profile.pagination.get('next_selector', 'li.next > a')}` links")
        lines.append(f"- **Next page sample**: `{listing_profile.pagination.get('next_href', '')}`")
    if listing_profile.link_selectors:
        for ls in listing_profile.link_selectors[:3]:
            lines.append(f"- **Detail link pattern**: `{ls.get('pattern', '')}` ({ls.get('count', 0)} found)")
    lines.append("")

    # Repeated containers
    if listing_profile.repeated_containers:
        lines += ["## Listing Page Containers", ""]
        for c in listing_profile.repeated_containers[:5]:
            lines.append(f"- `{c.get('selector', '')}` × {c.get('count', 0)} items")
            lines.append(f"  - Links per item: {c.get('avg_links', 0)}, Images: {c.get('avg_images', 0)}")
            sample = c.get("sample_text", "")[:100]
            if sample:
                lines.append(f"  - Sample: _{sample}_")
        lines.append("")

    # Candidate fields
    if listing_profile.candidate_fields:
        lines += ["## Detected Fields", ""]
        lines.append("| Field | Selector | Sample | Count |")
        lines.append("|---|---|---|---|")
        for cf in listing_profile.candidate_fields:
            lines.append(f"| {cf.get('name','')} | `{cf.get('selector','')}` | {cf.get('sample_value','')[:60]} | {cf.get('count',0)} |")
        lines.append("")

    # Detail page
    if detail_profile:
        lines += [
            "## Detail Page",
            "",
            f"- **URL**: `{detail_profile.url}`",
            f"- **Type**: {detail_profile.page_type}",
            f"- **Tables**: {detail_profile.total_tables}",
        ]
        if detail_profile.breadcrumb:
            bc_links = detail_profile.breadcrumb.get("links", [])
            bc_text = " > ".join(l.get("text", "") for l in bc_links)
            lines.append(f"- **Breadcrumb**: {bc_text}")
        lines.append("")

    # Schema
    lines += [
        "## Proposed Schema",
        "",
        f"- **Entity**: `{schema.entity_name}`",
        f"- **Record ID**: {schema.record_id_candidate}",
        "",
        "### Required Fields",
        "",
    ]
    for f in schema.required_fields:
        lines.append(f"- `{f}`")
    lines += ["", "### Optional Fields", ""]
    for f in schema.optional_fields:
        lines.append(f"- `{f}`")
    lines += ["", "### CSV Columns", ""]
    lines.append(f"`{', '.join(schema.csv_columns)}`")
    lines.append("")

    # Validation
    if schema.validation_rules:
        lines += ["## Validation Rules", ""]
        for rule in schema.validation_rules:
            lines.append(f"- `{rule['field']}`: {rule['rule']}")
        lines.append("")

    # Notes
    all_notes = listing_profile.notes + schema.notes
    if all_notes:
        lines += ["## Notes", ""]
        for n in all_notes:
            lines.append(f"- {n}")
        lines.append("")

    if listing_profile.warnings:
        lines += ["## Warnings", ""]
        for w in listing_profile.warnings:
            lines.append(f"- ⚠️ {w}")
        lines.append("")

    # Write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Adapter spec saved to %s", output_path)
    return output_path
