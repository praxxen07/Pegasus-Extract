import json
from datetime import datetime
from typing import Any, Dict

from core.ai_provider import ai_provider


async def create_extraction_plan(
    analysis: Dict[str, Any],
    url: str,
    client_description: str,
    max_pages: int = 100,
    dom_snapshot: str = "",
    dom_report: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Take AI analysis and create a concrete execution plan.
    Uses live DOM inspection report when available for accurate selectors.
    """

    SYSTEM = """You are a web scraping architect.
Create detailed, executable extraction plans.

CRITICAL RULES:
1. When a LIVE DOM INSPECTION REPORT is provided, base ALL your selectors
   on the EXACT class names, tag names, and structure shown in that report.
2. The DOM report comes from a real Chromium browser — it is ground truth.
3. The container_selector MUST match the repeating element shown in the report.
4. Field selectors MUST be RELATIVE to the container.
5. Do NOT invent or guess selectors from memory.

Respond with valid JSON only."""

    dom_section = ""
    if dom_report and dom_report.get("topCandidates"):
        report_str = json.dumps(dom_report, indent=2, default=str)
        if len(report_str) > 15000:
            report_str = report_str[:15000] + "\n...[truncated]"
        dom_section = f"""
LIVE DOM INSPECTION REPORT (from real Chromium browser — THIS IS GROUND TRUTH):
{report_str}

IMPORTANT: Base your selectors on the EXACT elements shown in this report.
The topCandidates show repeating containers with CSS selectors and sample content.
Tables show tabular data with headers and sample rows.
"""
    elif dom_snapshot:
        dom_section = f"""
ACTUAL DOM SNAPSHOT (real HTML captured from the live page):
{dom_snapshot}

IMPORTANT: The selectors you write MUST match the exact elements shown above.
Copy class names directly from the snapshot. Do NOT guess.
"""

    # Trim analysis to avoid prompt bloat — keep only key fields
    trimmed = {k: v for k, v in analysis.items()
               if k in ("site_summary", "selectors", "pagination",
                         "js_rendering_required", "estimated_records",
                         "confidence", "available_fields",
                         "recommended_fields", "extraction_notes")}

    USER = f"""Create an extraction plan.

Target URL: {url}
Client wants: {client_description}
Max pages allowed: {max_pages}
{dom_section}
Previous analysis (for reference only — DOM report is the ground truth):
{json.dumps(trimmed, indent=2)}

Return JSON:
{{
  "plan_id": "unique id",
  "target_url": "{url}",
  "description": "{client_description}",
  "strategy": "single_page|multi_page|paginated|sitemap",
  "crawler_config": {{
    "seed_urls": ["starting URLs"],
    "depth": number,
    "max_pages": number,
    "follow_pattern": "URL pattern to follow"
  }},
  "extraction_config": {{
    "container_selector": "CSS selector matching the repeating container from DOM report",
    "fields": {{
      "field_name": {{
        "selector": "CSS selector RELATIVE to container, from the DOM report",
        "attribute": "text|href|src|data-*",
        "required": true/false,
        "transform": "none|strip|lowercase|to_number"
      }}
    }}
  }},
  "browser_config": {{
    "headless": true,
    "js_enabled": true/false,
    "wait_for": "networkidle|domcontentloaded",
    "scroll_to_load": true/false
  }},
  "output_config": {{
    "format": ["csv", "json"],
    "filename_prefix": "extracted_data"
  }},
  "estimated_records": number,
  "estimated_minutes": number
}}"""

    result = await ai_provider.complete(SYSTEM, USER, json_mode=True)

    try:
        plan = json.loads(result["text"])
        plan["created_at"] = datetime.now().isoformat()
        plan["status"] = "ready"
        return plan
    except Exception:  # noqa: BLE001
        return {"error": "Planning failed"}

