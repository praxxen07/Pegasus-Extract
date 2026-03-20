import json
from typing import Any, Dict, List

from core.ai_provider import ai_provider


async def analyze_site(
    url: str,
    html: str,
    structure: Dict[str, Any],
    client_description: str,
    schema_fields: List[str],
    dom_snapshot: str = "",
    dom_report: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Send site info to AI. Get back extraction plan.
    Uses live DOM inspection report when available for accurate analysis.
    """
    SYSTEM = """You are an expert web scraping engineer 
with 10 years of experience. You can analyze any website 
HTML and identify exactly how to extract structured data.

CRITICAL RULES:
1. When a LIVE DOM INSPECTION REPORT is provided, base ALL your selectors
   on the EXACT class names, tag names, and structure shown in that report.
2. The DOM report comes from a real Chromium browser that fully rendered the
   page including JavaScript — it is the ground truth.
3. Look at topCandidates, tables, lists, and sampleHTML to identify the
   repeating data containers.
4. Use the estimated_records from the actual item count in the DOM report.

You always respond with valid JSON only. No explanation text."""

    # Build the most informative DOM context possible
    dom_context = ""
    if dom_report and dom_report.get("topCandidates"):
        # Use the live browser DOM report — this is the gold standard
        report_str = json.dumps(dom_report, indent=2, default=str)
        # Truncate if too long for the AI context
        if len(report_str) > 18000:
            report_str = report_str[:18000] + "\n...[truncated]"
        dom_context = f"""
LIVE DOM INSPECTION REPORT (from real Chromium browser — THIS IS THE GROUND TRUTH):
{report_str}
"""
    elif dom_snapshot:
        dom_context = f"""
DOM SNAPSHOT (from static HTML parse):
{dom_snapshot}
"""

    USER = f"""Analyze this website and create an extraction plan.

URL: {url}

CLIENT WANTS: {client_description}

REQUESTED FIELDS: {schema_fields}
{dom_context}
PAGE STRUCTURE ANALYSIS:
{json.dumps(structure, indent=2)}

HTML SAMPLE (for additional context only):
{html[:5000]}

IMPORTANT: Base your selectors on the LIVE DOM INSPECTION REPORT above.
Use the exact tag names, class names, and attributes shown in the report.
The topCandidates show repeating containers with their CSS selectors and
sample content. Tables show tabular data with headers and sample rows.
Do NOT guess or use outdated selectors from memory.

Return a JSON object with EXACTLY this structure:
{{
  "site_summary": "One sentence about what this site is",
  "available_fields": ["field1", "field2", ...],
  "recommended_fields": ["most relevant fields for client"],
  "selectors": {{
    "container": "CSS selector for each data row/item (from the DOM report)",
    "field1": "CSS selector RELATIVE to container (from the DOM report)",
    "field2": "CSS selector RELATIVE to container (from the DOM report)"
  }},
  "pagination": {{
    "type": "none|next_button|page_numbers|url_param|infinite_scroll",
    "selector": "CSS selector for next button if applicable",
    "url_pattern": "URL pattern if applicable e.g. ?page={{n}}",
    "max_pages": "estimated number or 'unknown'"
  }},
  "js_rendering_required": true or false,
  "login_required": true or false,
  "anti_bot_measures": "none|basic|advanced",
  "estimated_records": "number or range e.g. 100-500",
  "estimated_time_minutes": number,
  "difficulty": "easy|medium|hard",
  "confidence": "high|medium|low",
  "warnings": ["any issues or limitations"],
  "extraction_notes": "important technical notes"
}}"""

    result = await ai_provider.complete(SYSTEM, USER, json_mode=True)

    try:
        return json.loads(result["text"])
    except Exception:  # noqa: BLE001
        return {"error": "Analysis failed", "provider": result.get("provider")}

