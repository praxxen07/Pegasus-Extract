import json
from typing import Any, Dict, List

from core.ai_provider import ai_provider


class ExtractionBrain:
    async def analyze_page(
        self,
        html: str,
        url: str,
        description: str,
        schema_fields: List[str],
    ) -> Dict[str, Any]:
        """
        Analyze the page and return a structured extraction strategy.
        Never returns raw Python, only JSON decisions.
        """
        prompt = f"""Analyze this webpage and return extraction strategy.

URL: {url}
User wants: {description}
Fields requested: {schema_fields}

HTML (first 50000 chars):
{html[:50000]}

Return JSON only:
{{
  "page_type": "listing|detail|mixed|search",
  "data_container": "CSS selector that wraps each data item",
  "fields": {{
    "field_name": {{
      "selector": "CSS selector",
      "attribute": "innerText|href|src|data-xyz",
      "backup_selector": "alternative if first fails",
      "is_relative": true
    }}
  }},
  "pagination": {{
    "type": "none|next_button|page_numbers|url_increment|infinite_scroll",
    "next_selector": "CSS selector for next button",
    "url_template": "https://site.com/page/{{page_num}}",
    "first_page": 1,
    "detection_method": "how to know there are more pages"
  }},
  "needs_javascript": true/false,
  "needs_scroll": true/false,
  "needs_login": true/false,
  "anti_bot_risk": "low|medium|high",
  "estimated_items_per_page": 0,
  "clarification_needed": [
    "question to ask client if anything is unclear"
  ]
}}"""

        result = await ai_provider.complete(
            "You are an expert web scraping analyst. Return JSON only.",
            prompt,
            json_mode=True,
        )
        try:
            return json.loads(result["text"])
        except Exception:
            return {}

