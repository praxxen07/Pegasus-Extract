from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List


@dataclass
class ExtractionPlan:
    plan_id: str
    target_url: str
    strategy: str
    crawler_config: Dict[str, Any]
    extraction_config: Dict[str, Any]
    browser_config: Dict[str, Any]
    output_config: Dict[str, Any]
    estimated_records: int
    estimated_minutes: int
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    status: str = "ready"
    raw: Dict[str, Any] = field(default_factory=dict)


def summarize_analysis_for_preview(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a lightweight preview payload from the AI analysis.
    """
    estimated_records = analysis.get("estimated_records") or "unknown"
    estimated_time = analysis.get("estimated_time_minutes") or analysis.get(
        "estimated_time", "unknown"
    )
    confidence = analysis.get("confidence", "unknown")
    warnings = analysis.get("warnings") or []
    fields_found: List[str] = analysis.get("available_fields") or []

    return {
        "estimated_records": estimated_records,
        "estimated_time": (
            f"{estimated_time} minutes" if isinstance(estimated_time, (int, float)) else str(estimated_time)
        ),
        "fields_found": fields_found,
        "confidence": str(confidence).lower(),
        "warnings": warnings,
    }

