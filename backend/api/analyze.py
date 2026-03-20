import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, HttpUrl

from analyzer.ai_analyzer import analyze_site
from analyzer.html_parser import extract_dom_snapshot, parse_structure
from analyzer.site_visitor import visit_site
from core.logger import log
from planner.models import summarize_analysis_for_preview
from planner.strategy_planner import create_extraction_plan


router = APIRouter(redirect_slashes=False)


class AnalyzeRequest(BaseModel):
    url: HttpUrl
    description: str
    schema_fields: List[str] = []
    max_pages: int = 100


class AnalyzeJobResult(BaseModel):
    status: str
    url: str
    site_summary: Optional[str] = None
    analysis: Optional[Dict[str, Any]] = None
    extraction_plan: Optional[Dict[str, Any]] = None
    preview: Optional[Dict[str, Any]] = None
    ready_to_extract: bool = False
    provider_used: Optional[str] = None
    clarifications_needed: bool = False
    questions: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


JOBS: Dict[str, AnalyzeJobResult] = {}


def _build_fallback_plan(
    url: str,
    schema_fields: List[str],
    max_pages: int,
    container_selector: str = "",
) -> Dict[str, Any]:
    """
    Deterministic fallback plan when AI returns empty/invalid JSON.

    UniversalAdapter handles generic container/field extraction fallbacks,
    so this plan mainly needs to provide:
    - target_url
    - crawler_config.max_pages
    - extraction_config.fields with {selector, attribute}
    """

    field_map: Dict[str, Dict[str, str]] = {
        "title": {"selector": ".storylink", "attribute": "text"},
        "name": {"selector": ".storylink", "attribute": "text"},
        "link": {"selector": ".storylink", "attribute": "href"},
        "url": {"selector": ".storylink", "attribute": "href"},
        "price": {"selector": "[class*='price']", "attribute": "innerText"},
        "description": {
            "selector": "[class*='description'], p",
            "attribute": "innerText",
        },
        "author": {"selector": "small.author, .author", "attribute": "innerText"},
        "quote": {"selector": ".text, blockquote", "attribute": "innerText"},
        "tags": {"selector": "div.tags a, .tag", "attribute": "text"},
        "points": {"selector": ".score, [class*='points']", "attribute": "innerText"},
        "rating": {"selector": ".star-rating", "attribute": "class"},
    }

    fields: Dict[str, Any] = {}
    for field_name in schema_fields:
        key = str(field_name).lower()
        cfg = field_map.get(key, {"selector": "", "attribute": "innerText"})
        fields[str(field_name)] = {
            "selector": cfg["selector"],
            "attribute": cfg["attribute"],
        }

    now = datetime.now().isoformat()
    return {
        "plan_id": str(uuid.uuid4()),
        "target_url": url,
        "strategy": "fallback",
        "crawler_config": {
            "seed_urls": [url],
            "depth": 0,
            "max_pages": int(max_pages),
        },
        "extraction_config": {
            "container_selector": container_selector,
            "fields": fields,
            "pagination": {"type": "none"},
        },
        "browser_config": {
            "headless": True,
            "js_enabled": True,
            "wait_for": "domcontentloaded",
            "scroll_to_load": True,
        },
        "output_config": {
            "format": ["csv", "json"],
            "filename_prefix": "extracted_data",
        },
        "estimated_records": "unknown",
        "estimated_minutes": "unknown",
        "created_at": now,
        "status": "ready",
        "provider": "fallback",
    }


def _plan_is_valid(plan: Any) -> bool:
    if not isinstance(plan, dict):
        return False
    if not plan.get("target_url"):
        return False
    cc = plan.get("crawler_config") or {}
    ec = plan.get("extraction_config") or {}
    if not isinstance(cc, dict) or not isinstance(ec, dict):
        return False
    if not cc.get("max_pages"):
        return False
    fields = ec.get("fields")
    if not isinstance(fields, dict) or not fields:
        return False
    return True


async def _run_analysis_job(job_id: str, payload: AnalyzeRequest) -> None:
    log.info(f"Starting analysis job {job_id} for {payload.url}")

    try:
        visit_result = await visit_site(str(payload.url))
        if "error" in visit_result:
            JOBS[job_id] = AnalyzeJobResult(
                status="error",
                url=str(payload.url),
                error=visit_result["error"],
            )
            return

        html = visit_result["html"]
        structure = parse_structure(html)
        dom_snapshot = extract_dom_snapshot(html)
        dom_report = visit_result.get("dom_report") or {}
        # Keep fallback container discovery generic.
        # UniversalAdapter will discover the right repeating containers.
        container_selector = ""

        ai_analysis = await analyze_site(
            url=str(payload.url),
            html=html,
            structure=structure,
            client_description=payload.description,
            schema_fields=payload.schema_fields,
            dom_snapshot=dom_snapshot,
            dom_report=dom_report,
        )

        if "error" in ai_analysis or not ai_analysis:
            # AI provider returned empty/invalid analysis JSON.
            # Use a deterministic extraction plan based on requested fields.
            plan = _build_fallback_plan(
                url=str(payload.url),
                schema_fields=payload.schema_fields,
                max_pages=payload.max_pages,
            )
            preview = summarize_analysis_for_preview(ai_analysis or {})

            JOBS[job_id] = AnalyzeJobResult(
                status="success",
                url=str(payload.url),
                site_summary=(ai_analysis or {}).get("site_summary"),
                analysis=ai_analysis or {},
                extraction_plan=plan,
                preview=preview,
                ready_to_extract=True,
                provider_used=plan.get("provider"),
                clarifications_needed=False,
                questions=None,
                error=None,
            )
            return

        plan = await create_extraction_plan(
            analysis=ai_analysis,
            url=str(payload.url),
            client_description=payload.description,
            max_pages=payload.max_pages,
            dom_snapshot=dom_snapshot,
            dom_report=dom_report,
        )

        if "error" in plan or not _plan_is_valid(plan):
            plan = _build_fallback_plan(
                url=str(payload.url),
                schema_fields=payload.schema_fields,
                max_pages=payload.max_pages,
            )
            preview = summarize_analysis_for_preview(ai_analysis)
            JOBS[job_id] = AnalyzeJobResult(
                status="success",
                url=str(payload.url),
                site_summary=ai_analysis.get("site_summary"),
                analysis=ai_analysis,
                extraction_plan=plan,
                preview=preview,
                ready_to_extract=True,
                provider_used=plan.get("provider"),
                clarifications_needed=False,
                questions=None,
                error=None,
            )
            return

        preview = summarize_analysis_for_preview(ai_analysis)
        # Phase 1: skip clarification flow.
        # Extraction will proceed with the produced plan as-is.
        questions = None
        clarifications_needed = False

        JOBS[job_id] = AnalyzeJobResult(
            status="success",
            url=str(payload.url),
            site_summary=ai_analysis.get("site_summary"),
            analysis=ai_analysis,
            extraction_plan=plan,
            preview=preview,
            ready_to_extract=not clarifications_needed,
            provider_used=plan.get("provider") or ai_analysis.get("provider"),
            clarifications_needed=clarifications_needed,
            questions=questions or None,
        )
        log.info(f"Finished analysis job {job_id}")
    except Exception as e:  # noqa: BLE001
        log.error(f"Unexpected error in analysis job {job_id}: {e}")
        JOBS[job_id] = AnalyzeJobResult(
            status="error",
            url=str(payload.url),
            error="Unexpected server error",
        )


@router.post("", status_code=202)
async def analyze(payload: AnalyzeRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    JOBS[job_id] = AnalyzeJobResult(status="pending", url=str(payload.url))

    background_tasks.add_task(_run_analysis_job, job_id, payload)

    return {
        "job_id": job_id,
        "status": "pending",
        "message": "Analysis started. Poll GET /analyze/{job_id} for results.",
    }


@router.get("/{job_id}")
async def get_analysis(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


class ClarifyRequest(BaseModel):
    answers: Dict[str, Any]


@router.post("/{job_id}/clarify")
async def clarify(job_id: str, payload: ClarifyRequest):
    """
    Accept client answers to clarification questions and update the extraction plan.
    """
    job = JOBS.get(job_id)
    if not job or job.status != "success":
        raise HTTPException(status_code=404, detail="Job not found or not successful")

    if not job.extraction_plan:
        raise HTTPException(status_code=400, detail="No extraction plan to update")

    updated_plan = dict(job.extraction_plan or {})

    # Inline version of the old Clarifier.apply_answers logic.
    answers = payload.answers
    if "max_pages" in answers:
        updated_plan.setdefault("crawler_config", {})
        updated_plan["crawler_config"]["max_pages"] = int(answers["max_pages"])

    additional_fields = answers.get("additional_fields")
    if additional_fields:
        updated_plan.setdefault("extraction_config", {})
        updated_plan["extraction_config"].setdefault("fields", {})
        for field in additional_fields:
            updated_plan["extraction_config"]["fields"][field] = {
                "selector": "",
                "attribute": "innerText",
                "to_detect": True,
            }

    # Persist updated plan and mark job as ready to extract
    JOBS[job_id] = AnalyzeJobResult(
        status=job.status,
        url=job.url,
        site_summary=job.site_summary,
        analysis=job.analysis,
        extraction_plan=updated_plan,
        preview=job.preview,
        ready_to_extract=True,
        provider_used=job.provider_used,
        clarifications_needed=False,
        questions=None,
        error=job.error,
    )

    return {
        "status": "ready",
        "updated_plan": updated_plan,
    }

