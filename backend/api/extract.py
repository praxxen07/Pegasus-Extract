import os
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from api.analyze import JOBS as ANALYSIS_JOBS
from core.logger import log
from engine.extraction_runner import run_extraction


router = APIRouter(prefix="/extract", tags=["extract"], redirect_slashes=False)


class ExtractRequest(BaseModel):
    job_id: str
    confirm: bool = True


class ExtractJob(BaseModel):
    status: str
    progress: int = 0
    current_step: str = "Pending"
    records_extracted: int = 0
    failed_pages: int = 0
    output_files: Optional[Dict[str, str]] = None
    error: Optional[str] = None


EXTRACT_JOBS: Dict[str, ExtractJob] = {}

OUTPUT_ROOT = Path(os.getenv("PEGASUS_OUTPUT_DIR", "output")).resolve()


async def _progress_callback(
    extraction_job_id: str,
    progress: int,
    step: str,
    records_extracted: int,
) -> None:
    job = EXTRACT_JOBS.get(extraction_job_id)
    if not job:
        return
    EXTRACT_JOBS[extraction_job_id] = ExtractJob(
        status=job.status,
        progress=progress,
        current_step=step,
        records_extracted=records_extracted,
        failed_pages=job.failed_pages,
        output_files=job.output_files,
        error=job.error,
    )


async def _run_extraction_job(extraction_job_id: str, analysis_job_id: str) -> None:
    try:
        analysis = ANALYSIS_JOBS.get(analysis_job_id)
        if not analysis or analysis.status != "success":
            EXTRACT_JOBS[extraction_job_id] = ExtractJob(
                status="failed",
                error="Analysis job not found or not successful",
            )
            return

        plan = analysis.extraction_plan
        if not plan:
            EXTRACT_JOBS[extraction_job_id] = ExtractJob(
                status="failed",
                error="No extraction plan available",
            )
            return

        EXTRACT_JOBS[extraction_job_id] = ExtractJob(
            status="running",
            progress=0,
            current_step="Starting extraction",
            records_extracted=0,
            failed_pages=0,
            output_files=None,
            error=None,
        )

        result = await run_extraction(
            job_id=extraction_job_id,
            plan=plan,
            output_dir=os.fspath(OUTPUT_ROOT),
            progress_callback=_progress_callback,
        )

        EXTRACT_JOBS[extraction_job_id] = ExtractJob(
            status=result.get("status", "success"),
            progress=100,
            current_step="Extraction complete",
            records_extracted=result.get("records_extracted", 0),
            failed_pages=0,
            output_files=result.get("output_files"),
            error=None,
        )
    except Exception as e:  # noqa: BLE001
        log.error(f"Extraction job {extraction_job_id} failed: {e}")
        EXTRACT_JOBS[extraction_job_id] = ExtractJob(
            status="failed",
            error="Extraction failed due to server error",
        )


@router.post("", status_code=202)
async def start_extraction(payload: ExtractRequest, background_tasks: BackgroundTasks):
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Extraction not confirmed")

    extraction_job_id = str(uuid.uuid4())
    EXTRACT_JOBS[extraction_job_id] = ExtractJob(status="pending")

    background_tasks.add_task(_run_extraction_job, extraction_job_id, payload.job_id)

    return {
        "extraction_job_id": extraction_job_id,
        "status": "running",
        "message": "Extraction started",
    }


@router.get("/{extraction_job_id}")
async def get_extraction_status(extraction_job_id: str):
    job = EXTRACT_JOBS.get(extraction_job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Extraction job not found")
    return job


@router.get("/{extraction_job_id}/download/{fmt}")
async def download_result(
    extraction_job_id: str,
    fmt: str,
):
    job = EXTRACT_JOBS.get(extraction_job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail="Extraction job not found",
        )

    if job.status != "success":
        raise HTTPException(
            status_code=400,
            detail=f"Job not complete. Status: {job.status}",
        )

    output_files = job.output_files
    if not output_files:
        raise HTTPException(
            status_code=404,
            detail="No output files found for this job",
        )

    if fmt == "csv":
        file_path = output_files.get("csv", "")
        media_type = "text/csv"
    elif fmt == "json":
        file_path = output_files.get("json", "")
        media_type = "application/json"
    elif fmt in ("xlsx", "excel"):
        file_path = output_files.get("xlsx", "")
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        fmt = "xlsx"
    else:
        raise HTTPException(
            status_code=400,
            detail="Format must be csv, json, or xlsx",
        )

    if not file_path:
        raise HTTPException(
            status_code=404,
            detail=f"No {fmt} file path in job",
        )

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404,
            detail=f"File not found on disk: {file_path}",
        )

    filename = f"pegasus_extract_{extraction_job_id}.{fmt}"

    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )


@router.get("/{extraction_job_id}/debug")
async def debug_job(extraction_job_id: str):
    job = EXTRACT_JOBS.get(extraction_job_id)
    if not job:
        return {"error": "job not found"}
    files_exist = {}
    if job.output_files:
        for k, v in job.output_files.items():
            files_exist[k] = {
                "path": v,
                "exists": os.path.exists(v) if v else False,
                "size": os.path.getsize(v) if v and os.path.exists(v) else 0,
            }
    return {
        "status": job.status,
        "records": job.records_extracted,
        "output_files": files_exist,
    }

