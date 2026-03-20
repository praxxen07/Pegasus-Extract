"""
sites/autoevolution/crawler.py — Autoevolution SiteAdapter implementation.

Implements the SiteAdapter interface for autoevolution.com.
Orchestrates per-URL extraction: load page → harvest anchors →
click through engine tabs → extract specs → build engine objects.

Extracted from extract_l4_fixed.py process_model_row() and retry_patch.py process_url().
"""

import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from core.config import RunConfig
from core.models import ExtractResult, PageStatus
from core.retry import RetryPolicy
from core.validators import ValidationReport, Validator, clean
from sites.base import SiteAdapter
from sites.autoevolution.config import (
    CSV_COLUMNS,
    EXPECTED_COUNT_RE,
    REQUIRED_ENGINE_FIELDS,
    REQUIRED_CSV_FIELDS,
    valid_anchor,
    filter_anchors_for_url,
)
from sites.autoevolution.extractor import (
    dedupe_keep_order,
    harvest_real_anchors,
    extract_engine_from_anchor,
)
from sites.autoevolution.parser import make_id

logger = logging.getLogger("extraction-engine")


class AutoevolutionAdapter(SiteAdapter):
    """
    Site adapter for autoevolution.com engine spec extraction.

    Implements the full pipeline: brands → models → model-years → engines → specs.
    For the pilot run, we focus on L4 (model-year → engine extraction).
    """

    def __init__(self, brands_path: Optional[Path] = None, models_path: Optional[Path] = None):
        self._brand_lookup: Dict[str, str] = {}
        self._model_lookup: Dict[str, str] = {}

        if brands_path and brands_path.exists():
            self._brand_lookup = self._load_lookup(brands_path, "brand_id", ["brand_name", "name"])
        if models_path and models_path.exists():
            self._model_lookup = self._load_lookup(models_path, "model_id", ["model_name", "name"])

    @property
    def site_name(self) -> str:
        return "Autoevolution"

    @property
    def url_field(self) -> str:
        return "model_year_url"

    @property
    def label_field(self) -> str:
        return "model_year_label"

    def get_input_records(self, config: RunConfig) -> List[dict]:
        """Load model-year records from input JSON."""
        if not config.input_path or not config.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {config.input_path}")

        with open(config.input_path, encoding="utf-8") as f:
            records = json.load(f)

        if config.limit is not None:
            records = records[:config.limit]

        logger.info("Loaded %d input records from %s", len(records), config.input_path)
        return records

    async def extract_page(self, page: Page, record: dict, config: RunConfig) -> ExtractResult:
        """
        Extract all engines from a model-year page.

        Flow:
        1. Navigate to model-year URL
        2. Harvest engine anchors from sidebar
        3. For each anchor: click tab → extract visible specs → build engine object
        4. Return ExtractResult with all engines
        """
        url = record.get("model_year_url", "")
        label = record.get("model_year_label", url)
        brand_name = self._brand_lookup.get(str(record.get("brand_id", "")).strip(), "")
        model_name = self._model_lookup.get(str(record.get("model_id", "")).strip(), "")

        retry_policy = RetryPolicy(
            max_retries=config.max_retries,
            backoff_base=config.backoff_base,
        )

        for attempt in range(1, config.max_retries + 1):
            try:
                # Navigate to the page
                from core.browser import BrowserManager
                await BrowserManager.navigate_static(page, url, config.nav_timeout_ms, config.settle_ms)

                # Harvest anchors
                harvest = await harvest_real_anchors(page)
                body_text = harvest.get("body_text", "")

                # Check expected engine count
                expected_match = EXPECTED_COUNT_RE.search(body_text or "")
                expected_count = int(expected_match.group(1)) if expected_match else None

                if expected_count is not None:
                    harvest = await harvest_real_anchors(page, expected_count)

                anchors = dedupe_keep_order([
                    a for a in harvest.get("anchors", []) if valid_anchor(a)
                ])
                anchors = filter_anchors_for_url(anchors, url)

                logger.info(
                    "  [Attempt %d/%d] expected=%s anchors=%d method=%s",
                    attempt, config.max_retries, expected_count,
                    len(anchors), harvest.get("harvest_method"),
                )

                # No anchors found
                if not anchors:
                    status = retry_policy.classify_failure(
                        "zero_anchors", expected_count,
                        len(anchors), len(harvest.get("labels", [])),
                    )
                    if status == PageStatus.FAILED and retry_policy.should_retry(attempt):
                        logger.info("    ↺ no anchors; retrying")
                        await retry_policy.wait(attempt)
                        continue
                    return ExtractResult(
                        url=url, status=status,
                        reason="zero_anchors_after_harvest",
                        meta={"expected_count": expected_count},
                    )

                # Anchor count mismatch
                if expected_count is not None and len(anchors) < expected_count:
                    if retry_policy.should_retry(attempt):
                        logger.info("    ↺ anchor mismatch (%d/%d); retrying", len(anchors), expected_count)
                        await retry_policy.wait(attempt)
                        continue

                # Extract each engine by clicking its tab
                engines: List[dict] = []
                total_specs = 0

                for idx, anchor in enumerate(anchors, 1):
                    try:
                        engine_obj, spec_count = await extract_engine_from_anchor(
                            page, record, anchor, brand_name, model_name,
                            settle_ms=config.settle_ms,
                        )
                    except PlaywrightTimeoutError:
                        logger.info("    ✗ anchor %d/%d timeout: %s", idx, len(anchors), anchor)
                        if retry_policy.should_retry(attempt):
                            break
                        return ExtractResult(
                            url=url, status=PageStatus.FAILED,
                            reason=f"anchor_timeout:{anchor}",
                            meta={"expected_count": expected_count},
                        )
                    except Exception as e:
                        logger.info("    ✗ anchor %d/%d error: %s | %s", idx, len(anchors), anchor, e)
                        if retry_policy.should_retry(attempt):
                            break
                        return ExtractResult(
                            url=url, status=PageStatus.FAILED,
                            reason=f"anchor_error:{anchor}:{type(e).__name__}",
                        )

                    if not engine_obj:
                        logger.info("    ✗ anchor %d/%d zero specs: %s", idx, len(anchors), anchor)
                        continue

                    engines.append(engine_obj)
                    total_specs += spec_count
                    logger.info(
                        "    ✓ anchor %d/%d %s | engine='%s' | specs=%d",
                        idx, len(anchors), anchor,
                        engine_obj.get("engine_name", ""), spec_count,
                    )

                if not engines:
                    if retry_policy.should_retry(attempt):
                        logger.info("    ↺ zero engines; retrying")
                        await retry_policy.wait(attempt)
                        continue
                    return ExtractResult(
                        url=url, status=PageStatus.FAILED,
                        reason="zero_engines_with_specs",
                        meta={"expected_count": expected_count, "anchors": len(anchors)},
                    )

                logger.info("    ✓ page OK | engines=%d | specs=%d", len(engines), total_specs)
                return ExtractResult(
                    url=url, status=PageStatus.OK,
                    records=engines,
                    meta={"expected_count": expected_count, "total_specs": total_specs},
                )

            except PlaywrightTimeoutError:
                if retry_policy.should_retry(attempt):
                    logger.info("    ↺ page timeout attempt %d/%d; retrying", attempt, config.max_retries)
                    await retry_policy.wait(attempt)
                    continue
            except Exception as e:
                if retry_policy.should_retry(attempt):
                    logger.info("    ↺ page error attempt %d/%d: %s", attempt, config.max_retries, e)
                    await retry_policy.wait(attempt)
                    continue

        return ExtractResult(
            url=url, status=PageStatus.FAILED,
            reason="max_retries_exhausted",
        )

    def to_csv_rows(self, engine: dict) -> List[dict]:
        """Convert one engine record to flat CSV rows (one row per spec item)."""
        rows = []
        position = 0
        for section in engine.get("spec_sections", []):
            section_name = section.get("section_name", "")
            for item in section.get("items", []):
                position += 1
                rows.append({
                    "spec_id": make_id(
                        engine.get("engine_id", ""),
                        section_name,
                        item.get("label", ""),
                        item.get("value", ""),
                    ),
                    "engine_id": engine.get("engine_id", ""),
                    "engine_anchor_id": engine.get("engine_anchor_id", ""),
                    "engine_specs_url": engine.get("engine_specs_url", ""),
                    "engine_url": engine.get("engine_url", ""),
                    "model_year_id": engine.get("model_year_id", ""),
                    "model_id": engine.get("model_id", ""),
                    "brand_id": engine.get("brand_id", ""),
                    "brand_name": engine.get("brand_name", ""),
                    "model_name": engine.get("model_name", ""),
                    "model_year_url": engine.get("model_year_url", ""),
                    "model_year_label": engine.get("model_year_label", ""),
                    "engine_name": engine.get("engine_name", ""),
                    "section_name": section_name,
                    "spec_label": item.get("label", ""),
                    "spec_value": item.get("value", ""),
                    "position": position,
                })
        return rows

    def csv_columns(self) -> List[str]:
        """Return the 17-column CSV schema."""
        return CSV_COLUMNS

    def validate(self, records: List[dict], input_records: List[dict]) -> ValidationReport:
        """Run Autoevolution-specific validation on extracted engines."""
        report = Validator.check_required_fields(records, REQUIRED_ENGINE_FIELDS, "engine")

        # FK check: engines → model-years
        fk_report = Validator.check_foreign_keys(
            records, input_records,
            "model_year_id", "model_year_id", "engine_model_year",
        )
        report.issues.update(fk_report.issues)

        # Duplicate check
        dup_report = Validator.detect_duplicates(records, "engine_id", "engine")
        report.issues.update(dup_report.issues)

        return report

    @staticmethod
    def _load_lookup(path: Path, id_col: str, name_cols: List[str]) -> Dict[str, str]:
        """Load a CSV into an ID → name lookup dict."""
        lookup = {}
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = row.get(id_col, "").strip()
                if not key:
                    continue
                for col in name_cols:
                    val = row.get(col, "").strip()
                    if val:
                        lookup[key] = val
                        break
        return lookup
