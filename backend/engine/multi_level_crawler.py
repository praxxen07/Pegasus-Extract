"""
multi_level_crawler.py — Multi-level crawling engine for Pegasus Extract.

Activates ONLY when AI detects that the target data lives behind
intermediate link pages (e.g. brand → models → specs).

DESIGN PRINCIPLE — level-by-level extraction:
  Level 1: Visit landing page → LiveDOMExtractor extracts items + their URLs
  Level 2: Visit each URL from Level 1 → extract items + their URLs
  Level N: Visit each URL from Level N-1 → extract the final target fields

At every level the AI sees the LIVE DOM fresh and writes extraction JS.
Zero hardcoded selectors.  Parent data (e.g. brand_name) flows down and
gets merged into child records automatically.

Uses the same Playwright browser context already created by extraction_runner.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from playwright.async_api import BrowserContext, Page

from core.ai_provider import ai_provider
from engine.live_dom_extractor import DOM_INSPECTOR_JS, LiveDOMExtractor
from engine.stealth_browser import new_stealth_page

log = logging.getLogger("PegasusExtract")


# ---------------------------------------------------------------------------
# 1. AI decides: single page or multi-level?
# ---------------------------------------------------------------------------

_STRATEGY_SYSTEM = """You are an expert web scraping architect.
You analyze a page's DOM and a client's description to decide the crawl strategy.

CRITICAL RULES:
1. Return ONLY valid JSON. No markdown, no backticks, no explanation.
2. If ALL the data the client wants is visible on THIS page → "single".
3. If the client must follow links to deeper pages → "multilevel".
4. For multilevel, list each level with a short entity name and purpose.
5. max_links_per_level must be null unless the client explicitly says
   "top N", "first N", etc.
6. Do NOT include any CSS selectors — the extraction engine handles that."""


def _build_strategy_prompt(description: str, dom_report: dict) -> str:
    dom_ctx = json.dumps(dom_report, indent=2, default=str)
    if len(dom_ctx) > 12000:
        dom_ctx = dom_ctx[:12000] + "\n...[truncated]"

    return f"""CLIENT DESCRIPTION: {description}

CURRENT PAGE DOM REPORT:
{dom_ctx}

Read the client description word by word.

Return JSON:
{{
  "strategy": "single" | "multilevel",
  "reason": "one-sentence reason",
  "levels": [
    {{
      "level": 1,
      "entity": "short plural noun, e.g. brands",
      "purpose": "what this level lists"
    }},
    {{
      "level": 2,
      "entity": "e.g. models",
      "purpose": "what this level lists"
    }},
    {{
      "level": 3,
      "entity": "e.g. specifications",
      "purpose": "final data the client wants"
    }}
  ],
  "max_links_per_level": null
}}

For "single", levels should contain one entry with the final entity.
Return JSON now:"""


# ---------------------------------------------------------------------------
# 2. Helpers
# ---------------------------------------------------------------------------

def _parse_json_response(raw_text: str) -> Optional[dict]:
    """Robustly parse an AI JSON response."""
    text = raw_text.strip()
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            try:
                obj = json.loads(m.group())
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                pass
    return None


async def _smart_load(page: Page, url: str) -> None:
    """Lightweight page load with bot-challenge wait."""
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
    except Exception as e:
        log.warning(f"MultiLevel nav warning {url}: {e}")

    for _ in range(5):
        body_len = await page.evaluate("(document.body.innerText || '').length")
        html_head = await page.evaluate(
            "document.documentElement.outerHTML.substring(0, 500)"
        )
        is_challenge = any(
            tok in html_head
            for tok in ("AwsWafIntegration", "challenge-platform", "cf-challenge")
        )
        if not is_challenge and body_len > 100:
            break
        await page.wait_for_timeout(2000)

    await page.wait_for_timeout(1000)


def _resolve_urls(base_url: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Resolve relative URLs in record 'url' fields to absolute."""
    for rec in records:
        raw = rec.get("url", "")
        if raw and not raw.startswith(("http://", "https://")):
            rec["url"] = urljoin(base_url, raw)
    return records


# ---------------------------------------------------------------------------
# 3. Main class
# ---------------------------------------------------------------------------

class MultiLevelCrawler:
    """
    AI-driven multi-level crawler.

    At each intermediate level, LiveDOMExtractor extracts [entity_name, url].
    Those URLs feed the next level.  At the final level it extracts the
    client's requested fields.  Parent data merges down automatically.
    """

    # ── Strategy detection ──────────────────────────────────────────────

    async def get_crawl_plan(
        self, page: Page, description: str
    ) -> Optional[dict]:
        """Ask AI whether this site needs multi-level crawling."""
        try:
            dom_report = await page.evaluate(DOM_INSPECTOR_JS)
        except Exception as e:
            log.error(f"MultiLevel DOM inspection failed: {e}")
            return None

        prompt = _build_strategy_prompt(description, dom_report)
        result = await ai_provider.complete(
            _STRATEGY_SYSTEM, prompt, json_mode=True
        )

        raw = result.get("text", "{}")
        provider = result.get("provider", "unknown")
        plan = _parse_json_response(raw)

        if plan:
            log.info(
                f"CrawlPlan from {provider}: strategy={plan.get('strategy')}, "
                f"reason={plan.get('reason', '?')[:80]}"
            )
        else:
            log.warning(f"Failed to parse CrawlPlan: {raw[:300]}")

        return plan

    # ── Level-by-level execution ────────────────────────────────────────

    async def run(
        self,
        context: BrowserContext,
        plan: dict,
        crawl_plan: dict,
        start_url: str,
        description: str,
        field_names: List[str],
        max_pages: int = 10,
        progress_callback=None,
        job_id: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Execute level-by-level extraction.

        Returns flat list of record dicts (same format as LiveDOMExtractor).
        """
        levels = crawl_plan.get("levels", [])
        max_links = crawl_plan.get("max_links_per_level")  # None unless "top N"

        if not levels:
            log.warning("CrawlPlan has no levels — returning empty")
            return []

        total_levels = len(levels)
        pages_visited = 0

        # NOTE: max_pages is intentionally IGNORED for multi-level crawling.
        # Multi-level sites require visiting all discovered URLs at every
        # level to reach the final data.  The only cap is max_links_per_level
        # which the AI sets only when the client says "top N", "first N", etc.

        # current_queue: list of (url, parent_data_dict)
        current_queue: List[tuple] = [(start_url, {})]

        final_records: List[Dict[str, Any]] = []

        for li, level_cfg in enumerate(levels):
            level_num = level_cfg.get("level", li + 1)
            entity = level_cfg.get("entity", "items")
            purpose = level_cfg.get("purpose", "")
            is_final = (li == total_levels - 1)

            # Decide what fields to extract at this level
            if is_final:
                # Final level: extract the client's actual target fields
                level_fields = field_names
                level_desc = description
            else:
                # Intermediate level: extract entity name + URL
                level_fields = ["name", "url"]
                level_desc = (
                    f"Extract every {entity} listed on this page. "
                    f"For each {entity}, extract its name/title and the URL "
                    f"(href) that links to its detail page. "
                    f"Return an array of objects with keys: name, url"
                )

            log.info(
                f"Level {level_num}/{total_levels}: {purpose} "
                f"({len(current_queue)} URLs, entity={entity}, "
                f"final={is_final})"
            )

            next_queue: List[tuple] = []
            level_records: List[Dict[str, Any]] = []

            for idx, (url, parent_data) in enumerate(current_queue):
                # Respect per-level cap (only set when client says "top N")
                if max_links and idx >= max_links:
                    break

                page = await new_stealth_page(context)
                try:
                    await _smart_load(page, url)
                    pages_visited += 1
                    title = await page.evaluate("document.title || ''")
                    log.info(
                        f"Level {level_num}: [{idx + 1}/{len(current_queue)}] "
                        f"{title[:60]}"
                    )

                    # Use LiveDOMExtractor to extract records from this page
                    extractor = LiveDOMExtractor(
                        fields=level_fields, description=level_desc
                    )
                    records = await extractor.extract(page, url)
                    records = _resolve_urls(url, records)

                    if is_final:
                        # Merge parent data into final records
                        for rec in records:
                            for k, v in parent_data.items():
                                if k not in rec or not rec.get(k):
                                    rec[k] = v
                        level_records.extend(records)
                        log.info(
                            f"Level {level_num}: Extracted "
                            f"{len(records)} records from {url}"
                        )
                    else:
                        # Intermediate: build parent data for next level
                        # and queue each record's URL
                        valid = 0
                        for rec in records:
                            rec_url = rec.get("url", "").strip()
                            rec_name = rec.get("name", "").strip()
                            if not rec_url:
                                continue
                            # Build parent data: carry forward existing +
                            # add this level's entity name
                            child_parent = {**parent_data}
                            # Store entity name under a descriptive key
                            # e.g. entity="brands" → key="brand_name"
                            entity_key = entity.rstrip("s") + "_name"
                            if entity_key in field_names or any(
                                entity_key.replace("_", "") in f.replace("_", "").lower()
                                for f in field_names
                            ):
                                child_parent[entity_key] = rec_name
                            else:
                                # Also try the raw entity name as key
                                child_parent[entity] = rec_name

                            next_queue.append((rec_url, child_parent))
                            valid += 1

                        log.info(
                            f"Level {level_num}: Found {valid} {entity} "
                            f"with URLs from {url}"
                        )

                    # Progress
                    if progress_callback and job_id:
                        total_so_far = len(final_records) + len(level_records)
                        pct = min(
                            90,
                            int((idx + 1) / max(len(current_queue), 1) * 80),
                        )
                        await progress_callback(
                            job_id, pct,
                            f"Level {level_num}: {idx + 1}/{len(current_queue)} pages",
                            total_so_far,
                        )

                except Exception as e:
                    log.error(
                        f"Level {level_num} error on {url}: {e}"
                    )
                finally:
                    await page.close()
                    await asyncio.sleep(0.8)

            final_records.extend(level_records)
            log.info(
                f"Level {level_num} done. "
                f"Records this level: {len(level_records)}, "
                f"URLs for next: {len(next_queue)}, "
                f"Total records so far: {len(final_records)}"
            )

            # Advance queue
            if next_queue:
                current_queue = next_queue
            elif not is_final:
                log.warning(
                    f"Level {level_num}: No URLs for next level — stopping"
                )
                break

        log.info(
            f"MultiLevelCrawler finished: {len(final_records)} records, "
            f"{pages_visited} pages visited"
        )
        return final_records
