import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict

import pandas as pd
from playwright.async_api import async_playwright

from engine.agent_navigator import AgentNavigator
from engine.curl_fetcher import CurlFetcher
from engine.live_dom_extractor import LiveDOMExtractor
from engine.multi_level_crawler import MultiLevelCrawler
from engine.stealth_browser import (
    create_stealth_context,
    gentle_scroll_to_load,
    launch_stealth_browser,
    new_stealth_page,
    validate_and_retry,
)
from engine.universal_adapter import UniversalAdapter
from engine.xhr_interceptor import XHRInterceptor

log = logging.getLogger("PegasusExtract")

ProgressCallback = Callable[[str, int, str, int], Awaitable[None]]


def _resolve_chromium_executable() -> str | None:
    """
    Ensure Playwright can launch in environments where only the x64
    headless shell is available.
    """
    base = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if not base:
        return None

    root = Path(base)
    candidates = list(
        root.glob(
            "chromium_headless_shell-*/chrome-headless-shell-mac-*/chrome-headless-shell"
        )
    )
    if not candidates:
        return None

    arm = [c for c in candidates if "mac-arm64" in str(c)]
    if arm:
        return str(arm[0])
    x64 = [c for c in candidates if "mac-x64" in str(c)]
    if x64:
        return str(x64[0])

    return str(candidates[0])


async def _smart_load_page(page, url: str, browser_config: dict) -> None:
    """
    Smart page loading that handles ALL website types:
    - Static HTML, JS SPAs (React/Vue/Angular), lazy-loading
    - AWS WAF / Cloudflare / bot challenges (wait for them to resolve)
    """
    # NOTE: Do NOT set Sec-Ch-Ua / Sec-Ch-Ua-Mobile / Sec-Ch-Ua-Platform here.
    # Chromium generates these automatically and faking them causes WAF
    # fingerprint mismatches (AWS WAF, Cloudflare, etc.).

    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        # Wait for network to settle (JS rendering)
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
    except Exception as e:
        log.warning(f"Navigation warning {url}: {e}")

    # ── Wait for bot challenges (AWS WAF, Cloudflare) to resolve ──
    # These inject JS that refreshes/redirects after verification.
    # We poll until real content appears or timeout after ~30s.
    # NOTE: page.evaluate() can fail if the page navigates mid-poll
    # (execution context destroyed). We catch and retry after settling.
    for wait_round in range(15):
        try:
            body_len = await page.evaluate("(document.body.innerText || '').length")
            title = await page.evaluate("document.title || ''")
            page_html_sample = await page.evaluate(
                "document.documentElement.outerHTML.substring(0, 500)"
            )
        except Exception:
            # Page navigated/refreshed — context destroyed. Wait and retry.
            await page.wait_for_timeout(2000)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass
            continue

        is_challenge = (
            "AwsWafIntegration" in page_html_sample
            or "challenge-platform" in page_html_sample
            or "cf-challenge" in page_html_sample
            or "_cf_chl" in page_html_sample
            or "Just a moment" in title
            or "something is missing" in title.lower()
            or "oops" in title.lower()
        )

        if not is_challenge and body_len > 200:
            log.info(
                f"Page content ready after {wait_round * 2}s: "
                f"title='{title}', bodyText={body_len} chars"
            )
            break

        if wait_round == 0:
            log.info(
                f"Waiting for page to pass bot challenge "
                f"(bodyText={body_len}, title='{title}')"
            )

        await page.wait_for_timeout(2000)

        # If page navigated/refreshed from challenge, wait for new load
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5000)
        except Exception:
            pass
    else:
        # After all waits, log final state
        try:
            body_len = await page.evaluate("(document.body.innerText || '').length")
            title = await page.evaluate("document.title || ''")
            log.warning(
                f"Page may still be blocked after 30s wait: "
                f"title='{title}', bodyText={body_len} chars"
            )
        except Exception:
            log.warning("Page may still be blocked after 30s wait (context destroyed)")

    # Extra settle time for JS rendering after challenge passes
    await page.wait_for_timeout(2000)

    # Scroll to trigger lazy loading — essential for many modern sites
    if browser_config.get("scroll_to_load", True):
        try:
            prev_h = 0
            for _ in range(20):
                h = await page.evaluate("document.body.scrollHeight")
                if h == prev_h:
                    break
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(800)
                prev_h = h
            # Scroll back to top
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(1000)
        except Exception as e:
            log.warning(f"Scroll-to-load failed (page may have navigated): {e}")

    # Final diagnostic
    try:
        body_len = await page.evaluate("(document.body.innerText || '').length")
        title = await page.evaluate("document.title")
        log.info(f"Page fully loaded: title='{title}', bodyText={body_len} chars")
    except Exception:
        log.warning("Could not read final page state (context may have been destroyed)")


async def run_extraction(
    job_id: str,
    plan: dict,
    output_dir: str,
    progress_callback: ProgressCallback,
) -> Dict[str, Any]:
    """
    Dual-engine extraction:
    1. PRIMARY: LiveDOMExtractor — inspects live DOM like DevTools, AI writes JS
    2. FALLBACK: UniversalAdapter — CSS selector-based extraction
    """
    all_results: list[dict] = []
    current_page = 1
    max_pages = plan.get("crawler_config", {}).get("max_pages", 10)
    target_url = plan.get("target_url", "")
    browser_config = plan.get("browser_config", {})

    # Build field list from plan
    fields_config = plan.get("extraction_config", {}).get("fields", {})
    field_names = list(fields_config.keys()) if fields_config else []
    description = plan.get("description", "") or plan.get("target_url", "")

    # Initialize both engines
    live_extractor = LiveDOMExtractor(fields=field_names, description=description)
    adapter = UniversalAdapter(plan)
    xhr_interceptor = XHRInterceptor()
    curl_fetcher = CurlFetcher()
    agent_navigator = AgentNavigator()

    async with async_playwright() as p:
        browser = await launch_stealth_browser(p)
        context = await create_stealth_context(browser)

        # ── Check if AI recommends multi-level crawling ──
        # Only plan multilevel if the probe page is actually the target page.
        # If a bot wall redirects us to homepage, planning on the wrong page
        # produces a wrong strategy (e.g. 230-page home-page crawl).
        multilevel_crawler = MultiLevelCrawler()
        crawl_plan = None
        search_form_detected = False
        probe_page = await new_stealth_page(context)
        try:
            await _smart_load_page(probe_page, target_url, browser_config)
            probe_valid, probe_reason = await validate_and_retry(
                probe_page, target_url, browser_config
            )
            if probe_valid:
                # Check for search-form page BEFORE multi-level planning.
                # Search-form pages (portals) have links that mislead the
                # multi-level crawler into crawling random internal pages.
                probe_snapshot = await agent_navigator._get_dom_snapshot(
                    probe_page
                )
                if agent_navigator.is_search_form_page(
                    probe_snapshot, records_found=0
                ):
                    search_form_detected = True
                    log.info(
                        "Search form detected on probe — "
                        "skipping multilevel, will use AgentNavigator"
                    )
                else:
                    crawl_plan = await multilevel_crawler.get_crawl_plan(
                        probe_page, description
                    )
            else:
                log.info(
                    f"Bot wall on probe page ({probe_reason}) — "
                    "skipping multilevel planning, will use XHR tier"
                )
        except Exception as e:
            log.warning(f"CrawlPlan probe failed: {e}")
        finally:
            await probe_page.close()

        is_multilevel = (
            crawl_plan is not None
            and crawl_plan.get("strategy") == "multilevel"
        )

        if is_multilevel:
            # ── MULTI-LEVEL PATH ──
            log.info("Strategy: MULTILEVEL — executing crawl plan")
            all_results = await multilevel_crawler.run(
                context=context,
                plan=plan,
                crawl_plan=crawl_plan,
                start_url=target_url,
                description=description,
                field_names=field_names,
                max_pages=max_pages,
                progress_callback=progress_callback,
                job_id=job_id,
            )
        else:
            # ── SINGLE-LEVEL PATH (existing flow — unchanged) ──
            log.info("Strategy: SINGLE — using LiveDOMExtractor")

            # Build URL queue
            input_records = adapter.get_input_records()
            ptype = adapter.pagination.get("type", "none")
            use_dynamic = ptype in ("none", "next_button", "infinite_scroll")

            if use_dynamic:
                urls_queue = [input_records[0]["url"]] if input_records else [target_url]
            else:
                urls_queue = [r["url"] for r in input_records]

            visited: set[str] = set()

            while urls_queue and current_page <= max_pages:
                url = urls_queue.pop(0)
                if url in visited:
                    continue
                visited.add(url)

                await progress_callback(
                    job_id,
                    int(current_page / max_pages * 80),
                    f"Page {current_page}/{max_pages} — loading {url}",
                    len(all_results),
                )

                page = await new_stealth_page(context)
                try:
                    # ── Smart page load ──
                    await _smart_load_page(page, url, browser_config)

                    # ── Generic page validation (bot wall detection) ──
                    page_valid, val_reason = await validate_and_retry(
                        page, url, browser_config
                    )
                    bot_wall_detected = not page_valid
                    if bot_wall_detected:
                        log.warning(
                            f"Page {current_page}: LiveDOM returned 0 — bot wall detected ({val_reason})"
                        )

                    try:
                        body_len = await page.evaluate("(document.body.innerText || '').length")
                    except Exception:
                        body_len = 0

                    results: list[dict] = []
                    livedom_was_junk = False
                    if page_valid:
                        await progress_callback(
                            job_id,
                            int(current_page / max_pages * 85),
                            f"Page {current_page}/{max_pages} — inspecting DOM",
                            len(all_results),
                        )

                        # ── Step 0: Agentic navigation for search-form pages ──
                        nav_snapshot = await agent_navigator._get_dom_snapshot(page)
                        if agent_navigator.is_search_form_page(
                            nav_snapshot,
                            records_found=len(all_results),
                        ):
                            log.info("Search form detected — starting agentic navigation")
                            nav_result = await agent_navigator.navigate_to_results(
                                start_url=url,
                                client_description=description,
                                page=page,
                            )
                            if nav_result["success"]:
                                log.info(
                                    f"AgentNavigator: reached results "
                                    f"at {nav_result['results_url']}"
                                )
                                url = nav_result["results_url"]
                                page = nav_result["page"]
                            else:
                                log.warning(
                                    f"AgentNavigator: {nav_result['message']}"
                                )

                        # ── PRIMARY ENGINE: LiveDOMExtractor ──
                        log.info(f"Page {current_page}: LiveDOMExtractor starting on {url}")
                        results = await live_extractor.extract(
                            page, url, plan_fields=fields_config
                        )

                        # ── Quality gate: discard low-coverage junk ──
                        if results:
                            cov = live_extractor._field_coverage(results)
                            if cov < 0.60:
                                log.info(
                                    f"Page {current_page}: LiveDOM returned {len(results)} "
                                    f"records but only {cov:.0%} field coverage — "
                                    f"discarding as junk, falling through to XHR tiers"
                                )
                                results = []
                                livedom_was_junk = True

                        # ── Gentle scroll retry for lazy-load pages ──
                        if not results and body_len < 15000:
                            log.info(
                                f"Page {current_page}: 0 results + thin body "
                                f"({body_len} chars) — trying gentle scroll"
                            )
                            loaded = await gentle_scroll_to_load(page)
                            if loaded > 0:
                                results = await live_extractor.extract(
                                    page, url, plan_fields=fields_config
                                )
                                if results:
                                    log.info(
                                        f"Page {current_page}: scroll loaded "
                                        f"{len(results)} records"
                                    )

                    # ── TIER 3: XHR Interceptor (generic JS API extraction) ──
                    if not results and not bot_wall_detected:
                        log.info("LiveDOM returned 0 — switching to XHR interception")
                        results = await xhr_interceptor.extract(
                            url=url,
                            client_description=description,
                            target_fields=field_names,
                        )

                    # ── TIER 4: curl_cffi session bootstrap + XHR ──
                    elif not results and bot_wall_detected:
                        session = await curl_fetcher.bootstrap_session(url)
                        if session and session.get("cookies"):
                            results = await xhr_interceptor.extract(
                                url=url,
                                client_description=description,
                                target_fields=field_names,
                                session=session,
                            )

                    # ── TIER 5: UniversalAdapter (final fallback) ──
                    # Skip if LiveDOM found junk and XHR found 0 API calls:
                    # page is likely a search-form homepage that needs
                    # user interaction, not blind CSS scraping.
                    if not results and livedom_was_junk:
                        log.info(
                            f"Page {current_page}: Homepage has no listing API "
                            f"calls — search form interaction required"
                        )
                    elif not results:
                        log.info(
                            f"Page {current_page}: LiveDOM/XHR got 0 — trying UniversalAdapter"
                        )
                        await progress_callback(
                            job_id,
                            int(current_page / max_pages * 88),
                            f"Page {current_page}/{max_pages} — fallback extraction",
                            len(all_results),
                        )
                        results = await adapter.extract_page(
                            page,
                            {"url": url, "page_num": current_page},
                        )

                    if results:
                        all_results.extend(results)
                        log.info(
                            f"Page {current_page}: {len(results)} records. "
                            f"Total: {len(all_results)}"
                        )
                    else:
                        log.warning(f"Page {current_page}: 0 records from {url}")

                    # Discover next page
                    if use_dynamic and current_page < max_pages and page_valid:
                        next_url = await adapter.get_next_url(page, current_page)
                        if next_url and next_url not in visited:
                            urls_queue.append(next_url)
                            log.info(f"Next page: {next_url}")
                except Exception as e:
                    log.error(f"Page {current_page} error: {e}")
                finally:
                    await page.close()

                current_page += 1
                await asyncio.sleep(1.0)

        await browser.close()

    # ── Post-processing: clean & deduplicate ──
    unique = _postprocess_results(all_results, target_url)

    removed = len(all_results) - len(unique)
    if removed:
        log.info(f"Removed {removed} duplicate/junk records")

    # ── Export ──
    out = Path(output_dir) / job_id
    out.mkdir(parents=True, exist_ok=True)

    if unique:
        df = pd.DataFrame(unique)
        csv_path = out / "results.csv"
        json_path = out / "results.json"
        xlsx_path = out / "results.xlsx"
        report_path = out / "extraction_report.txt"

        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient="records", indent=2)
        try:
            df.to_excel(xlsx_path, index=False, engine="openpyxl")
        except Exception:
            xlsx_path = None

        report = f"""PEGASUS EXTRACT — Extraction Report
=====================================
URL: {target_url}
Pages: {current_page - 1}
Records: {len(unique)}
Duplicates removed: {removed}
Fields: {list(unique[0].keys())}
Engine: LiveDOMExtractor + UniversalAdapter
=====================================
"""
        report_path.write_text(report, encoding="utf-8")

        output_files = {
            "csv": str(csv_path),
            "json": str(json_path),
            "report": str(report_path),
        }
        if xlsx_path:
            output_files["xlsx"] = str(xlsx_path)

        return {
            "status": "success",
            "records_extracted": len(unique),
            "output_files": output_files,
        }

    return {
        "status": "failed",
        "records_extracted": 0,
        "error": "No records extracted from any page",
    }


def _postprocess_results(all_results: list, target_url: str) -> list:
    """Clean junk records and deduplicate — without over-filtering."""

    def is_junk_record(record: dict) -> bool:
        values = [str(v).strip() for v in record.values() if v is not None]
        values = [v for v in values if v]
        if not values:
            return True
        # Skip if all values are identical (header/nav artifacts)
        if len(set(values)) == 1 and len(values) > 1:
            return True
        # Skip records where every value is very short (likely noise)
        if all(len(v) <= 2 for v in values):
            return True
        return False

    def dedup_results(results: list) -> list:
        seen = set()
        unique = []
        for r in results:
            key_parts = []
            for v in r.values():
                if v and str(v).strip():
                    key_parts.append(str(v).strip()[:80])
                    if len(key_parts) >= 3:
                        break
            key = "||".join(key_parts)
            if key and key not in seen:
                seen.add(key)
                unique.append(r)
        return unique

    filtered = [r for r in all_results if not is_junk_record(r)]
    return dedup_results(filtered)
