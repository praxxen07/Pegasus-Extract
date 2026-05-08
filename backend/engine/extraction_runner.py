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


_DATA_RECORD_COUNTER_JS = """
(() => {
    // Count table data rows (strong signal — tables with 5+ rows are real data)
    let tableRows = 0;
    document.querySelectorAll('table').forEach(t => {
        const rows = t.querySelectorAll('tbody tr, tr').length;
        if (rows > tableRows) tableRows = rows;
    });
    if (tableRows >= 5) return tableRows;

    // Count repeated containers — STRICT filtering
    // Skip: nav, menu, header, footer, sidebar, carousel, slider, promo, hero, banner
    const skipRe = /nav|menu|header|footer|sidebar|breadcrumb|pagination|social|share|banner|cookie|toolbar|topbar|carousel|slider|swiper|hero|promo|featured|sponsor|advert|modal|popup|tooltip|dropdown/i;
    // Only count data-centric selectors (not generic li or item)
    const selectors = [
        '[class*="result"]', '[class*="listing"]',
        '[class*="product"]', '[class*="entry"]',
        'article',
    ];
    let best = 0;
    for (const sel of selectors) {
        try {
            const els = document.querySelectorAll(sel);
            if (els.length < 5) continue;

            // Check that elements are NOT inside a carousel/slider/promo ancestor
            let validCount = 0;
            for (let i = 0; i < Math.min(els.length, 10); i++) {
                let el = els[i];
                let skip = false;
                // Walk up to check for promo/carousel ancestors
                let ancestor = el.parentElement;
                for (let d = 0; d < 5 && ancestor; d++) {
                    const ac = (ancestor.className || '') + ' ' + (ancestor.id || '');
                    if (skipRe.test(ac)) { skip = true; break; }
                    const tag = ancestor.tagName.toLowerCase();
                    if (tag === 'nav' || tag === 'header' || tag === 'footer') { skip = true; break; }
                    ancestor = ancestor.parentElement;
                }
                // Require meaningful text (not just images/icons)
                if (!skip) {
                    const txt = (el.innerText || '').trim();
                    if (txt.length > 20) validCount++;
                }
            }
            // At least 50% of sampled elements must be valid data
            if (validCount >= Math.min(els.length, 10) * 0.5) {
                if (els.length > best) best = els.length;
            }
        } catch(e) {}
    }
    return best;
})()
"""


async def _count_visible_records(page) -> int:
    """
    Count visible data records on a live page.

    Uses two signals via lightweight JS:
    1. Table data rows (>= 5 rows)
    2. Repeated container elements (cards, results, listings, etc.)
       — skips nav/menu/header/footer containers

    Returns the highest record count found, or 0.
    """
    try:
        count = await page.evaluate(_DATA_RECORD_COUNTER_JS)
        return int(count) if count else 0
    except Exception:
        return 0


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
                probe_records = await _count_visible_records(probe_page)
                probe_snapshot = await agent_navigator._get_dom_snapshot(
                    probe_page
                )
                if probe_records > 0:
                    log.info(
                        f"Probe: {probe_records} data records visible "
                        f"— data page, skipping AgentNavigator"
                    )
                    # Data page — go to multilevel planning or single-level
                    crawl_plan = await multilevel_crawler.get_crawl_plan(
                        probe_page, description
                    )
                elif agent_navigator.is_search_form_page(
                    probe_snapshot, records_found=0
                ):
                    search_form_detected = True
                    log.info(
                        "Probe: 0 records + search form detected — "
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
            # ── Override multilevel if listing page can extract all fields ──
            if (
                crawl_plan is not None
                and crawl_plan.get("strategy") == "multilevel"
                and probe_valid
            ):
                try:
                    probe_results = await live_extractor.extract(
                        probe_page, target_url, plan_fields=fields_config
                    )
                    if probe_results:
                        probe_cov = live_extractor._field_coverage(probe_results)
                        if probe_cov >= 0.80:
                            log.info(
                                f"Listing page extraction: {len(probe_results)} records, "
                                f"{probe_cov:.0%} coverage — overriding to SINGLE strategy"
                            )
                            crawl_plan["strategy"] = "single"
                        else:
                            log.info(
                                f"Listing page extraction: {probe_cov:.0%} coverage "
                                f"— keeping MULTILEVEL strategy"
                            )
                except Exception as e:
                    log.debug(f"Listing page pre-check failed: {e}")

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
                    agent_navigated = False
                    if page_valid:
                        await progress_callback(
                            job_id,
                            int(current_page / max_pages * 85),
                            f"Page {current_page}/{max_pages} — inspecting DOM",
                            len(all_results),
                        )

                        # ── DECISION GATE: count visible data before AgentNavigator ──
                        visible_records = await _count_visible_records(page)
                        nav_snapshot = await agent_navigator._get_dom_snapshot(page)

                        if visible_records > 0:
                            # Page already has data. AgentNavigator forbidden.
                            log.info(
                                f"Decision gate: {visible_records} data records "
                                f"visible on landing page — skipping AgentNavigator, "
                                f"going directly to Tier 1"
                            )
                        elif agent_navigator.is_search_form_page(
                            nav_snapshot,
                            records_found=len(all_results),
                        ):
                            # No data + search form = AgentNavigator needed
                            log.info("Decision gate: 0 records + search form detected — starting AgentNavigator")
                            nav_result = await agent_navigator.navigate_to_results(
                                start_url=url,
                                client_description=description,
                                page=page,
                            )
                            if nav_result["success"]:
                                agent_navigated = True
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
                        else:
                            # No data, no form — proceed to tiers directly
                            log.info(
                                "Decision gate: 0 records, no search form — "
                                "proceeding to Tier 1 directly"
                            )

                        # ── PRIMARY ENGINE: LiveDOMExtractor ──
                        log.info(f"Page {current_page}: LiveDOMExtractor starting on {url}")
                        results = await live_extractor.extract(
                            page, url, plan_fields=fields_config
                        )

                        # ── Quality gate: 80% requested-field coverage ──
                        livedom_stash = []
                        livedom_cov = 0.0
                        if results:
                            livedom_cov = live_extractor._field_coverage(results)
                            if livedom_cov >= 0.80:
                                log.info(
                                    f"Page {current_page}: LiveDOM coverage "
                                    f"{livedom_cov:.0%} on requested fields — accepted "
                                    f"({len(results)} records)"
                                )
                            else:
                                log.info(
                                    f"Page {current_page}: LiveDOM coverage "
                                    f"{livedom_cov:.0%} on requested fields — "
                                    f"too low, stashing {len(results)} records, trying XHR"
                                )
                                livedom_stash = results
                                livedom_was_junk = livedom_cov < 0.25
                                results = []

                        # ── Gentle scroll retry for lazy-load pages ──
                        if not results and not livedom_stash and body_len < 15000:
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
                                    cov = live_extractor._field_coverage(results)
                                    if cov >= 0.80:
                                        log.info(
                                            f"Page {current_page}: scroll loaded "
                                            f"{len(results)} records, coverage {cov:.0%} — accepted"
                                        )
                                    else:
                                        log.info(
                                            f"Page {current_page}: scroll loaded "
                                            f"{len(results)} records but coverage {cov:.0%} — stashing"
                                        )
                                        if not livedom_stash or cov > livedom_cov:
                                            livedom_stash = results
                                            livedom_cov = cov
                                        results = []

                    # ── TIER 3: XHR Interceptor (generic JS API extraction) ──
                    if not results and not bot_wall_detected:
                        log.info(
                            f"LiveDOM coverage insufficient — switching to XHR interception"
                        )
                        xhr_results = await xhr_interceptor.extract(
                            url=url,
                            client_description=description,
                            target_fields=field_names,
                        )
                        if xhr_results:
                            xhr_cov = live_extractor._field_coverage(xhr_results)
                            log.info(f"XHR returned {len(xhr_results)} records, coverage {xhr_cov:.0%}")
                            if xhr_cov > livedom_cov:
                                results = xhr_results
                            elif livedom_stash:
                                log.info("XHR coverage not better — using LiveDOM stash")
                                results = livedom_stash
                            else:
                                results = xhr_results
                        elif livedom_stash:
                            log.info(
                                f"XHR returned 0 — restoring LiveDOM stash "
                                f"({len(livedom_stash)} records, {livedom_cov:.0%} coverage)"
                            )
                            results = livedom_stash

                    # ── TIER 4: curl_cffi session bootstrap + XHR ──
                    elif not results and bot_wall_detected:
                        session = await curl_fetcher.bootstrap_session(url)
                        if session and session.get("cookies"):
                            xhr_results = await xhr_interceptor.extract(
                                url=url,
                                client_description=description,
                                target_fields=field_names,
                                session=session,
                            )
                            if xhr_results:
                                xhr_cov = live_extractor._field_coverage(xhr_results)
                                if xhr_cov > livedom_cov:
                                    results = xhr_results
                                elif livedom_stash:
                                    results = livedom_stash
                                else:
                                    results = xhr_results
                            elif livedom_stash:
                                results = livedom_stash

                    # Restore stash if nothing worked yet
                    if not results and livedom_stash:
                        log.info(
                            f"All XHR tiers failed — restoring LiveDOM stash "
                            f"({len(livedom_stash)} records, {livedom_cov:.0%} coverage)"
                        )
                        results = livedom_stash

                    # ── TIER 5: UniversalAdapter (final fallback) ──
                    # Skip if LiveDOM found junk and XHR found 0 API calls
                    # AND we didn't navigate (still on homepage).
                    # After agent navigation we're on a results page — always try.
                    if not results and livedom_was_junk and not agent_navigated:
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
    if all_results:
        log.info(f"Pre-postprocess: {len(all_results)} records. Sample: {all_results[0]}")
    unique = _postprocess_results(all_results, target_url)

    removed = len(all_results) - len(unique)
    if removed:
        log.info(f"Removed {removed} duplicate/junk records")
        if removed == len(all_results) and all_results:
            log.warning(f"ALL records removed! Sample record: {all_results[0]}")

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
        # Skip records where every value is very short AND few (likely noise)
        if all(len(v) <= 2 for v in values) and len(values) <= 2:
            return True
        return False

    def dedup_results(results: list) -> list:
        seen = set()
        unique = []
        for r in results:
            key_parts = []
            for v in r.values():
                s = str(v).strip() if v else ""
                if s and len(s) > 2:
                    key_parts.append(s[:80])
                    if len(key_parts) >= 3:
                        break
            key = "||".join(key_parts)
            if not key:
                # No dedup key possible — include the record
                unique.append(r)
            elif key not in seen:
                seen.add(key)
                unique.append(r)
        return unique

    junk_count = sum(1 for r in all_results if is_junk_record(r))
    if junk_count:
        log.info(f"Post-process: {junk_count}/{len(all_results)} records are junk")
        if junk_count > 0 and all_results:
            # Log first junk record for debugging
            for r in all_results:
                if is_junk_record(r):
                    log.info(f"Post-process: sample junk record: {r}")
                    break

    filtered = [r for r in all_results if not is_junk_record(r)]
    unique = dedup_results(filtered)

    deduped = len(filtered) - len(unique)
    if deduped:
        log.info(f"Post-process: removed {deduped} duplicate records")

    return unique
