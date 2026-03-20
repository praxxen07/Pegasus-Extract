import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict

import pandas as pd
from playwright.async_api import async_playwright

from engine.live_dom_extractor import LiveDOMExtractor
from engine.universal_adapter import UniversalAdapter

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
    for wait_round in range(15):
        body_len = await page.evaluate("(document.body.innerText || '').length")
        title = await page.evaluate("document.title || ''")
        page_html_sample = await page.evaluate(
            "document.documentElement.outerHTML.substring(0, 500)"
        )

        is_challenge = (
            "AwsWafIntegration" in page_html_sample
            or "challenge-platform" in page_html_sample
            or "cf-challenge" in page_html_sample
            or "_cf_chl" in page_html_sample
            or "Just a moment" in title
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
        body_len = await page.evaluate("(document.body.innerText || '').length")
        title = await page.evaluate("document.title || ''")
        log.warning(
            f"Page may still be blocked after 30s wait: "
            f"title='{title}', bodyText={body_len} chars"
        )

    # Extra settle time for JS rendering after challenge passes
    await page.wait_for_timeout(2000)

    # Scroll to trigger lazy loading — essential for many modern sites
    if browser_config.get("scroll_to_load", True):
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

    # Final diagnostic
    body_len = await page.evaluate("(document.body.innerText || '').length")
    title = await page.evaluate("document.title")
    log.info(f"Page fully loaded: title='{title}', bodyText={body_len} chars")


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

    async with async_playwright() as p:
        exe = _resolve_chromium_executable()
        launch_kwargs: Dict[str, Any] = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--window-size=1440,900",
            ],
        }
        if exe:
            launch_kwargs["executable_path"] = exe
        browser = await p.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-User": "?1",
                "Sec-Fetch-Dest": "document",
            },
        )

        # Comprehensive anti-bot stealth
        await context.add_init_script(
            """
            // Hide webdriver flag
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            // Fake chrome runtime
            window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){} };
            // Fake plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            // Fake languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            // Remove automation indicators
            delete window.__playwright;
            delete window.__pw_manual;
            // Fake permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            """
        )

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

            page = await context.new_page()
            try:
                # ── Smart page load ──
                await _smart_load_page(page, url, browser_config)

                await progress_callback(
                    job_id,
                    int(current_page / max_pages * 85),
                    f"Page {current_page}/{max_pages} — inspecting DOM",
                    len(all_results),
                )

                # ── PRIMARY ENGINE: LiveDOMExtractor ──
                log.info(f"Page {current_page}: LiveDOMExtractor starting on {url}")
                results = await live_extractor.extract(
                    page, url, plan_fields=fields_config
                )

                # ── FALLBACK: UniversalAdapter (if primary got 0 results) ──
                if not results:
                    log.info(
                        f"Page {current_page}: LiveDOM got 0 — trying UniversalAdapter"
                    )
                    await progress_callback(
                        job_id,
                        int(current_page / max_pages * 88),
                        f"Page {current_page}/{max_pages} — fallback extraction",
                        len(all_results),
                    )
                    # Re-load page for adapter (it has its own load logic)
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
                if use_dynamic and current_page < max_pages:
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
