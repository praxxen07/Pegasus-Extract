import asyncio
import base64
import json
import os
import time
from typing import Any, Dict
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from core.logger import log


def _resolve_chromium_executable() -> str | None:
    """
    Playwright in this environment may have only an x64 binary available
    even when running on arm64. We resolve an existing executable and
    pass it explicitly to `chromium.launch(executable_path=...)`.
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


# ── Live DOM Inspector JS — same logic used at extraction time ──
# Runs inside the browser to inspect the fully rendered page like DevTools.
_LIVE_DOM_INSPECTOR_JS = """
() => {
    function getSelector(el) {
        if (el.id) return '#' + CSS.escape(el.id);
        let sel = el.tagName.toLowerCase();
        if (el.className && typeof el.className === 'string') {
            const classes = el.className.trim().split(/\\s+/).filter(c => c.length > 0 && c.length < 50);
            if (classes.length > 0) sel += '.' + classes.slice(0, 3).map(c => CSS.escape(c)).join('.');
        }
        return sel;
    }
    function getText(el, limit) {
        limit = limit || 200;
        return (el.innerText || el.textContent || '').trim().substring(0, limit);
    }
    function isVisible(el) {
        if (!el.offsetParent && el.tagName !== 'BODY' && el.tagName !== 'HTML') return false;
        const s = window.getComputedStyle(el);
        return s.display !== 'none' && s.visibility !== 'hidden' && s.opacity !== '0';
    }

    const sigMap = {};
    const allEls = document.querySelectorAll('body *');
    const skipTags = new Set(['script','style','noscript','link','meta','br','hr','img','svg','path',
        'input','button','select','textarea','iframe','video','audio','source','canvas']);

    for (const el of allEls) {
        if (!isVisible(el)) continue;
        const tag = el.tagName.toLowerCase();
        if (skipTags.has(tag)) continue;
        const classes = (el.className && typeof el.className === 'string')
            ? el.className.trim().split(/\\s+/).filter(c => c.length > 1 && c.length < 50).sort().join('.')
            : '';
        if (!classes && !el.id) continue;
        const sig = tag + (classes ? '.' + classes : '') + (el.id ? '#' + el.id : '');
        if (!sigMap[sig]) sigMap[sig] = { count: 0, selector: '', samples: [], childTags: new Set(), textLengths: [] };
        sigMap[sig].count++;
        sigMap[sig].selector = getSelector(el);

        if (sigMap[sig].samples.length < 3) {
            const childInfo = [];
            for (const child of el.children) {
                if (!isVisible(child)) continue;
                childInfo.push({
                    tag: child.tagName.toLowerCase(),
                    classes: (child.className && typeof child.className === 'string') ? child.className.trim().substring(0, 100) : '',
                    text: getText(child, 100),
                    href: child.getAttribute('href') || '',
                    childCount: child.children.length
                });
                sigMap[sig].childTags.add(child.tagName.toLowerCase());
            }
            sigMap[sig].textLengths.push(getText(el, 300).length);
            sigMap[sig].samples.push({
                text: getText(el, 300),
                html: el.outerHTML.substring(0, 1500),
                children: childInfo.slice(0, 15),
                attrs: { id: el.id || '', role: el.getAttribute('role') || '' }
            });
        }
    }

    const candidates = [];
    for (const [sig, info] of Object.entries(sigMap)) {
        if (info.count < 3) continue;
        const avgTL = info.textLengths.reduce((a,b)=>a+b,0) / info.textLengths.length;
        if (avgTL < 5) continue;
        const score = info.count * (info.childTags.size + 1) * Math.min(avgTL, 200);
        candidates.push({
            signature: sig, selector: info.selector, count: info.count,
            avgTextLen: Math.round(avgTL), childTags: Array.from(info.childTags),
            score: Math.round(score), samples: info.samples
        });
    }
    candidates.sort((a,b) => b.score - a.score);

    const sampleHTML = [];
    if (candidates.length > 0) {
        const best = candidates[0];
        try {
            const els = document.querySelectorAll(best.selector);
            for (let i = 0; i < Math.min(3, els.length); i++)
                sampleHTML.push(els[i].outerHTML.substring(0, 2000));
        } catch(e) {}
    }

    const tables = [];
    for (const table of document.querySelectorAll('table')) {
        if (!isVisible(table)) continue;
        const headers = Array.from(table.querySelectorAll('th')).map(th => getText(th, 50));
        const rows = table.querySelectorAll('tbody tr, tr');
        if (rows.length < 2) continue;
        const sampleRows = [];
        for (let i = 0; i < Math.min(3, rows.length); i++) {
            const cells = Array.from(rows[i].querySelectorAll('td, th')).map(td => getText(td, 100));
            if (cells.some(c => c.length > 0)) sampleRows.push(cells);
        }
        tables.push({
            selector: getSelector(table), headers, rowCount: rows.length,
            sampleRows, sampleHTML: rows.length > 0 ? rows[0].outerHTML.substring(0, 1500) : ''
        });
    }

    const lists = [];
    for (const list of document.querySelectorAll('ul, ol')) {
        if (!isVisible(list)) continue;
        const items = list.querySelectorAll(':scope > li');
        if (items.length < 3) continue;
        const sItems = [];
        for (let i = 0; i < Math.min(3, items.length); i++)
            sItems.push({ text: getText(items[i], 200), html: items[i].outerHTML.substring(0, 1000) });
        lists.push({ selector: getSelector(list), itemCount: items.length, samples: sItems });
    }

    return {
        pageInfo: { title: document.title, url: window.location.href, totalElements: allEls.length, visibleText: getText(document.body, 500) },
        topCandidates: candidates.slice(0, 8),
        sampleHTML,
        tables,
        lists: lists.slice(0, 5)
    };
}
"""


async def visit_site(url: str) -> Dict[str, Any]:
    """
    Visit URL with Playwright (real Chromium browser). Return:
    - html: first 80,000 chars of fully-rendered page HTML
    - screenshot_b64: base64 screenshot
    - page_title: <title> content
    - final_url: after redirects
    - js_heavy: bool (check if React/Vue/Angular present)
    - load_time_ms: how long page took to load
    - dom_report: live DOM inspection report (repeating containers, tables, lists)
    """
    start = time.monotonic()

    async def _run():
        async with async_playwright() as p:
            exe = _resolve_chromium_executable()
            launch_kwargs = {
                "headless": True,
                "args": [
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1440,900",
                ],
                "timeout": 60000,
            }
            if exe:
                launch_kwargs["executable_path"] = exe
            browser = await p.chromium.launch(**launch_kwargs)
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
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

            # Anti-bot stealth
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                delete window.__playwright;
                delete window.__pw_manual;
            """)

            page = await context.new_page()
            try:
                # Navigate
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass

                # Scroll to bottom to trigger lazy loading
                prev_h = 0
                for _ in range(15):
                    h = await page.evaluate("document.body.scrollHeight")
                    if h == prev_h:
                        break
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(600)
                    prev_h = h
                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(2000)

                final_url = page.url
                page_title = await page.title()

                # Full page screenshot
                screenshot_bytes = await page.screenshot(full_page=True)
                screenshot_b64 = base64.b64encode(screenshot_bytes).decode("utf-8")

                # HTML after JS renders + scroll
                html = await page.content()
                html_truncated = html[:80000]

                js_heavy = any(
                    marker in html.lower()
                    for marker in [
                        "__next_data__",
                        "react",
                        "vue",
                        "__nuxt",
                    ]
                )

                # ── Run live DOM inspection (like Chrome DevTools) ──
                dom_report = {}
                try:
                    dom_report = await page.evaluate(_LIVE_DOM_INSPECTOR_JS)
                    cand_count = len(dom_report.get("topCandidates", []))
                    table_count = len(dom_report.get("tables", []))
                    log.info(
                        f"Live DOM inspection: {cand_count} candidates, "
                        f"{table_count} tables found"
                    )
                except Exception as e:
                    log.warning(f"DOM inspection during visit failed: {e}")

                load_time_ms = int((time.monotonic() - start) * 1000)

                return {
                    "html": html_truncated,
                    "screenshot_b64": screenshot_b64,
                    "page_title": page_title,
                    "final_url": final_url,
                    "js_heavy": js_heavy,
                    "load_time_ms": load_time_ms,
                    "dom_report": dom_report,
                }
            finally:
                await browser.close()

    try:
        return await asyncio.wait_for(_run(), timeout=120.0)
    except PlaywrightTimeoutError:
        log.warning("Playwright timed out while loading page")
        return {"error": "Timeout while loading page"}
    except Exception as e:  # noqa: BLE001
        log.error(f"Error visiting site: {type(e).__name__}: {e!r}")
        log.exception("visit_site exception traceback")
        return {"error": "Failed to visit site"}

