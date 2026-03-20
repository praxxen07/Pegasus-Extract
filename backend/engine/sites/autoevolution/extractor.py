"""
sites/autoevolution/extractor.py — Page-level extraction for Autoevolution.

Handles the JavaScript-based interaction with autoevolution.com:
- Sidebar anchor/label harvest from engine list
- Tab clicking via engine_show() to reveal specs
- Visible spec table extraction (only reads displayed tables)
- Engine name reading from active sidebar element

Extracted from extract_l4_fixed.py L381-618, L1055-1296.
"""

import logging
import re
from typing import Dict, List, Optional, Tuple

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

from sites.autoevolution.config import valid_anchor, filter_anchors_for_url, EXPECTED_COUNT_RE
from sites.autoevolution.parser import (
    normalize_space,
    make_id,
    extract_anchor_from_url,
    parse_engine_tables_from_html,
)
from sites.autoevolution.transforms import (
    build_engine_name_from_anchor,
    build_engine_name_from_specs,
)

logger = logging.getLogger("extraction-engine")


def dedupe_keep_order(items: list) -> list:
    """Deduplicate list while preserving insertion order."""
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


# ── JavaScript evaluation functions ────────────────────────────────────────

# This JS runs in-page to discover all engine anchors and labels.
# It: 1) reads sidebar li[id^="li_eng_"], 2) clicks any toggle buttons,
# 3) returns anchors + labels + expected count.
HARVEST_JS = r"""
(expectedCount) => {
    const norm = s => (s||'').replace(/\u00a0/g,' ').replace(/\u200b/g,' ')
                              .replace(/\s+/g,' ').trim();

    const anchors = [];
    const labels = [];

    // Strategy 1: sidebar engine list items
    const liItems = document.querySelectorAll('li[id^="li_eng_"]');
    for (const li of liItems) {
        const id = li.id.replace(/^li_eng_/, 'aeng_');
        if (id && id.startsWith('aeng_')) {
            anchors.push(id);
            const title = li.getAttribute('title') || '';
            if (title) labels.push(norm(title));
        }
    }

    // Strategy 2: direct anchor elements with aeng_ ids
    if (!anchors.length) {
        for (const a of document.querySelectorAll('a[id^="aeng_"]')) {
            anchors.push(a.id);
        }
    }

    // Strategy 3: any element with id starting with aeng_
    if (!anchors.length) {
        for (const el of document.querySelectorAll('[id^="aeng_"]')) {
            anchors.push(el.id);
        }
    }

    // Read body text for expected engine count
    const bodyText = norm(document.body.innerText || '').substring(0, 5000);

    return {
        anchors: [...new Set(anchors)],
        labels: labels,
        harvest_method: liItems.length ? 'sidebar_li' : (anchors.length ? 'fallback_id' : 'none'),
        body_text: bodyText,
    };
}
"""

# JS to extract only visible spec tables (handles CSS-hidden engine tabs)
VISIBLE_SPECS_JS = r"""
() => {
    const norm = s =>
        (s || '').replace(/\u00a0/g,' ').replace(/\u200b/g,' ')
                 .replace(/\s+/g,' ').trim();

    const isVisible = el => {
        if (!el) return false;
        let cur = el;
        while (cur && cur !== document.body) {
            const st = window.getComputedStyle(cur);
            if (!st) return false;
            if (st.display === 'none' || st.visibility === 'hidden') return false;
            cur = cur.parentElement;
        }
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    };

    const SECTION_KEYS = new Set([
        'ENGINE SPECS','PERFORMANCE SPECS','TRANSMISSION SPECS','DRIVETRAIN SPECS',
        'BRAKES SPECS','TIRES SPECS','DIMENSIONS','WEIGHT SPECS','FUEL ECONOMY',
        'FUEL ECONOMY (NEDC)','FUEL ECONOMY (WLTP)','POWER SYSTEM SPECS',
        'BATTERY SPECS','CHARGING SPECS','SUSPENSION SPECS','STEERING SPECS',
        'EMISSION SPECS','CO2 EMISSIONS','AERODYNAMICS'
    ]);

    const isSectionHeader = t => {
        const u = t.toUpperCase().replace(/\s*[-\u2013]\s+.*$/, '').trim();
        if (SECTION_KEYS.has(u)) return true;
        if (u.startsWith('ENGINE SPECS')) return true;
        if (u.endsWith(' SPECS')) return true;
        return false;
    };

    const cleanSecName = t => t.replace(/\s*[-\u2013]\s+.*$/, '').trim();

    const allSections = [];
    let currentSection = null;
    let currentItems = [];

    const flush = () => {
        if (currentSection && currentItems.length) {
            allSections.push({section_name: currentSection, items: [...currentItems]});
        }
        currentSection = null;
        currentItems = [];
    };

    for (const table of document.querySelectorAll('table')) {
        if (!isVisible(table)) continue;

        for (const row of table.querySelectorAll('tr')) {
            const cells = Array.from(row.querySelectorAll('td, th'));
            if (!cells.length) continue;

            const isSpan = cells.length === 1 ||
                (cells[0].getAttribute('colspan') || '1') !== '1';

            if (isSpan) {
                const raw = norm(cells[0].innerText || cells[0].textContent || '');
                if (!raw) continue;
                if (isSectionHeader(raw)) {
                    flush();
                    currentSection = cleanSecName(raw);
                }
                continue;
            }

            if (cells.length >= 2) {
                const label = norm(
                    (cells[0].innerText || cells[0].textContent || '')
                ).replace(/:$/, '').trim();

                const valLines = (cells[1].innerText || cells[1].textContent || '')
                    .split('\n').map(l => norm(l)).filter(Boolean);
                const value = valLines.join(' | ');

                if (!label || !value) continue;

                if (isSectionHeader(label)) {
                    flush();
                    currentSection = cleanSecName(label);
                } else {
                    if (!currentSection) currentSection = 'SPECS';
                    currentItems.push({label, value});
                }
            }
        }
    }
    flush();
    return allSections;
}
"""

# JS to read engine name from visible sidebar or header
ENGINE_NAME_JS = r"""
(targetAnchor) => {
    const norm = (s) => (s || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\u200b/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    const anchor = String(targetAnchor || '').replace(/^#/, '');
    const rx = /(?:\b\d+(?:\.\d+)?\s*[LK]WH\b|\b\d+(?:\.\d+)?L\b|\bV\d\b|\bAWD\b|\bRWD\b|\bFWD\b).*?\(\s*\d+\s*HP\s*\)/i;

    const isVisible = (el) => {
        if (!el) return false;
        let cur = el;
        while (cur && cur !== document.body) {
            const st = window.getComputedStyle(cur);
            if (!st) return false;
            if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return false;
            cur = cur.parentElement;
        }
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    };

    const getLines = (el) => norm(el.innerText || el.textContent || '').split(/\n+/).map(norm).filter(Boolean);
    const findEngineLine = (el) => {
        if (!el || !isVisible(el)) return '';
        for (const line of getLines(el)) {
            if (rx.test(line)) return norm(line.match(rx)[0]);
        }
        const whole = norm(el.innerText || el.textContent || '');
        const m = whole.match(rx);
        return m ? norm(m[0]) : '';
    };

    const candidates = [];
    const push = (el, score) => {
        if (!el) return;
        const txt = findEngineLine(el);
        if (!txt) return;
        const r = el.getBoundingClientRect();
        candidates.push({txt, score: score - txt.length, left: r.left, top: r.top});
    };

    for (const sel of [
        `a[href="#${anchor}"]`, `a[href$="#${anchor}"]`,
        `[href*="#${anchor}"]`, `[onclick*="${anchor}"]`,
        `#${anchor}`
    ]) {
        for (const el of document.querySelectorAll(sel)) {
            push(el, 1200);
            push(el.closest('a'), 1180);
            push(el.closest('li'), 1160);
        }
    }

    for (const el of document.querySelectorAll('h1,h2,h3,h4,td,th,div,strong,b')) {
        if (!isVisible(el)) continue;
        const txt = norm(el.innerText || el.textContent || '');
        if (!/^ENGINE\s+SPECS\s*[-\u2013]\s*/i.test(txt)) continue;
        const stripped = norm(txt.replace(/^ENGINE\s+SPECS\s*[-\u2013]\s*/i, ''));
        const m = stripped.match(rx);
        if (m) candidates.push({txt: norm(m[0]), score: 700 - norm(m[0]).length, left: 9999, top: 9999});
    }

    if (!candidates.length) return '';
    candidates.sort((a, b) => b.score - a.score || a.left - b.left || a.top - b.top);
    return candidates[0].txt || '';
}
"""


# ── Page interaction functions ─────────────────────────────────────────────

async def harvest_real_anchors(page: Page, expected_count: Optional[int] = None) -> dict:
    """
    Harvest engine anchors and labels from the current page.

    Runs JavaScript to discover sidebar engine list items,
    optionally clicking to reveal all engines if expected count known.
    """
    result = await page.evaluate(HARVEST_JS, expected_count)
    return result


async def click_engine_tab(page: Page, anchor: str, settle_ms: int = 800) -> None:
    """
    Activate a specific engine's tab.

    Uses JS-first approach: calling engine_show() is more resilient than
    DOM clicking because it doesn't depend on element visibility or
    click interception by overlays. DOM click is the fallback.
    """
    # Sanitize anchor to prevent JS injection
    safe_anchor = anchor.replace('"', '').replace("'", '').replace('\\', '')

    # Strategy 1: JS function call (most resilient)
    try:
        await page.evaluate(
            f'typeof engine_show === "function" && engine_show("{safe_anchor}")'
        )
        await page.wait_for_timeout(settle_ms)
        return
    except Exception:
        pass

    # Strategy 2: Click the sidebar li element by ID
    li_id = anchor.replace("aeng_", "li_eng_")
    try:
        li = page.locator(f"#{li_id}")
        if await li.count() > 0:
            await li.click(timeout=3000)
            await page.wait_for_timeout(settle_ms)
            return
    except Exception:
        pass

    # Strategy 3: Click any link pointing to this anchor
    try:
        link = page.locator(f'a[href$="#{safe_anchor}"]')
        if await link.count() > 0:
            await link.first.click(timeout=3000)
            await page.wait_for_timeout(settle_ms)
    except Exception:
        pass


async def extract_visible_specs(page: Page) -> List[dict]:
    """
    Extract spec sections from ONLY the visible tables on screen.

    This is the core fix: autoevolution loads ALL engines' tables in HTML
    but shows only ONE at a time via CSS. This JS reads visible tables only.
    """
    try:
        return await page.evaluate(VISIBLE_SPECS_JS)
    except Exception:
        return []


async def get_engine_name_from_page(page: Page, anchor: str) -> str:
    """Read engine label from visible sidebar element for the given anchor."""
    try:
        return await page.evaluate(ENGINE_NAME_JS, anchor)
    except Exception:
        return ""


async def wait_for_engine_header(page: Page, timeout_ms: int = 5000) -> None:
    """Wait until an ENGINE SPECS header is visible on the page."""
    await page.wait_for_function(
        r"""
        () => {
            const norm = (s) => (s || '')
                .replace(/\u00a0/g, ' ')
                .replace(/\u200b/g, ' ')
                .replace(/\s+/g, ' ')
                .trim();
            const nodes = Array.from(
                document.querySelectorAll('h1,h2,h3,h4,td,th,div,strong,b')
            );
            for (const el of nodes) {
                const t = norm(el.innerText || el.textContent || '');
                if (/^ENGINE\s+SPECS\s*[-\u2013]\s*/i.test(t)) return true;
            }
            return false;
        }
        """,
        timeout=timeout_ms,
    )


# ── Engine extraction per anchor ───────────────────────────────────────────

async def extract_engine_from_anchor(
    page: Page,
    row: dict,
    anchor: str,
    brand_name: str,
    model_name: str,
    settle_ms: int = 800,
) -> Tuple[Optional[dict], int]:
    """
    Extract one engine's data by activating its tab and reading visible specs.

    Returns (engine_dict, spec_count) or (None, 0) on failure.
    """
    # Click the engine tab (no page reload — stays on same page)
    await click_engine_tab(page, anchor, settle_ms)

    try:
        await wait_for_engine_header(page, timeout_ms=5000)
    except Exception:
        pass

    # Extract visible specs
    spec_sections = await extract_visible_specs(page)

    # Fallback to BeautifulSoup
    if not spec_sections:
        html = await page.content()
        _, spec_sections = parse_engine_tables_from_html(html)

    spec_count = sum(len(sec["items"]) for sec in spec_sections)
    if spec_count == 0:
        return None, 0

    # Resolve engine name (3-strategy cascade)
    engine_name = ""

    # 1) Visible label from sidebar/header
    engine_name = await get_engine_name_from_page(page, anchor)

    # 2) Decode from anchor ID
    if not engine_name:
        engine_name = build_engine_name_from_anchor(anchor, spec_sections)

    # 3) Derive from spec values
    if not engine_name:
        engine_name = build_engine_name_from_specs(spec_sections)

    # Build engine object
    engine_specs_url = f"{row.get('model_year_url', '')}#{anchor}"
    engine_obj = {
        "engine_id": make_id(
            row.get("model_year_id", ""),
            anchor,
            engine_name,
        ),
        "brand_id": row.get("brand_id", ""),
        "model_id": row.get("model_id", ""),
        "model_year_id": row.get("model_year_id", ""),
        "model_year_url": row.get("model_year_url", ""),
        "model_year_label": row.get("model_year_label", ""),
        "engine_name": engine_name,
        "engine_anchor_id": anchor,
        "engine_specs_url": engine_specs_url,
        "engine_url": engine_specs_url,
        "brand_name": brand_name,
        "model_name": model_name,
        "spec_sections": spec_sections,
    }

    return engine_obj, spec_count
