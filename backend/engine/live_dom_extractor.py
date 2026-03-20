"""
live_dom_extractor.py — The brain of Pegasus Extract.

Instead of trusting pre-generated CSS selectors (which fail on complex sites),
this module:

1. Runs comprehensive DOM inspection JS *inside* the live Chromium page
   (exactly like a human opening Chrome DevTools → Elements panel).
2. Sends the REAL DOM structure + visible text to the AI.
3. AI generates a self-contained JavaScript extraction function.
4. We execute that JS in the page context and get structured data back.
5. If extraction yields 0 records, we retry with a different strategy
   (self-healing).

This makes extraction work on ANY website — static HTML, React SPAs,
server-rendered pages, tables, lists, grids, etc.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from playwright.async_api import Page

from core.ai_provider import ai_provider
from core.dom_preprocessor import preprocess_dom

log = logging.getLogger("PegasusExtract")

# ---------------------------------------------------------------------------
# 1. DOM Inspection — runs inside the browser like Chrome DevTools
# ---------------------------------------------------------------------------

DOM_INSPECTOR_JS = """
() => {
    // ── Helper: get a concise CSS selector for an element ──
    function getSelector(el) {
        if (el.id) return '#' + CSS.escape(el.id);
        let sel = el.tagName.toLowerCase();
        if (el.className && typeof el.className === 'string') {
            const classes = el.className.trim().split(/\\s+/).filter(c => c.length > 0 && c.length < 50);
            if (classes.length > 0) {
                sel += '.' + classes.slice(0, 3).map(c => CSS.escape(c)).join('.');
            }
        }
        return sel;
    }

    // ── Helper: get clean visible text (first N chars) ──
    function getText(el, limit) {
        limit = limit || 200;
        const text = (el.innerText || el.textContent || '').trim();
        return text.substring(0, limit);
    }

    // ── Helper: check if element is visible ──
    // NOTE: Do NOT use el.offsetParent — it returns null in headless Chromium
    // for many visible elements (position:fixed, inside <td>, etc.)
    function isVisible(el) {
        try {
            const style = window.getComputedStyle(el);
            if (style.display === 'none') return false;
            if (style.visibility === 'hidden') return false;
            if (style.opacity === '0') return false;
            // Check dimensions only via bounding rect (works in headless)
            const rect = el.getBoundingClientRect();
            if (rect.width === 0 && rect.height === 0) return false;
            return true;
        } catch(e) {
            return true; // if getComputedStyle fails, assume visible
        }
    }

    const skipTags = new Set(['script','style','noscript','link','meta','br','hr',
        'img','svg','path','input','button','select','textarea','iframe',
        'video','audio','source','canvas','picture','figure']);

    // ── Step 1: Find ALL repeating patterns (like DevTools "Elements" panel scan) ──
    const signatureMap = {};
    const allElements = document.querySelectorAll('body *');

    for (const el of allElements) {
        const tag = el.tagName.toLowerCase();
        if (skipTags.has(tag)) continue;
        if (!isVisible(el)) continue;

        const classes = (el.className && typeof el.className === 'string')
            ? el.className.trim().split(/\\s+/).filter(c => c.length > 1 && c.length < 50).sort().join('.')
            : '';

        // Accept elements with classes, id, role, or data-testid
        const hasIdentity = classes || el.id ||
            el.getAttribute('role') || el.getAttribute('data-testid');
        if (!hasIdentity) continue;

        const sig = tag + (classes ? '.' + classes : '') + (el.id ? '#' + el.id : '');
        if (!signatureMap[sig]) {
            signatureMap[sig] = { count: 0, selector: '', samples: [], childTags: new Set(), textLengths: [] };
        }
        signatureMap[sig].count++;
        signatureMap[sig].selector = getSelector(el);

        // Collect child structure info for first 3 samples
        if (signatureMap[sig].samples.length < 3) {
            const childInfo = [];
            for (const child of el.children) {
                childInfo.push({
                    tag: child.tagName.toLowerCase(),
                    classes: (child.className && typeof child.className === 'string') ? child.className.trim().substring(0, 100) : '',
                    text: getText(child, 100),
                    href: child.getAttribute('href') || '',
                    childCount: child.children.length
                });
                signatureMap[sig].childTags.add(child.tagName.toLowerCase());
            }
            const text = getText(el, 300);
            signatureMap[sig].textLengths.push(text.length);
            signatureMap[sig].samples.push({
                text: text,
                children: childInfo.slice(0, 15),
                attrs: {
                    id: el.id || '',
                    role: el.getAttribute('role') || '',
                    dataTestId: el.getAttribute('data-testid') || ''
                }
            });
        }
    }

    // ── Step 2: Score and rank candidates ──
    const candidates = [];
    for (const [sig, info] of Object.entries(signatureMap)) {
        if (info.count < 3) continue;
        const avgTextLen = info.textLengths.reduce((a,b) => a+b, 0) / info.textLengths.length;
        if (avgTextLen < 3) continue;
        const childTagCount = info.childTags.size;

        const score = info.count * (childTagCount + 1) * Math.min(avgTextLen, 200);

        candidates.push({
            signature: sig,
            selector: info.selector,
            count: info.count,
            avgTextLen: Math.round(avgTextLen),
            childTags: Array.from(info.childTags),
            score: Math.round(score),
            samples: info.samples
        });
    }

    candidates.sort((a, b) => b.score - a.score);

    // ── Step 3: Get full HTML of top samples from best candidate ──
    const topCandidates = candidates.slice(0, 8);
    const sampleHTML = [];

    if (topCandidates.length > 0) {
        const best = topCandidates[0];
        try {
            const els = document.querySelectorAll(best.selector);
            for (let i = 0; i < Math.min(3, els.length); i++) {
                sampleHTML.push(els[i].outerHTML.substring(0, 2000));
            }
        } catch(e) {}
    }

    // ── Step 4: Also check for tables (very common data format) ──
    const tables = [];
    for (const table of document.querySelectorAll('table')) {
        const headers = Array.from(table.querySelectorAll('th')).map(th => getText(th, 50));
        const rows = table.querySelectorAll('tbody tr, tr');
        if (rows.length < 2) continue;

        const sampleRows = [];
        for (let i = 0; i < Math.min(3, rows.length); i++) {
            const cells = Array.from(rows[i].querySelectorAll('td, th')).map(td => getText(td, 100));
            if (cells.some(c => c.length > 0)) {
                sampleRows.push(cells);
            }
        }

        tables.push({
            selector: getSelector(table),
            headers: headers,
            rowCount: rows.length,
            sampleRows: sampleRows,
            sampleHTML: rows.length > 0 ? rows[0].outerHTML.substring(0, 1500) : ''
        });
    }

    // ── Step 5: Collect lists (ul/ol with multiple li) ──
    const lists = [];
    for (const list of document.querySelectorAll('ul, ol')) {
        const items = list.querySelectorAll(':scope > li');
        if (items.length < 3) continue;

        const sampleItems = [];
        for (let i = 0; i < Math.min(3, items.length); i++) {
            sampleItems.push({
                text: getText(items[i], 200),
                html: items[i].outerHTML.substring(0, 1000)
            });
        }
        lists.push({
            selector: getSelector(list),
            itemCount: items.length,
            samples: sampleItems
        });
    }

    // ── Step 6: Page diagnostics ──
    const bodyText = getText(document.body, 1000);
    const pageInfo = {
        title: document.title,
        url: window.location.href,
        totalElements: allElements.length,
        bodyTextLength: (document.body.innerText || '').length,
        visibleText: bodyText
    };

    return {
        pageInfo: pageInfo,
        topCandidates: topCandidates,
        sampleHTML: sampleHTML,
        tables: tables,
        lists: lists.slice(0, 5)
    };
}
"""


# ---------------------------------------------------------------------------
# 2. AI Code Generator — writes extraction JS from real DOM inspection
# ---------------------------------------------------------------------------

async def _generate_extraction_js(
    dom_report: dict,
    fields: List[str],
    description: str,
    url: str,
    attempt: int = 1,
    previous_error: str = "",
) -> str:
    """
    Ask AI to write a JavaScript extraction function based on the REAL
    DOM inspection report from the live page.
    """
    retry_hint = ""
    if attempt > 1 and previous_error:
        retry_hint = f"""
PREVIOUS ATTEMPT FAILED: {previous_error}
You MUST use a DIFFERENT strategy this time. Try:
- Different container selectors
- Table-based extraction if there are tables
- List-based extraction if there are lists
- Direct querySelectorAll on the most repeated elements
- Walking the DOM tree manually
"""

    SYSTEM = """You are an expert JavaScript developer specializing in web scraping.
You write extraction functions that run inside a browser page context (like Chrome DevTools Console).

CRITICAL RULES:
1. Return ONLY the JavaScript function body. No markdown, no backticks, no explanation.
2. The function must return an array of objects.
3. Use ONLY vanilla JavaScript (document.querySelectorAll, etc).
4. Base your selectors ONLY on the DOM inspection report provided — use the EXACT
   class names, tag names, and structure shown.
5. Handle edge cases: missing elements, empty text, etc.
6. The code will be wrapped in: (function() { YOUR_CODE_HERE })()
7. Always return [] if nothing is found, never throw errors.
8. Clean text values: trim whitespace, remove excessive newlines.
9. For numeric fields (rating, price, rank, year), extract JUST the number."""

    # Build a focused DOM context for the AI — use compressed summary
    dom_context = preprocess_dom(dom_report)

    USER = f"""Write a JavaScript extraction function for this page.

TARGET URL: {url}
USER WANTS: {description}
FIELDS TO EXTRACT: {json.dumps(fields)}
{retry_hint}

LIVE DOM INSPECTION REPORT (from real Chromium browser):
{dom_context}

IMPORTANT:
- Look at the topCandidates, tables, lists, and sampleHTML to understand the page structure.
- Pick the BEST data container (could be table rows, list items, or repeated divs).
- Write selectors that match the EXACT classes/tags shown in the report.
- The function runs inside (function() {{ ... }})() — return an array of objects.
- Each object must have these keys: {json.dumps(fields)}
- Return the array directly. Example: return [{{...}}, {{...}}];
- Wrap everything in try/catch and return [] on error.

Write the JavaScript function body NOW:"""

    result = await ai_provider.complete(SYSTEM, USER, json_mode=False)
    js_code = result.get("text", "return [];")

    # Clean markdown fences if present
    js_code = re.sub(r'```(?:javascript|js)?\s*', '', js_code)
    js_code = re.sub(r'```\s*', '', js_code)
    js_code = js_code.strip()

    # Remove any leading "javascript" or function wrapper the AI might add
    js_code = re.sub(r'^(?:javascript\s*)', '', js_code, flags=re.IGNORECASE)
    # If AI wrapped in (function(){...})(), unwrap it
    if js_code.startswith('(function()') and js_code.rstrip().endswith(')()'):
        inner = js_code[len('(function(){'):]
        inner = inner.rstrip()
        if inner.endswith('})()'):
            inner = inner[:-len('})()')]
        js_code = inner

    log.info(f"AI generated {len(js_code)} chars of extraction JS (attempt {attempt})")
    return js_code


# ---------------------------------------------------------------------------
# 3. Main Extraction Class
# ---------------------------------------------------------------------------

class LiveDOMExtractor:
    """
    Extracts data from ANY website using live DOM inspection + AI-generated JS.

    Flow:
    1. Page is already loaded in Playwright
    2. Run DOM_INSPECTOR_JS to understand page structure
    3. AI generates extraction JavaScript based on real DOM
    4. Execute that JS in the page
    5. If 0 results, retry with different strategy (self-healing)
    """

    MAX_ATTEMPTS = 3

    def __init__(self, fields: List[str], description: str = ""):
        self.fields = fields
        self.description = description

    async def inspect_dom(self, page: Page) -> dict:
        """Run comprehensive DOM inspection inside the live browser page."""
        try:
            report = await page.evaluate(DOM_INSPECTOR_JS)
            top = report.get("topCandidates", [])
            tables = report.get("tables", [])
            lists = report.get("lists", [])
            page_info = report.get("pageInfo", {})
            log.info(
                f"DOM inspection: {len(top)} candidates, "
                f"{len(tables)} tables, {len(lists)} lists | "
                f"title='{page_info.get('title', '?')}' "
                f"elements={page_info.get('totalElements', 0)} "
                f"bodyText={page_info.get('bodyTextLength', 0)} chars"
            )
            if page_info.get("bodyTextLength", 0) < 100:
                log.warning(
                    f"Page body text very short ({page_info.get('bodyTextLength', 0)} chars) — "
                    f"page may not have rendered. Visible: {page_info.get('visibleText', '')[:200]}"
                )
            return report
        except Exception as e:
            log.error(f"DOM inspection failed: {e}")
            return {}

    async def extract(
        self,
        page: Page,
        url: str,
        plan_fields: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Main extraction entry point.
        Returns list of extracted records.
        """
        # Step 1: Inspect the live DOM
        dom_report = await self.inspect_dom(page)
        if not dom_report:
            log.warning("Empty DOM report — page may not have loaded")
            return []

        # Step 2: Deterministic shortcut — skip AI entirely for obvious pages
        results = await self._try_deterministic_shortcut(page, dom_report)
        if results:
            return results

        # Step 3: Try AI-generated JS extraction with self-healing
        results = []
        last_error = ""

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            try:
                js_code = await _generate_extraction_js(
                    dom_report=dom_report,
                    fields=self.fields,
                    description=self.description,
                    url=url,
                    attempt=attempt,
                    previous_error=last_error,
                )

                # Execute the AI-generated JS in the page
                wrapped = f"(function() {{ try {{ {js_code} }} catch(e) {{ return []; }} }})()"
                raw_results = await page.evaluate(wrapped)

                if isinstance(raw_results, list) and len(raw_results) > 0:
                    # Validate and clean results
                    cleaned = self._clean_results(raw_results)
                    if cleaned:
                        log.info(
                            f"Attempt {attempt}: extracted {len(cleaned)} records"
                        )
                        results = cleaned
                        break
                    else:
                        last_error = f"Attempt {attempt} returned {len(raw_results)} raw records but all were empty/invalid after cleaning"
                        log.warning(last_error)
                else:
                    last_error = f"Attempt {attempt} returned {type(raw_results).__name__} with {len(raw_results) if isinstance(raw_results, list) else 0} items"
                    log.warning(last_error)

            except Exception as e:
                last_error = f"Attempt {attempt} JS execution error: {str(e)[:200]}"
                log.error(last_error)

        # Step 3: If AI JS failed all attempts, try deterministic table/list extraction
        if not results:
            log.info("AI JS extraction failed — trying deterministic fallback")
            results = await self._deterministic_extract(page, dom_report)

        return results

    def _field_coverage(self, records: List[Dict[str, Any]]) -> float:
        """Return fraction (0.0-1.0) of requested fields that have data."""
        if not records or not self.fields:
            return 0.0
        total_cells = len(records) * len(self.fields)
        filled = sum(
            1
            for rec in records
            for f in self.fields
            if rec.get(f) and str(rec[f]).strip()
            and str(rec[f]).strip().lower() not in ("none", "null", "undefined", "")
        )
        return filled / max(total_cells, 1)

    async def _try_deterministic_shortcut(
        self, page: Page, dom_report: dict
    ) -> List[Dict[str, Any]]:
        """
        If the DOM has an obvious table or repeated pattern, extract
        directly without calling AI.  Returns [] if no shortcut applies.

        Quality gate: extracted records must populate ≥30% of requested
        fields — otherwise the shortcut found the wrong container.
        """
        compressed = preprocess_dom(dom_report)
        min_coverage = 0.30

        # ── Shortcut A: TABLE_FOUND — extract table with zero AI tokens ──
        if "TABLE_FOUND" in compressed:
            tables = dom_report.get("tables", [])
            if tables:
                best_table = max(tables, key=lambda t: t.get("rowCount", 0))
                if best_table.get("rowCount", 0) >= 5:
                    results = await self._extract_from_table(page, best_table)
                    if results:
                        cov = self._field_coverage(results)
                        if cov >= min_coverage:
                            log.info(
                                f"Table detected — zero AI tokens used "
                                f"({len(results)} records, {cov:.0%} field coverage)"
                            )
                            return results
                        log.info(
                            f"Table shortcut skipped — low field coverage "
                            f"({cov:.0%} < {min_coverage:.0%}), falling through to AI"
                        )

        # ── Shortcut B: REPEATED_PATTERN_FOUND — use selector directly ──
        if "REPEATED_PATTERN_FOUND" in compressed:
            candidates = dom_report.get("topCandidates", [])
            for cand in candidates[:3]:
                if cand.get("count", 0) >= 10:
                    results = await self._extract_from_containers(page, cand)
                    if results:
                        cov = self._field_coverage(results)
                        if cov >= min_coverage:
                            log.info(
                                f"Pattern detected — zero AI tokens used "
                                f"({len(results)} records via {cand.get('selector', '?')}, "
                                f"{cov:.0%} field coverage)"
                            )
                            return results
                        log.info(
                            f"Pattern shortcut skipped ({cand.get('selector', '?')}) — "
                            f"low field coverage ({cov:.0%} < {min_coverage:.0%}), "
                            f"falling through to AI"
                        )

        return []

    # UI junk patterns to strip from extracted values
    _JUNK_PATTERNS = re.compile(
        r'(Rate\b|Mark as watched|Add to watchlist|Watch options|See full cast'
        r'|See production info|See more|Read more|Show more|Load more'
        r'|Sign in|Log in|Add to list|Not yet rated)',
        re.IGNORECASE,
    )

    @staticmethod
    def _smart_clean_value(field: str, raw_val: str) -> str:
        """
        Post-process a raw extracted value based on field semantics.
        Fixes common AI extraction issues like concatenated sibling text.
        """
        if not raw_val:
            return raw_val

        fl = field.lower()

        # ── Year field: extract first 4-digit year (1900-2099) ──
        if any(k in fl for k in ('year', 'released', 'release_date', 'date')):
            m = re.search(r'((?:19|20)\d{2})', raw_val)
            if m:
                return m.group(1)

        # ── Rating/score field: extract first decimal number (X.X) ──
        if any(k in fl for k in ('rating', 'score', 'imdb', 'stars', 'grade')):
            m = re.search(r'(\d{1,2}\.\d{1,2})', raw_val)
            if m:
                return m.group(1)

        # ── Rank/position field: extract first integer ──
        if any(k in fl for k in ('rank', 'position', '#', 'number', 'no', 'index')):
            m = re.match(r'(\d+)', raw_val.strip())
            if m:
                return m.group(1)

        # ── Price field: extract currency + number ──
        if any(k in fl for k in ('price', 'cost', 'amount')):
            m = re.search(r'([$€£¥]\s*\d[\d,.]*)', raw_val)
            if m:
                return m.group(1)

        # ── Duration field: extract Xh Xm or X min ──
        if any(k in fl for k in ('duration', 'runtime', 'length')):
            m = re.search(r'(\d+\s*h\s*\d*\s*m(?:in)?|\d+\s*min)', raw_val, re.IGNORECASE)
            if m:
                return m.group(1)

        return raw_val

    def _clean_results(self, raw: List[Any]) -> List[Dict[str, Any]]:
        """Validate, clean, and post-process raw extraction results."""
        cleaned = []
        for item in raw:
            if not isinstance(item, dict):
                continue

            record = {}
            has_value = False
            for field in self.fields:
                # Try exact match first, then case-insensitive
                val = item.get(field)
                if val is None:
                    # Case-insensitive lookup
                    for k, v in item.items():
                        if k.lower().replace(" ", "_") == field.lower().replace(" ", "_"):
                            val = v
                            break
                        if k.lower().replace("_", " ") == field.lower().replace("_", " "):
                            val = v
                            break
                        if k.lower().replace(" ", "").replace("_", "") == field.lower().replace(" ", "").replace("_", ""):
                            val = v
                            break

                if val is None:
                    # Try to find in item by checking all keys
                    for k, v in item.items():
                        if field.lower() in k.lower() or k.lower() in field.lower():
                            val = v
                            break

                val_str = str(val).strip() if val is not None else ""
                # Remove excessive whitespace
                val_str = re.sub(r'\s+', ' ', val_str)
                # Strip UI junk text
                val_str = self._JUNK_PATTERNS.sub('', val_str).strip()
                # Smart post-processing based on field type
                val_str = self._smart_clean_value(field, val_str)

                record[field] = val_str
                if val_str and val_str.lower() not in ('none', 'null', 'undefined', ''):
                    has_value = True

            if has_value:
                cleaned.append(record)

        return cleaned

    async def _deterministic_extract(
        self, page: Page, dom_report: dict
    ) -> List[Dict[str, Any]]:
        """
        Last-resort deterministic extraction: no AI involved.
        Tries tables first, then the top repeating container, then lists.
        """
        results = []

        # Strategy A: Table extraction (most reliable for structured data)
        tables = dom_report.get("tables", [])
        if tables:
            best_table = max(tables, key=lambda t: t.get("rowCount", 0))
            if best_table.get("rowCount", 0) >= 3:
                results = await self._extract_from_table(page, best_table)
                if results:
                    log.info(f"Deterministic table extraction: {len(results)} records")
                    return results

        # Strategy B: Top repeating container
        candidates = dom_report.get("topCandidates", [])
        if candidates:
            for cand in candidates[:3]:
                results = await self._extract_from_containers(page, cand)
                if results:
                    log.info(
                        f"Deterministic container extraction ({cand['selector']}): "
                        f"{len(results)} records"
                    )
                    return results

        # Strategy C: List extraction
        lists = dom_report.get("lists", [])
        if lists:
            best_list = max(lists, key=lambda l: l.get("itemCount", 0))
            results = await self._extract_from_list(page, best_list)
            if results:
                log.info(f"Deterministic list extraction: {len(results)} records")
                return results

        return results

    async def _extract_from_table(
        self, page: Page, table_info: dict
    ) -> List[Dict[str, Any]]:
        """Extract data from an HTML table."""
        selector = table_info.get("selector", "table")
        headers = table_info.get("headers", [])

        js = f"""
        (function() {{
            try {{
                const table = document.querySelector('{selector}');
                if (!table) return [];

                // Get headers
                let headers = {json.dumps(headers)};
                if (!headers.length) {{
                    const ths = table.querySelectorAll('thead th, tr:first-child th');
                    headers = Array.from(ths).map(th => th.innerText.trim());
                }}

                // Map headers to requested fields
                const fieldMap = {{}};
                const requestedFields = {json.dumps(self.fields)};
                for (const field of requestedFields) {{
                    const fl = field.toLowerCase().replace(/[_\\s]+/g, '');
                    for (let i = 0; i < headers.length; i++) {{
                        const hl = headers[i].toLowerCase().replace(/[_\\s]+/g, '');
                        if (hl.includes(fl) || fl.includes(hl) || hl === fl) {{
                            fieldMap[field] = i;
                            break;
                        }}
                    }}
                }}

                // Extract rows
                const rows = table.querySelectorAll('tbody tr, tr');
                const results = [];
                for (const row of rows) {{
                    const cells = row.querySelectorAll('td');
                    if (cells.length < 2) continue;

                    const record = {{}};
                    let hasData = false;
                    for (const [field, idx] of Object.entries(fieldMap)) {{
                        if (idx < cells.length) {{
                            const val = cells[idx].innerText.trim();
                            if (val) {{
                                record[field] = val;
                                hasData = true;
                            }} else {{
                                record[field] = '';
                            }}
                        }} else {{
                            record[field] = '';
                        }}
                    }}

                    // For unmapped fields, try to assign remaining cells
                    for (const field of requestedFields) {{
                        if (record[field] === undefined) {{
                            // Try to find a cell with matching content
                            for (let i = 0; i < cells.length; i++) {{
                                const val = cells[i].innerText.trim();
                                if (val && !Object.values(fieldMap).includes(i)) {{
                                    record[field] = val;
                                    break;
                                }}
                            }}
                            if (record[field] === undefined) record[field] = '';
                        }}
                    }}

                    if (hasData) results.push(record);
                }}
                return results;
            }} catch(e) {{ return []; }}
        }})()
        """

        try:
            raw = await page.evaluate(js)
            if isinstance(raw, list):
                return self._clean_results(raw)
        except Exception as e:
            log.error(f"Table extraction failed: {e}")
        return []

    async def _extract_from_containers(
        self, page: Page, candidate: dict
    ) -> List[Dict[str, Any]]:
        """Extract data from repeating container elements using smart value classification."""
        selector = candidate.get("selector", "")
        if not selector:
            return []

        js = f"""
        (function() {{
            try {{
                const containers = document.querySelectorAll('{selector}');
                if (containers.length < 2) return [];

                const requestedFields = {json.dumps(self.fields)};
                const results = [];
                let containerIndex = 0;

                for (const container of containers) {{
                    containerIndex++;
                    const record = {{}};
                    let hasData = false;

                    // ── Collect ALL text pieces with semantic types ──
                    const pieces = [];
                    const seen = new Set();

                    function addPiece(text, elType, href, el) {{
                        text = (text || '').trim();
                        if (!text || text.length < 1 || seen.has(text)) return;
                        // Skip if text is just the container's full text (too broad)
                        if (text.length > 200) return;
                        seen.add(text);

                        // ── Classify this value ──
                        let vtype = 'text';
                        const numOnly = text.replace(/[,\\s]/g, '');

                        // Year: exactly 4 digits, 1900-2099
                        if (/^\\(?\\d{{4}}\\)?$/.test(text.replace(/[()]/g, ''))) {{
                            const yr = parseInt(text.replace(/[()]/g, ''));
                            if (yr >= 1900 && yr <= 2099) vtype = 'year';
                        }}
                        // Rating: decimal like 9.3, 8.7, 4.5 (1.0 - 10.0)
                        else if (/^\\d{{1,2}}\\.\\d{{1,2}}$/.test(text)) {{
                            const r = parseFloat(text);
                            if (r >= 0.1 && r <= 10.0) vtype = 'rating';
                        }}
                        // Rating with votes: "9.3 (3.2M)" or "8.7 (1.5M)"
                        else if (/^\\d{{1,2}}\\.\\d.*\\(/.test(text)) {{
                            vtype = 'rating_with_info';
                        }}
                        // Rank: standalone small integer (1-9999)
                        else if (/^\\d{{1,4}}$/.test(numOnly) && parseInt(numOnly) >= 1 && parseInt(numOnly) <= 9999) {{
                            // Disambiguate: if it looks like it could be containerIndex, it's rank
                            vtype = 'rank_or_number';
                        }}
                        // Duration: "2h 22m", "1h 30m", "142 min"
                        else if (/\\d+\\s*h\\s*\\d*\\s*m/i.test(text) || /\\d+\\s*min/i.test(text)) {{
                            vtype = 'duration';
                        }}
                        // Price: $XX.XX or XX.XX with currency
                        else if (/^[\\$\\€\\£]\\s*\\d/.test(text) || /\\d+\\.\\d{{2}}$/.test(text)) {{
                            vtype = 'price';
                        }}
                        // Large number with suffix (vote count): "3.2M", "1,500"
                        else if (/\\d+[.,]\\d+[MKBmkb]/.test(text) || /^[\\d,]+$/.test(numOnly) && numOnly.length > 4) {{
                            vtype = 'count';
                        }}
                        // Age rating: PG-13, R, PG, G, etc.
                        else if (/^(PG-13|PG|R|G|NC-17|TV-MA|TV-14|TV-PG|TV-G|TV-Y|NR|UR)$/i.test(text)) {{
                            vtype = 'age_rating';
                        }}
                        // Link/URL
                        else if (href && href.length > 1) {{
                            vtype = 'link';
                        }}
                        // Title: multi-word text, not a number
                        else if (text.length > 3 && /[a-zA-Z]/.test(text) && (elType === 'heading' || elType === 'link')) {{
                            vtype = 'title';
                        }}

                        pieces.push({{ text, vtype, elType, href: href || '' }});
                    }}

                    // Headings (highest priority for titles)
                    for (const h of container.querySelectorAll('h1,h2,h3,h4,h5,h6')) {{
                        addPiece(h.innerText, 'heading', '', h);
                    }}
                    // Links
                    for (const a of container.querySelectorAll('a[href]')) {{
                        const t = a.innerText.trim();
                        const href = a.getAttribute('href') || '';
                        if (t && !href.includes('javascript:')) {{
                            addPiece(t, 'link', href, a);
                        }}
                    }}
                    // Leaf spans/elements with short text (metrics, numbers)
                    for (const s of container.querySelectorAll('span, td, time, [class*="rating"], [class*="score"], [class*="year"], [class*="rank"], [class*="vote"]')) {{
                        const t = s.innerText.trim();
                        if (t && t.length < 50) addPiece(t, 'metric', '', s);
                    }}
                    // Direct text nodes of container children
                    for (const child of container.children) {{
                        const t = child.innerText.trim();
                        if (t && t.length < 80 && t.length > 0) addPiece(t, 'child', '', child);
                    }}

                    // ── Smart field assignment based on value classification ──
                    const usedPieces = new Set();

                    for (const field of requestedFields) {{
                        const fl = field.toLowerCase();
                        let bestVal = '';

                        // Determine what this field WANTS
                        const isRank = ['rank','position','#','number','no','index','pos'].some(k => fl === k || fl.startsWith(k));
                        const isTitle = ['title','name','movie','book','product','item','show','series','song','album','artist','game'].some(k => fl.includes(k));
                        const isYear = ['year','date','released','release'].some(k => fl.includes(k));
                        const isRating = ['rating','score','imdb','stars','grade'].some(k => fl.includes(k));
                        const isLink = ['link','url','href'].some(k => fl.includes(k));
                        const isPrice = ['price','cost','amount'].some(k => fl.includes(k));
                        const isDuration = ['duration','runtime','length','time'].some(k => fl.includes(k));
                        const isVotes = ['votes','vote','count','reviews','popularity'].some(k => fl.includes(k));

                        if (isRank) {{
                            // Rank: use container index (most reliable) or find small number
                            bestVal = String(containerIndex);
                            // But try to find an explicit rank number in the content
                            for (const p of pieces) {{
                                if (p.vtype === 'rank_or_number' && !usedPieces.has(p.text)) {{
                                    const n = parseInt(p.text.replace(/[,\\s]/g, ''));
                                    if (n >= 1 && n <= containers.length + 50) {{
                                        bestVal = String(n);
                                        usedPieces.add(p.text);
                                        break;
                                    }}
                                }}
                            }}
                        }} else if (isTitle) {{
                            // Title: prefer headings, then links, longest multi-word text
                            let best = null;
                            for (const p of pieces) {{
                                if (p.vtype === 'title' && !usedPieces.has(p.text)) {{
                                    if (!best || p.text.length > best.text.length ||
                                        (p.elType === 'heading' && best.elType !== 'heading')) {{
                                        best = p;
                                    }}
                                }}
                            }}
                            if (best) {{ bestVal = best.text; usedPieces.add(best.text); }}
                        }} else if (isYear) {{
                            for (const p of pieces) {{
                                if (p.vtype === 'year' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text.replace(/[()]/g, '');
                                    usedPieces.add(p.text);
                                    break;
                                }}
                            }}
                        }} else if (isRating) {{
                            // Rating: prefer clean decimal, fallback to rating_with_info
                            for (const p of pieces) {{
                                if (p.vtype === 'rating' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text;
                                    usedPieces.add(p.text);
                                    break;
                                }}
                            }}
                            if (!bestVal) {{
                                for (const p of pieces) {{
                                    if (p.vtype === 'rating_with_info' && !usedPieces.has(p.text)) {{
                                        // Extract just the number: "9.3 (3.2M)" → "9.3"
                                        const m = p.text.match(/(\\d+\\.\\d+)/);
                                        bestVal = m ? m[1] : p.text;
                                        usedPieces.add(p.text);
                                        break;
                                    }}
                                }}
                            }}
                        }} else if (isLink) {{
                            for (const p of pieces) {{
                                if (p.href && !usedPieces.has(p.href)) {{
                                    bestVal = p.href.startsWith('http') ? p.href : new URL(p.href, window.location.origin).href;
                                    usedPieces.add(p.href);
                                    break;
                                }}
                            }}
                        }} else if (isPrice) {{
                            for (const p of pieces) {{
                                if (p.vtype === 'price' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text;
                                    usedPieces.add(p.text);
                                    break;
                                }}
                            }}
                        }} else if (isDuration) {{
                            for (const p of pieces) {{
                                if (p.vtype === 'duration' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text;
                                    usedPieces.add(p.text);
                                    break;
                                }}
                            }}
                        }} else if (isVotes) {{
                            for (const p of pieces) {{
                                if (p.vtype === 'count' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text;
                                    usedPieces.add(p.text);
                                    break;
                                }}
                            }}
                        }} else {{
                            // Generic field: pick first unused non-classified text
                            for (const p of pieces) {{
                                if (!usedPieces.has(p.text) && p.text.length > 1) {{
                                    bestVal = p.text;
                                    usedPieces.add(p.text);
                                    break;
                                }}
                            }}
                        }}

                        record[field] = bestVal;
                        if (bestVal) hasData = true;
                    }}

                    if (hasData) results.push(record);
                }}

                return results;
            }} catch(e) {{ return []; }}
        }})()
        """

        try:
            raw = await page.evaluate(js)
            if isinstance(raw, list):
                return self._clean_results(raw)
        except Exception as e:
            log.error(f"Container extraction failed: {e}")
        return []

    async def _extract_from_list(
        self, page: Page, list_info: dict
    ) -> List[Dict[str, Any]]:
        """Extract data from a list (ul/ol) using smart value classification."""
        selector = list_info.get("selector", "ul")

        js = f"""
        (function() {{
            try {{
                const list = document.querySelector('{selector}');
                if (!list) return [];

                const items = list.querySelectorAll(':scope > li');
                const requestedFields = {json.dumps(self.fields)};
                const results = [];
                let itemIndex = 0;

                for (const item of items) {{
                    itemIndex++;
                    const record = {{}};
                    let hasData = false;

                    // Collect pieces with classification
                    const pieces = [];
                    const seen = new Set();

                    function addPiece(text, elType, href) {{
                        text = (text || '').trim();
                        if (!text || text.length < 1 || text.length > 200 || seen.has(text)) return;
                        seen.add(text);

                        let vtype = 'text';
                        const numOnly = text.replace(/[,\\s]/g, '');

                        if (/^\\(?\\d{{4}}\\)?$/.test(text.replace(/[()]/g, ''))) {{
                            const yr = parseInt(text.replace(/[()]/g, ''));
                            if (yr >= 1900 && yr <= 2099) vtype = 'year';
                        }} else if (/^\\d{{1,2}}\\.\\d{{1,2}}$/.test(text)) {{
                            const r = parseFloat(text);
                            if (r >= 0.1 && r <= 10.0) vtype = 'rating';
                        }} else if (/^\\d{{1,2}}\\.\\d.*\\(/.test(text)) {{
                            vtype = 'rating_with_info';
                        }} else if (/^\\d{{1,4}}$/.test(numOnly) && parseInt(numOnly) >= 1 && parseInt(numOnly) <= 9999) {{
                            vtype = 'rank_or_number';
                        }} else if (/\\d+\\s*h\\s*\\d*\\s*m/i.test(text) || /\\d+\\s*min/i.test(text)) {{
                            vtype = 'duration';
                        }} else if (/^[\\$\\€\\£]\\s*\\d/.test(text)) {{
                            vtype = 'price';
                        }} else if (/\\d+[.,]\\d+[MKBmkb]/.test(text)) {{
                            vtype = 'count';
                        }} else if (text.length > 3 && /[a-zA-Z]/.test(text) && (elType === 'heading' || elType === 'link')) {{
                            vtype = 'title';
                        }}

                        pieces.push({{ text, vtype, elType, href: href || '' }});
                    }}

                    for (const h of item.querySelectorAll('h1,h2,h3,h4,h5,h6')) addPiece(h.innerText, 'heading', '');
                    for (const a of item.querySelectorAll('a[href]')) {{
                        const t = a.innerText.trim();
                        if (t) addPiece(t, 'link', a.getAttribute('href') || '');
                    }}
                    for (const s of item.querySelectorAll('span,td,time')) {{
                        const t = s.innerText.trim();
                        if (t && t.length < 50) addPiece(t, 'metric', '');
                    }}

                    const usedPieces = new Set();
                    for (const field of requestedFields) {{
                        const fl = field.toLowerCase();
                        let bestVal = '';

                        const isRank = ['rank','position','#','number','no','index'].some(k => fl === k || fl.startsWith(k));
                        const isTitle = ['title','name','movie','book','product','item'].some(k => fl.includes(k));
                        const isYear = ['year','date','released'].some(k => fl.includes(k));
                        const isRating = ['rating','score','imdb','stars'].some(k => fl.includes(k));
                        const isLink = ['link','url','href'].some(k => fl.includes(k));

                        if (isRank) {{
                            bestVal = String(itemIndex);
                            for (const p of pieces) {{
                                if (p.vtype === 'rank_or_number' && !usedPieces.has(p.text)) {{
                                    const n = parseInt(p.text.replace(/[,\\s]/g, ''));
                                    if (n >= 1 && n <= items.length + 50) {{
                                        bestVal = String(n); usedPieces.add(p.text); break;
                                    }}
                                }}
                            }}
                        }} else if (isTitle) {{
                            for (const p of pieces) {{
                                if (p.vtype === 'title' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text; usedPieces.add(p.text); break;
                                }}
                            }}
                        }} else if (isYear) {{
                            for (const p of pieces) {{
                                if (p.vtype === 'year' && !usedPieces.has(p.text)) {{
                                    bestVal = p.text.replace(/[()]/g, ''); usedPieces.add(p.text); break;
                                }}
                            }}
                        }} else if (isRating) {{
                            for (const p of pieces) {{
                                if ((p.vtype === 'rating' || p.vtype === 'rating_with_info') && !usedPieces.has(p.text)) {{
                                    const m = p.text.match(/(\\d+\\.\\d+)/);
                                    bestVal = m ? m[1] : p.text;
                                    usedPieces.add(p.text); break;
                                }}
                            }}
                        }} else if (isLink) {{
                            for (const p of pieces) {{
                                if (p.href && !usedPieces.has(p.href)) {{
                                    bestVal = p.href.startsWith('http') ? p.href : new URL(p.href, window.location.origin).href;
                                    usedPieces.add(p.href); break;
                                }}
                            }}
                        }} else {{
                            for (const p of pieces) {{
                                if (!usedPieces.has(p.text) && p.text.length > 1) {{
                                    bestVal = p.text; usedPieces.add(p.text); break;
                                }}
                            }}
                        }}

                        record[field] = bestVal;
                        if (bestVal) hasData = true;
                    }}

                    if (hasData) results.push(record);
                }}

                return results;
            }} catch(e) {{ return []; }}
        }})()
        """

        try:
            raw = await page.evaluate(js)
            if isinstance(raw, list):
                return self._clean_results(raw)
        except Exception as e:
            log.error(f"List extraction failed: {e}")
        return []
