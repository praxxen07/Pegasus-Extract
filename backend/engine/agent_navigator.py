"""
agent_navigator.py — Agentic browser controller for search-form sites.

Reads the live DOM like a human, understands what form fields exist,
reads the client description to decide what values to fill, and
interacts with the page step-by-step until results are visible.

Zero hardcoded selectors. Zero site-specific logic. Zero hardcoded keywords.
Every decision is AI-driven based on live DOM + client description.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import List, Optional

from playwright.async_api import Page

from core.ai_provider import ai_provider
from engine.stealth_browser import (
    launch_stealth_browser,
    create_stealth_context,
    new_stealth_page,
)

log = logging.getLogger("PegasusExtract")

MAX_AGENT_STEPS = 12


# ── JavaScript: collect ALL interactive elements as structured JSON ──
_SNAPSHOT_JS = """() => {
    const visible = (el) => {
        const rect = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        return (
            rect.width > 0 &&
            rect.height > 0 &&
            style.visibility !== 'hidden' &&
            style.display !== 'none'
        );
    };

    const bodyText = document.body ? (document.body.innerText || '').trim() : '';

    const selectors = [
        'input', 'select', 'textarea', 'button', 'form',
        '[role="button"]', '[role="combobox"]', '[role="listbox"]',
        '[role="tab"]', '[role="option"]',
        '[tabindex]:not([tabindex="-1"])', '[aria-label]', '[data-testid]',
        '[class*="btn"]', '[class*="button"]', '[class*="cta"]', '[class*="search"]',
        'a[href]'
    ];

    const seen = new Set();
    const interactive = [];

    for (const sel of selectors) {
        try {
            document.querySelectorAll(sel).forEach(el => {
                if (seen.has(el)) return;
                if (!visible(el)) return;

                const tag = el.tagName.toLowerCase();
                const type = (el.getAttribute('type') || '').toLowerCase();
                const role = (el.getAttribute('role') || '').toLowerCase();

                // Skip hidden inputs
                if (tag === 'input' && type === 'hidden') return;

                const text = ((el.innerText || '').trim().replace(/\\s+/g, ' ')).substring(0, 50);
                const placeholder = el.getAttribute('placeholder') || '';
                const ariaLabel = el.getAttribute('aria-label') || '';
                const classNames = (el.className ? String(el.className) : '')
                    .trim().replace(/\\s+/g, ' ').substring(0, 80);

                interactive.push({
                    tag,
                    type,
                    role,
                    text,
                    placeholder,
                    ariaLabel,
                    classNames,
                    id: el.id || '',
                    name: el.getAttribute('name') || ''
                });

                seen.add(el);
            });
        } catch (e) {}
    }

    const tableDataRows = Array.from(document.querySelectorAll('table')).reduce((max, t) => {
        const rows = t.querySelectorAll('tr').length;
        return rows > max ? rows : max;
    }, 0);

    return {
        url:            location.href,
        title:          document.title,
        bodyTextLength: bodyText.length,
        interactiveElements: interactive,
        tableDataRows:  tableDataRows,
    };
}"""


class AgentNavigator:
    """
    Agentic browser controller that navigates search-form pages.

    Loop: snapshot DOM → ask AI "what next?" → execute action → repeat
    until AI says DONE (results visible) or FAILED (no path forward).
    """

    # ── Public API ──────────────────────────────────────────────────

    async def navigate_to_results(
        self,
        start_url: str,
        client_description: str,
        page: Optional[Page] = None,
    ) -> dict:
        """
        Agentic loop — thinks and acts like a human.

        Returns dict with:
            success, results_url, page, steps_taken, message,
            _browser, _owns_browser
        """
        steps_taken: List[dict] = []
        failed_actions: set = set()  # track (type, selector) that failed
        browser = None
        owns_browser = False

        try:
            if page is None:
                from playwright.async_api import async_playwright

                pw = await (async_playwright()).__aenter__()
                browser = await launch_stealth_browser(pw)
                context = await create_stealth_context(browser)
                page = await new_stealth_page(context)
                owns_browser = True

                log.info(f"AgentNavigator: navigating to {start_url}")
                await page.goto(
                    start_url, wait_until="domcontentloaded", timeout=30000
                )
                await page.wait_for_timeout(2000)

            for step_num in range(1, MAX_AGENT_STEPS + 1):
                snapshot = await self._get_dom_snapshot(page)

                action = await self._decide_next_action(
                    snapshot=snapshot,
                    client_description=client_description,
                    current_url=page.url,
                    steps_taken=steps_taken,
                    attempt=step_num,
                )

                action_type = action.get("type", "FAILED").upper()
                selector = action.get("selector", "")
                reasoning = action.get("reasoning", "")

                # Dedup: if AI repeats an action that already failed, skip it
                action_key = (action_type, selector)
                if action_key in failed_actions:
                    log.info(
                        f"AgentNavigator step {step_num}: "
                        f"SKIPPED duplicate failed action "
                        f"{action_type} {selector}"
                    )
                    steps_taken.append({
                        "step": step_num,
                        "type": "SKIPPED",
                        "selector": selector,
                        "value": action.get("value", ""),
                        "reasoning": f"Duplicate of previously failed: {action_type} {selector}",
                    })
                    continue

                log.info(
                    f"AgentNavigator step {step_num}: "
                    f"{action_type} — {reasoning}"
                )

                steps_taken.append({
                    "step": step_num,
                    "type": action_type,
                    "selector": selector,
                    "value": action.get("value", ""),
                    "reasoning": reasoning,
                })

                if action_type == "DONE":
                    log.info(
                        f"AgentNavigator: reached results at {page.url} "
                        f"after {step_num} steps"
                    )
                    return self._result(
                        True, page, steps_taken, browser, owns_browser,
                        f"Results reached after {step_num} steps",
                    )

                if action_type == "FAILED":
                    msg = (
                        f"AgentNavigator: gave up after "
                        f"{step_num} steps — {reasoning}"
                    )
                    log.warning(msg)
                    return self._result(
                        False, page, steps_taken, browser, owns_browser, msg,
                    )

                success = await self._execute_action(page, action)
                if not success:
                    failed_actions.add(action_key)

            msg = (
                f"AgentNavigator: exhausted {MAX_AGENT_STEPS} steps "
                "without reaching results"
            )
            log.warning(msg)
            return self._result(
                False, page, steps_taken, browser, owns_browser, msg,
            )

        except Exception as e:
            msg = f"AgentNavigator error: {e}"
            log.error(msg)
            return self._result(
                False, page, steps_taken, browser, owns_browser, msg,
            )

    @staticmethod
    def _result(success, page, steps, browser, owns, msg):
        return {
            "success": success,
            "results_url": page.url if page else "",
            "page": page,
            "steps_taken": steps,
            "message": msg,
            "_browser": browser,
            "_owns_browser": owns,
        }

    # ── Search-form detection — 100 % generic, zero keywords ───────

    @staticmethod
    def is_search_form_page(snapshot: dict, records_found: int) -> bool:
        """
        Generic detector: is this a search-form page with no data yet?

        Works on the structured JSON snapshot returned by _get_dom_snapshot.

        Returns True if ALL of these are true:
          1. records_found == 0  (no data extracted yet)
          2. bodyTextLength > 200  (page actually loaded)
          3. Page has ≥1 input OR select element (including ARIA combobox/listbox)
          4. Page has ≥1 button OR submit element

        Returns False if:
          - Page already has data records (records_found > 0 or data table detected)
          - Page is effectively empty (body text too short)
          - No interactive elements (inputs/selects/buttons) are present
        No hardcoded keywords. No site-specific logic.
        """
        if records_found > 0:
            return False

        if not isinstance(snapshot, dict):
            return False

        body_len = snapshot.get("bodyTextLength", 0)
        if body_len <= 200:
            return False

        elements = snapshot.get("interactiveElements", [])
        table_rows = snapshot.get("tableDataRows", 0)

        if not elements:
            log.info(
                f"AgentNavigator: search form check — body={body_len}, "
                f"inputs=False, buttons=False, tableRows={table_rows} "
                f"→ False (no interactive elements)"
            )
            return False

        has_input = False
        has_button = False

        for el in elements:
            tag = el.get("tag", "")
            el_type = el.get("type", "").lower()
            role = el.get("role", "").lower()

            if tag in ("input", "select", "textarea"):
                if el_type not in ("hidden", "checkbox", "radio", "submit", "button"):
                    has_input = True
            if role in ("combobox", "listbox", "searchbox"):
                has_input = True

            if tag == "button" or el_type in ("submit", "button"):
                has_button = True
            if role == "button":
                has_button = True

        # Large data table means this is a data page, not a search form
        if table_rows >= 10:
            log.info(
                f"AgentNavigator: search form check — "
                f"body={body_len}, inputs={has_input}, "
                f"buttons={has_button}, tableRows={table_rows} "
                f"→ False (data table found)"
            )
            return False

        is_form = has_input and has_button

        log.info(
            f"AgentNavigator: search form check — "
            f"body={body_len}, inputs={has_input}, "
            f"buttons={has_button}, tableRows={table_rows} "
            f"→ {is_form}"
        )
        return is_form

    # ── DOM snapshot with generic page-ready wait ──────────────────

    async def _get_dom_snapshot(self, page: Page) -> dict:
        """
        Wait for JS-rendered content, then collect ALL interactive
        elements as structured JSON.

        Generic wait + robustness (works on any JS-rendered site):
        1. Wait for networkidle
        2. If bodyText < 500 → wait 3s and re-check
        3. If still < 500 → wait 3s more
        4. Dismiss common overlays (cookie/modal/popups) once if found
        5. Take multi-pass snapshots to surface lazy content:
           - Pass 1: baseline
           - If <10 elements → scroll down/up and re-snapshot
           - If still <10 → wait 2s and final snapshot
        Use the snapshot with the most interactive elements.
        """
        # Step 1: wait for network to settle
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass

        # Step 2-3: generic body-text readiness loop
        for _ in range(2):
            try:
                body_len = await page.evaluate(
                    "(document.body && document.body.innerText || '').length"
                )
            except Exception:
                body_len = 0

            if body_len >= 500:
                break
            await page.wait_for_timeout(3000)

        overlay_dismissed = False

        # Step 4: generic overlay dismissal
        try:
            handle = await page.evaluate_handle(
                """() => {
                    const texts = [
                        'accept','close','got it','dismiss','ok','allow','continue','skip'
                    ];
                    const isVisible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 &&
                            style.visibility !== 'hidden' && style.display !== 'none';
                    };

                    const candidates = [];

                    // 1) Buttons with common dismissal text
                    const btnLike = document.querySelectorAll('button, [role="button"], a, div, span');
                    for (const el of btnLike) {
                        const txt = (el.innerText || '').trim().toLowerCase();
                        if (!txt) continue;
                        if (texts.some(t => txt.includes(t))) {
                            if (isVisible(el)) return el;
                        }
                    }

                    // 2) Dialog / modal containers and their first button
                    const dialog = document.querySelector(
                        '[role="dialog"], [class*="modal"], [class*="overlay"], [class*="popup"], [class*="cookie"], [class*="banner"], [class*="consent"], [class*="notification"]'
                    );
                    if (dialog && isVisible(dialog)) {
                        const btn = dialog.querySelector('button, [role="button"], [aria-label], [data-testid]');
                        if (btn && isVisible(btn)) return btn;
                        return dialog;
                    }

                    return null;
                }"""
            )
            el = handle.as_element()
            if el:
                try:
                    await el.click()
                    overlay_dismissed = True
                    await page.wait_for_timeout(1000)
                except Exception:
                    pass
        except Exception:
            pass

        async def take_snapshot() -> dict:
            try:
                result = await page.evaluate(_SNAPSHOT_JS)
                if isinstance(result, dict):
                    return result
            except Exception as e:  # noqa: BLE001
                log.warning(f"AgentNavigator: snapshot JS failed — {e}")
            return {
                "url": page.url,
                "title": "",
                "bodyTextLength": 0,
                "interactiveElements": [],
                "tableDataRows": 0,
            }

        best_snapshot: dict = {}
        best_count = -1
        passes_taken = 0

        def update_best(snap: dict):
            nonlocal best_snapshot, best_count, passes_taken
            count = len(snap.get("interactiveElements", []))
            passes_taken += 1
            log.info(f"AgentNavigator: Snapshot pass {passes_taken}: found {count} elements")
            if count > best_count:
                best_snapshot = snap
                best_count = count

        # Pass 1: baseline
        snap1 = await take_snapshot()
        update_best(snap1)

        # Pass 2: scroll down/up to trigger lazy mounts if few elements
        if best_count < 10:
            try:
                await page.evaluate("window.scrollBy({top: 300, behavior: 'smooth'})")
            except Exception:
                pass
            await page.wait_for_timeout(1500)
            try:
                await page.evaluate("window.scrollTo({top: 0, behavior: 'smooth'})")
            except Exception:
                pass
            await page.wait_for_timeout(1500)
            snap2 = await take_snapshot()
            update_best(snap2)

        # Pass 3: extra wait if still low element count
        if best_count < 10:
            await page.wait_for_timeout(2000)
            snap3 = await take_snapshot()
            update_best(snap3)

        # Attach metadata about passes and overlays
        best_snapshot.setdefault("_meta", {})
        best_snapshot["_meta"].update({
            "snapshotPassesTaken": passes_taken,
            "overlayDismissed": overlay_dismissed,
        })

        return best_snapshot

    # ── AI Brain: decide next action ───────────────────────────────

    async def _decide_next_action(
        self,
        snapshot: dict,
        client_description: str,
        current_url: str,
        steps_taken: list,
        attempt: int,
    ) -> dict:
        """
        Sends structured element JSON + client description to AI.
        AI returns exactly one action.
        """
        elements_json = json.dumps(
            snapshot.get("interactiveElements", []),
            indent=1,
            ensure_ascii=False,
        )
        # Cap element list to stay within token limits
        if len(elements_json) > 6000:
            elements_json = elements_json[:6000] + "\n...(truncated)"

        if steps_taken:
            steps_summary = "\n".join(
                f"  Step {s['step']}: {s['type']} "
                f"selector={s.get('selector', '')} "
                f"value={s.get('value', '')} "
                f"— {s.get('reasoning', '')}"
                for s in steps_taken
            )
        else:
            steps_summary = "  (none yet — first action)"

        system_prompt = (
            "You are controlling a web browser to help a client find "
            "data they need. You receive a structured JSON list of every "
            "interactive element on the current page (tag, type, role, "
            "text<=50, placeholder, aria-label, classNames). Decide the "
            "single best next action. Base all selectors ONLY on this JSON."
        )

        user_prompt = f"""CLIENT WANTS:
{client_description}

CURRENT PAGE:
  URL: {current_url}
  Title: {snapshot.get('title', '')}
  Body text length: {snapshot.get('bodyTextLength', 0)} chars

ALL INTERACTIVE ELEMENTS ON THIS PAGE (structured JSON):
{elements_json}

ACTIONS ALREADY TAKEN:
{steps_summary}

YOUR JOB:
Look at the interactive elements. Understand what this page offers.
Decide the single best next action to reach the data the client wants.

Ask yourself:
- Is the data already visible? (body text > 5000 AND no search form) → DONE
- Is there an input field I should type into? → TYPE
- Is there a dropdown I should select from? → SELECT
- Is there a button or tab I should click? → CLICK
- Should I scroll to reveal more content? → SCROLL
- Should I wait for content to load? → WAIT
- No path forward? → FAILED

Return exactly ONE action as JSON:
{{
  "type": "CLICK" | "TYPE" | "SELECT" | "SCROLL" | "WAIT" | "DONE" | "FAILED",
  "selector": "CSS selector to target the element. Build it from the element's id, name, class, placeholder, or aria-label that you see in the JSON. Examples: #someId, input[placeholder='...'], button[aria-label='...'], [class*='search']",
  "value": "text to type or option to select (for TYPE and SELECT only — derive from what the client wants)",
  "reasoning": "what you see on the page and why this action",
  "results_visible": false
}}

RULES:
- Base selectors ONLY on element data you see in the JSON above
- Never guess or hallucinate selectors
- Never repeat an action from ACTIONS ALREADY TAKEN
- Ignore cookie banners, login popups, ads
- For input fields: look at placeholder and aria-label to understand what to type
- After filling relevant fields → click the search/submit/find button
- After results load → return DONE with results_visible=true
- For CLICK: you can also use text= prefix like "text=Search"
- For TYPE: target the input field's CSS selector"""

        # Retry with backoff if all providers are rate-limited
        for retry in range(3):
            response = await ai_provider.complete(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                json_mode=True,
            )

            text = response.get("text", "{}")
            provider = response.get("provider", "none")

            if provider != "none":
                log.info(f"AgentNavigator AI response from {provider}")
                return self._parse_action(text)

            if retry < 2:
                wait = 60 * (retry + 1)
                log.info(
                    f"AgentNavigator: all providers busy, "
                    f"waiting {wait}s for rate limit reset "
                    f"(retry {retry + 1}/2)"
                )
                await asyncio.sleep(wait)

        log.warning("AgentNavigator: all providers failed after retries")
        return {"type": "FAILED", "reasoning": "All AI providers exhausted"}

    # ── Action execution ───────────────────────────────────────────

    async def _execute_action(self, page: Page, action: dict) -> bool:
        """Execute a browser action with human-like delays.
        Returns True if the action succeeded, False if it failed."""
        action_type = action.get("type", "").upper()
        selector = action.get("selector", "")
        value = action.get("value", "")

        try:
            if action_type == "CLICK":
                return await self._do_click(page, selector)
            elif action_type == "TYPE":
                return await self._do_type(page, selector, value)
            elif action_type == "SELECT":
                return await self._do_select(page, selector, value)
            elif action_type == "SCROLL":
                await self._do_scroll(page)
                return True
            elif action_type == "WAIT":
                await page.wait_for_timeout(2000)
                return True
        except Exception as e:
            log.warning(f"AgentNavigator: action {action_type} failed — {e}")
        return False

    async def _do_click(self, page: Page, selector: str) -> bool:
        el = await self._resolve_element(page, selector)
        if not el:
            log.warning(f"AgentNavigator: CLICK target not found: {selector}")
            return False
        await el.scroll_into_view_if_needed()
        await page.wait_for_timeout(random.randint(300, 700))
        await el.click()
        await page.wait_for_timeout(random.randint(1000, 2000))
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
        return True

    async def _do_type(self, page: Page, selector: str, value: str) -> bool:
        el = await self._resolve_element(page, selector)
        if not el:
            log.warning(f"AgentNavigator: TYPE target not found: {selector}")
            return False
        await el.scroll_into_view_if_needed()
        await page.wait_for_timeout(random.randint(200, 400))
        await el.click()
        await page.wait_for_timeout(200)
        await el.fill("")
        await page.wait_for_timeout(100)
        for ch in value:
            await el.type(ch, delay=random.randint(50, 120))
        await page.wait_for_timeout(1500)
        await self._click_autocomplete(page, value)
        return True

    async def _click_autocomplete(self, page: Page, value: str) -> None:
        """Click first matching autocomplete suggestion — fully generic.

        Uses JavaScript to find ANY small visible element whose text
        contains the typed value, regardless of the site's CSS class names.
        """
        val = value.lower().strip()

        # JS: find clickable leaf elements whose text matches the value
        candidates = await page.evaluate("""(val) => {
            const found = [];
            const all = document.querySelectorAll('*');
            for (const el of all) {
                const text = (el.innerText || '').trim();
                if (text.length < 3 || text.length > 120) continue;
                if (!text.toLowerCase().includes(val)) continue;
                const rect = el.getBoundingClientRect();
                if (rect.width === 0 || rect.height === 0) continue;
                // Skip huge container elements — we want leaf items
                if (el.children.length > 5) continue;
                // Skip elements in footer/header/nav
                const tag = el.tagName.toLowerCase();
                const inFooter = el.closest('footer') || el.closest('[class*="footer"]');
                const inNav = el.closest('nav') || el.closest('[class*="nav"]:not([class*="navig"])');
                if (inFooter || inNav) continue;
                found.push({
                    tag: tag,
                    text: text.substring(0, 80),
                    cls: (el.className ? String(el.className) : '').substring(0, 60),
                    idx: found.length,
                    children: el.children.length,
                    area: rect.width * rect.height,
                });
            }
            // Sort: prefer smaller elements (leaf nodes / suggestion items)
            found.sort((a, b) => a.area - b.area);
            return found.slice(0, 10);
        }""", val)

        if not candidates:
            log.info(f"AgentNavigator: no autocomplete found for '{value}'")
            return

        log.info(
            f"AgentNavigator: autocomplete candidates: "
            f"{[c['text'][:30] for c in candidates[:5]]}"
        )

        # Click the best candidate using get_by_text
        for cand in candidates:
            cand_text = cand["text"]
            try:
                loc = page.get_by_text(cand_text, exact=True).first
                if await loc.count() > 0 and await loc.is_visible():
                    await loc.click()
                    log.info(
                        f"AgentNavigator: autocomplete clicked '{cand_text}'"
                    )
                    await page.wait_for_timeout(800)
                    return
            except Exception:
                continue

        # Last resort: click first candidate by its class
        first = candidates[0]
        try:
            sel = f"{first['tag']}.{first['cls'].split()[0]}" if first["cls"] else first["tag"]
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                await el.click()
                log.info(
                    f"AgentNavigator: autocomplete fallback '{first['text']}'"
                )
                await page.wait_for_timeout(800)
                return
        except Exception:
            pass
        log.info(f"AgentNavigator: autocomplete click failed for '{value}'")

    async def _do_select(self, page: Page, selector: str, value: str) -> bool:
        el = await self._resolve_element(page, selector)
        if not el:
            log.warning(f"AgentNavigator: SELECT target not found: {selector}")
            return False
        tag = await el.evaluate("el => el.tagName.toLowerCase()")
        if tag == "select":
            try:
                await el.select_option(label=value)
                await page.wait_for_timeout(500)
                return
            except Exception:
                pass
            try:
                await el.select_option(value=value)
                await page.wait_for_timeout(500)
                return
            except Exception:
                pass
        await el.click()
        await page.wait_for_timeout(800)
        val = value.lower().strip()
        for opt_sel in ["li", "[role='option']", "[class*='option']"]:
            try:
                options = await page.query_selector_all(opt_sel)
                for opt in options:
                    text = (await opt.inner_text()).strip().lower()
                    if val in text or text in val:
                        if await opt.is_visible():
                            await opt.click()
                            log.info(
                                f"AgentNavigator: selected '{text}'"
                            )
                            await page.wait_for_timeout(500)
                            return
            except Exception:
                continue

    async def _do_scroll(self, page: Page) -> None:
        await page.evaluate(
            "window.scrollBy({top: window.innerHeight, behavior: 'smooth'})"
        )
        await page.wait_for_timeout(800)

    # ── Element resolution ─────────────────────────────────────────

    async def _resolve_element(self, page: Page, selector: str):
        """Resolve selector → ElementHandle. Tries CSS, text=, getByText,
        getByPlaceholder, aria-label — fully generic."""
        if not selector:
            return None

        if selector.startswith("text="):
            try:
                loc = page.locator(selector).first
                if await loc.count() > 0 and await loc.is_visible():
                    return await loc.element_handle()
            except Exception:
                pass

        try:
            el = await page.query_selector(selector)
            if el and await el.is_visible():
                return el
        except Exception:
            pass

        try:
            loc = page.get_by_text(selector, exact=False).first
            if await loc.count() > 0 and await loc.is_visible():
                return await loc.element_handle()
        except Exception:
            pass

        try:
            loc = page.get_by_placeholder(selector, exact=False).first
            if await loc.count() > 0 and await loc.is_visible():
                return await loc.element_handle()
        except Exception:
            pass

        try:
            el = await page.query_selector(f'[aria-label*="{selector}" i]')
            if el and await el.is_visible():
                return el
        except Exception:
            pass

        log.warning(f"AgentNavigator: could not resolve: {selector}")
        return None

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_action(text: str) -> dict:
        cleaned = re.sub(r"```(?:json)?\s*", "", text)
        cleaned = re.sub(r"```\s*$", "", cleaned)
        cleaned = cleaned.strip()
        try:
            action = json.loads(cleaned)
            if isinstance(action, dict) and "type" in action:
                return action
        except (json.JSONDecodeError, TypeError):
            pass
        match = re.search(
            r'\{[^{}]*"type"\s*:\s*"[^"]+?"[^{}]*\}', cleaned, re.DOTALL
        )
        if match:
            try:
                return json.loads(match.group())
            except (json.JSONDecodeError, TypeError):
                pass
        log.warning(
            f"AgentNavigator: could not parse AI action: {text[:200]}"
        )
        return {"type": "FAILED", "reasoning": "Could not parse AI response"}
