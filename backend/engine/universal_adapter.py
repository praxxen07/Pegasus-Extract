from __future__ import annotations

import logging
import re
from typing import Any, Dict, List
from urllib.parse import urljoin, urlparse

from playwright.async_api import Page

log = logging.getLogger("PegasusExtract")


class UniversalAdapter:
    """
    Works for ANY website using the AI extraction plan.

    Contract:
    - AI decides WHAT to scrape (selectors, pagination, fields).
    - This adapter executes purely with Playwright and the plan.
    - No dynamic code generation / no exec().
    """

    def __init__(self, plan: dict):
        self.plan = plan
        self.target_url = plan.get("target_url", "")
        self.max_pages = plan.get("crawler_config", {}).get("max_pages", 10)
        self.container_sel = (
            plan.get("extraction_config", {}).get("container_selector", "")
        )
        self.fields = plan.get("extraction_config", {}).get("fields", {})
        self.pagination = (
            plan.get("extraction_config", {}).get("pagination", {})
            or plan.get("pagination", {})
        )
        self.browser_config = plan.get("browser_config", {})
        self._field_names = list(self.fields.keys())

    def get_input_records(self, config=None) -> List[dict]:
        """
        Generate seed URLs.

        Dynamic pagination will be discovered during crawl via `get_next_url`.
        """
        ptype = self.pagination.get("type", "none")
        seed = self.target_url

        if ptype in ("none", "next_button", "infinite_scroll"):
            return [{"url": seed, "page_num": 1}]

        if ptype in ("url_increment", "page_numbers"):
            template = self.pagination.get("url_template", "")
            first = int(self.pagination.get("first_page", 1))
            if template:
                records: List[dict] = []
                for i in range(first, first + self.max_pages):
                    url = template.replace("{page_num}", str(i))
                    records.append({"url": url, "page_num": i})
                return records

        return [{"url": seed, "page_num": 1}]

    async def _load_page_properly(self, page: Page, url: str) -> None:
        """
        Smart loading for ALL website types:
        - Static HTML
        - JS-heavy SPA pages (React/Vue/Angular)
        - Lazy-loading sites
        - Pages with basic anti-bot measures
        """
        await page.set_extra_http_headers(
            {
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Cache-Control": "no-cache",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )

        try:
            await page.goto(url, timeout=45000, wait_until="domcontentloaded")
            # Wait for JS to settle
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
        except Exception as e:
            log.warning(f"Navigation warning {url}: {e}")

        # Wait for container (proves JS rendered at least once)
        if self.container_sel:
            try:
                await page.wait_for_selector(self.container_sel, timeout=8000)
            except Exception:
                log.warning(
                    f"Container '{self.container_sel}' not visible after wait"
                )

        # Optional infinite-scroll / lazy loading
        if self.browser_config.get("scroll_to_load"):
            await self._scroll_fully(page)

        # Small buffer for late client rendering
        await page.wait_for_timeout(1500)

    async def _scroll_fully(self, page: Page) -> None:
        """Scroll to bottom to trigger lazy loading."""
        prev = 0
        for _ in range(10):
            h = await page.evaluate("document.body.scrollHeight")
            if h == prev:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(800)
            prev = h

    async def _find_containers(self, page: Page) -> List[Any]:
        """
        Find data containers using multiple strategies.
        NEVER gives up — always tries fallbacks.
        """
        # Strategy 1: Plan's container selector
        if self.container_sel:
            els = await page.query_selector_all(self.container_sel)
            if len(els) >= 2:
                refined = await self._refine_containers_by_fields(els)
                if refined and len(refined) >= 2:
                    log.info(
                        f"Container '{self.container_sel}': {len(refined)} found (refined)"
                    )
                    return refined
                log.info(
                    f"Container '{self.container_sel}': {len(els)} found"
                )
                return els

        # Strategy 2: Common patterns across site types
        patterns = [
            # E-commerce
            ".product",
            ".product-item",
            ".product-card",
            ".product-tile",
            ".product-thumb",
            "[class*='product']",
            # Generic cards/items
            ".item",
            ".card",
            ".listing-item",
            ".result",
            ".entry",
            ".post",
            ".article-item",
            "[class*='item']",
            "[class*='card']",
            # Bootstrap grids (very common)
            "li.col-sm-4",
            "li.col-md-4",
            "li.col-lg-4",
            ".col-sm-4",
            ".col-md-4",
            "div.col-sm-4",
            "div.col-md-4",
            # Thumbnails (often repeated)
            ".thumbnail",
            ".thumb",
            # Tables (rows)
            "tbody tr",
            "tr[class]",
            # Articles/blog
            "article",
            ".post",
            ".blog-post",
            # Quotes/reviews
            ".quote",
            ".review",
            ".testimonial",
            # News
            ".story",
            ".news-item",
            ".headline",
        ]

        best_els: List[Any] = []
        best_sel = ""
        for pat in patterns:
            try:
                els = await page.query_selector_all(pat)
                if len(els) > len(best_els):
                    # Avoid choosing tiny inline elements (common false positives,
                    # e.g. tag chips) as "containers".
                    try:
                        tag0 = await els[0].evaluate("el => el.tagName")
                        if tag0 in {"SPAN", "A"}:
                            continue
                    except Exception:
                        # If tag detection fails, still allow the candidate.
                        pass
                    best_els = els
                    best_sel = pat
            except Exception:
                continue

        if len(best_els) >= 2:
            refined = await self._refine_containers_by_fields(best_els)
            if refined and len(refined) >= 2:
                log.info(
                    f"Fallback container '{best_sel}': {len(refined)} found (refined)"
                )
                return refined
            log.info(
                f"Fallback container '{best_sel}': {len(best_els)} found"
            )
            return best_els

        # Strategy 3: Most repeated elements
        return await self._find_repeated_elements(page)

    async def _refine_containers_by_fields(
        self,
        containers: List[Any],
        max_selectors: int = 4,
    ) -> List[Any]:
        """
        Refine candidate "containers" to avoid overly-broad matches.

        Generic approach:
        - Count how many of the field selectors match within each container.
        - Keep only containers with the highest match count.

        Example:
        - books.toscrape: keeps `article.product_pod` (matches both price+rating)
          and drops nested `div.product_price` (matches price only).
        """
        selectors: List[str] = []
        for _, fconfig in self.fields.items():
            sel = (fconfig or {}).get("selector", "")
            if sel:
                selectors.append(sel)
        selectors = selectors[:max_selectors]
        if not selectors or not containers:
            return containers

        counts: List[int] = []
        best = 0
        for c in containers:
            match_count = 0
            for sel in selectors:
                try:
                    found = await c.query_selector(sel)
                    if found:
                        match_count += 1
                except Exception:
                    continue
            counts.append(match_count)
            if match_count > best:
                best = match_count

        if best <= 0:
            return containers

        refined = [c for c, cnt in zip(containers, counts) if cnt == best]
        if (
            refined
            and best > 1
            and len(containers) >= 10
            and len(refined) < (len(containers) * 0.5)
        ):
            # If we kept too few containers, relax threshold to reduce
            # over-filtering when selectors are imperfect.
            relaxed = [c for c, cnt in zip(containers, counts) if cnt >= best - 1]
            if len(relaxed) >= 2:
                return relaxed

        return refined if refined else containers

    async def _find_repeated_elements(self, page: Page) -> List[Any]:
        """Find elements that repeat 3-200 times."""
        classes = await page.evaluate(
            """
            () => {
                const counts = {};
                document.querySelectorAll('*').forEach(el => {
                    const cls = el.className;
                    if (cls && typeof cls === 'string') {
                        const first = cls.trim().split(/\\s+/)[0];
                        if (first && first.length > 1) {
                            counts[first] = (counts[first] || 0) + 1;
                        }
                    }
                });
                return Object.entries(counts)
                    .filter(([k,v]) => v>=3 && v<=200)
                    .sort((a,b)=>b[1]-a[1])
                    .slice(0,10)
                    .map(([cls])=>cls);
            }
            """
        )

        for cls in classes or []:
            try:
                els = await page.query_selector_all(f".{cls}")
                if len(els) >= 3:
                    try:
                        tag0 = await els[0].evaluate("el => el.tagName")
                        if tag0 in {"SPAN", "A"}:
                            continue
                    except Exception:
                        pass
                    log.info(f"Repeated element '.{cls}': {len(els)} found")
                    refined = await self._refine_containers_by_fields(els)
                    return refined if refined and len(refined) >= 2 else els
            except Exception:
                continue

        return []

    @staticmethod
    def _relax_selector(sel: str) -> str:
        """Drop tag prefix from CSS selectors: h1.class -> .class"""
        relaxed = re.sub(r'^[a-z][a-z0-9]*(?=\.)', '', sel)
        return relaxed if relaxed != sel else ""

    async def _extract_value(
        self,
        context,
        selector: str,
        attribute: str,
        backup: str = "",
        multiple: bool = False,
    ) -> str:
        """
        Extract field value correctly for ALL attribute types.
        Handles text, href/src, class, title/alt, and data-* attributes.
        """
        selectors_to_try: list[str] = []
        for s in [selector, backup]:
            if not s:
                continue
            selectors_to_try.append(s)
            relaxed = self._relax_selector(s)
            if relaxed:
                selectors_to_try.append(relaxed)
        for sel in selectors_to_try:
            if not sel:
                continue
            try:
                if attribute in ("text", "innerText", "textContent"):
                    if multiple:
                        els = await context.query_selector_all(sel)
                        if els:
                            texts: List[str] = []
                            for el in els[:10]:
                                t = await el.inner_text()
                                cleaned = self.clean_value(t)
                                if cleaned:
                                    texts.append(cleaned)
                            if texts:
                                return " | ".join(texts) if len(texts) > 1 else texts[0]
                    else:
                        el = await context.query_selector(sel)
                        if el:
                            t = await el.inner_text()
                            cleaned = self.clean_value(t)
                            if cleaned:
                                return cleaned
                else:
                    el = await context.query_selector(sel)
                    if not el:
                        continue

                    if attribute in ("href", "src"):
                        val = await el.get_attribute(attribute)
                        if val:
                            if val.startswith("http"):
                                return val
                            return urljoin(self.target_url, val)
                    if attribute == "class":
                        val = await el.get_attribute("class")
                        return val.strip() if val else ""

                    val = await el.get_attribute(attribute)
                    if val:
                        return val.strip()

                    # Fallback to text
                    val = await el.inner_text()
                    return self.clean_value(val) if val else ""

            except Exception as e:
                log.debug(f"Extract failed ({sel}): {e}")
                continue

        return ""

    @staticmethod
    def clean_value(val: Any) -> str:
        """
        Clean noisy multiline text (price + stock + buttons).
        Returns the first meaningful line, stripped.
        """
        if val is None:
            return ""
        s = str(val)
        if not s:
            return ""
        lines = [l.strip() for l in s.split("\n") if l.strip()]
        if lines:
            return lines[0]
        return s.strip()

    async def _extract_value_from_adjacent_siblings(
        self,
        container,
        selector: str,
        attribute: str,
        backup: str = "",
        multiple: bool = False,
        max_steps: int = 4,
    ) -> str:
        """
        When fields are not inside the container (common in table UIs),
        search next sibling elements and retry the same extraction.
        """
        current = container
        for _ in range(max_steps):
            next_handle = await current.evaluate_handle(
                "el => el.nextElementSibling"
            )
            next_el = next_handle.as_element()  # type: ignore[union-attr]
            if not next_el:
                break

            val = await self._extract_value(
                next_el,
                selector,
                attribute,
                backup,
                multiple=multiple,
            )
            if val:
                return val

            current = next_el
        return ""

    async def _build_content_inventory(self, container) -> List[Dict[str, Any]]:
        """
        Read the container DOM and collect all meaningful content items.
        Each item is categorised by its structural role (heading, paragraph,
        metric, link) — purely based on the HTML element it came from.
        Fully generic: works for any website.
        """
        items: List[Dict[str, Any]] = []
        seen_text: set = set()

        # --- Headings (structurally prominent = primary identifiers) ---
        for tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            try:
                for el in await container.query_selector_all(tag):
                    txt = self.clean_value(await el.inner_text())
                    if not txt or len(txt) <= 2 or txt in seen_text:
                        continue
                    seen_text.add(txt)
                    href = ""
                    try:
                        a = await el.query_selector("a[href]")
                        if a:
                            href = await a.get_attribute("href") or ""
                    except Exception:
                        pass
                    items.append({
                        "cat": "heading", "text": txt, "href": href,
                        "has_digits": any(c.isdigit() for c in txt),
                        "length": len(txt),
                    })
            except Exception:
                continue

        # --- Paragraphs (long-form = descriptive content) ---
        try:
            for el in await container.query_selector_all("p"):
                txt = self.clean_value(await el.inner_text())
                if not txt or len(txt) <= 15 or txt in seen_text:
                    continue
                seen_text.add(txt)
                items.append({
                    "cat": "paragraph", "text": txt, "href": "",
                    "has_digits": any(c.isdigit() for c in txt),
                    "length": len(txt),
                })
        except Exception:
            pass

        # --- Metrics (short text containing digits = numeric data) ---
        try:
            for el in await container.query_selector_all("span, td, time"):
                txt = (await el.inner_text() or "").strip()
                if (not txt or len(txt) < 2 or len(txt) > 40
                        or txt in seen_text
                        or not any(c.isdigit() for c in txt)):
                    continue
                # Skip if this text is a substring of an already-captured item
                if any(txt in it["text"] for it in items):
                    continue
                seen_text.add(txt)
                items.append({
                    "cat": "metric", "text": txt, "href": "",
                    "has_digits": True, "length": len(txt),
                })
        except Exception:
            pass

        # --- Content links (anchors with real content, skip UI buttons) ---
        try:
            for a in await container.query_selector_all("a[href]"):
                href = await a.get_attribute("href") or ""
                if not href:
                    continue
                # Skip obvious non-content links
                if any(s in href for s in (
                    "javascript:", "/login", "/signup", "/session",
                    "vote?", "#", "mailto:",
                )):
                    continue
                txt = (await a.inner_text() or "").strip()
                if not txt or len(txt) < 2 or txt in seen_text:
                    continue
                seen_text.add(txt)
                items.append({
                    "cat": "link", "text": txt, "href": href,
                    "has_digits": any(c.isdigit() for c in txt),
                    "length": len(txt),
                })
        except Exception:
            pass

        return items

    @staticmethod
    def _score_content_fit(
        field_name: str,
        attribute: str,
        item: Dict[str, Any],
        transform: str = "",
    ) -> float:
        """
        Score how well a content item fits a field.
        Uses the content's structural category, intrinsic characteristics,
        and the AI plan's own transform hint to match content to fields.
        Fully generic — no site-specific logic.
        """
        score = 0.0
        cat = item["cat"]
        text = item["text"]
        tlen = item["length"]
        has_digits = item["has_digits"]
        tfm = (transform or "").lower()

        # Whether the plan itself says this field expects a number
        wants_number = tfm in (
            "to_number", "number", "int", "float", "numeric",
            "to_int", "to_float", "integer", "parse_number",
        )

        # ── Attribute-level matching ──────────────────────────────
        if attribute in ("href", "src"):
            return 10.0 if item["href"] else -10.0

        # ── Transform-guided scoring ─────────────────────────────
        if wants_number:
            # Field expects numeric content → strongly prefer metrics
            if cat == "metric" and has_digits:
                score += 12
            elif has_digits:
                score += 6
            else:
                score -= 5          # penalise non-numeric for numeric field
            return score

        # ── Structural category signals (non-numeric fields) ──────
        # Paragraphs carry descriptive prose → best for text fields
        if cat == "paragraph":
            score += 6
            if tlen > 40:
                score += 3          # longer = more descriptive
        # Headings are structurally prominent → good for labels
        elif cat == "heading":
            score += 5
            if tlen < 80:
                score += 2
        # Metrics are numeric → mild fit for generic text fields
        elif cat == "metric":
            score += 2
        # Link text can fill any role
        elif cat == "link":
            score += 2
            if " " in text:
                score += 1

        return score

    async def _smart_extract_from_dom(
        self,
        container,
        unmatched_fields: List[tuple],  # (fname, attr, transform)
        existing_values: List[str] | None = None,
    ) -> Dict[str, str]:
        """
        Intelligent, fully generic DOM content extraction.

        1. Reads the container → builds a categorised content inventory.
        2. For each unmatched field, scores every unused content item.
        3. Assigns the highest-scoring unused item to each field,
           guaranteeing that different fields get DIFFERENT content.
        4. Skips content that duplicates values already assigned in
           earlier passes (selector hits), ensuring cross-pass diversity.

        No website-specific or field-name-specific logic.
        """
        items = await self._build_content_inventory(container)
        if not items:
            return {}

        # Values already assigned by selectors — avoid duplicating them
        taken: set = set()
        if existing_values:
            for ev in existing_values:
                if ev and ev.strip():
                    taken.add(ev.strip())

        result: Dict[str, str] = {}
        used_indices: set = set()

        for field_tuple in unmatched_fields:
            fname, attr = field_tuple[0], field_tuple[1]
            tfm = field_tuple[2] if len(field_tuple) > 2 else ""
            best_idx = -1
            best_score = -999.0

            for i, item in enumerate(items):
                if i in used_indices:
                    continue
                sc = self._score_content_fit(fname, attr, item, transform=tfm)
                # Heavily penalise items whose text duplicates an
                # already-assigned value (from selectors or earlier fields)
                if item["text"] in taken:
                    sc -= 20
                if sc > best_score:
                    best_score = sc
                    best_idx = i

            if best_idx < 0 or best_score <= 0:
                continue

            chosen = items[best_idx]
            used_indices.add(best_idx)

            if attr in ("href", "src") and chosen["href"]:
                h = chosen["href"]
                val = h if h.startswith("http") else urljoin(self.target_url, h)
            else:
                val = chosen["text"]

            result[fname] = val
            taken.add(val.strip())

        return result

    async def _extract_anchor_value(self, context, attribute: str) -> str:
        """
        Generic fallback for title/link extraction:
        pick the best non-vote <a> within the context.
        Prefers anchors with multi-word text (content) over single-word (buttons).
        """
        anchors = await context.query_selector_all("a[href]")
        fallback_href = ""
        fallback_txt = ""
        for a in anchors:
            href = await a.get_attribute("href") or ""
            if "vote?id=" in href:
                continue

            txt = await a.inner_text()
            if not txt or not txt.strip():
                continue
            cleaned = txt.strip()

            # Prefer anchors with multi-word text (likely content, not buttons)
            if " " in cleaned or len(cleaned) > 15:
                if attribute in ("href", "src"):
                    if href.startswith("http"):
                        return href
                    return urljoin(self.target_url, href)
                if attribute in ("text", "innerText", "textContent"):
                    return self.clean_value(cleaned)

            # Remember first single-word anchor as fallback
            if not fallback_txt:
                fallback_txt = cleaned
                fallback_href = href

        # No multi-word anchor found, use fallback
        if fallback_txt:
            if attribute in ("href", "src"):
                if fallback_href.startswith("http"):
                    return fallback_href
                return urljoin(self.target_url, fallback_href)
            if attribute in ("text", "innerText", "textContent"):
                return self.clean_value(fallback_txt)

        return ""

    async def extract_page(
        self,
        page: Page,
        record: dict,
        config=None,
    ) -> List[Dict[str, Any]]:
        """Main extraction. Called for each URL."""
        url = record.get("url", self.target_url)
        results: List[Dict[str, Any]] = []

        try:
            await self._load_page_properly(page, url)
            containers = await self._find_containers(page)

            if not containers:
                log.warning(f"No containers on {url}")
                return []

            log.info(f"Extracting {len(containers)} items from {url}")

            for container in containers:
                item: Dict[str, Any] = {}
                unmatched: List[tuple] = []

                # ── Pass 1: try AI selectors + adjacent-sibling fallback ──
                for fname, fconfig in self.fields.items():
                    sel = fconfig.get("selector", "")
                    attr = fconfig.get("attribute", "innerText")
                    backup = fconfig.get("backup_selector", "")
                    multiple = bool(fconfig.get("multiple", False))
                    field_key = str(fname).lower()
                    is_points_field = ("point" in field_key) or (
                        "score" in field_key
                    )
                    is_relative = fconfig.get("is_relative", None)

                    v = ""
                    if is_relative is None:
                        v = await self._extract_value(
                            container, sel, attr, backup,
                            multiple=multiple,
                        )
                        if not v and is_points_field:
                            v = await self._extract_value_from_adjacent_siblings(
                                container, sel, attr, backup,
                                multiple=multiple,
                            )
                    else:
                        ctx = container if is_relative else page
                        v = await self._extract_value(
                            ctx, sel, attr, backup,
                            multiple=multiple,
                        )
                        if not v and is_points_field:
                            v = await self._extract_value_from_adjacent_siblings(
                                container, sel, attr, backup,
                                multiple=multiple,
                            )

                    transform = fconfig.get("transform", "")

                    if v:
                        item[fname] = v
                    else:
                        item[fname] = ""
                        unmatched.append((fname, attr, transform))

                # ── Pass 2: smart DOM extraction for ALL unmatched fields ─
                #     Reads the container once, assigns DIFFERENT content
                #     to each field via scoring — fully generic.
                if unmatched:
                    already = [v for v in item.values() if isinstance(v, str) and v.strip()]
                    dom_values = await self._smart_extract_from_dom(
                        container, unmatched, existing_values=already,
                    )
                    for fname, val in dom_values.items():
                        if val:
                            item[fname] = val

                    # Pass 3: anchor fallback — true last resort
                    for field_t in unmatched:
                        fname, attr = field_t[0], field_t[1]
                        if item.get(fname):
                            continue
                        fkey = str(fname).lower()
                        is_pts = ("point" in fkey) or ("score" in fkey)
                        if not is_pts:
                            v = await self._extract_anchor_value(
                                container, attr,
                            )
                            if v:
                                item[fname] = v

                if any(v.strip() for v in item.values() if isinstance(v, str) and v):
                    results.append(item)

            log.info(f"Got {len(results)} records from {url}")
        except Exception as e:
            log.error(f"extract_page failed {url}: {e}")

        return results

    async def get_next_url(self, page: Page, current_page: int) -> str:
        """Detect next page URL for ALL pagination types."""
        ptype = self.pagination.get("type", "none")

        # If pagination is unspecified (or explicitly "none"), we still try
        # generic next-button selectors. Many sites follow common markup.
        if ptype in ("url_increment", "page_numbers"):
            template = self.pagination.get("url_template", "")
            if template:
                return template.replace("{page_num}", str(current_page + 1))
            return ""

        if ptype == "none":
            ptype = "next_button"

        if ptype in ("next_button", "next_link"):
            selectors = [
                self.pagination.get("selector", ""),
                self.pagination.get("next_selector", ""),
                "a[rel='next']",
                ".next a",
                "li.next a",
                ".next > a",
                "a.next",
                ".pagination .next a",
                "a:has-text('Next')",
                "a:has-text('next')",
                "[aria-label='Next page']",
                "[aria-label='Next']",
            ]
            for sel in selectors:
                if not sel:
                    continue
                try:
                    el = await page.query_selector(sel)
                    if not el:
                        continue
                    href = await el.get_attribute("href")
                    if href:
                        if href.startswith("http"):
                            return href
                        return urljoin(page.url, href)
                except Exception:
                    continue

        return ""

    def csv_columns(self) -> list:
        return self._field_names

    def to_csv_rows(self, record: dict) -> list:
        return [record]

    @property
    def site_name(self):
        return urlparse(self.target_url).netloc

