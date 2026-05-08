"""Generic XHR/API response interceptor for JS-rendered websites."""

import asyncio
import json
import logging
import re
from typing import Any, Optional

from playwright.async_api import Page, Response, async_playwright

from core.ai_provider import ai_provider
from engine.stealth_browser import (
    create_stealth_context,
    launch_stealth_browser,
    new_stealth_page,
)

log = logging.getLogger("PegasusExtract")


class XHRInterceptor:
    """Capture and extract listing data from network JSON responses."""

    _API_KEYWORDS = (
        "api",
        "search",
        "listing",
        "property",
        "results",
        "data",
        "query",
        "fetch",
        "graphql",
        "ajax",
        "xhr",
        "endpoint",
        "service",
    )

    def __init__(self, max_candidates: int = 20):
        self.max_candidates = max_candidates
        self.candidates: list[dict] = []
        self._seen_signatures: set[str] = set()

    async def extract(
        self,
        url: str,
        client_description: str,
        target_fields: list[str],
        session: Optional[dict] = None,
    ) -> list[dict]:
        """Run a full interception pass and return flat records."""
        self.candidates = []
        self._seen_signatures = set()

        async with async_playwright() as p:
            browser = await launch_stealth_browser(p)
            context = await create_stealth_context(browser)

            if session and session.get("cookies"):
                try:
                    await context.add_cookies(session["cookies"])
                except Exception as e:
                    log.warning(f"XHR: failed to add bootstrapped cookies: {e}")

            page = await new_stealth_page(context)

            try:
                self._register_listener(page)
                log.info("XHR: page booted, capturing API responses")

                await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass

                await self._trigger_lazy_requests(page)
                await page.wait_for_timeout(2500)

                log.info(f"XHR: captured {len(self.candidates)} network responses")
                if not self.candidates:
                    return []

                selection = await self._identify_data_response(
                    self.candidates,
                    client_description,
                    target_fields,
                )
                if not selection:
                    return []

                idx = selection.get("selected_index")
                if not isinstance(idx, int) or idx < 0 or idx >= len(self.candidates):
                    log.warning(
                        f"XHR: AI returned invalid index {idx!r} "
                        f"(have {len(self.candidates)} candidates) — auto-selecting"
                    )
                    # Fallback: pick candidate with the most records
                    best_idx, best_count = -1, 0
                    for ci, cand in enumerate(self.candidates):
                        recs = self._resolve_records(cand["body"], "")
                        if len(recs) > best_count:
                            best_count = len(recs)
                            best_idx = ci
                    if best_idx < 0:
                        log.warning("XHR: no candidate has records — giving up")
                        return []
                    idx = best_idx
                    log.info(f"XHR: auto-selected response {idx} ({best_count} records)")
                    # Use AI's field_mapping if provided, even with bad index
                    if not selection.get("field_mapping"):
                        selection["field_mapping"] = {}

                reason = str(selection.get("reason", "")).strip() or "candidate appears relevant"
                log.info(f"XHR: AI selected response {idx} — {reason}")

                records = self._extract_from_json(
                    json_data=self.candidates[idx]["body"],
                    records_path=str(selection.get("records_path", "") or ""),
                    field_mapping=selection.get("field_mapping", {}) or {},
                    target_fields=target_fields,
                )
                log.info(f"XHR: extracted {len(records)} records")
                return records
            except Exception as e:
                log.error(f"XHR extraction failed: {e}")
                return []
            finally:
                await page.close()
                await browser.close()

    def _register_listener(self, page: Page) -> None:
        """Register generic response listener before navigation."""

        def on_response(response: Response) -> None:
            asyncio.create_task(self._capture_candidate(response))

        page.on("response", on_response)

    async def _capture_candidate(self, response: Response) -> None:
        if len(self.candidates) >= self.max_candidates:
            return
        try:
            if response.status != 200:
                return

            url = response.url
            content_type = response.headers.get("content-type", "").lower()
            is_json_type = "json" in content_type
            is_api_like_url = any(k in url.lower() for k in self._API_KEYWORDS)
            if not (is_json_type or is_api_like_url):
                return

            try:
                body_json = await response.json()
                body_text = json.dumps(body_json, ensure_ascii=False)
            except Exception:
                return

            if not self._contains_data_array(body_json):
                return

            signature = f"{url}|{body_text[:120]}"
            if signature in self._seen_signatures:
                return
            self._seen_signatures.add(signature)

            self.candidates.append(
                {
                    "url": url,
                    "body": body_json,
                    "preview": body_text[:1500],
                }
            )
        except Exception as e:
            log.debug(f"XHR listener error: {e}")

    async def _trigger_lazy_requests(self, page: Page) -> None:
        """Trigger additional API calls on lazy/infinite pages."""
        for _ in range(3):
            try:
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
            except Exception:
                break
            await page.wait_for_timeout(900)
        try:
            await page.evaluate("window.scrollTo(0, 0)")
        except Exception:
            pass

    def _contains_data_array(self, obj: Any, depth: int = 0, max_depth: int = 6) -> bool:
        if depth > max_depth:
            return False
        if isinstance(obj, list):
            if len(obj) >= 5 and all(isinstance(item, dict) for item in obj[:5]):
                return True
            return any(self._contains_data_array(item, depth + 1, max_depth) for item in obj[:10])
        if isinstance(obj, dict):
            return any(
                self._contains_data_array(value, depth + 1, max_depth)
                for value in obj.values()
            )
        return False

    @staticmethod
    def _flatten_keys(obj: Any, prefix: str = "", max_depth: int = 6) -> list[str]:
        """Flatten a JSON object into dot-notation key paths."""
        keys: list[str] = []
        if max_depth <= 0:
            return keys
        if isinstance(obj, dict):
            for k, v in obj.items():
                full_key = f"{prefix}.{k}" if prefix else k
                if isinstance(v, (dict, list)):
                    keys.extend(XHRInterceptor._flatten_keys(v, full_key, max_depth - 1))
                else:
                    keys.append(f"{full_key} = {str(v)[:80]}")
        elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
            keys.extend(XHRInterceptor._flatten_keys(obj[0], prefix, max_depth - 1))
        return keys

    async def _identify_data_response(
        self,
        candidates: list[dict],
        client_description: str,
        target_fields: list[str],
    ) -> Optional[dict]:
        rows = []
        for idx, candidate in enumerate(candidates):
            # Show flattened keys of the first record so AI can map nested fields
            flat_sample = ""
            records_array = self._resolve_records(candidate["body"], "")
            if records_array:
                flat_keys = self._flatten_keys(records_array[0], max_depth=6)
                if flat_keys:
                    flat_sample = "\nFlattened sample record keys:\n" + "\n".join(
                        f"  {k}" for k in flat_keys[:40]
                    )
            rows.append(
                f"Response {idx}\nURL: {candidate['url']}\n"
                f"Preview: {candidate['preview']}{flat_sample}"
            )

        system_prompt = "You map API JSON responses to client-required records. Return strict JSON only."
        user_prompt = (
            f"Client wants: {client_description}\n"
            f"Requested fields: {target_fields}\n\n"
            f"Captured API responses ({len(candidates)}):\n\n"
            + "\n\n".join(rows)
            + "\n\nIMPORTANT: The JSON may have deeply nested fields. "
            "Use the flattened sample keys above to find the correct paths.\n"
            "In field_mapping, use FULL dot-notation paths for nested values.\n"
            "Example: if price is at cardDetails.price.value, map:\n"
            '  "price": "cardDetails.price.value"\n\n'
            "Return JSON with this exact shape:\n"
            "{"
            '\n  "selected_index": <int or null>,'
            '\n  "reason": "...",'
            '\n  "records_path": "dot.path.to.array or empty",'
            '\n  "field_mapping": {"client_field": "full.dot.path.to.value"}'
            "\n}"
        )

        try:
            result = await ai_provider.complete(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                json_mode=True,
            )
            parsed = self._safe_json_parse(result.get("text", "{}"))
            if not isinstance(parsed, dict):
                return None
            return parsed
        except Exception as e:
            log.warning(f"XHR AI selection failed: {e}")
            return None

    def _safe_json_parse(self, text: str) -> Any:
        raw = (text or "").strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            match = re.search(r"\{[\s\S]*\}", raw)
            if not match:
                return None
            try:
                return json.loads(match.group(0))
            except Exception:
                return None

    def _extract_from_json(
        self,
        json_data: Any,
        records_path: str,
        field_mapping: dict,
        target_fields: list[str],
    ) -> list[dict]:
        records = self._resolve_records(json_data, records_path)
        if not records:
            return []

        output: list[dict] = []
        for item in records:
            if not isinstance(item, dict):
                continue
            row: dict[str, Any] = {}
            for field in target_fields:
                source_path = field_mapping.get(field, field) if isinstance(field_mapping, dict) else field
                value = self._dig(item, str(source_path))
                row[field] = value if value is not None else ""
            output.append(row)
        return output

    def _resolve_records(self, json_data: Any, records_path: str) -> list[dict]:
        if records_path:
            current = self._dig(json_data, records_path)
            if isinstance(current, list):
                return [r for r in current if isinstance(r, dict)]

        candidates: list[list[dict]] = []

        def walk(node: Any, depth: int = 0) -> None:
            if depth > 6:
                return
            if isinstance(node, list):
                if len(node) >= 5 and all(isinstance(x, dict) for x in node[:5]):
                    candidates.append([x for x in node if isinstance(x, dict)])
                for x in node[:20]:
                    walk(x, depth + 1)
            elif isinstance(node, dict):
                for v in node.values():
                    walk(v, depth + 1)

        walk(json_data)
        if not candidates:
            return []
        return max(candidates, key=len)

    def _dig(self, obj: Any, path: str) -> Any:
        """Traverse nested dicts/lists using dot-notation path.
        Supports: 'a.b.c', 'a.0.b' (list index), unlimited depth.
        """
        cur = obj
        for part in [p for p in path.split(".") if p]:
            if isinstance(cur, dict):
                cur = cur.get(part)
            elif isinstance(cur, list):
                try:
                    cur = cur[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        # If final value is a dict/list, stringify it
        if isinstance(cur, (dict, list)):
            return json.dumps(cur, ensure_ascii=False)
        return cur
