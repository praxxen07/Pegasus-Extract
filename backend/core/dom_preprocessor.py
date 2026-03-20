"""
dom_preprocessor.py — Compresses raw DOM inspection reports to < 800 tokens.

Strips raw HTML, inline styles, script contents. Produces a compact summary
with flags (TABLE_FOUND, REPEATED_PATTERN_FOUND, LIST_FOUND) that downstream
consumers (AI or deterministic extractors) can act on.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict

log = logging.getLogger("PegasusExtract")

# Rough token estimator: ~4 chars per token for English/HTML mixed content
_CHARS_PER_TOKEN = 4
_MAX_OUTPUT_TOKENS = 800
_MAX_OUTPUT_CHARS = _MAX_OUTPUT_TOKENS * _CHARS_PER_TOKEN  # 3200 chars


def _estimate_tokens(text: str) -> int:
    """Rough token count estimate."""
    return max(1, len(text) // _CHARS_PER_TOKEN)


def _strip_html(text: str) -> str:
    """Remove HTML tags, inline styles, and script contents."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'style="[^"]*"', "", text)
    text = re.sub(r"style='[^']*'", "", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def preprocess_dom(raw_dom_report: Any) -> str:
    """
    Compress a raw DOM inspection report dict into a compact text summary
    that is always under 800 tokens.

    Returns a string with structural flags and minimal sample data.
    """
    if isinstance(raw_dom_report, str):
        try:
            raw_dom_report = json.loads(raw_dom_report)
        except (json.JSONDecodeError, TypeError):
            return raw_dom_report[:_MAX_OUTPUT_CHARS]

    if not isinstance(raw_dom_report, dict):
        return str(raw_dom_report)[:_MAX_OUTPUT_CHARS]

    raw_json = json.dumps(raw_dom_report, default=str)
    tokens_before = _estimate_tokens(raw_json)

    parts: list[str] = []
    flags: list[str] = []

    # ── Page info ──
    page_info = raw_dom_report.get("pageInfo", {})
    if page_info:
        parts.append(
            f"PAGE: title={page_info.get('title', '?')} | "
            f"url={page_info.get('url', '?')} | "
            f"elements={page_info.get('totalElements', 0)} | "
            f"bodyChars={page_info.get('bodyTextLength', 0)}"
        )

    # ── Tables ──
    tables = raw_dom_report.get("tables", [])
    for tbl in tables:
        row_count = tbl.get("rowCount", 0)
        if row_count < 5:
            continue
        flags.append("TABLE_FOUND")
        headers = tbl.get("headers", [])
        selector = tbl.get("selector", "table")
        sample_rows = tbl.get("sampleRows", [])
        tbl_summary = (
            f"TABLE: selector={selector} | rows={row_count} | "
            f"headers={json.dumps(headers[:15])}"
        )
        for i, row in enumerate(sample_rows[:2]):
            tbl_summary += f"\n  sampleRow{i + 1}={json.dumps(row[:15])}"
        parts.append(tbl_summary)

    # ── Repeated patterns (topCandidates) ──
    candidates = raw_dom_report.get("topCandidates", [])
    for cand in candidates[:3]:
        count = cand.get("count", 0)
        if count < 10:
            continue
        flags.append("REPEATED_PATTERN_FOUND")
        selector = cand.get("selector", "")
        child_tags = cand.get("childTags", [])
        samples = cand.get("samples", [])
        cand_summary = (
            f"PATTERN: selector={selector} | count={count} | "
            f"childTags={json.dumps(child_tags[:8])}"
        )
        for i, sample in enumerate(samples[:2]):
            text = sample.get("text", "")[:120]
            children = sample.get("children", [])
            child_info = []
            for ch in children[:5]:
                ch_text = ch.get("text", "")[:60]
                ch_tag = ch.get("tag", "")
                ch_cls = ch.get("classes", "")[:60]
                ch_href = ch.get("href", "")
                child_info.append(
                    f"{ch_tag}"
                    + (f".{ch_cls.split()[0]}" if ch_cls else "")
                    + (f"[href={ch_href[:60]}]" if ch_href else "")
                    + (f"={ch_text}" if ch_text else "")
                )
            cand_summary += f"\n  sample{i + 1}: text={text}"
            if child_info:
                cand_summary += f"\n    children: {' | '.join(child_info)}"
        parts.append(cand_summary)

    # ── Lists ──
    lists = raw_dom_report.get("lists", [])
    for lst in lists[:3]:
        item_count = lst.get("itemCount", 0)
        if item_count < 3:
            continue
        flags.append("LIST_FOUND")
        selector = lst.get("selector", "ul")
        samples = lst.get("samples", [])
        lst_summary = f"LIST: selector={selector} | items={item_count}"
        for i, sample in enumerate(samples[:2]):
            text = _strip_html(sample.get("text", ""))[:100]
            lst_summary += f"\n  item{i + 1}={text}"
        parts.append(lst_summary)

    # ── Flags header ──
    unique_flags = list(dict.fromkeys(flags))  # deduplicate, preserve order
    header = "FLAGS: " + ", ".join(unique_flags) if unique_flags else "FLAGS: NONE"
    parts.insert(0, header)

    # ── Assemble and enforce token budget ──
    output = "\n\n".join(parts)

    # Trim if over budget
    if len(output) > _MAX_OUTPUT_CHARS:
        output = output[:_MAX_OUTPUT_CHARS] + "\n...[truncated to 800 tokens]"

    tokens_after = _estimate_tokens(output)
    reduction = round((1 - tokens_after / max(tokens_before, 1)) * 100)
    log.info(
        f"DOM compressed: {tokens_before} tokens → {tokens_after} tokens "
        f"({reduction}% reduction)"
    )

    return output
