import re
from collections import Counter
from typing import Any, Dict, List

from bs4 import BeautifulSoup


def _get_visible_text(soup: BeautifulSoup, limit: int = 2000) -> str:
    texts: List[str] = []
    for element in soup.stripped_strings:
        texts.append(element)
        if sum(len(t) for t in texts) >= limit:
            break
    return " ".join(texts)[:limit]


def parse_structure(html: str) -> Dict[str, Any]:
    """
    Use BeautifulSoup to extract page structure.
    Return:
    - repeating_elements: list of CSS patterns that
      appear 5+ times (likely data rows)
    - links: sample of href patterns
    - forms: any forms found
    - tables: any <table> elements
    - pagination_hints: next/prev buttons, page numbers
    - text_sample: first 2000 chars of visible text
    """
    soup = BeautifulSoup(html, "html.parser")

    # Repeating elements based on class combinations
    class_counter: Counter[str] = Counter()
    for el in soup.find_all(class_=True):
        classes = el.get("class") or []
        if not classes:
            continue
        key = ".".join(sorted(set(classes)))
        class_counter[key] += 1

    repeating_elements = [
        {"class_pattern": cls, "count": count}
        for cls, count in class_counter.items()
        if count >= 5
    ]

    # Links
    links_sample: List[str] = []
    for a in soup.find_all("a", href=True)[:100]:
        links_sample.append(a["href"])

    # Forms
    forms: List[dict] = []
    for form in soup.find_all("form"):
        forms.append(
            {
                "action": form.get("action"),
                "method": (form.get("method") or "GET").upper(),
                "inputs": [
                    {
                        "name": inp.get("name"),
                        "type": inp.get("type"),
                    }
                    for inp in form.find_all("input")
                ],
            }
        )

    # Tables
    tables: List[dict] = []
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        tables.append(
            {
                "headers": headers,
                "sample_rows": [
                    [td.get_text(strip=True) for td in row.find_all("td")]
                    for row in table.find_all("tr")[:5]
                ],
            }
        )

    # Pagination hints
    pagination_hints: List[dict] = []
    # Links containing "next", "page", "→", ">"
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"]
        classes = " ".join(a.get("class") or []).lower()

        if any(
            kw in text
            for kw in ["next", "prev", "previous", "page", "»", "«", "→", "←"]
        ):
            pagination_hints.append(
                {
                    "type": "text_link",
                    "text": text,
                    "href": href,
                    "class": classes,
                }
            )

        if any(kw in classes for kw in ["pagination", "pager", "page"]):
            pagination_hints.append(
                {
                    "type": "class_hint",
                    "text": text,
                    "href": href,
                    "class": classes,
                }
            )

        # URL patterns like ?page=2, /page/2
        if "?page=" in href or "/page/" in href:
            pagination_hints.append(
                {
                    "type": "url_pattern",
                    "text": text,
                    "href": href,
                    "class": classes,
                }
            )

    text_sample = _get_visible_text(soup, limit=2000)

    return {
        "repeating_elements": repeating_elements,
        "links": links_sample,
        "forms": forms,
        "tables": tables,
        "pagination_hints": pagination_hints,
        "text_sample": text_sample,
    }


def _clean_html_for_snapshot(html_str: str, max_len: int = 3000) -> str:
    """Remove noise from HTML while keeping structural info (tags, classes, text)."""
    # Remove data-* attributes (except data-testid)
    html_str = re.sub(r'\s+data-(?!testid)[a-z-]+="[^"]*"', '', html_str)
    # Remove style attributes
    html_str = re.sub(r'\s+style="[^"]*"', '', html_str)
    # Remove event handlers
    html_str = re.sub(r'\s+on\w+="[^"]*"', '', html_str)
    # Remove SVG blocks
    html_str = re.sub(r'<svg[^>]*>.*?</svg>', '', html_str, flags=re.DOTALL)
    # Remove srcset
    html_str = re.sub(r'\s+srcset="[^"]*"', '', html_str)
    # Remove aria-* attributes
    html_str = re.sub(r'\s+aria-[a-z-]+="[^"]*"', '', html_str)
    # Remove very long attribute values (>120 chars)
    html_str = re.sub(r'="[^"]{120,}"', '="..."', html_str)
    # Collapse whitespace
    html_str = re.sub(r'\s+', ' ', html_str)
    if len(html_str) > max_len:
        html_str = html_str[:max_len] + "...[truncated]"
    return html_str.strip()


def extract_dom_snapshot(html: str, max_samples: int = 2) -> str:
    """
    Find the most likely repeating data container on the page and return
    a clean HTML snapshot of a few sample items.

    This gives the AI an EXACT view of the real DOM structure so it can
    write accurate CSS selectors — instead of guessing from memory.

    Fully generic: works for any website by detecting structural patterns.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove noise elements that pollute container detection
    for tag in soup.find_all(["script", "style", "noscript", "link", "meta"]):
        tag.decompose()

    # Group elements by tag.class signature
    sig_map: Dict[str, list] = {}
    for el in soup.find_all(True):
        classes = el.get("class")
        if not classes:
            continue
        sig = f"{el.name}.{'.'.join(sorted(set(classes)))}"
        sig_map.setdefault(sig, []).append(el)

    # Score candidates: must repeat, have child tags, and have meaningful text
    scored: List[tuple] = []
    for sig, els in sig_map.items():
        if len(els) < 3:
            continue
        sample = els[0]
        n_children = len(sample.find_all(True))
        text_len = len(sample.get_text(strip=True))
        # Must have some structure and text
        if n_children < 2 or text_len < 15:
            continue
        # Higher score = richer container (more children × more text)
        scored.append((sig, els, n_children * text_len))

    if not scored:
        return ""

    # Pick the richest repeating container
    scored.sort(key=lambda x: x[2], reverse=True)
    best_sig, best_els, _ = scored[0]

    # Convert signature back to a usable CSS selector
    parts = best_sig.split(".")
    tag_name = parts[0]
    class_sel = ".".join(parts[1:])
    css_selector = f"{tag_name}.{class_sel}" if class_sel else tag_name

    # Build snapshot
    lines = [
        f"REPEATING CONTAINER CSS SELECTOR: {css_selector}",
        f"TOTAL ITEMS FOUND: {len(best_els)}",
        "",
    ]
    for i, el in enumerate(best_els[:max_samples]):
        cleaned = _clean_html_for_snapshot(str(el))
        lines.append(f"=== SAMPLE ITEM {i + 1} ===")
        lines.append(cleaned)
        lines.append("")

    return "\n".join(lines)

