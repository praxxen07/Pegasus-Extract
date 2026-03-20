"""
core/site_profiler.py — Reusable site analyzer.

Accepts a URL, loads it via Playwright, and inspects the DOM to produce
a structured SiteProfile with page-type guesses, selectors, pagination
clues, and candidate fields.

This is a generic V2 capability — no site-specific logic here.
"""

import json
import logging
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import Page

logger = logging.getLogger("extraction-engine")


@dataclass
class FieldCandidate:
    """A detected candidate field on the page."""
    name: str
    selector: str
    sample_value: str = ""
    confidence: float = 0.0
    notes: str = ""


@dataclass
class SiteProfile:
    """Structured result of analyzing a web page."""
    url: str
    page_title: str = ""
    page_type: str = ""  # listing, detail, category, homepage, unknown
    is_js_heavy: bool = False

    # Structural clues
    repeated_containers: List[Dict[str, Any]] = field(default_factory=list)
    link_selectors: List[Dict[str, Any]] = field(default_factory=list)
    pagination: Dict[str, Any] = field(default_factory=dict)
    breadcrumb: Dict[str, Any] = field(default_factory=dict)

    # Candidate fields
    candidate_fields: List[Dict[str, Any]] = field(default_factory=list)

    # Metadata
    total_links: int = 0
    total_images: int = 0
    total_tables: int = 0
    total_forms: int = 0
    confidence: float = 0.0
    notes: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
        logger.info("Site profile saved to %s", path)


# ── JS analysis script ─────────────────────────────────────────────────────

ANALYZE_PAGE_JS = r"""
() => {
    const norm = s => (s||'').replace(/\s+/g,' ').trim();

    // Page basics
    const title = norm(document.title);
    const bodyText = norm(document.body.innerText || '').substring(0, 3000);

    // Count elements
    const totalLinks = document.querySelectorAll('a[href]').length;
    const totalImages = document.querySelectorAll('img').length;
    const totalTables = document.querySelectorAll('table').length;
    const totalForms = document.querySelectorAll('form').length;

    // Detect repeated containers (cards, products, articles)
    const containerCandidates = [];
    for (const tag of ['article', 'li', 'div', 'section']) {
        const els = document.querySelectorAll(tag);
        // Group by class
        const classGroups = {};
        for (const el of els) {
            const cls = el.className ? tag + '.' + el.className.split(/\s+/).sort().join('.') : tag;
            if (!classGroups[cls]) classGroups[cls] = [];
            classGroups[cls].push(el);
        }
        for (const [sel, group] of Object.entries(classGroups)) {
            if (group.length >= 3) {
                const sample = norm(group[0].innerText || '').substring(0, 200);
                const links = group[0].querySelectorAll('a[href]').length;
                const images = group[0].querySelectorAll('img').length;
                containerCandidates.push({
                    selector: sel,
                    count: group.length,
                    avg_links: links,
                    avg_images: images,
                    sample_text: sample,
                });
            }
        }
    }
    containerCandidates.sort((a, b) => b.count - a.count);

    // Detect pagination
    const pagination = {};
    const nextLink = document.querySelector('li.next > a, a.next, a[rel="next"], .pagination a:last-child');
    if (nextLink) {
        pagination.next_selector = nextLink.tagName.toLowerCase();
        pagination.next_href = nextLink.getAttribute('href') || '';
        pagination.next_text = norm(nextLink.innerText);
        if (nextLink.closest('li.next')) pagination.next_selector = 'li.next > a';
        if (nextLink.getAttribute('rel') === 'next') pagination.next_selector = 'a[rel="next"]';
    }
    const pageLinks = document.querySelectorAll('.pager a, .pagination a, nav a');
    pagination.page_link_count = pageLinks.length;
    const pageCurrent = document.querySelector('.pager .current, .pagination .current, .current-page');
    if (pageCurrent) pagination.current_text = norm(pageCurrent.innerText);

    // Detect breadcrumb
    const breadcrumb = {};
    const bcEl = document.querySelector('.breadcrumb, [aria-label="breadcrumb"], nav.breadcrumbs');
    if (bcEl) {
        breadcrumb.selector = bcEl.tagName.toLowerCase() + (bcEl.className ? '.' + bcEl.className.split(/\s+/).join('.') : '');
        const bcLinks = Array.from(bcEl.querySelectorAll('a')).map(a => ({
            text: norm(a.innerText),
            href: a.getAttribute('href') || '',
        }));
        breadcrumb.links = bcLinks;
        breadcrumb.depth = bcLinks.length;
    }

    // Detect candidate fields
    const fields = [];
    const priceEls = document.querySelectorAll('[class*="price"], .price, .price_color');
    if (priceEls.length) {
        fields.push({
            name: 'price',
            selector: priceEls[0].className ? '.' + priceEls[0].className.split(/\s+/).join('.') : 'price',
            sample_value: norm(priceEls[0].innerText),
            count: priceEls.length,
        });
    }

    const ratingEls = document.querySelectorAll('[class*="star-rating"], [class*="rating"], .star-rating');
    if (ratingEls.length) {
        fields.push({
            name: 'rating',
            selector: ratingEls[0].className ? '.' + ratingEls[0].className.split(/\s+/).join('.') : 'rating',
            sample_value: ratingEls[0].className || '',
            count: ratingEls.length,
        });
    }

    const availEls = document.querySelectorAll('[class*="availability"], .availability, .instock');
    if (availEls.length) {
        fields.push({
            name: 'availability',
            selector: availEls[0].className ? '.' + availEls[0].className.split(/\s+/).join('.') : 'availability',
            sample_value: norm(availEls[0].innerText),
            count: availEls.length,
        });
    }

    // Title candidates
    const h1 = document.querySelector('h1');
    const h2s = document.querySelectorAll('h2, h3');
    const titleEls = document.querySelectorAll('[class*="title"], .product_title, a[title]');
    if (h1) fields.push({ name: 'title_h1', selector: 'h1', sample_value: norm(h1.innerText), count: 1 });
    if (h2s.length >= 3) fields.push({ name: 'title_repeating', selector: 'h3,h2', sample_value: norm(h2s[0].innerText), count: h2s.length });
    if (titleEls.length) fields.push({ name: 'title_class', selector: '.' + (titleEls[0].className || '').split(/\s+/).join('.'), sample_value: norm(titleEls[0].innerText || titleEls[0].getAttribute('title') || ''), count: titleEls.length });

    // Image candidates
    const productImgs = document.querySelectorAll('.product_pod img, .product img, article img, .thumbnail img');
    if (productImgs.length) {
        fields.push({
            name: 'image',
            selector: 'product img',
            sample_value: productImgs[0].getAttribute('src') || '',
            count: productImgs.length,
        });
    }

    // Link patterns
    const linkPatterns = {};
    for (const a of document.querySelectorAll('a[href]')) {
        const href = a.getAttribute('href') || '';
        if (href.includes('catalogue/') && href.includes('/index.html')) {
            const pattern = 'catalogue/*/index.html';
            if (!linkPatterns[pattern]) linkPatterns[pattern] = 0;
            linkPatterns[pattern]++;
        } else if (href.includes('category/')) {
            const pattern = 'category/*';
            if (!linkPatterns[pattern]) linkPatterns[pattern] = 0;
            linkPatterns[pattern]++;
        }
    }
    const detailLinks = Object.entries(linkPatterns)
        .map(([pattern, count]) => ({ pattern, count }))
        .sort((a, b) => b.count - a.count);

    // JS detection
    const scripts = document.querySelectorAll('script[src]');
    const isJsHeavy = scripts.length > 10 || !!document.querySelector('[data-reactroot], [ng-app], #__next');

    // Page type guess
    let pageType = 'unknown';
    if (containerCandidates.length && containerCandidates[0].count >= 5 && pagination.next_href) {
        pageType = 'listing';
    } else if (containerCandidates.length && containerCandidates[0].count >= 5) {
        pageType = 'listing';
    } else if (h1 && totalTables >= 1 && bcEl) {
        pageType = 'detail';
    } else if (bcEl && containerCandidates.length) {
        pageType = 'category';
    }

    return {
        title, bodyText, pageType, isJsHeavy,
        totalLinks, totalImages, totalTables, totalForms,
        containers: containerCandidates.slice(0, 10),
        pagination,
        breadcrumb,
        fields,
        detailLinks,
    };
}
"""


async def profile_page(page: Page, url: str) -> SiteProfile:
    """
    Analyze a single page and return a SiteProfile.

    Args:
        page: Playwright Page instance.
        url: URL to analyze.

    Returns:
        SiteProfile with structural analysis.
    """
    from core.browser import BrowserManager

    await BrowserManager.navigate_static(page, url, settle_ms=1500)

    raw = await page.evaluate(ANALYZE_PAGE_JS)

    profile = SiteProfile(
        url=url,
        page_title=raw.get("title", ""),
        page_type=raw.get("pageType", "unknown"),
        is_js_heavy=raw.get("isJsHeavy", False),
        repeated_containers=raw.get("containers", []),
        pagination=raw.get("pagination", {}),
        breadcrumb=raw.get("breadcrumb", {}),
        candidate_fields=raw.get("fields", []),
        link_selectors=raw.get("detailLinks", []),
        total_links=raw.get("totalLinks", 0),
        total_images=raw.get("totalImages", 0),
        total_tables=raw.get("totalTables", 0),
        total_forms=raw.get("totalForms", 0),
    )

    # Calculate confidence
    clues = 0
    if profile.page_type != "unknown":
        clues += 1
    if profile.repeated_containers:
        clues += 1
    if profile.pagination:
        clues += 1
    if profile.candidate_fields:
        clues += 1
    if profile.link_selectors:
        clues += 1
    profile.confidence = min(clues / 5.0, 1.0)

    # Add notes
    if profile.page_type == "listing":
        profile.notes.append(f"Detected listing page with {len(profile.repeated_containers)} container candidates")
    if profile.pagination.get("next_href"):
        profile.notes.append(f"Pagination detected: next → {profile.pagination['next_href']}")
    if not profile.is_js_heavy:
        profile.notes.append("Page appears static — BeautifulSoup fallback viable")
    if profile.is_js_heavy:
        profile.warnings.append("Page appears JS-heavy — may need Playwright evaluation")

    logger.info("Profiled %s → type=%s confidence=%.0f%%", url, profile.page_type, profile.confidence * 100)
    return profile
