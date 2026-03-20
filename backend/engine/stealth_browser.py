"""
stealth_browser.py — Generic stealth browser layer for Pegasus Extract.

Makes headless Chromium undetectable on bot-protected websites.
100% generic — no site-specific code, no domain checks, no hardcoded selectors.

Capabilities:
  1. Stealth browser launch (real Chrome → Chromium fallback)
  2. playwright-stealth fingerprint patching on every context
  3. Generic page content validation (bot wall detection)
  4. Gentle scroll for infinite-scroll / lazy-load pages
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
)
from playwright_stealth import Stealth

log = logging.getLogger("PegasusExtract")

# Shared stealth config — all evasions enabled except chrome_runtime
# which can conflict with some sites' own chrome.runtime usage.
_STEALTH = Stealth(
    navigator_platform_override="MacIntel",
    navigator_vendor_override="Google Inc.",
)


# ---------------------------------------------------------------------------
# 1. Stealth browser launch
# ---------------------------------------------------------------------------

def _resolve_chromium_executable() -> str | None:
    """Find a usable Chromium headless shell if PLAYWRIGHT_BROWSERS_PATH is set."""
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


async def launch_stealth_browser(pw: Playwright) -> Browser:
    """
    Launch a browser with maximum stealth.
    Tries real Chrome first (much harder to fingerprint),
    falls back to bundled Chromium.
    """
    stealth_args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--window-size=1440,900",
    ]

    # Attempt 1: real Chrome with native --headless=new
    # Chrome's new headless mode is virtually identical to headed mode
    # and passes nearly all bot detection checks.
    try:
        browser = await pw.chromium.launch(
            channel="chrome",
            headless=False,
            args=["--headless=new"] + stealth_args,
        )
        log.info("Stealth browser: launched real Chrome (--headless=new)")
        return browser
    except Exception as e:
        log.info(f"Real Chrome --headless=new not available ({e})")

    # Attempt 2: real Chrome with standard headless
    try:
        browser = await pw.chromium.launch(
            channel="chrome",
            headless=True,
            args=stealth_args,
        )
        log.info("Stealth browser: launched real Chrome (standard headless)")
        return browser
    except Exception as e:
        log.info(f"Real Chrome not available ({e}), falling back to Chromium")

    # Attempt 3: bundled Chromium (with optional custom executable)
    launch_kwargs: Dict[str, Any] = {
        "headless": True,
        "args": stealth_args,
    }
    exe = _resolve_chromium_executable()
    if exe:
        launch_kwargs["executable_path"] = exe
    browser = await pw.chromium.launch(**launch_kwargs)
    log.info("Stealth browser: launched Chromium (fallback)")
    return browser


# ---------------------------------------------------------------------------
# 2. Stealth context + page creation
# ---------------------------------------------------------------------------

async def create_stealth_context(browser: Browser) -> BrowserContext:
    """
    Create a browser context with realistic fingerprint and headers.
    playwright-stealth patches are applied at context level so every
    page created from this context inherits them automatically.
    """
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
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
        },
    )

    # Apply comprehensive stealth patches (fingerprint evasions)
    await _STEALTH.apply_stealth_async(context)
    log.info("Stealth patches applied to browser context")

    return context


async def new_stealth_page(context: BrowserContext) -> Page:
    """Create a new page from a stealth context."""
    page = await context.new_page()
    return page


# ---------------------------------------------------------------------------
# 3. Generic page content validation
# ---------------------------------------------------------------------------

_CHALLENGE_SIGNALS = [
    "just a moment",
    "attention required",
    "access denied",
    "403 forbidden",
    "captcha",
    "something is missing",
    "oops",
    "robot",
    "are you human",
    "checking your browser",
    "please verify",
    "security check",
    "blocked",
    "unusual traffic",
]


def _extract_domain(url: str) -> str:
    """Return the registered domain from a URL."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        # Strip 'www.' prefix for comparison
        if host.startswith("www."):
            host = host[4:]
        return host.lower()
    except Exception:
        return ""


def is_page_content_valid(
    page_title: str,
    page_url: str,
    original_url: str,
    body_text_length: int,
) -> Tuple[bool, str]:
    """
    Generic check: did we land on the right page?
    Works for any website — no domain-specific logic.
    """
    # Redirected to a completely different domain?
    if page_url and original_url:
        orig_domain = _extract_domain(original_url)
        curr_domain = _extract_domain(page_url)
        if orig_domain and curr_domain and orig_domain != curr_domain:
            return False, f"Redirected to different domain: {curr_domain}"

    # Redirected to homepage when we requested a deep URL?
    if page_url and original_url:
        orig_parsed = urlparse(original_url)
        curr_parsed = urlparse(page_url)
        orig_depth = (orig_parsed.path or "/").rstrip("/")
        curr_depth = (curr_parsed.path or "/").rstrip("/")
        orig_has_query = bool(orig_parsed.query)
        # Original URL had a meaningful path or query, but we ended up at root
        if (len(orig_depth) > 1 or orig_has_query) and len(curr_depth) <= 1:
            # Strip fragment — landing on domain.com/# is same as domain.com/
            if not curr_parsed.query:
                return False, (
                    f"Redirected to homepage (lost path/query): "
                    f"{original_url} → {page_url}"
                )

    # Page has almost no content (bot wall / empty page)?
    if body_text_length < 500:
        return False, f"Page content too thin ({body_text_length} chars) — possible bot wall"

    # Generic challenge/error page titles
    title_lower = (page_title or "").lower()
    for signal in _CHALLENGE_SIGNALS:
        if signal in title_lower:
            return False, f"Bot challenge detected in title: '{page_title}'"

    return True, "OK"


async def validate_and_retry(
    page: Page,
    original_url: str,
    browser_config: dict,
) -> Tuple[bool, str]:
    """
    Validate page content after load. If a bot wall or homepage redirect
    is detected, re-navigate to the original URL and retry.
    Returns (is_valid, message).
    """
    try:
        title = await page.evaluate("document.title || ''")
        current_url = page.url
        body_len = await page.evaluate("(document.body.innerText || '').length")
    except Exception:
        await page.wait_for_timeout(2000)
        return False, "Could not evaluate page state"

    valid, reason = is_page_content_valid(title, current_url, original_url, body_len)
    if valid:
        return True, "OK"

    # ── Domain warm-up strategy ──
    # Many bot-protected sites allow the homepage but block direct deep URLs.
    # Real users browse: homepage → click link → deep page.
    # We simulate this: since we're already on the homepage after the bot
    # challenge resolved, we set a Referer header (homepage) and navigate
    # to the target URL. This mimics organic internal navigation.
    log.warning(f"Page validation failed — {reason}. Attempting domain warm-up strategy...")

    # Step 1: Let the homepage fully settle (JS, cookies, session tokens)
    await page.wait_for_timeout(5000)

    # Step 2: Extract homepage URL for referer
    try:
        homepage_url = await page.evaluate("window.location.origin")
    except Exception:
        homepage_url = ""

    # Step 3: Set referer to homepage (simulates clicking a link from homepage)
    if homepage_url:
        try:
            await page.set_extra_http_headers({"Referer": homepage_url + "/"})
        except Exception:
            pass

    # Step 4: Navigate to the original URL from the homepage context
    # Try JS-based navigation first (preserves SPA state), then page.goto
    log.info(f"Warm-up: navigating to target URL with homepage referer...")
    navigated = False
    for nav_method in ("js", "goto"):
        try:
            if nav_method == "js":
                await page.evaluate(f"window.location.href = {repr(original_url)}")
            else:
                await page.goto(original_url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_load_state("domcontentloaded", timeout=60000)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            navigated = True
            break
        except Exception as e:
            # Context may have been destroyed by navigation — wait and continue
            await page.wait_for_timeout(3000)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
            except Exception:
                pass

    if not navigated:
        log.warning("Re-navigation to original URL failed entirely")
        return False, reason

    # Step 5: Wait for bot challenge on the re-navigation to resolve
    for wait in range(15):
        try:
            body_len = await page.evaluate("(document.body.innerText || '').length")
            title = await page.evaluate("document.title || ''")
        except Exception:
            await page.wait_for_timeout(2000)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            continue

        # Check for bot challenge titles on this attempt too
        title_lower = title.lower()
        is_challenge = any(s in title_lower for s in _CHALLENGE_SIGNALS)
        if is_challenge or body_len < 200:
            await page.wait_for_timeout(2000)
            continue

        v2, r2 = is_page_content_valid(title, page.url, original_url, body_len)
        if v2:
            log.info(
                f"Page valid after warm-up: title='{title}', "
                f"bodyText={body_len}"
            )
            return True, "OK"

        # If still on homepage, break early — site is truly blocking us
        break

    # Final state
    try:
        title = await page.evaluate("document.title || ''")
        body_len = await page.evaluate("(document.body.innerText || '').length")
    except Exception:
        title = "unknown"
        body_len = 0

    log.warning(
        f"Site blocked headless browser even after stealth + warm-up. "
        f"title='{title}', bodyText={body_len}. "
        f"Consider using residential proxies for this site."
    )
    return False, reason


# ---------------------------------------------------------------------------
# 4. Gentle scroll for infinite-scroll / lazy-load pages
# ---------------------------------------------------------------------------

async def gentle_scroll_to_load(
    page: Page,
    scroll_count: int = 5,
    pause_ms: int = 1500,
) -> int:
    """
    Generic infinite scroll handler.
    Scrolls gradually to trigger lazy loading on any site.
    Returns the number of scrolls that actually loaded new content.
    """
    effective_scrolls = 0
    prev_height = await page.evaluate("document.body.scrollHeight")

    for i in range(scroll_count):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await page.wait_for_timeout(pause_ms)

        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height > prev_height:
            effective_scrolls += 1
            prev_height = new_height

    # Scroll back to top so DOM inspection sees everything
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)

    if effective_scrolls > 0:
        log.info(f"Gentle scroll: {effective_scrolls}/{scroll_count} scrolls loaded new content")

    return effective_scrolls
