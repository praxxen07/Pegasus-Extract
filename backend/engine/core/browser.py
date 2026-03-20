"""
core/browser.py — Playwright BrowserManager.

Manages Playwright browser lifecycle: launch, context creation,
page navigation, static asset blocking, and clean shutdown.

Extracted from extract_l4_fixed.py and retry_patch.py browser setup patterns.
"""

from pathlib import Path
from typing import Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
)

from core.config import RunConfig


class BrowserManager:
    """
    Manages Playwright browser lifecycle.

    Usage:
        async with BrowserManager(config) as bm:
            ctx = await bm.create_context()
            page = await ctx.new_page()
            await bm.navigate(page, url)
    """

    def __init__(self, config: RunConfig):
        self.config = config
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None

    async def __aenter__(self) -> "BrowserManager":
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.config.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def create_context(self) -> BrowserContext:
        """Create a new browser context with configured UA, viewport, and asset blocking."""
        if not self._browser:
            raise RuntimeError("BrowserManager not started. Use 'async with'.")

        ctx = await self._browser.new_context(
            user_agent=self.config.user_agent,
            viewport={
                "width": self.config.viewport_width,
                "height": self.config.viewport_height,
            },
            java_script_enabled=True,
            ignore_https_errors=True,
        )

        if self.config.block_assets:
            await ctx.route("**/*", self._block_static_assets)

        return ctx

    async def navigate(
        self,
        page: Page,
        url: str,
        timeout_ms: Optional[int] = None,
        settle_ms: Optional[int] = None,
    ) -> None:
        """
        Navigate to a URL with domcontentloaded + settle wait.

        Uses domcontentloaded (not networkidle) because spec tables are in
        the initial HTML — no need to wait for ad networks. This is the
        optimization that gave 9x speedup in retry_patch.py.
        """
        nav_timeout = timeout_ms or self.config.nav_timeout_ms
        settle = settle_ms or self.config.settle_ms

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            await page.wait_for_timeout(settle)
        except PlaywrightTimeoutError:
            # Page partially loaded — give it a bit more time
            await page.wait_for_timeout(3000)
            raise

    @staticmethod
    async def navigate_static(
        page: Page,
        url: str,
        nav_timeout_ms: int = 60000,
        settle_ms: int = 1800,
    ) -> None:
        """Static version of navigate — usable without a BrowserManager instance."""
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            await page.wait_for_timeout(settle_ms)
        except PlaywrightTimeoutError:
            await page.wait_for_timeout(3000)
            raise

    @staticmethod
    async def _block_static_assets(route) -> None:
        """Block images, fonts, and media to speed up page loads."""
        if route.request.resource_type in {"image", "font", "media"}:
            await route.abort()
        else:
            await route.continue_()
