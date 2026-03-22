"""
curl_fetcher.py — TLS fingerprint impersonator for Pegasus Extract.

Uses curl_cffi to make HTTP requests with Chrome120 TLS fingerprint,
bypassing server-side TLS checks that block Chromium's signature.

100% generic — no site-specific code, no domain checks.
Single responsibility: bootstrap session cookies for Playwright.
"""

import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse

log = logging.getLogger("PegasusExtract")


class CurlFetcher:
    """TLS fingerprint impersonator using curl_cffi."""

    async def bootstrap_session(self, url: str) -> Optional[dict]:
        """
        Make ONE HTTP request with Chrome120 TLS fingerprint.
        Extract cookies and headers from response.
        Returns {cookies: [...], headers: {...}} for Playwright to use.
        
        This bypasses server-side TLS fingerprinting that blocks
        Chromium's TLS signature before any JavaScript runs.
        """
        try:
            from curl_cffi.requests import Session

            log.info("Bootstrapping session with curl_cffi...")

            def _fetch_once() -> tuple[list[dict], dict]:
                session = Session()
                response = session.get(
                    url,
                    impersonate="chrome120",
                    timeout=30,
                    allow_redirects=True,
                )

                parsed = urlparse(url)
                host = parsed.hostname or ""
                default_domain = host if host else "localhost"

                cookies: list[dict] = []
                for cookie in session.cookies:
                    cookie_domain = getattr(cookie, "domain", "") or default_domain
                    cookie_path = getattr(cookie, "path", "") or "/"

                    cookie_item = {
                        "name": str(getattr(cookie, "name", "")),
                        "value": str(getattr(cookie, "value", "")),
                        "domain": cookie_domain,
                        "path": cookie_path,
                    }

                    expires = getattr(cookie, "expires", None)
                    if isinstance(expires, (int, float)) and expires > 0:
                        cookie_item["expires"] = float(expires)

                    secure = getattr(cookie, "secure", None)
                    if isinstance(secure, bool):
                        cookie_item["secure"] = secure

                    http_only = getattr(cookie, "httponly", None)
                    if isinstance(http_only, bool):
                        cookie_item["httpOnly"] = http_only

                    if cookie_item["name"]:
                        cookies.append(cookie_item)

                headers = {
                    "user-agent": response.headers.get("user-agent", ""),
                    "accept-language": response.headers.get("accept-language", ""),
                }
                return cookies, headers

            cookies, headers = await asyncio.to_thread(_fetch_once)
            log.info(f"Session acquired — cookies handed to Playwright ({len(cookies)})")

            return {
                "cookies": cookies,
                "headers": headers,
            }

        except ImportError:
            log.error(
                "curl_cffi not installed. Run: pip install curl-cffi\n"
                "This is required for sites with TLS fingerprinting."
            )
            return None

        except Exception as e:
            log.error(f"curl_cffi: TLS bootstrap failed: {e}")
            log.info(
                "Site may require residential proxy to bypass IP reputation check. "
                "Add PROXY_URL to .env to enable."
            )
            return None
