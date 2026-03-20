"""
core/retry.py — RetryPolicy with exponential backoff.

Provides configurable retry behavior with failure classification.
Extracted from the retry loops in extract_l4_fixed.py and retry_patch.py.
"""

import asyncio
import random
from dataclasses import dataclass
from typing import Optional

from core.models import PageStatus


@dataclass
class RetryPolicy:
    """Configurable retry policy with exponential backoff."""

    max_retries: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 20.0
    jitter: float = 1.0

    def wait_time(self, attempt: int) -> float:
        """
        Calculate wait time for a given attempt number (1-indexed).

        Uses exponential backoff: base * 2^(attempt-2), capped at backoff_max,
        with random jitter added.
        """
        if attempt <= 1:
            return 0.0
        base_wait = min(
            self.backoff_base * (2 ** (attempt - 2)),
            self.backoff_max,
        )
        return base_wait + random.uniform(0, self.jitter)

    async def wait(self, attempt: int) -> None:
        """Sleep for the calculated backoff time."""
        seconds = self.wait_time(attempt)
        if seconds > 0:
            await asyncio.sleep(seconds)

    def should_retry(self, attempt: int) -> bool:
        """Return True if another attempt is allowed."""
        return attempt < self.max_retries

    @staticmethod
    def classify_failure(
        reason: str,
        expected_count: Optional[int] = None,
        anchors_found: int = 0,
        labels_found: int = 0,
    ) -> PageStatus:
        """
        Classify a failure as FAILED (worth retrying later) or FLAGGED (genuinely no data).

        Pages with expected_count > 0 or visible labels/anchors are FAILED
        (the data exists but we couldn't get it). Pages with nothing are FLAGGED
        (the page genuinely has no engines).
        """
        if expected_count is not None and expected_count > 0:
            return PageStatus.FAILED
        if labels_found > 0 or anchors_found > 0:
            return PageStatus.FAILED
        return PageStatus.FLAGGED
