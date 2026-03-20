"""
core/models.py — Shared data models.

Defines the common types used by the engine core and site adapters:
page status classifications, extraction results, and run metrics.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class PageStatus(str, Enum):
    """Classification of a page extraction attempt."""
    OK = "ok"
    FAILED = "failed"
    FLAGGED = "flagged"


@dataclass
class ExtractResult:
    """Result of extracting data from a single URL."""
    url: str
    status: PageStatus
    records: List[dict] = field(default_factory=list)
    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == PageStatus.OK

    @property
    def record_count(self) -> int:
        return len(self.records)


@dataclass
class RunMetrics:
    """Accumulated metrics for an extraction run."""
    total_input: int = 0
    total_processed: int = 0
    total_ok: int = 0
    total_failed: int = 0
    total_flagged: int = 0
    total_records: int = 0
    total_csv_rows: int = 0
    elapsed_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        return self.total_ok / self.total_processed

    @property
    def records_per_second(self) -> float:
        if self.elapsed_seconds == 0:
            return 0.0
        return self.total_processed / self.elapsed_seconds

    def summary_lines(self) -> List[str]:
        """Return a list of formatted summary lines."""
        return [
            f"Input records     : {self.total_input:,}",
            f"Processed         : {self.total_processed:,}",
            f"OK                : {self.total_ok:,}",
            f"Failed            : {self.total_failed:,}",
            f"Flagged           : {self.total_flagged:,}",
            f"Extracted records : {self.total_records:,}",
            f"CSV rows          : {self.total_csv_rows:,}",
            f"Success rate      : {self.success_rate:.1%}",
            f"Elapsed           : {self.elapsed_seconds:.1f}s",
            f"Rate              : {self.records_per_second:.2f} pages/s",
        ]
