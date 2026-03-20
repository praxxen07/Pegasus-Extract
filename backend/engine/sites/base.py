"""
sites/base.py — Abstract SiteAdapter interface.

Defines the contract that every site adapter must implement.
The engine core operates on this interface, ensuring clean separation
between reusable logic and site-specific code.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from core.config import RunConfig
from core.models import ExtractResult
from core.validators import ValidationReport


class SiteAdapter(ABC):
    """
    Abstract base for site-specific adapters.

    Each adapter knows how to:
    - Load and prepare input records for extraction
    - Extract data from a page given a Playwright page handle
    - Convert extracted records to flat CSV rows
    - Define the CSV schema
    - Validate extracted data
    """

    @abstractmethod
    def get_input_records(self, config: RunConfig) -> List[dict]:
        """
        Load input records for this site.

        Returns a list of dicts, each containing at minimum a URL
        to visit and a label for logging.
        """
        ...

    @abstractmethod
    async def extract_page(self, page, record: dict, config: RunConfig) -> ExtractResult:
        """
        Extract data from a single page.

        Args:
            page: Playwright Page instance (already navigated).
            record: Input record with URL, IDs, labels.
            config: Run configuration.

        Returns:
            ExtractResult with status, records, and metadata.
        """
        ...

    @abstractmethod
    def to_csv_rows(self, record: dict) -> List[dict]:
        """
        Convert an extracted record (engine) into flat CSV rows.

        One engine with N spec items in M sections produces M*N rows.
        """
        ...

    @abstractmethod
    def csv_columns(self) -> List[str]:
        """Return the ordered list of CSV column names."""
        ...

    @abstractmethod
    def validate(self, records: List[dict], input_records: List[dict]) -> ValidationReport:
        """
        Run site-specific validation on extracted records.

        Checks foreign keys, orphans, field completeness, etc.
        """
        ...

    @property
    @abstractmethod
    def site_name(self) -> str:
        """Human-readable site name for logging."""
        ...

    @property
    def url_field(self) -> str:
        """Key in input records that holds the URL."""
        return "url"

    @property
    def label_field(self) -> str:
        """Key in input records that holds the display label."""
        return "label"
