"""
core/config.py — RunConfig dataclass.

Central configuration for extraction runs. All tunable parameters live here
so site adapters and the engine core share a single source of truth.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class RunConfig:
    """Configuration for an extraction run."""

    # ── Paths ──────────────────────────────────────────────────────────────
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    input_path: Optional[Path] = None
    output_dir: Optional[Path] = None
    checkpoint_dir: Optional[Path] = None
    review_dir: Optional[Path] = None
    log_dir: Optional[Path] = None

    # ── Browser ────────────────────────────────────────────────────────────
    headless: bool = True
    user_agent: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    viewport_width: int = 1440
    viewport_height: int = 900
    block_assets: bool = True  # block images, fonts, media for speed

    # ── Concurrency & timing ───────────────────────────────────────────────
    concurrency: int = 3
    nav_timeout_ms: int = 60_000
    settle_ms: int = 1800
    delay_between_pages: float = 3.0
    delay_jitter: float = 1.5

    # ── Retry ──────────────────────────────────────────────────────────────
    max_retries: int = 3
    backoff_base: float = 2.0
    backoff_max: float = 20.0

    # ── Checkpointing ─────────────────────────────────────────────────────
    checkpoint_every: int = 10
    checkpoint_seconds: int = 30

    # ── Limits ─────────────────────────────────────────────────────────────
    limit: Optional[int] = None  # max records to process (None = all)

    # ── Site isolation ────────────────────────────────────────────────────
    site_name: str = ""  # when set, appends to output/checkpoint/review paths

    def __post_init__(self):
        """Resolve default paths relative to project_root."""
        suffix = self.site_name if self.site_name else ""
        if self.output_dir is None:
            base = self.project_root / "data" / "output"
            self.output_dir = base / suffix if suffix else base
        if self.checkpoint_dir is None:
            base = self.project_root / "data" / "checkpoints"
            self.checkpoint_dir = base / suffix if suffix else base
        if self.review_dir is None:
            base = self.project_root / "data" / "review"
            self.review_dir = base / suffix if suffix else base
        if self.log_dir is None:
            self.log_dir = self.project_root / "logs"

        # Ensure all directories exist
        for d in [self.output_dir, self.checkpoint_dir, self.review_dir, self.log_dir]:
            d.mkdir(parents=True, exist_ok=True)
