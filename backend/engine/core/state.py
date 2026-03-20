"""
core/state.py — Checkpoint/resume StateStore.

Tracks completed URLs, accumulated results, and failed/flagged records.
Supports time-based and count-based checkpointing so long runs survive crashes.

Extracted from the proven StateStore in extract_l4_fixed.py and PatchState in retry_patch.py.
"""

import csv
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set


class StateStore:
    """
    Persistent state for extraction runs with checkpoint/resume.

    Tracks done URLs, accumulated records, failures, and flags.
    Periodically writes to disk based on dirty count or elapsed time.
    """

    def __init__(
        self,
        checkpoint_dir: Path,
        output_dir: Path,
        review_dir: Path,
        json_filename: str = "records.json",
        csv_filename: str = "records.csv",
        progress_filename: str = "progress.json",
        failed_filename: str = "failed_pages.json",
        flagged_filename: str = "flagged_urls.json",
    ):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.output_dir = Path(output_dir)
        self.review_dir = Path(review_dir)

        for d in [self.checkpoint_dir, self.output_dir, self.review_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self._json_path = self.output_dir / json_filename
        self._csv_path = self.output_dir / csv_filename
        self._progress_path = self.checkpoint_dir / progress_filename
        self._failed_path = self.review_dir / failed_filename
        self._flagged_path = self.review_dir / flagged_filename

        # Load existing state for resume
        self.records: List[dict] = self._load_json(self._json_path, [])
        self.failed: List[dict] = self._load_json(self._failed_path, [])
        self.flagged: List[dict] = self._load_json(self._flagged_path, [])
        progress = self._load_json(self._progress_path, {"done_urls": []})
        self.done_urls: Set[str] = set(progress.get("done_urls", []))

        # Also mark failed URLs as done so we don't retry them in same run
        for f in self.failed:
            url = f.get("url", "")
            if url:
                self.done_urls.add(url)

        # Dedup tracking
        self.record_ids: Set[str] = set()
        for r in self.records:
            rid = r.get("engine_id") or r.get("record_id") or ""
            if rid:
                self.record_ids.add(rid)

        # CSV rows
        self.csv_rows: List[dict] = []
        if self._csv_path.exists() and self._csv_path.stat().st_size > 0:
            with open(self._csv_path, newline="", encoding="utf-8") as f:
                self.csv_rows = list(csv.DictReader(f))

        # Dirty tracking for checkpointing
        self._dirty = False
        self._dirty_count = 0
        self._last_persist_ts = time.time()

    # ── Mutations ──────────────────────────────────────────────────────────

    def add_success(
        self,
        url: str,
        records: List[dict],
        csv_rows: List[dict],
        id_field: str = "engine_id",
    ) -> int:
        """Add successfully extracted records. Returns count of new records added."""
        added = 0
        for rec in records:
            rid = rec.get(id_field, "")
            if rid and rid in self.record_ids:
                continue
            self.records.append(rec)
            if rid:
                self.record_ids.add(rid)
            added += 1

        self.csv_rows.extend(csv_rows)
        self.done_urls.add(url)
        self._mark_dirty()
        return added

    def add_failed(self, payload: dict) -> None:
        """Record a failed URL."""
        self._upsert_by_url(self.failed, payload)
        self.done_urls.add(payload.get("url", ""))
        self._mark_dirty()

    def add_flagged(self, payload: dict) -> None:
        """Record a flagged URL (needs manual review)."""
        self._upsert_by_url(self.flagged, payload)
        self.done_urls.add(payload.get("url", ""))
        self._mark_dirty()

    def is_done(self, url: str) -> bool:
        """Check if a URL has already been processed."""
        return url in self.done_urls

    # ── Persistence ────────────────────────────────────────────────────────

    def persist(
        self,
        force: bool = False,
        checkpoint_every: int = 10,
        checkpoint_seconds: int = 30,
        csv_columns: Optional[List[str]] = None,
    ) -> bool:
        """
        Write state to disk if dirty and threshold reached.
        Returns True if data was written.
        """
        if not self._dirty:
            return False

        if not force:
            age = time.time() - self._last_persist_ts
            if self._dirty_count < checkpoint_every and age < checkpoint_seconds:
                return False

        self._save_json(self._json_path, self.records)
        self._save_json(self._failed_path, self.failed)
        self._save_json(self._flagged_path, self.flagged)
        self._save_json(self._progress_path, {
            "done_urls": sorted(self.done_urls),
            "count": len(self.done_urls),
        })

        if csv_columns and self.csv_rows:
            with open(self._csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=csv_columns)
                writer.writeheader()
                writer.writerows(self.csv_rows)

        self._dirty = False
        self._dirty_count = 0
        self._last_persist_ts = time.time()
        return True

    # ── Internals ──────────────────────────────────────────────────────────

    def _mark_dirty(self) -> None:
        self._dirty = True
        self._dirty_count += 1

    def _upsert_by_url(self, collection: List[dict], payload: dict) -> None:
        url = payload.get("url", "")
        for i, item in enumerate(collection):
            if item.get("url") == url:
                collection[i] = payload
                return
        collection.append(payload)

    @staticmethod
    def _load_json(path: Path, default: Any) -> Any:
        if path.exists() and path.stat().st_size > 0:
            try:
                with open(path, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return default if not isinstance(default, (list, dict)) else type(default)(default)

    @staticmethod
    def _save_json(path: Path, data: Any) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
