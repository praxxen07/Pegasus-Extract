"""
core/exporters.py — CSV/JSON export with dedup and merge.

Handles writing extraction results to disk in multiple formats,
with built-in deduplication by record ID.

Extracted from engine_to_csv_rows() and merge logic in retry_patch.py.
"""

import csv
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set


class Exporter:
    """Multi-format exporter with deduplication and merge support."""

    @staticmethod
    def write_json(path: Path, data: Any, indent: int = 2) -> None:
        """Write data to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)

    @staticmethod
    def write_csv(
        path: Path,
        rows: List[dict],
        columns: List[str],
    ) -> None:
        """Write rows to CSV with explicit column ordering."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def load_json(path: Path, default: Any = None) -> Any:
        """Load JSON file, returning default if not found or invalid."""
        path = Path(path)
        if not path.exists() or path.stat().st_size == 0:
            return default if default is not None else []
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return default if default is not None else []

    @staticmethod
    def load_csv(path: Path) -> List[dict]:
        """Load CSV file as list of dicts."""
        path = Path(path)
        if not path.exists() or path.stat().st_size == 0:
            return []
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def dedup_by_id(records: List[dict], id_field: str = "engine_id") -> List[dict]:
        """Remove duplicate records, keeping first occurrence."""
        seen: Set[str] = set()
        result = []
        for r in records:
            rid = r.get(id_field, "")
            if rid in seen:
                continue
            seen.add(rid)
            result.append(r)
        return result

    @staticmethod
    def merge(
        base: List[dict],
        patch: List[dict],
        id_field: str = "engine_id",
    ) -> List[dict]:
        """
        Merge base + patch records, with patch taking priority on conflicts.
        Maintains insertion order with base records first.
        """
        merged = list(base)
        seen: Set[str] = {r.get(id_field, "") for r in base if r.get(id_field)}

        for rec in patch:
            rid = rec.get(id_field, "")
            if rid and rid not in seen:
                merged.append(rec)
                seen.add(rid)

        return merged
