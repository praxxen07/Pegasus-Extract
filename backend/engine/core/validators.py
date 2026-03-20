"""
core/validators.py — Post-extraction validation.

Checks data quality: required fields, foreign key integrity, orphan detection,
and duplicate identification.

Extracted from 09_audit_repaired_l4_final.py and 07_check_corrected_targeted_l4_fix.py.
"""

from collections import Counter
from typing import Any, Dict, List, Optional, Set


def clean(x: Any) -> str:
    """Normalize a value to a stripped string."""
    return "" if x is None else str(x).strip()


class ValidationReport:
    """Accumulator for validation issues with formatted output."""

    def __init__(self):
        self.issues: Counter = Counter()
        self.details: Dict[str, List[dict]] = {}

    def add_issue(self, category: str, count: int = 1, detail: Optional[dict] = None) -> None:
        self.issues[category] += count
        if detail is not None:
            self.details.setdefault(category, []).append(detail)

    @property
    def total_issues(self) -> int:
        return sum(self.issues.values())

    @property
    def is_clean(self) -> bool:
        return self.total_issues == 0

    def summary_lines(self) -> List[str]:
        """Return formatted summary lines."""
        lines = []
        for k in sorted(self.issues):
            lines.append(f"{k} = {self.issues[k]}")
        return lines


class Validator:
    """Post-extraction data quality validator."""

    @staticmethod
    def check_required_fields(
        records: List[dict],
        required_fields: List[str],
        record_type: str = "record",
    ) -> ValidationReport:
        """
        Check that all required fields are present and non-empty.

        Returns a ValidationReport with counts of missing fields.
        """
        report = ValidationReport()
        for i, rec in enumerate(records):
            for field in required_fields:
                if field not in rec or clean(rec.get(field)) == "":
                    report.add_issue(
                        f"missing_{record_type}_{field}",
                        detail={"index": i, "record": rec},
                    )
        return report

    @staticmethod
    def check_foreign_keys(
        child_records: List[dict],
        parent_records: List[dict],
        child_fk_field: str,
        parent_pk_field: str,
        label: str = "fk",
    ) -> ValidationReport:
        """
        Check that every child record's FK exists in the parent's PKs.

        Example: check that every engine's model_year_id exists in model_years.
        """
        report = ValidationReport()
        parent_ids: Set[str] = {
            clean(r.get(parent_pk_field))
            for r in parent_records
            if clean(r.get(parent_pk_field))
        }

        for rec in child_records:
            fk = clean(rec.get(child_fk_field))
            if fk and fk not in parent_ids:
                report.add_issue(f"{label}_orphan")

        return report

    @staticmethod
    def detect_orphans(
        records: List[dict],
        valid_ids: Set[str],
        id_field: str,
        label: str = "orphan",
    ) -> ValidationReport:
        """Detect records whose ID is not in the valid set."""
        report = ValidationReport()
        for rec in records:
            rid = clean(rec.get(id_field))
            if rid and rid not in valid_ids:
                report.add_issue(
                    f"{label}_detected",
                    detail={"id": rid},
                )
        return report

    @staticmethod
    def detect_duplicates(
        records: List[dict],
        id_field: str = "engine_id",
        label: str = "duplicate",
    ) -> ValidationReport:
        """Detect duplicate records by ID."""
        report = ValidationReport()
        seen: Counter = Counter()
        for rec in records:
            rid = clean(rec.get(id_field))
            if rid:
                seen[rid] += 1

        for rid, count in seen.items():
            if count > 1:
                report.add_issue(
                    f"{label}_detected",
                    count=count - 1,
                    detail={"id": rid, "count": count},
                )

        return report
