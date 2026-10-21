"""Reconcile pipeline counts against the generator's manifest (pure Python, no Spark)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    name: str
    expected: object
    actual: object

    @property
    def ok(self) -> bool:
        return self.expected == self.actual


def reconcile_file(manifest: dict, file_name: str, metrics: dict) -> list[Check]:
    """Compare one processed claims file with what the generator says it contains.

    metrics keys: rows_in, quarantined, quarantined_by_reason, duplicates_removed,
    status_standardized, inserted, updated, unchanged.
    """
    batch = file_name.removesuffix(".csv")
    injected = manifest["injected_defects"][batch]
    reject_names = [n for n, action in manifest["defect_actions"].items() if action == "reject"]
    by_reason = metrics["quarantined_by_reason"]

    checks = [Check("rows read equal rows in the source file", manifest["row_counts"][file_name], metrics["rows_in"])]
    for name in reject_names:
        checks.append(Check(f"quarantined: {name}", injected[name], by_reason.get(name, 0)))
    unexpected = sum(n for reason, n in by_reason.items() if reason not in reject_names)
    checks.append(Check("quarantined: unexpected reasons", 0, unexpected))
    checks.append(Check("status values standardized", injected["status_case"], metrics["status_standardized"]))
    checks.append(Check("in-file duplicates removed", injected["dup_in_file"], metrics["duplicates_removed"]))

    unique_valid = metrics["inserted"] + metrics["updated"] + metrics["unchanged"]
    accounted = metrics["quarantined"] + metrics["duplicates_removed"] + unique_valid
    checks.append(Check("every input row accounted for", metrics["rows_in"], accounted))

    if batch == "claims_batch_2":
        checks.append(Check("corrections applied (updated rows)", manifest["corrections_in_batch_2"], metrics["updated"]))
        checks.append(Check("unchanged re-sends left alone", manifest["unchanged_resends_in_batch_2"], metrics["unchanged"]))
    return checks


def reconcile_layers(totals: dict, silver_provider_count: int) -> list[Check]:
    """Gold must equal Silver: no claim lost, none counted twice, no amount drift."""
    return [
        Check("gold monthly claims equal silver claims", totals["silver_claims"], totals["monthly_claims"]),
        Check("gold provider claims equal silver claims", totals["silver_claims"], totals["provider_claims"]),
        Check("gold monthly amount equals silver amount", totals["silver_amount"], totals["monthly_amount"]),
        Check("gold provider amount equals silver amount", totals["silver_amount"], totals["provider_amount"]),
        Check("outside-coverage claims agree across gold tables",
              totals["monthly_outside_coverage"], totals["provider_outside_coverage"]),
        Check("one provider KPI row per silver provider", silver_provider_count, totals["provider_rows"]),
    ]


def format_report(file_name: str, checks: list[Check]) -> str:
    lines = [f"Reconciliation for {file_name}"]
    for c in checks:
        mark = "PASS" if c.ok else "FAIL"
        lines.append(f"  [{mark}] {c.name}: expected {c.expected}, got {c.actual}")
    failed = sum(1 for c in checks if not c.ok)
    lines.append("  all checks passed" if failed == 0 else f"  {failed} check(s) failed")
    return "\n".join(lines)
