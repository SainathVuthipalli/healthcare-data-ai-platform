"""Tests for reconcile.py: it must pass clean numbers and catch every kind of drift."""

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reconcile import format_report, reconcile_file, reconcile_layers  # noqa: E402

MANIFEST = {
    "row_counts": {"claims_batch_1.csv": 120, "claims_batch_2.csv": 130},
    "defect_actions": {
        "null_patient_id": "reject",
        "negative_amount": "reject",
        "status_case": "standardize",
        "dup_in_file": "deduplicate",
    },
    "injected_defects": {
        "claims_batch_1": {"null_patient_id": 5, "negative_amount": 5, "status_case": 5, "dup_in_file": 5},
        "claims_batch_2": {"null_patient_id": 6, "negative_amount": 6, "status_case": 6, "dup_in_file": 6},
    },
    "corrections_in_batch_2": 4,
    "unchanged_resends_in_batch_2": 3,
}

# batch 1: 120 rows = 10 quarantined + 5 duplicates + 105 unique valid
GOOD_1 = {
    "rows_in": 120, "quarantined": 10,
    "quarantined_by_reason": {"null_patient_id": 5, "negative_amount": 5},
    "duplicates_removed": 5, "status_standardized": 5,
    "inserted": 105, "updated": 0, "unchanged": 0,
}
# batch 2: 130 rows = 12 quarantined + 6 duplicates + 112 unique (105 inserted + 4 updated + 3 unchanged)
GOOD_2 = {
    "rows_in": 130, "quarantined": 12,
    "quarantined_by_reason": {"null_patient_id": 6, "negative_amount": 6},
    "duplicates_removed": 6, "status_standardized": 6,
    "inserted": 105, "updated": 4, "unchanged": 3,
}


def failed(checks):
    return [c.name for c in checks if not c.ok]


def test_clean_numbers_pass_for_both_batches():
    assert failed(reconcile_file(MANIFEST, "claims_batch_1.csv", GOOD_1)) == []
    assert failed(reconcile_file(MANIFEST, "claims_batch_2.csv", GOOD_2)) == []


def test_batch_1_has_no_correction_checks():
    names = [c.name for c in reconcile_file(MANIFEST, "claims_batch_1.csv", GOOD_1)]
    assert not any("corrections" in n or "re-sends" in n for n in names)


def test_missing_quarantine_rows_are_caught():
    bad = copy.deepcopy(GOOD_1)
    bad["quarantined_by_reason"]["negative_amount"] = 4
    assert "quarantined: negative_amount" in failed(reconcile_file(MANIFEST, "claims_batch_1.csv", bad))


def test_unexpected_reason_is_caught():
    bad = copy.deepcopy(GOOD_1)
    bad["quarantined_by_reason"]["orphan_patient"] = 2
    assert "quarantined: unexpected reasons" in failed(reconcile_file(MANIFEST, "claims_batch_1.csv", bad))


def test_lost_rows_are_caught_by_the_accounting_identity():
    bad = copy.deepcopy(GOOD_1)
    bad["inserted"] = 104  # one valid row vanished
    assert "every input row accounted for" in failed(reconcile_file(MANIFEST, "claims_batch_1.csv", bad))


def test_wrong_correction_count_is_caught():
    bad = copy.deepcopy(GOOD_2)
    bad["updated"] = 3
    bad["inserted"] = 106
    assert "corrections applied (updated rows)" in failed(reconcile_file(MANIFEST, "claims_batch_2.csv", bad))


def test_report_marks_failures():
    bad = copy.deepcopy(GOOD_1)
    bad["duplicates_removed"] = 4
    text = format_report("claims_batch_1.csv", reconcile_file(MANIFEST, "claims_batch_1.csv", bad))
    assert "[FAIL] in-file duplicates removed" in text
    assert "check(s) failed" in text


# ---------- Gold versus Silver ----------

GOOD_TOTALS = {
    "silver_claims": 1000, "silver_amount": "123456.78",
    "monthly_claims": 1000, "monthly_amount": "123456.78", "monthly_outside_coverage": 12,
    "provider_claims": 1000, "provider_amount": "123456.78", "provider_outside_coverage": 12,
    "provider_rows": 20,
}


def test_matching_layers_pass():
    assert failed(reconcile_layers(GOOD_TOTALS, 20)) == []


def test_a_lost_claim_in_gold_is_caught():
    bad = dict(GOOD_TOTALS, monthly_claims=999)
    assert failed(reconcile_layers(bad, 20)) == ["gold monthly claims equal silver claims"]


def test_a_double_counted_claim_in_gold_is_caught():
    bad = dict(GOOD_TOTALS, provider_claims=1001)
    assert failed(reconcile_layers(bad, 20)) == ["gold provider claims equal silver claims"]


def test_one_cent_of_amount_drift_is_caught():
    bad = dict(GOOD_TOTALS, provider_amount="123456.79")
    assert failed(reconcile_layers(bad, 20)) == ["gold provider amount equals silver amount"]


def test_gold_tables_disagreeing_on_coverage_is_caught():
    bad = dict(GOOD_TOTALS, provider_outside_coverage=11)
    assert failed(reconcile_layers(bad, 20)) == ["outside-coverage claims agree across gold tables"]


def test_a_missing_provider_row_is_caught():
    bad = dict(GOOD_TOTALS, provider_rows=19)
    assert failed(reconcile_layers(bad, 20)) == ["one provider KPI row per silver provider"]
