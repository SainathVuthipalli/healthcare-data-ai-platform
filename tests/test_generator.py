"""Tests for the synthetic data generator.

The key idea: the manifest says how many defects were injected, and these tests
re-detect them with independent validators. If the two disagree, the ground truth
cannot be trusted, and neither can any reconciliation built on it.
"""

import csv
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from generate_data import DEFECT_ACTIONS, PERIOD_DAYS, Config, generate, write_outputs  # noqa: E402

DX_PATTERN = re.compile(r"^[A-Z]\d{2}(\.[0-9A-Z]{1,4})?$")
VALID_STATUSES = {"PAID", "DENIED", "PENDING"}

SMALL = Config(seed=7, n_patients=200, n_providers=20, n_claims_per_batch=1000)


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def parse_date(value: str):
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


@pytest.fixture(scope="module", params=[7, 8, 42, 2026])
def landing(request, tmp_path_factory):
    """Landing files built from several seeds, so a defect that only fails for some random draws is caught."""
    cfg = Config(seed=request.param, n_patients=200, n_providers=20, n_claims_per_batch=1000)
    out = tmp_path_factory.mktemp(f"landing_{request.param}")
    manifest = write_outputs(out, cfg, generate(cfg))
    return out, manifest


def digest(directory: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(directory.glob("*")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()


def test_same_seed_gives_identical_files(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    write_outputs(a, SMALL, generate(SMALL))
    write_outputs(b, SMALL, generate(SMALL))
    assert digest(a) == digest(b)


def test_different_seed_changes_files(tmp_path):
    other = Config(seed=8, n_patients=200, n_providers=20, n_claims_per_batch=1000)
    a, b = tmp_path / "a", tmp_path / "b"
    write_outputs(a, SMALL, generate(SMALL))
    write_outputs(b, other, generate(other))
    assert digest(a) != digest(b)


def test_row_counts_match_manifest(landing):
    out, manifest = landing
    for name, expected in manifest["row_counts"].items():
        assert len(read_csv(out / name)) == expected, name


def test_synthetic_identifiers_are_marked(landing):
    out, _ = landing
    for row in read_csv(out / "patients.csv"):
        assert row["health_card_no"].startswith("SYN-")


@pytest.mark.parametrize("batch", ["claims_batch_1", "claims_batch_2"])
def test_injected_defect_counts_match_independent_detection(landing, batch):
    out, manifest = landing
    rows = read_csv(out / f"{batch}.csv")
    providers = {r["provider_id"] for r in read_csv(out / "providers.csv")}
    injected = manifest["injected_defects"][batch]

    found = {name: 0 for name in DEFECT_ACTIONS}
    for r in rows:
        if not r["patient_id"]:
            found["null_patient_id"] += 1
        if r["provider_id"] not in providers:
            found["orphan_provider"] += 1
        if float(r["billed_amount"]) < 0:
            found["negative_amount"] += 1
        if not DX_PATTERN.match(r["dx_code"]):
            found["invalid_dx_format"] += 1
        service, submitted = parse_date(r["service_date"]), parse_date(r["submitted_date"])
        if service is None:
            found["invalid_service_date"] += 1
        elif service > submitted:
            found["service_after_submission"] += 1
        if r["status"] not in VALID_STATUSES:
            found["status_case"] += 1

    for name in DEFECT_ACTIONS:
        if name == "dup_in_file":
            continue
        assert found[name] == injected[name], f"{batch}: {name}"

    # exact duplicates inside one file: extra copies of a row whose full content repeats
    counts = Counter(tuple(r.values()) for r in rows)
    extra_copies = sum(c - 1 for c in counts.values() if c > 1)
    assert extra_copies == injected["dup_in_file"]


def test_defects_hit_distinct_rows(landing):
    out, manifest = landing
    rows = read_csv(out / "claims_batch_1.csv")
    # a row with two different defects would make counts ambiguous
    providers = {r["provider_id"] for r in read_csv(out / "providers.csv")}
    for r in rows:
        problems = 0
        problems += not r["patient_id"]
        problems += r["provider_id"] not in providers
        problems += float(r["billed_amount"]) < 0
        problems += not DX_PATTERN.match(r["dx_code"])
        problems += parse_date(r["service_date"]) is None
        problems += r["status"] not in VALID_STATUSES
        assert problems <= 1, r["claim_id"]


def test_late_arrivals_only_in_batch_2_and_belong_to_period_1(landing):
    out, manifest = landing
    b1_ids = {r["claim_id"] for r in read_csv(out / "claims_batch_1.csv")}
    b2 = read_csv(out / "claims_batch_2.csv")
    period_1_end = date(2025, 1, 1).toordinal() + PERIOD_DAYS
    late = []
    for r in b2:
        service = parse_date(r["service_date"])
        if service and service.toordinal() < period_1_end and r["claim_id"] not in b1_ids:
            late.append(r)
    assert len(late) == manifest["late_arriving_in_batch_2"]


def test_corrections_have_newer_updated_at_than_batch_1(landing):
    out, manifest = landing
    b1 = {r["claim_id"]: r for r in read_csv(out / "claims_batch_1.csv")}
    changed = 0
    for r in read_csv(out / "claims_batch_2.csv"):
        original = b1.get(r["claim_id"])
        if original and r["updated_at"] > original["updated_at"]:
            changed += 1
    assert changed == manifest["corrections_in_batch_2"]


def test_clean_claims_reference_existing_patients_and_providers(landing):
    out, _ = landing
    patients = {r["patient_id"] for r in read_csv(out / "patients.csv")}
    providers = {r["provider_id"] for r in read_csv(out / "providers.csv")}
    ok = 0
    for batch in ("claims_batch_1", "claims_batch_2"):
        for r in read_csv(out / f"{batch}.csv"):
            if r["patient_id"] and r["provider_id"] in providers:
                assert r["patient_id"] in patients
                ok += 1
    assert ok > 0


def test_manifest_is_valid_json_with_expected_keys(landing):
    out, _ = landing
    data = json.loads((out / "manifest.json").read_text())
    for key in ("config", "row_counts", "defect_actions", "injected_defects"):
        assert key in data


def test_too_many_defects_for_tiny_config_is_rejected():
    tiny = Config(seed=1, n_patients=10, n_providers=3, n_claims_per_batch=20, defect_rate=0.2)
    with pytest.raises(ValueError):
        generate(tiny)
