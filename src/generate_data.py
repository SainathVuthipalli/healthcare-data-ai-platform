"""Synthetic healthcare data generator (Day 1).

Produces patients, providers, eligibility and two batches of claims as CSV files,
plus a manifest.json that records exactly what was injected. Everything is seeded,
so the same config always produces byte-identical files.

All data is synthetic. Names, IDs and health card numbers are fake and carry a
SYN prefix where they could be mistaken for real identifiers.

Usage:
    python src/generate_data.py --seed 42 --out data/landing
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

PERIOD_DAYS = 28

DX_CODES = ["E11.9", "I10", "J45.909", "M54.5", "F41.1", "N39.0", "K21.9", "E78.5", "J06.9", "R51.9"]
PROC_CODES = ["P1001", "P1002", "P2001", "P2002", "P3001", "P3002", "P4001", "P5001"]
STATUSES = ["PAID", "DENIED", "PENDING"]
PLANS = ["PLAN-A", "PLAN-B", "PLAN-C"]
SPECIALTIES = ["Family Medicine", "Cardiology", "Orthopedics", "Pediatrics", "Radiology", "Psychiatry"]
CITIES = ["Toronto", "Ottawa", "Hamilton", "London", "Markham", "Kingston", "Windsor", "Barrie"]
FIRST_NAMES = ["Aiden", "Priya", "Noah", "Mei", "Liam", "Sofia", "Omar", "Hana", "Lucas", "Amara", "Ethan", "Zara"]
LAST_NAMES = ["Nguyen", "Patel", "Smith", "Tremblay", "Singh", "Garcia", "Kim", "Brown", "Roy", "Ali", "Chen", "Lee"]
POSTAL_PREFIXES = ["M1B", "M5V", "L3R", "K1A", "L8P", "N6A", "K7L", "N9A"]

# defect name -> what a pipeline should eventually do with it
DEFECT_ACTIONS = {
    "null_patient_id": "reject",
    "orphan_provider": "reject",
    "negative_amount": "reject",
    "invalid_dx_format": "reject",
    "invalid_service_date": "reject",
    "service_after_submission": "reject",
    "status_case": "standardize",
    "dup_in_file": "deduplicate",
}

CLAIM_FIELDS = [
    "claim_id", "patient_id", "provider_id", "service_date", "submitted_date",
    "dx_code", "proc_code", "billed_amount", "status", "updated_at",
]
PATIENT_FIELDS = ["patient_id", "health_card_no", "first_name", "last_name", "dob", "sex", "postal_prefix", "plan_id"]
PROVIDER_FIELDS = ["provider_id", "provider_name", "specialty", "city"]
ELIGIBILITY_FIELDS = ["patient_id", "plan_id", "start_date", "end_date", "status"]


@dataclass(frozen=True)
class Config:
    seed: int = 42
    n_patients: int = 2000
    n_providers: int = 150
    n_claims_per_batch: int = 10000
    start: date = date(2025, 1, 1)
    late_rate: float = 0.03        # period-1 claims that first arrive in batch 2
    correction_rate: float = 0.02  # batch-1 claims re-sent in batch 2 with changed values
    resend_rate: float = 0.02      # batch-1 claims re-sent in batch 2 unchanged
    defect_rate: float = 0.01      # share of each batch given EACH defect type


def make_providers(rng: random.Random, cfg: Config) -> list[dict]:
    rows = []
    for i in range(1, cfg.n_providers + 1):
        rows.append({
            "provider_id": f"PR{i:04d}",
            "provider_name": f"Synthetic Clinic {i:04d}",
            "specialty": rng.choice(SPECIALTIES),
            "city": rng.choice(CITIES),
        })
    return rows


def make_patients(rng: random.Random, cfg: Config) -> list[dict]:
    rows = []
    for i in range(1, cfg.n_patients + 1):
        dob = date(1940, 1, 1) + timedelta(days=rng.randint(0, 365 * 80))
        rows.append({
            "patient_id": f"P{i:06d}",
            "health_card_no": f"SYN-{rng.randint(0, 99999999):08d}",
            "first_name": rng.choice(FIRST_NAMES),
            "last_name": rng.choice(LAST_NAMES),
            "dob": dob.isoformat(),
            "sex": rng.choice(["F", "M"]),
            "postal_prefix": rng.choice(POSTAL_PREFIXES),
            "plan_id": rng.choice(PLANS),
        })
    return rows


def make_eligibility(rng: random.Random, cfg: Config, patients: list[dict]) -> list[dict]:
    rows = []
    for p in patients:
        start = cfg.start - timedelta(days=rng.randint(30, 900))
        ended = rng.random() < 0.10
        end = cfg.start + timedelta(days=rng.randint(10, 2 * PERIOD_DAYS)) if ended else None
        rows.append({
            "patient_id": p["patient_id"],
            "plan_id": p["plan_id"],
            "start_date": start.isoformat(),
            "end_date": end.isoformat() if end else "",
            "status": "TERMINATED" if ended else "ACTIVE",
        })
    return rows


def make_claims(rng: random.Random, cfg: Config, patients: list[dict], providers: list[dict],
                period_start: date, first_number: int) -> list[dict]:
    rows = []
    for n in range(cfg.n_claims_per_batch):
        service = period_start + timedelta(days=rng.randint(0, PERIOD_DAYS - 1))
        submitted = service + timedelta(days=rng.randint(1, 14))
        amount = min(5000.0, max(20.0, rng.lognormvariate(5.0, 0.8)))
        rows.append({
            "claim_id": f"C{first_number + n:07d}",
            "patient_id": rng.choice(patients)["patient_id"],
            "provider_id": rng.choice(providers)["provider_id"],
            "service_date": service.isoformat(),
            "submitted_date": submitted.isoformat(),
            "dx_code": rng.choice(DX_CODES),
            "proc_code": rng.choice(PROC_CODES),
            "billed_amount": round(amount, 2),
            "status": rng.choices(STATUSES, weights=[70, 15, 15])[0],
            "updated_at": f"{submitted.isoformat()}T08:00:00",
            "_kind": "new",
            "_protected": False,
        })
    return rows


def _apply_defect(rng: random.Random, cfg: Config, name: str, row: dict) -> None:
    if name == "null_patient_id":
        row["patient_id"] = ""
    elif name == "orphan_provider":
        row["provider_id"] = f"PRX{rng.randint(1000, 9999)}"
    elif name == "negative_amount":
        row["billed_amount"] = -abs(row["billed_amount"])
    elif name == "invalid_dx_format":
        # drop the dot and add a digit: invalid for every code, including ones without a dot
        row["dx_code"] = row["dx_code"].replace(".", "") + "9"
    elif name == "invalid_service_date":
        row["service_date"] = f"{cfg.start.year}-02-30"
    elif name == "service_after_submission":
        row["service_date"] = (date.fromisoformat(row["submitted_date"]) + timedelta(days=5)).isoformat()
    elif name == "status_case":
        row["status"] = row["status"].lower()
    else:
        raise ValueError(f"unknown defect {name}")


def inject_defects(rng: random.Random, cfg: Config, rows: list[dict]) -> dict[str, int]:
    """Mutate distinct 'new' rows so each defect type has an exact, known count."""
    eligible = [i for i, r in enumerate(rows) if r["_kind"] == "new" and not r["_protected"]]
    rng.shuffle(eligible)
    k = max(1, round(len(rows) * cfg.defect_rate))
    needed = k * len(DEFECT_ACTIONS)
    if needed > len(eligible):
        raise ValueError("not enough clean rows to inject defects; lower defect_rate or raise claims")
    counts: dict[str, int] = {}
    cursor = 0
    extra_rows = []
    for name in DEFECT_ACTIONS:
        chosen = eligible[cursor:cursor + k]
        cursor += k
        if name == "dup_in_file":
            for i in chosen:
                extra_rows.append(dict(rows[i], _kind="dup_in_file", _protected=True))
        else:
            for i in chosen:
                _apply_defect(rng, cfg, name, rows[i])
        counts[name] = len(chosen)
    rows.extend(extra_rows)
    return counts


def generate(cfg: Config) -> dict:
    """Build every table in memory. Pure function of cfg: no clock, no global random."""
    rng = random.Random(cfg.seed)
    providers = make_providers(rng, cfg)
    patients = make_patients(rng, cfg)
    eligibility = make_eligibility(rng, cfg, patients)

    p1_start = cfg.start
    p2_start = cfg.start + timedelta(days=PERIOD_DAYS)

    b1 = make_claims(rng, cfg, patients, providers, p1_start, 1)
    b2_new = make_claims(rng, cfg, patients, providers, p2_start, cfg.n_claims_per_batch + 1)

    # late arrivals: period-1 claims withheld from batch 1, first seen in batch 2
    late_idx = set(rng.sample(range(len(b1)), round(len(b1) * cfg.late_rate)))
    late = [dict(b1[i], _kind="late") for i in sorted(late_idx)]
    b1 = [r for i, r in enumerate(b1) if i not in late_idx]

    # corrections and unchanged re-sends: protect their sources from defect injection
    pool = list(range(len(b1)))
    rng.shuffle(pool)
    n_corr = round(len(b1) * cfg.correction_rate)
    n_resend = round(len(b1) * cfg.resend_rate)
    corr_idx = pool[:n_corr]
    resend_idx = pool[n_corr:n_corr + n_resend]
    corrections, resent = [], []
    for i in corr_idx:
        r = dict(b1[i], _kind="correction")
        r["billed_amount"] = round(r["billed_amount"] * rng.uniform(0.8, 1.2), 2)
        r["status"] = rng.choice(STATUSES)
        # a correction must always be newer than the row it corrects
        original_day = date.fromisoformat(b1[i]["updated_at"][:10])
        r["updated_at"] = f"{(original_day + timedelta(days=rng.randint(1, 5))).isoformat()}T06:00:00"
        corrections.append(r)
        b1[i]["_protected"] = True
    for i in resend_idx:
        resent.append(dict(b1[i], _kind="resent"))
        b1[i]["_protected"] = True

    b2 = b2_new + late + corrections + resent
    rng.shuffle(b2)

    defects = {
        "claims_batch_1": inject_defects(rng, cfg, b1),
        "claims_batch_2": inject_defects(rng, cfg, b2),
    }
    rng.shuffle(b1)
    rng.shuffle(b2)

    return {
        "patients": patients,
        "providers": providers,
        "eligibility": eligibility,
        "claims_batch_1": b1,
        "claims_batch_2": b2,
        "defects": defects,
        "late_count": len(late),
        "correction_count": len(corrections),
        "resend_count": len(resent),
    }


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for r in rows:
            out = dict(r)
            if isinstance(out.get("billed_amount"), float):
                out["billed_amount"] = f"{out['billed_amount']:.2f}"
            writer.writerow(out)


def write_outputs(out_dir: Path, cfg: Config, tables: dict) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "patients.csv": (PATIENT_FIELDS, tables["patients"]),
        "providers.csv": (PROVIDER_FIELDS, tables["providers"]),
        "eligibility.csv": (ELIGIBILITY_FIELDS, tables["eligibility"]),
        "claims_batch_1.csv": (CLAIM_FIELDS, tables["claims_batch_1"]),
        "claims_batch_2.csv": (CLAIM_FIELDS, tables["claims_batch_2"]),
    }
    for name, (fields, rows) in files.items():
        _write_csv(out_dir / name, fields, rows)

    manifest = {
        "note": "Synthetic data. Ground truth for what was injected; use it to reconcile pipeline results.",
        "config": {**asdict(cfg), "start": cfg.start.isoformat()},
        "row_counts": {name: len(rows) for name, (_, rows) in files.items()},
        "defect_actions": DEFECT_ACTIONS,
        "injected_defects": tables["defects"],
        "late_arriving_in_batch_2": tables["late_count"],
        "corrections_in_batch_2": tables["correction_count"],
        "unchanged_resends_in_batch_2": tables["resend_count"],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare landing files.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path("data/landing"))
    parser.add_argument("--patients", type=int, default=2000)
    parser.add_argument("--providers", type=int, default=150)
    parser.add_argument("--claims-per-batch", type=int, default=10000)
    args = parser.parse_args()

    cfg = Config(seed=args.seed, n_patients=args.patients, n_providers=args.providers,
                 n_claims_per_batch=args.claims_per_batch)
    manifest = write_outputs(args.out, cfg, generate(cfg))
    print(json.dumps(manifest["row_counts"], indent=2))
    print(f"wrote files and manifest.json to {args.out}")


if __name__ == "__main__":
    main()
