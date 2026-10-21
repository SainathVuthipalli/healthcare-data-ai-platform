"""Tests for the Silver reference builders and Gold aggregates, on a local Spark session.

Skipped automatically when pyspark is not installed.
"""

import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from generate_data import Config, generate, write_outputs  # noqa: E402
from gold_rules import (  # noqa: E402
    build_silver_eligibility, build_silver_patients, build_silver_providers,
    claims_monthly, enrich_claims, layer_totals, provider_kpis,
)
from platform_config import CLAIM_COLUMNS  # noqa: E402
from reconcile import reconcile_layers  # noqa: E402
from silver_rules import apply_upserts, process_claims  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("gold-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()


def strings(spark, columns, rows):
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


# ---------- Silver reference builders ----------

def test_eligibility_blank_end_date_becomes_open_coverage(spark):
    bronze = strings(spark, ["patient_id", "plan_id", "start_date", "end_date", "status", "_source_file"], [
        ("P1", "A", "2024-06-01", "", "ACTIVE", "e.csv"),
        ("P2", "A", "2024-06-01", "2025-01-20", "TERMINATED", "e.csv"),
        ("P3", "A", "not-a-date", "", "ACTIVE", "e.csv"),
    ])
    rows = {r["patient_id"]: r for r in build_silver_eligibility(bronze).collect()}
    assert set(rows) == {"P1", "P2"}  # the unparseable start date is dropped
    assert rows["P1"]["end_date"] is None
    assert rows["P2"]["end_date"] == date(2025, 1, 20)


def test_reference_builders_deduplicate_on_their_key(spark):
    patients = strings(spark, ["patient_id", "health_card_no", "first_name", "last_name", "dob", "sex",
                               "postal_prefix", "plan_id", "_source_file"],
                       [("P1", "SYN-1", "A", "B", "1980-01-01", "F", "M1B", "A", "p.csv")] * 2)
    providers = strings(spark, ["provider_id", "provider_name", "specialty", "city", "_source_file"],
                        [("PR1", "Clinic", "Cardiology", "Toronto", "v.csv")] * 3)
    assert build_silver_patients(patients).count() == 1
    assert build_silver_providers(providers).count() == 1


# ---------- enrichment ----------

def make_claims(spark, rows):
    cols = ["claim_id", "patient_id", "provider_id", "service_date", "submitted_date", "dx_code", "proc_code",
            "billed_amount", "status", "updated_at"]
    df = strings(spark, cols, rows)
    return (df.withColumn("service_date", F.col("service_date").cast("date"))
              .withColumn("submitted_date", F.col("submitted_date").cast("date"))
              .withColumn("billed_amount", F.col("billed_amount").cast("decimal(12,2)")))


def claim(cid, patient, provider, service, submitted, amount, status):
    return (cid, patient, provider, service, submitted, "E11.9", "P1001", amount, status, "2025-01-31T08:00:00")


def reference(spark):
    patients = spark.createDataFrame(
        [("P1", "A"), ("P2", "B")],
        StructType([StructField("patient_id", StringType()), StructField("plan_id", StringType())]))
    eligibility = strings(spark, ["patient_id", "plan_id", "start_date", "end_date", "status", "_source_file"], [
        ("P1", "A", "2024-01-01", "", "ACTIVE", "e.csv"),
        ("P2", "B", "2024-01-01", "2025-01-15", "TERMINATED", "e.csv"),
    ])
    return patients, build_silver_eligibility(eligibility)


def test_coverage_flag_and_row_count_is_preserved(spark):
    patients, elig = reference(spark)
    claims = make_claims(spark, [
        claim("c1", "P1", "PR1", "2025-01-10", "2025-01-12", "100.00", "PAID"),    # open coverage
        claim("c2", "P2", "PR1", "2025-01-15", "2025-01-16", "50.00", "PAID"),     # last covered day (inclusive)
        claim("c3", "P2", "PR1", "2025-01-16", "2025-01-18", "75.00", "DENIED"),   # one day after termination
    ])
    out = {r["claim_id"]: r for r in enrich_claims(claims, patients, elig).collect()}
    assert len(out) == 3
    assert [out[c]["outside_coverage"] for c in ("c1", "c2", "c3")] == [False, False, True]
    assert out["c3"]["plan_id"] == "B" and out["c3"]["service_month"] == "2025-01"


def test_overlapping_eligibility_rows_never_duplicate_a_claim(spark):
    patients = spark.createDataFrame([("P1", "A")], StructType(
        [StructField("patient_id", StringType()), StructField("plan_id", StringType())]))
    elig = build_silver_eligibility(strings(
        spark, ["patient_id", "plan_id", "start_date", "end_date", "status", "_source_file"],
        [("P1", "A", "2024-01-01", "", "ACTIVE", "e.csv"), ("P1", "A", "2024-06-01", "", "ACTIVE", "e.csv")]))
    claims = make_claims(spark, [claim("c1", "P1", "PR1", "2025-01-10", "2025-01-12", "10.00", "PAID")])
    assert enrich_claims(claims, patients, elig).count() == 1


# ---------- aggregates ----------

def test_provider_kpis_values_and_zero_claim_provider(spark):
    patients, elig = reference(spark)
    claims = make_claims(spark, [
        claim("c1", "P1", "PR1", "2025-01-10", "2025-01-12", "100.00", "PAID"),
        claim("c2", "P1", "PR1", "2025-01-11", "2025-01-15", "50.00", "DENIED"),
        claim("c3", "P2", "PR1", "2025-01-16", "2025-01-18", "30.00", "PENDING"),  # outside coverage
    ])
    providers = strings(spark, ["provider_id", "provider_name", "specialty", "city", "_source_file"], [
        ("PR1", "Clinic 1", "Cardiology", "Toronto", "v.csv"),
        ("PR2", "Clinic 2", "Radiology", "Ottawa", "v.csv"),
    ])
    kpis = {r["provider_id"]: r for r in provider_kpis(
        enrich_claims(claims, patients, elig), build_silver_providers(providers)).collect()}
    p1, p2 = kpis["PR1"], kpis["PR2"]
    assert p1["claim_count"] == 3 and str(p1["billed_amount"]) == "180.00" and str(p1["paid_amount"]) == "100.00"
    assert p1["denied_claims"] == 1 and float(p1["denial_rate"]) == 0.3333
    assert float(p1["avg_days_to_submit"]) == 2.67  # (2 + 4 + 2) / 3 days, rounded to 2 places
    assert p1["outside_coverage_claims"] == 1
    assert p2["claim_count"] == 0 and p2["denial_rate"] is None and str(p2["billed_amount"]) == "0.00"


def test_monthly_groups_by_month_plan_and_status(spark):
    patients, elig = reference(spark)
    claims = make_claims(spark, [
        claim("c1", "P1", "PR1", "2025-01-10", "2025-01-12", "100.00", "PAID"),
        claim("c2", "P1", "PR1", "2025-01-11", "2025-01-15", "50.00", "PAID"),
        claim("c3", "P1", "PR1", "2025-02-02", "2025-02-04", "20.00", "PAID"),
    ])
    rows = claims_monthly(enrich_claims(claims, patients, elig)).collect()
    got = {(r["service_month"], r["plan_id"], r["status"]): (r["claim_count"], str(r["billed_amount"])) for r in rows}
    assert got == {("2025-01", "A", "PAID"): (2, "150.00"), ("2025-02", "A", "PAID"): (1, "20.00")}


# ---------- the whole flow on seed data, checked in plain Python ----------

@pytest.fixture(scope="module", params=[7, 42])
def gold_flow(request, spark, tmp_path_factory):
    cfg = Config(seed=request.param, n_patients=200, n_providers=20, n_claims_per_batch=1000)
    out = tmp_path_factory.mktemp(f"gold_{request.param}")
    write_outputs(out, cfg, generate(cfg))

    def read(name):
        return (spark.read.option("header", True).csv(str(out / name))
                .withColumn("_source_file", F.lit(name)))

    patients = build_silver_patients(read("patients.csv"))
    providers = build_silver_providers(read("providers.csv"))
    eligibility = build_silver_eligibility(read("eligibility.csv"))

    def silver(name):
        return process_claims(read(name), patients, providers).valid

    claims = apply_upserts(silver("claims_batch_1.csv"), silver("claims_batch_2.csv"))
    enriched = enrich_claims(claims, patients, eligibility)
    monthly = claims_monthly(enriched)
    kpis = provider_kpis(enriched, providers)
    return claims, patients, providers, eligibility, monthly, kpis


def test_gold_reconciles_with_silver(gold_flow):
    claims, _, providers, _, monthly, kpis = gold_flow
    checks = reconcile_layers(layer_totals(claims, monthly, kpis), providers.count())
    assert [c.name for c in checks if not c.ok] == []


def test_gold_matches_an_independent_python_calculation(gold_flow):
    claims, patients, providers, eligibility, monthly, kpis = gold_flow
    plan = {r["patient_id"]: r["plan_id"] for r in patients.collect()}
    periods = defaultdict(list)
    for r in eligibility.collect():
        periods[r["patient_id"]].append((r["start_date"], r["end_date"]))

    expected_monthly = defaultdict(lambda: [0, Decimal("0")])
    expected_provider = defaultdict(lambda: {"n": 0, "amt": Decimal("0"), "denied": 0, "outside": 0})
    for r in claims.collect():
        covered = any(s <= r["service_date"] and (e is None or r["service_date"] <= e)
                      for s, e in periods[r["patient_id"]])
        key = (r["service_date"].strftime("%Y-%m"), plan[r["patient_id"]], r["status"])
        expected_monthly[key][0] += 1
        expected_monthly[key][1] += r["billed_amount"]
        p = expected_provider[r["provider_id"]]
        p["n"] += 1
        p["amt"] += r["billed_amount"]
        p["denied"] += r["status"] == "DENIED"
        p["outside"] += not covered

    got_monthly = {(r["service_month"], r["plan_id"], r["status"]): [r["claim_count"], r["billed_amount"]]
                   for r in monthly.collect()}
    assert got_monthly == {k: v for k, v in expected_monthly.items()}

    for r in kpis.collect():
        e = expected_provider.get(r["provider_id"], {"n": 0, "amt": Decimal("0"), "denied": 0, "outside": 0})
        assert (r["claim_count"], r["billed_amount"], r["denied_claims"], r["outside_coverage_claims"]) == \
               (e["n"], e["amt"], e["denied"], e["outside"]), r["provider_id"]


def test_gold_tables_contain_no_direct_identifiers(gold_flow):
    *_, monthly, kpis = gold_flow
    banned = {"first_name", "last_name", "health_card_no", "dob", "patient_id", "postal_prefix"}
    assert not banned & set(monthly.columns)
    assert not banned & set(kpis.columns)
