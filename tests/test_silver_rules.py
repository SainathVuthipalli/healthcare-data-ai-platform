"""Tests for the Silver rules, run on a local Spark session.

Skipped automatically when pyspark is not installed. To run them:
    pip install pyspark pytest     (PySpark also needs Java 17 or newer)
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from generate_data import Config, generate, write_outputs  # noqa: E402
from platform_config import CLAIM_COLUMNS  # noqa: E402
from reconcile import reconcile_file  # noqa: E402
from silver_rules import (  # noqa: E402
    apply_upserts, collect_metrics, merge_outcome, process_claims,
)

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in CLAIM_COLUMNS + ["_source_file"]])


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("silver-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()


def ids(spark, name, values):
    return spark.createDataFrame([(v,) for v in values], StructType([StructField(name, StringType())]))


# ---------- each rule, on hand-built rows ----------

def row(claim_id, **override):
    base = {
        "claim_id": claim_id, "patient_id": "P1", "provider_id": "PR1",
        "service_date": "2025-01-10", "submitted_date": "2025-01-12", "dx_code": "E11.9",
        "proc_code": "P1001", "billed_amount": "100.00", "status": "PAID",
        "updated_at": "2025-01-12T08:00:00", "_source_file": "f.csv",
    }
    base.update(override)
    return tuple(base[c.name] for c in RAW_SCHEMA.fields)


def test_each_rule_flags_exactly_its_own_defect(spark):
    rows = [
        row("ok"),
        row("lower", status="paid"),
        row("blank_patient", patient_id=None),
        row("blank_patient2", patient_id="  "),
        row("orphan_patient", patient_id="P999"),
        row("orphan_provider", provider_id="PRX1"),
        row("negative", billed_amount="-5.00"),
        row("bad_amount", billed_amount="abc"),
        row("bad_dx", dx_code="E1199"),
        row("bad_service_date", service_date="2025-02-30"),
        row("bad_submitted_date", submitted_date="nope"),
        row("after", service_date="2025-01-20", submitted_date="2025-01-12"),
        row("bad_status", status="UNKNOWN"),
        row("bad_updated", updated_at="not-a-timestamp"),
    ]
    raw = spark.createDataFrame(rows, RAW_SCHEMA)
    result = process_claims(raw, ids(spark, "patient_id", ["P1"]), ids(spark, "provider_id", ["PR1"]))

    reasons = {r["claim_id"]: list(r["reject_reasons"]) for r in result.quarantine.collect()}
    assert reasons == {
        "blank_patient": ["null_patient_id"],
        "blank_patient2": ["null_patient_id"],
        "orphan_patient": ["orphan_patient"],
        "orphan_provider": ["orphan_provider"],
        "negative": ["negative_amount"],
        "bad_amount": ["invalid_amount"],
        "bad_dx": ["invalid_dx_format"],
        "bad_service_date": ["invalid_service_date"],
        "bad_submitted_date": ["invalid_submitted_date"],
        "after": ["service_after_submission"],
        "bad_status": ["invalid_status"],
        "bad_updated": ["invalid_updated_at"],
    }
    valid = {r["claim_id"]: r for r in result.valid.collect()}
    assert set(valid) == {"ok", "lower"}
    assert valid["lower"]["status"] == "PAID"
    assert [r["claim_id"] for r in result.status_standardized.collect()] == ["lower"]


def test_a_row_with_two_problems_gets_both_reasons(spark):
    raw = spark.createDataFrame([row("two", patient_id=None, billed_amount="-1")], RAW_SCHEMA)
    result = process_claims(raw, ids(spark, "patient_id", ["P1"]), ids(spark, "provider_id", ["PR1"]))
    got = sorted(result.quarantine.first()["reject_reasons"])
    assert got == ["negative_amount", "null_patient_id"]


def test_quarantine_keeps_original_raw_values_and_a_stable_hash(spark):
    raw = spark.createDataFrame([row("x", billed_amount="-5.00")], RAW_SCHEMA)
    args = (ids(spark, "patient_id", ["P1"]), ids(spark, "provider_id", ["PR1"]))
    first = process_claims(raw, *args).quarantine.first()
    second = process_claims(raw, *args).quarantine.first()
    assert first["billed_amount"] == "-5.00"
    assert first["row_hash"] == second["row_hash"] and len(first["row_hash"]) == 64


def test_latest_updated_at_wins_inside_a_batch(spark):
    older = row("c1", billed_amount="10.00", updated_at="2025-01-12T08:00:00")
    newer = row("c1", billed_amount="20.00", updated_at="2025-01-13T08:00:00")
    raw = spark.createDataFrame([older, newer, older], RAW_SCHEMA)
    result = process_claims(raw, ids(spark, "patient_id", ["P1"]), ids(spark, "provider_id", ["PR1"]))
    only = result.valid.collect()
    assert len(only) == 1 and str(only[0]["billed_amount"]) == "20.00"


# ---------- the whole flow, checked against the generator's manifest ----------

@pytest.fixture(scope="module", params=[7, 42])
def flow(request, spark, tmp_path_factory):
    cfg = Config(seed=request.param, n_patients=200, n_providers=20, n_claims_per_batch=1000)
    out = tmp_path_factory.mktemp(f"flow_{request.param}")
    manifest = write_outputs(out, cfg, generate(cfg))

    def read(name):
        return spark.read.option("header", True).csv(str(out / name))

    patients, providers = read("patients.csv"), read("providers.csv")

    def run(name):
        raw = read(name).withColumn("_source_file", F.lit(name))
        result = process_claims(raw, patients, providers)
        return raw, result, collect_metrics(raw, result)

    return manifest, run("claims_batch_1.csv"), run("claims_batch_2.csv")


def test_batch_1_reconciles_with_the_manifest(flow):
    manifest, (raw1, res1, m1), _ = flow
    empty_target = res1.valid.limit(0)
    m1.update(merge_outcome(empty_target, res1.valid))
    checks = reconcile_file(manifest, "claims_batch_1.csv", m1)
    assert [c for c in checks if not c.ok] == []


def test_batch_2_reconciles_after_batch_1(flow):
    manifest, (_, res1, _), (_, res2, m2) = flow
    m2.update(merge_outcome(res1.valid, res2.valid))
    checks = reconcile_file(manifest, "claims_batch_2.csv", m2)
    assert [c for c in checks if not c.ok] == []


def test_final_table_size_is_batch_1_plus_inserts(flow):
    _, (_, res1, _), (_, res2, _) = flow
    outcome = merge_outcome(res1.valid, res2.valid)
    final = apply_upserts(res1.valid, res2.valid)
    assert final.count() == res1.valid.count() + outcome["inserted"]
    assert final.select("claim_id").distinct().count() == final.count()


def test_rerunning_a_batch_changes_nothing(flow):
    _, (_, res1, _), (_, res2, _) = flow
    once = apply_upserts(res1.valid, res2.valid)
    twice = apply_upserts(once, res2.valid)
    assert once.exceptAll(twice).count() == 0
    assert twice.exceptAll(once).count() == 0
    assert merge_outcome(once, res2.valid)["inserted"] == 0
    assert merge_outcome(once, res2.valid)["updated"] == 0


def test_corrections_replace_the_old_values(flow):
    _, (_, res1, _), (_, res2, _) = flow
    s, t = res2.valid.alias("s"), res1.valid.alias("t")
    corrected = (s.join(t, F.col("s.claim_id") == F.col("t.claim_id"))
                 .filter(F.col("s.updated_at") > F.col("t.updated_at"))
                 .select(F.col("s.claim_id").alias("claim_id")))
    assert corrected.count() > 0
    final = apply_upserts(res1.valid, res2.valid)
    cols = ["claim_id", "billed_amount", "status", "updated_at"]
    got = final.join(corrected, "claim_id").select(*cols)
    want = res2.valid.join(corrected, "claim_id").select(*cols)
    assert got.exceptAll(want).count() == 0 and want.exceptAll(got).count() == 0
