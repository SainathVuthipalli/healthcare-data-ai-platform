"""Silver-layer validation and upsert rules for claims (Week 2).

Pure PySpark: no Delta and no Databricks APIs, so everything here runs and is tested
locally. The Databricks notebooks call these functions and add the Delta MERGE on top.

Design:
- Every rule is evaluated for every row, so a row can fail several rules at once.
  Rejected rows go to quarantine with ALL their reasons, never silently dropped.
- Casts use try_cast, so one bad value becomes a rule failure, not a failed job
  (Databricks serverless runs with ANSI mode on, where a plain cast would raise).
- Status case is standardized, not rejected: "paid" becomes "PAID".
- Exact duplicates inside a batch collapse to one row per claim_id (latest updated_at).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from platform_config import CLAIM_COLUMNS

VALID_STATUSES = ["PAID", "DENIED", "PENDING"]
DX_REGEX = r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$"

# Names written to quarantine. The first six match the defects injected by the generator.
REJECT_RULES = [
    "null_patient_id",
    "orphan_patient",
    "orphan_provider",
    "negative_amount",
    "invalid_amount",
    "invalid_dx_format",
    "invalid_service_date",
    "invalid_submitted_date",
    "service_after_submission",
    "invalid_status",
    "invalid_updated_at",
]


@dataclass
class ClaimsResult:
    valid: DataFrame               # typed, standardized, one row per claim_id
    valid_before_dedup: DataFrame  # same rows before in-batch deduplication
    quarantine: DataFrame          # original raw values, row_hash and reject_reasons
    status_standardized: DataFrame # valid rows whose status text was changed


def flag_claims(raw: DataFrame, patient_ids: DataFrame, provider_ids: DataFrame) -> DataFrame:
    """Add typed helper columns and an array column _reject_reasons (empty means valid)."""
    patients = (patient_ids.select(F.col("patient_id").alias("_p_id")).distinct()
                .withColumn("_p_known", F.lit(True)))
    providers = (provider_ids.select(F.col("provider_id").alias("_v_id")).distinct()
                 .withColumn("_v_known", F.lit(True)))

    df = (
        raw
        .withColumn("_status_clean", F.upper(F.trim(F.col("status"))))
        .withColumn("_amount", F.expr("try_cast(billed_amount as decimal(12,2))"))
        .withColumn("_service_dt", F.expr("try_cast(service_date as date)"))
        .withColumn("_submitted_dt", F.expr("try_cast(submitted_date as date)"))
        .withColumn("_updated_ts", F.expr("try_cast(updated_at as timestamp)"))
        .join(F.broadcast(patients), F.col("patient_id") == F.col("_p_id"), "left")
        .join(F.broadcast(providers), F.col("provider_id") == F.col("_v_id"), "left")
    )

    blank_patient = F.col("patient_id").isNull() | (F.trim(F.col("patient_id")) == "")
    checks = [
        ("null_patient_id", blank_patient),
        ("orphan_patient", ~blank_patient & F.col("_p_known").isNull()),
        ("orphan_provider", F.col("_v_known").isNull()),
        ("negative_amount", F.col("_amount") < 0),
        ("invalid_amount", F.col("_amount").isNull()),
        ("invalid_dx_format", ~F.coalesce(F.col("dx_code").rlike(DX_REGEX), F.lit(False))),
        ("invalid_service_date", F.col("_service_dt").isNull()),
        ("invalid_submitted_date", F.col("_submitted_dt").isNull()),
        ("service_after_submission", F.col("_service_dt") > F.col("_submitted_dt")),
        ("invalid_status", ~F.coalesce(F.col("_status_clean").isin(VALID_STATUSES), F.lit(False))),
        ("invalid_updated_at", F.col("_updated_ts").isNull()),
    ]
    assert [name for name, _ in checks] == REJECT_RULES

    reasons = F.filter(
        F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in checks]),
        lambda x: x.isNotNull(),
    )
    return df.withColumn("_reject_reasons", reasons)


def latest_per_claim(df: DataFrame) -> DataFrame:
    """One row per claim_id: the one with the newest updated_at."""
    w = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def process_claims(raw: DataFrame, patient_ids: DataFrame, provider_ids: DataFrame) -> ClaimsResult:
    """Split raw (all-string) claims into valid rows and quarantined rows.

    Columns of `raw` other than the ten claim columns (for example _source_file) are
    carried through to both outputs.
    """
    raw_cols = raw.columns
    meta_cols = [c for c in raw_cols if c not in CLAIM_COLUMNS]

    flagged = flag_claims(raw, patient_ids, provider_ids)
    ok = flagged.filter(F.size("_reject_reasons") == 0)
    bad = flagged.filter(F.size("_reject_reasons") > 0)

    hash_inputs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in raw_cols]
    quarantine = bad.select(
        *raw_cols,
        F.sha2(F.concat_ws("|", *hash_inputs), 256).alias("row_hash"),
        F.col("_reject_reasons").alias("reject_reasons"),
    )

    typed = ok.select(
        "claim_id", "patient_id", "provider_id",
        F.col("_service_dt").alias("service_date"),
        F.col("_submitted_dt").alias("submitted_date"),
        "dx_code", "proc_code",
        F.col("_amount").alias("billed_amount"),
        F.col("_status_clean").alias("status"),
        F.col("_updated_ts").alias("updated_at"),
        *meta_cols,
    )
    standardized = ok.filter(F.col("status") != F.col("_status_clean"))

    return ClaimsResult(
        valid=latest_per_claim(typed),
        valid_before_dedup=typed,
        quarantine=quarantine,
        status_standardized=standardized,
    )


def collect_metrics(raw: DataFrame, result: ClaimsResult) -> dict:
    """Counts that reconcile.py compares with the generator's manifest."""
    by_reason = {
        r["reason"]: int(r["n"])
        for r in (result.quarantine
                  .select(F.explode("reject_reasons").alias("reason"))
                  .groupBy("reason").agg(F.count("*").alias("n"))
                  .collect())
    }
    unique_valid = result.valid.count()
    return {
        "rows_in": raw.count(),
        "quarantined": result.quarantine.count(),
        "quarantined_by_reason": by_reason,
        "duplicates_removed": result.valid_before_dedup.count() - unique_valid,
        "status_standardized": result.status_standardized.count(),
        "unique_valid": unique_valid,
    }


def merge_outcome(target: DataFrame, source: DataFrame) -> dict:
    """What the MERGE will do: insert new claim_ids, update only if strictly newer, else leave."""
    t = target.select("claim_id", F.col("updated_at").alias("t_updated"))
    joined = source.select("claim_id", F.col("updated_at").alias("s_updated")).join(t, "claim_id", "left")
    row = joined.agg(
        F.count("*").alias("source_rows"),
        F.sum(F.when(F.col("t_updated").isNull(), 1).otherwise(0)).alias("inserted"),
        F.sum(F.when(F.col("s_updated") > F.col("t_updated"), 1).otherwise(0)).alias("updated"),
    ).first()
    inserted = int(row["inserted"] or 0)
    updated = int(row["updated"] or 0)
    return {"inserted": inserted, "updated": updated, "unchanged": int(row["source_rows"]) - inserted - updated}


def apply_upserts(target: DataFrame, source: DataFrame) -> DataFrame:
    """In-memory equivalent of the Delta MERGE, used to prove the semantics in tests.

    A claim is replaced only when the source row is strictly newer. On a tie the
    existing row wins, which is what makes re-running a load change nothing.
    """
    combined = (target.withColumn("_src", F.lit(0))
                .unionByName(source.withColumn("_src", F.lit(1)), allowMissingColumns=True))
    w = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc(), F.col("_src").asc())
    return combined.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn", "_src")
