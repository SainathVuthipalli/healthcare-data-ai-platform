"""Silver reference tables and Gold aggregates (Week 3).

Pure PySpark, no Delta or Databricks APIs, so everything here is tested locally.

Design:
- Gold is a pure function of Silver. The notebook rebuilds it with an overwrite, so
  re-running gives the same tables. There is no incremental state to get wrong.
- Gold carries no direct identifiers (no names, no health card numbers). Those stay in
  silver_patients, where Week 4 puts masks and row filters on them.
- Coverage is attached with a semi-join, so a claim can never be counted twice even if
  a patient has several eligibility rows.
- All division is guarded, because Databricks serverless runs with ANSI mode on.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ---------- Silver reference tables ----------

def build_silver_patients(bronze: DataFrame) -> DataFrame:
    return (
        bronze
        .select(
            F.trim("patient_id").alias("patient_id"),
            "health_card_no", "first_name", "last_name",
            F.expr("try_cast(dob as date)").alias("dob"),
            "sex", "postal_prefix", "plan_id", "_source_file",
        )
        .filter(F.col("patient_id") != "")
        .dropDuplicates(["patient_id"])
    )


def build_silver_providers(bronze: DataFrame) -> DataFrame:
    return (
        bronze
        .select(F.trim("provider_id").alias("provider_id"), "provider_name", "specialty", "city", "_source_file")
        .filter(F.col("provider_id") != "")
        .dropDuplicates(["provider_id"])
    )


def build_silver_eligibility(bronze: DataFrame) -> DataFrame:
    """An empty end_date means coverage is still open, so it becomes null."""
    return (
        bronze
        .select(
            F.trim("patient_id").alias("patient_id"), "plan_id",
            F.expr("try_cast(start_date as date)").alias("start_date"),
            F.expr("try_cast(end_date as date)").alias("end_date"),
            "status", "_source_file",
        )
        .filter(F.col("start_date").isNotNull())
        .dropDuplicates(["patient_id", "plan_id", "start_date"])
    )


# ---------- Gold ----------

def enrich_claims(claims: DataFrame, patients: DataFrame, eligibility: DataFrame) -> DataFrame:
    """Add plan_id, service_month and outside_coverage to each claim without changing the row count.

    outside_coverage is true when the patient has no eligibility period that contains the service date.
    """
    covered = (
        claims.select("claim_id", "patient_id", "service_date")
        .join(
            eligibility,
            (claims["patient_id"] == eligibility["patient_id"])
            & (claims["service_date"] >= eligibility["start_date"])
            & (eligibility["end_date"].isNull() | (claims["service_date"] <= eligibility["end_date"])),
            "left_semi",
        )
        .select("claim_id")
        .distinct()
        .withColumn("_covered", F.lit(True))
    )
    plans = patients.select("patient_id", "plan_id")
    return (
        claims
        .join(plans, "patient_id", "left")
        .join(covered, "claim_id", "left")
        .withColumn("service_month", F.date_format("service_date", "yyyy-MM"))
        .withColumn("outside_coverage", F.col("_covered").isNull())
        .drop("_covered")
    )


def claims_monthly(enriched: DataFrame) -> DataFrame:
    return (
        enriched
        .groupBy("service_month", "plan_id", "status")
        .agg(
            F.count("*").alias("claim_count"),
            F.sum("billed_amount").alias("billed_amount"),
            F.sum(F.when(F.col("outside_coverage"), 1).otherwise(0)).alias("outside_coverage_claims"),
        )
        .orderBy("service_month", "plan_id", "status")
    )


def provider_kpis(enriched: DataFrame, providers: DataFrame) -> DataFrame:
    """One row per provider, including providers with no claims (counts 0, rates null)."""
    per_provider = enriched.groupBy("provider_id").agg(
        F.count("*").alias("claim_count"),
        F.sum("billed_amount").alias("billed_amount"),
        F.sum(F.when(F.col("status") == "PAID", F.col("billed_amount"))).alias("paid_amount"),
        F.sum(F.when(F.col("status") == "DENIED", 1).otherwise(0)).alias("denied_claims"),
        F.round(F.avg(F.datediff("submitted_date", "service_date")), 2).alias("avg_days_to_submit"),
        F.sum(F.when(F.col("outside_coverage"), 1).otherwise(0)).alias("outside_coverage_claims"),
    )
    joined = providers.select("provider_id", "provider_name", "specialty", "city").join(per_provider, "provider_id", "left")
    return (
        joined
        .withColumn("claim_count", F.coalesce("claim_count", F.lit(0)))
        .withColumn("denied_claims", F.coalesce("denied_claims", F.lit(0)))
        .withColumn("outside_coverage_claims", F.coalesce("outside_coverage_claims", F.lit(0)))
        .withColumn("billed_amount", F.coalesce("billed_amount", F.lit(0).cast("decimal(12,2)")))
        .withColumn("paid_amount", F.coalesce("paid_amount", F.lit(0).cast("decimal(12,2)")))
        .withColumn(
            "denial_rate",
            F.when(F.col("claim_count") > 0, F.round(F.col("denied_claims") / F.col("claim_count"), 4)),
        )
        .orderBy("provider_id")
    )


def layer_totals(silver_claims: DataFrame, monthly: DataFrame, kpis: DataFrame) -> dict:
    """Totals used to prove Gold equals Silver. Amounts are returned as strings to avoid float drift."""
    s = silver_claims.agg(F.count("*").alias("n"), F.sum("billed_amount").alias("amt")).first()
    m = monthly.agg(F.sum("claim_count").alias("n"), F.sum("billed_amount").alias("amt"),
                    F.sum("outside_coverage_claims").alias("oc")).first()
    k = kpis.agg(F.sum("claim_count").alias("n"), F.sum("billed_amount").alias("amt"),
                 F.sum("outside_coverage_claims").alias("oc")).first()

    def amount(v):
        return f"{(v if v is not None else 0):.2f}"

    return {
        "silver_claims": int(s["n"]),
        "silver_amount": amount(s["amt"]),
        "monthly_claims": int(m["n"] or 0),
        "monthly_amount": amount(m["amt"]),
        "monthly_outside_coverage": int(m["oc"] or 0),
        "provider_claims": int(k["n"] or 0),
        "provider_amount": amount(k["amt"]),
        "provider_outside_coverage": int(k["oc"] or 0),
        "provider_rows": kpis.count(),
    }
