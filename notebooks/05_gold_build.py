# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold build
# MAGIC
# MAGIC **Goal:** business-ready tables built from Silver, with a hard check that Gold equals Silver.
# MAGIC
# MAGIC | Table | One row per | Answers |
# MAGIC | --- | --- | --- |
# MAGIC | `gold_claims_monthly` | service month, plan and claim status | volume and billed amount trends, claims outside coverage |
# MAGIC | `gold_provider_kpis` | provider | claim count, billed and paid amounts, denial rate, days to submit |
# MAGIC
# MAGIC Gold is a pure function of Silver, so it is rebuilt with an overwrite. There is no incremental state to drift.
# MAGIC No names, health card numbers or patient ids appear in Gold.
# MAGIC
# MAGIC **Outside coverage** means the patient had no eligibility period containing the claim's service date.

# COMMAND ----------

import os
import sys

SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(SRC_DIR):
    SRC_DIR = os.getcwd()
sys.path.insert(0, SRC_DIR)

import platform_config as cfg
from gold_rules import claims_monthly, enrich_claims, layer_totals, provider_kpis
from reconcile import format_report, reconcile_layers

# COMMAND ----------

# Guard: if a row filter on silver_patients hides patients from the user running this notebook, Gold would
# silently be built with missing plans. The pipeline must run as a steward (see notebook 06).
visible = spark.table(cfg.SILVER_PATIENTS).count()
expected = spark.table(cfg.BRONZE_PATIENTS).select("patient_id").distinct().count()
assert visible == expected, (
    f"You can see {visible} of {expected} patients, so Gold would be wrong. "
    "Run this notebook as a steward: run the last cells of notebook 06 or set your role to steward."
)

silver_claims = spark.table(cfg.SILVER_CLAIMS)
silver_patients = spark.table(cfg.SILVER_PATIENTS)
silver_providers = spark.table(cfg.SILVER_PROVIDERS)
silver_eligibility = spark.table(cfg.SILVER_ELIGIBILITY)

enriched = enrich_claims(silver_claims, silver_patients, silver_eligibility)

for target, frame in [
    (cfg.GOLD_CLAIMS_MONTHLY, claims_monthly(enriched)),
    (cfg.GOLD_PROVIDER_KPIS, provider_kpis(enriched, silver_providers)),
]:
    frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"{target}: {spark.table(target).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconcile Gold with Silver
# MAGIC
# MAGIC This reads the tables back from the catalog, so it checks what was actually saved, not what was in memory.

# COMMAND ----------

totals = layer_totals(
    spark.table(cfg.SILVER_CLAIMS),
    spark.table(cfg.GOLD_CLAIMS_MONTHLY),
    spark.table(cfg.GOLD_PROVIDER_KPIS),
)
checks = reconcile_layers(totals, spark.table(cfg.SILVER_PROVIDERS).count())
print(format_report("gold vs silver", checks))

# COMMAND ----------

assert all(c.ok for c in checks), "Gold does not match Silver. Read the FAIL lines above."
print("Gold matches Silver: no claim lost or counted twice, and amounts agree to the cent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at the results
# MAGIC
# MAGIC Expected with the seed-42 data and both batches loaded: **18776** claims, **$3,873,229.27** billed, **627** claims outside coverage,
# MAGIC **18** monthly rows, **150** provider rows.

# COMMAND ----------

display(spark.table(cfg.GOLD_CLAIMS_MONTHLY).orderBy("service_month", "plan_id", "status"))

# COMMAND ----------

display(spark.table(cfg.GOLD_PROVIDER_KPIS).orderBy("denial_rate", ascending=False).limit(10))
