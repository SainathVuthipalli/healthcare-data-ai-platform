# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver claims
# MAGIC
# MAGIC **Goal:** turn raw Bronze claims into clean, typed, de-duplicated Silver rows, and keep every rejected row, with its reasons, in a quarantine table.
# MAGIC
# MAGIC For each Bronze file that has not been processed yet:
# MAGIC 1. Apply the validation rules in `src/silver_rules.py` (tested locally against the generator's manifest).
# MAGIC 2. MERGE rejected rows into `quarantine_claims`, keyed by a row hash, so a replay never duplicates them.
# MAGIC 3. MERGE valid rows into `silver_claims` by `claim_id`: insert new claims, update only when `updated_at` is strictly newer, leave everything else alone.
# MAGIC 4. Write one row of counts to `silver_file_log`. A file in that log is never processed again.
# MAGIC
# MAGIC Because of steps 2 and 3, running this notebook twice gives the same tables as running it once.

# COMMAND ----------

import os
import sys

SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(SRC_DIR):
    SRC_DIR = os.getcwd()
sys.path.insert(0, SRC_DIR)

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructField, StructType, TimestampType

import platform_config as cfg
from silver_rules import collect_metrics, process_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.SILVER_CLAIMS} (
  claim_id STRING, patient_id STRING, provider_id STRING,
  service_date DATE, submitted_date DATE, dx_code STRING, proc_code STRING,
  billed_amount DECIMAL(12,2), status STRING, updated_at TIMESTAMP,
  _source_file STRING, _silver_loaded_at TIMESTAMP)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.QUARANTINE_CLAIMS} (
  claim_id STRING, patient_id STRING, provider_id STRING, service_date STRING,
  submitted_date STRING, dx_code STRING, proc_code STRING, billed_amount STRING,
  status STRING, updated_at STRING, _source_file STRING,
  row_hash STRING, reject_reasons ARRAY<STRING>, _quarantined_at TIMESTAMP)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.SILVER_FILE_LOG} (
  source_file STRING, rows_in BIGINT, quarantined BIGINT,
  quarantined_by_reason MAP<STRING, BIGINT>, duplicates_removed BIGINT,
  status_standardized BIGINT, inserted BIGINT, updated BIGINT, unchanged BIGINT,
  processed_at TIMESTAMP)
""")

LOG_SCHEMA = StructType([
    StructField("source_file", StringType()),
    StructField("rows_in", LongType()),
    StructField("quarantined", LongType()),
    StructField("quarantined_by_reason", MapType(StringType(), LongType())),
    StructField("duplicates_removed", LongType()),
    StructField("status_standardized", LongType()),
    StructField("inserted", LongType()),
    StructField("updated", LongType()),
    StructField("unchanged", LongType()),
    StructField("processed_at", TimestampType()),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which Bronze files still need processing?

# COMMAND ----------

bronze_files = [
    r["_source_file"]
    for r in (
        spark.table(cfg.BRONZE_CLAIMS)
        .groupBy("_source_file").agg(F.min("_ingested_at").alias("first_seen"))
        .orderBy("first_seen", "_source_file")
        .collect()
    )
]
already_done = {r["source_file"] for r in spark.table(cfg.SILVER_FILE_LOG).select("source_file").collect()}
todo = [f for f in bronze_files if f not in already_done]
print("in Bronze      :", bronze_files)
print("already done   :", sorted(already_done))
print("to process now :", todo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate, quarantine, merge

# COMMAND ----------

patient_ids = spark.table(cfg.BRONZE_PATIENTS).select("patient_id")
provider_ids = spark.table(cfg.BRONZE_PROVIDERS).select("provider_id")

for source_file in todo:
    raw = (
        spark.table(cfg.BRONZE_CLAIMS)
        .filter(F.col("_source_file") == source_file)
        .select(*cfg.CLAIM_COLUMNS, "_source_file")
    )
    result = process_claims(raw, patient_ids, provider_ids)
    metrics = collect_metrics(raw, result)

    # 1. Quarantine: insert rows we have not stored before.
    quarantine_out = (
        result.quarantine
        .dropDuplicates(["row_hash"])
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    (
        DeltaTable.forName(spark, cfg.QUARANTINE_CLAIMS).alias("t")
        .merge(quarantine_out.alias("s"), "t.row_hash = s.row_hash")
        .whenNotMatchedInsertAll()
        .execute()
    )

    # 2. Silver: insert new claims, update only when strictly newer, otherwise leave alone.
    silver_in = result.valid.withColumn("_silver_loaded_at", F.current_timestamp())
    silver = DeltaTable.forName(spark, cfg.SILVER_CLAIMS)
    (
        silver.alias("t")
        .merge(silver_in.alias("s"), "t.claim_id = s.claim_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute()
    )
    op = silver.history(1).select("operationMetrics").first()[0]
    metrics["inserted"] = int(op.get("numTargetRowsInserted", 0))
    metrics["updated"] = int(op.get("numTargetRowsUpdated", 0))
    metrics["unchanged"] = metrics["unique_valid"] - metrics["inserted"] - metrics["updated"]

    # 3. Log the counts. A file in the log is skipped next time.
    log_row = spark.createDataFrame([(
        source_file, metrics["rows_in"], metrics["quarantined"], metrics["quarantined_by_reason"],
        metrics["duplicates_removed"], metrics["status_standardized"],
        metrics["inserted"], metrics["updated"], metrics["unchanged"], None,
    )], LOG_SCHEMA).withColumn("processed_at", F.current_timestamp())
    log_row.write.mode("append").saveAsTable(cfg.SILVER_FILE_LOG)

    print(source_file, {k: v for k, v in metrics.items() if k != "quarantined_by_reason"})

if not todo:
    print("Nothing new to process. Silver and quarantine are unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Expected with the seed-42 data:
# MAGIC
# MAGIC | After | silver_claims rows | quarantine_claims rows |
# MAGIC | --- | --- | --- |
# MAGIC | batch 1 | 9118 | 582 |
# MAGIC | batch 2 | 18776 | 1224 |
# MAGIC
# MAGIC Run this notebook again with no new files: both counts must stay the same.

# COMMAND ----------

print("silver_claims    :", spark.table(cfg.SILVER_CLAIMS).count())
print("quarantine_claims:", spark.table(cfg.QUARANTINE_CLAIMS).count())
display(spark.table(cfg.SILVER_FILE_LOG).orderBy("processed_at"))

# COMMAND ----------

display(
    spark.table(cfg.QUARANTINE_CLAIMS)
    .select(F.explode("reject_reasons").alias("reason"))
    .groupBy("reason").count().orderBy("reason")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Known limit (Week 2)
# MAGIC
# MAGIC If the notebook stops after the Silver MERGE but before the log row is written, the next run reprocesses that file. The tables end up correct (both MERGEs are idempotent), but that file's logged counts would show 0 inserted. Making the whole file a single atomic unit is a good exercise for Week 3.
