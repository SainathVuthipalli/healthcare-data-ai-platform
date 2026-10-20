# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze ingest
# MAGIC
# MAGIC **Goal:** land the raw files in Delta tables exactly as they arrived, with lineage columns, and never load the same file twice.
# MAGIC
# MAGIC - Reference files (patients, providers, eligibility) are small snapshots, so they are overwritten each run.
# MAGIC - Claims arrive as new files in `claims/`. Auto Loader remembers which files it has read in its checkpoint, so re-running this notebook ingests only files it has not seen.
# MAGIC - Everything is kept as strings in Bronze. Typing and validation belong to Silver, so bad values are never lost at the door.

# COMMAND ----------

import os
import sys

# The notebook lives in notebooks/ and the code in src/. Fall back to the notebook's own folder
# if you imported the modules next to the notebook instead of using a Git folder.
SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(SRC_DIR):
    SRC_DIR = os.getcwd()
sys.path.insert(0, SRC_DIR)

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import platform_config as cfg

print("code folder  :", SRC_DIR)
print("claims folder:", cfg.CLAIMS_PATH)
display(dbutils.fs.ls(cfg.CLAIMS_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference tables (snapshot, overwrite)

# COMMAND ----------

for name, file_name in cfg.REFERENCE_FILES.items():
    df = (
        spark.read.option("header", True)
        .csv(f"{cfg.REFERENCE_PATH}/{file_name}")
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    target = cfg.table(f"bronze_{name}")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"{target}: {spark.table(target).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims with Auto Loader (incremental, exactly once per file)
# MAGIC
# MAGIC `availableNow` processes every file that has arrived and then stops, so this runs like a batch job but keeps streaming's bookkeeping.

# COMMAND ----------

claims_schema = StructType([StructField(c, StringType(), True) for c in cfg.CLAIM_COLUMNS])

claims_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(claims_schema)
    .load(cfg.CLAIMS_PATH)
    .withColumn("_source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    claims_stream.writeStream
    .option("checkpointLocation", cfg.CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(cfg.BRONZE_CLAIMS)
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check
# MAGIC
# MAGIC Expected after batch 1 only: `claims_batch_1.csv` = **9797** rows.
# MAGIC After you add batch 2 and re-run: `claims_batch_2.csv` = **10795** rows, and batch 1 is still 9797.
# MAGIC Re-running with no new files must change nothing.

# COMMAND ----------

display(spark.table(cfg.BRONZE_CLAIMS).groupBy("_source_file").count().orderBy("_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fallback if Auto Loader is not available on your compute
# MAGIC
# MAGIC `COPY INTO` is also idempotent per file. Create the table once, then run the copy (use a SQL cell):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.healthcare.bronze_claims (
# MAGIC   claim_id STRING, patient_id STRING, provider_id STRING, service_date STRING,
# MAGIC   submitted_date STRING, dx_code STRING, proc_code STRING, billed_amount STRING,
# MAGIC   status STRING, updated_at STRING, _source_file STRING, _ingested_at TIMESTAMP);
# MAGIC
# MAGIC COPY INTO workspace.healthcare.bronze_claims
# MAGIC FROM (
# MAGIC   SELECT claim_id, patient_id, provider_id, service_date, submitted_date, dx_code, proc_code,
# MAGIC          billed_amount, status, updated_at,
# MAGIC          _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
# MAGIC   FROM '/Volumes/workspace/healthcare/landing/claims/')
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true');
# MAGIC ```
