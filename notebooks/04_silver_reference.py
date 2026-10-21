# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver reference tables
# MAGIC
# MAGIC **Goal:** typed, de-duplicated versions of the patient, provider and eligibility snapshots, ready for Gold to join.
# MAGIC
# MAGIC - Reference files are snapshots, so each run rebuilds the Silver table from Bronze (overwrite). Re-running gives the same result.
# MAGIC - `silver_patients` keeps names, date of birth and the (fake) health card number. These are the sensitive columns that Week 4 protects with column masks and row filters.
# MAGIC - Gold deliberately never reads those columns.
# MAGIC - If you have already run notebook 06, run this one as a `steward`. If the masks or row filter are missing afterwards, re-run the "Attach the policies" cell in 06.

# COMMAND ----------

import os
import sys

SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(SRC_DIR):
    SRC_DIR = os.getcwd()
sys.path.insert(0, SRC_DIR)

import platform_config as cfg
from gold_rules import build_silver_eligibility, build_silver_patients, build_silver_providers

# COMMAND ----------

builds = [
    ("patients", cfg.BRONZE_PATIENTS, cfg.SILVER_PATIENTS, build_silver_patients),
    ("providers", cfg.BRONZE_PROVIDERS, cfg.SILVER_PROVIDERS, build_silver_providers),
    ("eligibility", cfg.BRONZE_ELIGIBILITY, cfg.SILVER_ELIGIBILITY, build_silver_eligibility),
]

for name, source, target, build in builds:
    bronze_rows = spark.table(source).count()
    build(spark.table(source)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    silver_rows = spark.table(target).count()
    print(f"{name:12s} bronze {bronze_rows:5d} -> silver {silver_rows:5d}   ({target})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check
# MAGIC
# MAGIC Expected with the seed-42 data: patients **2000**, providers **150**, eligibility **2000** (nothing is rejected, because the generator injects no defects into reference files).
# MAGIC Re-run the notebook: the counts must not change.

# COMMAND ----------

display(spark.table(cfg.SILVER_ELIGIBILITY).groupBy("status").count().orderBy("status"))
