# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Reconcile
# MAGIC
# MAGIC **Goal:** prove no rows were lost or invented between the source files and Silver.
# MAGIC
# MAGIC The generator's `manifest.json` says exactly what was injected. This notebook compares it with what the pipeline logged for each file. If any check fails, the notebook fails, so a scheduled job would stop and alert.

# COMMAND ----------

import json
import os
import sys

SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(SRC_DIR):
    SRC_DIR = os.getcwd()
sys.path.insert(0, SRC_DIR)

import platform_config as cfg
from reconcile import format_report, reconcile_file

# COMMAND ----------

if not os.path.exists(cfg.MANIFEST_PATH):
    raise FileNotFoundError(
        f"Upload manifest.json to {cfg.VOLUME_PATH}/ (top level of the volume, next to the claims folder)."
    )
with open(cfg.MANIFEST_PATH) as fh:
    manifest = json.load(fh)

log_rows = sorted(spark.table(cfg.SILVER_FILE_LOG).collect(), key=lambda r: r["source_file"])
print("files in the log:", [r["source_file"] for r in log_rows])

# COMMAND ----------

all_ok = True
for r in log_rows:
    metrics = {
        "rows_in": r["rows_in"],
        "quarantined": r["quarantined"],
        "quarantined_by_reason": dict(r["quarantined_by_reason"]),
        "duplicates_removed": r["duplicates_removed"],
        "status_standardized": r["status_standardized"],
        "inserted": r["inserted"],
        "updated": r["updated"],
        "unchanged": r["unchanged"],
    }
    checks = reconcile_file(manifest, r["source_file"], metrics)
    print(format_report(r["source_file"], checks))
    print()
    all_ok = all_ok and all(c.ok for c in checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table-level checks
# MAGIC
# MAGIC The file log can only describe what was processed. These compare the tables themselves.

# COMMAND ----------

silver_rows = spark.table(cfg.SILVER_CLAIMS).count()
silver_ids = spark.table(cfg.SILVER_CLAIMS).select("claim_id").distinct().count()
quarantine_rows = spark.table(cfg.QUARANTINE_CLAIMS).count()

table_checks = {
    "silver rows equal total inserted across files": (silver_rows, sum(r["inserted"] for r in log_rows)),
    "silver has one row per claim_id": (silver_ids, silver_rows),
    "quarantine rows equal total quarantined across files": (quarantine_rows, sum(r["quarantined"] for r in log_rows)),
}
for name, (actual, expected) in table_checks.items():
    ok = actual == expected
    all_ok = all_ok and ok
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: got {actual}, expected {expected}")

# COMMAND ----------

assert all_ok, "Reconciliation failed. Read the FAIL lines above before trusting Silver."
print("Reconciliation passed: every source row is accounted for.")
