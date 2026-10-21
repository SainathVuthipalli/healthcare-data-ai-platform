# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Governance: column masks and a row filter on `silver_patients`
# MAGIC
# MAGIC **Goal:** protect the sensitive patient columns so that who you are decides what you see.
# MAGIC
# MAGIC | Role | Sees |
# MAGIC | --- | --- |
# MAGIC | `steward` | real values, every row |
# MAGIC | `analyst` | names hidden (`***`), health card number shows only the last 4, date of birth shows only the year, and only the rows of their plan scope |
# MAGIC | not in the roles table | masked values and **no rows** (default deny) |
# MAGIC
# MAGIC **Why a roles table:** this workspace has no account console, so account groups are not available. The mask and filter functions look you up in `governance_roles` instead. In production you would replace that lookup with `IS_ACCOUNT_GROUP_MEMBER('group')`.
# MAGIC
# MAGIC **Important:** the pipeline (notebooks 04 and 05) must run as a `steward`. A row filter would otherwise hide patients from it and quietly change Gold. This notebook always ends by restoring you to `steward`, and notebook 05 stops if it cannot see every patient.
# MAGIC
# MAGIC The logic of every function was tested on a local Spark session. The `ALTER TABLE ... SET MASK` step needs Unity Catalog, so this notebook is where it is proven for real.

# COMMAND ----------

import os
import sys

SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if not os.path.isdir(SRC_DIR):
    SRC_DIR = os.getcwd()
sys.path.insert(0, SRC_DIR)

import governance_sql as gov
import platform_config as cfg

ME = spark.sql("SELECT session_user()").first()[0]
print("you are:", ME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Roles table and functions
# MAGIC
# MAGIC The roles table must exist first, because the functions read it. You start as a steward.

# COMMAND ----------

def set_role(role, plan_scope="ALL"):
    for stmt in gov.set_role_statements(cfg.GOVERNANCE_ROLES, ME, role, plan_scope):
        spark.sql(stmt)


spark.sql(gov.create_roles_table_statement(cfg.GOVERNANCE_ROLES))
set_role("steward")

for stmt in gov.create_function_statements(prefix=cfg.FUNCTION_PREFIX, roles_table=cfg.GOVERNANCE_ROLES):
    spark.sql(stmt)

print("is_steward() ->", spark.sql(f"SELECT {cfg.FUNCTION_PREFIX}is_steward()").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC **Stop here if the cell above failed.** If Unity Catalog rejects a lookup table inside a function, tell me the exact error. A fallback is to compare `SESSION_USER()` with your email directly inside each function.
# MAGIC Expected: `is_steward() -> True`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Attach the policies
# MAGIC
# MAGIC The table must exist (run notebook 04 first). Re-running this cell is safe.

# COMMAND ----------

for stmt in gov.apply_statements(cfg.SILVER_PATIENTS, prefix=cfg.FUNCTION_PREFIX):
    spark.sql(stmt)
    print("ok:", stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prove it, one role at a time
# MAGIC
# MAGIC The expected row counts come from `bronze_patients`, which has no policy, so the check does not depend on the thing being tested.

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(cfg.BRONZE_PATIENTS)
total_patients = bronze.select("patient_id").distinct().count()
plan_a_patients = bronze.filter(F.col("plan_id") == "PLAN-A").select("patient_id").distinct().count()
print("patients in total:", total_patients, "| on PLAN-A:", plan_a_patients)


def look(label):
    df = spark.table(cfg.SILVER_PATIENTS)
    print(f"\n--- {label} ---")
    df.select("patient_id", "first_name", "last_name", "health_card_no", "dob", "plan_id").show(3, truncate=False)
    return df


results = {}
try:
    # steward: everything, unmasked
    set_role("steward")
    df = look("steward")
    results["steward rows"] = (df.count(), total_patients)
    results["steward sees no masked names"] = (df.filter(F.col("first_name") == "***").count(), 0)

    # analyst limited to one plan
    set_role("analyst", "PLAN-A")
    df = look("analyst, PLAN-A only")
    results["analyst rows (PLAN-A only)"] = (df.count(), plan_a_patients)
    results["analyst plans seen"] = (sorted(r[0] for r in df.select("plan_id").distinct().collect()), ["PLAN-A"])
    results["analyst first names masked"] = (sorted(r[0] for r in df.select("first_name").distinct().collect()), ["***"])
    results["analyst last names masked"] = (sorted(r[0] for r in df.select("last_name").distinct().collect()), ["***"])
    results["analyst health card shows last 4 only"] = (df.filter(~F.col("health_card_no").rlike(r"^\*+[0-9]{4}$")).count(), 0)
    results["analyst dob is the year only"] = (df.filter(~((F.month("dob") == 1) & (F.dayofmonth("dob") == 1))).count(), 0)

    # analyst on every plan: all rows, still masked
    set_role("analyst", "ALL")
    df = look("analyst, ALL plans")
    results["analyst ALL rows"] = (df.count(), total_patients)
    results["analyst ALL still masked"] = (df.filter(F.col("first_name") != "***").count(), 0)

    # no role at all: default deny
    spark.sql(gov.clear_role_statement(cfg.GOVERNANCE_ROLES, ME))
    df = look("no role")
    results["no role sees zero rows"] = (df.count(), 0)
finally:
    set_role("steward")  # always leave the pipeline able to see everything

for name, (actual, expected) in results.items():
    print(f"[{'PASS' if actual == expected else 'FAIL'}] {name}: got {actual}, expected {expected}")

assert all(a == e for a, e in results.values()), "A governance check failed. Read the FAIL lines above."
print("\nGovernance works: each role sees exactly what it should. You are back to steward.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The pipeline still works
# MAGIC
# MAGIC As a steward, Silver sees every patient, so notebook 05 will pass its guard and Gold is unchanged.

# COMMAND ----------

print("you can see", spark.table(cfg.SILVER_PATIENTS).count(), "of", total_patients, "patients")
assert spark.table(cfg.SILVER_PATIENTS).count() == total_patients

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Take a screenshot for the README
# MAGIC
# MAGIC Switch to analyst and show the masked table: run the next cell, screenshot the result, then run the one after it to switch back.

# COMMAND ----------

set_role("analyst", "PLAN-A")
display(spark.table(cfg.SILVER_PATIENTS).limit(10))

# COMMAND ----------

set_role("steward")
display(spark.table(cfg.SILVER_PATIENTS).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Undo (only if you want to remove the policies)
# MAGIC
# MAGIC Set `RUN_UNDO = True` and run the cell. Policies are dropped before anything else, because a table whose policy function is missing becomes unreadable.

# COMMAND ----------

RUN_UNDO = False

if RUN_UNDO:
    for stmt in gov.remove_statements(cfg.SILVER_PATIENTS):
        spark.sql(stmt)
        print("ok:", stmt)
else:
    print("Not running undo. Policies stay in place.")
