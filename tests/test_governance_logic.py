"""Runs the real mask and row-filter functions on local Spark, with a temp view as the roles table.

Skipped automatically when pyspark is not installed. Only the ALTER TABLE ... SET MASK step needs Unity
Catalog; the logic that decides what each role sees is exactly what is tested here.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from gold_rules import build_silver_patients  # noqa: E402
from governance_sql import MASKS, create_function_statements  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("governance-tests")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    # the roles table must exist before the functions that read it are created (as in notebook 06)
    session.sql("CREATE OR REPLACE TEMP VIEW governance_roles AS "
                "SELECT CAST(NULL AS STRING) AS user_email, CAST(NULL AS STRING) AS role, "
                "CAST(NULL AS STRING) AS plan_scope WHERE 1 = 0")
    for stmt in create_function_statements(temporary=True):
        session.sql(stmt)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def me(spark):
    return spark.sql("SELECT session_user()").first()[0]


def set_roles(spark, rows):
    schema = StructType([StructField(c, StringType()) for c in ("user_email", "role", "plan_scope")])
    spark.createDataFrame(rows, schema).createOrReplaceTempView("governance_roles")


def one(spark, expr):
    return spark.sql(f"SELECT {expr}").first()[0]


HEALTH = "'SYN-12345678'"


def test_steward_sees_real_values_and_every_plan(spark, me):
    set_roles(spark, [(me, "steward", "ALL")])
    assert one(spark, "is_steward()") is True
    assert one(spark, "mask_name('Aiden')") == "Aiden"
    assert one(spark, f"mask_health_card({HEALTH})") == "SYN-12345678"
    assert one(spark, "mask_dob(DATE '1980-05-17')") == date(1980, 5, 17)
    assert one(spark, "plan_row_filter('PLAN-A')") is True and one(spark, "plan_row_filter('PLAN-C')") is True


def test_analyst_sees_masked_values_and_only_their_plan(spark, me):
    set_roles(spark, [(me, "analyst", "PLAN-A")])
    assert one(spark, "is_steward()") is False
    assert one(spark, "mask_name('Aiden')") == "***"
    assert one(spark, f"mask_health_card({HEALTH})") == "********5678"
    assert one(spark, "mask_dob(DATE '1980-05-17')") == date(1980, 1, 1)
    assert one(spark, "plan_row_filter('PLAN-A')") is True and one(spark, "plan_row_filter('PLAN-B')") is False


def test_analyst_with_scope_all_sees_every_row_but_still_masked(spark, me):
    set_roles(spark, [(me, "analyst", "ALL")])
    assert one(spark, "plan_row_filter('PLAN-C')") is True
    assert one(spark, "mask_name('Aiden')") == "***"


def test_user_with_no_role_sees_nothing_and_masked_values(spark):
    set_roles(spark, [("someone.else@example.com", "steward", "ALL")])
    assert one(spark, "is_steward()") is False
    assert one(spark, "mask_name('Aiden')") == "***"
    assert one(spark, "plan_row_filter('PLAN-A')") is False


def test_empty_roles_table_denies_everyone(spark):
    set_roles(spark, [])
    assert one(spark, "is_steward()") is False
    assert one(spark, "plan_row_filter('PLAN-A')") is False


def test_several_analyst_rows_give_the_union_of_plans(spark, me):
    set_roles(spark, [(me, "analyst", "PLAN-A"), (me, "analyst", "PLAN-B")])
    assert [one(spark, f"plan_row_filter('{p}')") for p in ("PLAN-A", "PLAN-B", "PLAN-C")] == [True, True, False]


def test_one_steward_row_wins_over_analyst_rows(spark, me):
    set_roles(spark, [(me, "analyst", "PLAN-A"), (me, "steward", "ALL")])
    assert one(spark, "is_steward()") is True and one(spark, "mask_name('Aiden')") == "Aiden"


def test_nulls_pass_through_every_mask(spark, me):
    set_roles(spark, [(me, "analyst", "PLAN-A")])
    assert one(spark, "mask_name(CAST(NULL AS STRING))") is None
    assert one(spark, "mask_health_card(CAST(NULL AS STRING))") is None
    assert one(spark, "mask_dob(CAST(NULL AS DATE))") is None


def test_short_health_card_numbers_are_hidden_entirely(spark, me):
    set_roles(spark, [(me, "analyst", "PLAN-A")])
    assert one(spark, "mask_health_card('123')") == "***"
    assert one(spark, "mask_health_card('1234')") == "****"
    assert one(spark, "mask_health_card('12345')") == "*2345"  # 5 characters: hide 1, keep the last 4
    assert one(spark, "mask_health_card('')") == ""


def test_changing_the_roles_table_changes_what_is_seen_immediately(spark, me):
    set_roles(spark, [(me, "steward", "ALL")])
    assert one(spark, "mask_name('Aiden')") == "Aiden"
    set_roles(spark, [(me, "analyst", "PLAN-A")])
    assert one(spark, "mask_name('Aiden')") == "***"


def test_masks_work_on_a_whole_table_the_way_the_policy_would(spark, me):
    """Apply each mask expression over silver_patients-shaped data and check the result columns."""
    bronze = spark.createDataFrame(
        [("P1", "SYN-11112222", "Aiden", "Nguyen", "1980-05-17", "F", "M1B", "PLAN-A", "p.csv"),
         ("P2", "SYN-33334444", "Priya", "Patel", "1975-11-02", "F", "M5V", "PLAN-B", "p.csv")],
        StructType([StructField(c, StringType()) for c in
                    ("patient_id", "health_card_no", "first_name", "last_name", "dob", "sex",
                     "postal_prefix", "plan_id", "_source_file")]))
    build_silver_patients(bronze).createOrReplaceTempView("silver_patients_test")
    set_roles(spark, [(me, "analyst", "PLAN-A")])
    rows = spark.sql(
        "SELECT mask_name(first_name) AS first_name, mask_name(last_name) AS last_name, "
        "mask_health_card(health_card_no) AS health_card_no, mask_dob(dob) AS dob, plan_id "
        "FROM silver_patients_test WHERE plan_row_filter(plan_id)").collect()
    assert len(rows) == 1
    r = rows[0]
    assert (r["first_name"], r["last_name"], r["health_card_no"], r["dob"], r["plan_id"]) == \
           ("***", "***", "********2222", date(1980, 1, 1), "PLAN-A")


def test_mask_parameter_types_match_the_silver_patients_columns(spark):
    """A mask function must take and return its column's type."""
    bronze = spark.createDataFrame(
        [("P1", "SYN-1", "A", "B", "1980-05-17", "F", "M1B", "PLAN-A", "p.csv")],
        StructType([StructField(c, StringType()) for c in
                    ("patient_id", "health_card_no", "first_name", "last_name", "dob", "sex",
                     "postal_prefix", "plan_id", "_source_file")]))
    types = {f.name: f.dataType.simpleString().upper() for f in build_silver_patients(bronze).schema.fields}
    for m in MASKS:
        assert types[m.column] == m.sql_type, m.column
