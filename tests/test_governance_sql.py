"""Tests for the governance SQL builder. Pure Python: runs without Spark."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from governance_sql import (  # noqa: E402
    MASKS, ROW_FILTER_FUNCTION, apply_statements, clear_role_statement, create_function_statements,
    remove_statements, set_role_statements,
)

TABLE = "workspace.healthcare.silver_patients"
PREFIX = "workspace.healthcare."


def test_every_sensitive_column_has_exactly_one_mask():
    columns = [m.column for m in MASKS]
    assert len(columns) == len(set(columns))
    assert set(columns) == {"first_name", "last_name", "health_card_no", "dob"}


def test_apply_statements_cover_all_masks_and_the_row_filter():
    stmts = apply_statements(TABLE, PREFIX)
    assert len(stmts) == len(MASKS) + 1
    assert f"ALTER TABLE {TABLE} ALTER COLUMN dob SET MASK {PREFIX}mask_dob" in stmts
    assert f"ALTER TABLE {TABLE} SET ROW FILTER {PREFIX}{ROW_FILTER_FUNCTION} ON (plan_id)" in stmts


def test_remove_statements_undo_every_policy():
    stmts = remove_statements(TABLE)
    assert f"ALTER TABLE {TABLE} DROP ROW FILTER" in stmts
    assert sum("DROP MASK" in s for s in stmts) == len(MASKS)


def test_functions_are_created_in_dependency_order():
    stmts = create_function_statements(PREFIX)
    assert "is_steward" in stmts[0] and all("is_steward" in s for s in stmts[1:4])
    for name in ("mask_name", "mask_health_card", "mask_dob", ROW_FILTER_FUNCTION):
        assert any(f"FUNCTION {PREFIX}{name}(" in s for s in stmts)


def test_temporary_functions_have_no_schema_prefix():
    assert all("TEMPORARY FUNCTION" in s for s in create_function_statements(temporary=True))


def test_steward_role_always_gets_scope_all():
    delete, insert = set_role_statements("t", "me@x.com", "steward", "PLAN-A")
    assert "'steward', 'ALL'" in insert and delete.startswith("DELETE FROM t")


def test_analyst_role_keeps_its_plan_scope():
    _, insert = set_role_statements("t", "me@x.com", "analyst", "PLAN-B")
    assert "'analyst', 'PLAN-B'" in insert


@pytest.mark.parametrize("role,scope", [("admin", "ALL"), ("analyst", "PLAN-A'; DROP TABLE t; --"),
                                        ("analyst", "plan-a"), ("analyst", "")])
def test_bad_roles_and_scopes_are_rejected(role, scope):
    with pytest.raises(ValueError):
        set_role_statements("t", "me@x.com", role, scope)


def test_quotes_in_an_email_are_escaped():
    _, insert = set_role_statements("t", "o'brien@x.com", "analyst", "PLAN-A")
    assert "'o''brien@x.com'" in insert
    assert "o'brien" not in clear_role_statement("t", "o'brien@x.com").replace("o''brien", "")
