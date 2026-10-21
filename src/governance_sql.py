"""SQL for column masks and a row filter on silver_patients (Week 4).

Pure Python that builds SQL strings, so the logic can be tested without Databricks:
the tests create the same functions as temporary SQL functions on a local Spark session.
Only the ALTER TABLE statements that attach policies need Unity Catalog.

How access is decided
- A small table, governance_roles(user_email, role, plan_scope), says who is who.
- steward: sees real values and every row.
- analyst: sees masked sensitive columns and only the rows of their plan_scope ('ALL' or one plan).
- Anyone not in the table is treated as an analyst with no plan, so they see masked values and no rows.
  Default deny: forgetting to add someone never exposes data.
- A user with several rows gets the union of their plan scopes, and one steward row makes them a steward.

Why a table: this workspace has no account console, so account groups cannot be used. In production you
would replace the table lookups with IS_ACCOUNT_GROUP_MEMBER('group') and manage access with groups.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

ROLES = ("steward", "analyst")
_SCOPE_PATTERN = re.compile(r"^(ALL|PLAN-[A-Z0-9]+)$")


@dataclass(frozen=True)
class Mask:
    column: str
    function: str
    sql_type: str  # the column's type: a mask function must take and return it


MASKS = [
    Mask("first_name", "mask_name", "STRING"),
    Mask("last_name", "mask_name", "STRING"),
    Mask("health_card_no", "mask_health_card", "STRING"),
    Mask("dob", "mask_dob", "DATE"),
]
ROW_FILTER_FUNCTION = "plan_row_filter"
ROW_FILTER_COLUMN = "plan_id"


def _q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def create_roles_table_statement(roles_table: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS {roles_table} "
            "(user_email STRING, role STRING, plan_scope STRING)")


def create_function_statements(prefix: str = "", roles_table: str = "governance_roles",
                               temporary: bool = False) -> list[str]:
    """CREATE FUNCTION statements, in dependency order (is_steward first)."""
    kind = "TEMPORARY FUNCTION" if temporary else "FUNCTION"
    f = lambda name: f"{prefix}{name}"  # noqa: E731
    mine = f"SELECT 1 FROM {roles_table} r WHERE r.user_email = SESSION_USER()"
    return [
        f"CREATE OR REPLACE {kind} {f('is_steward')}() RETURNS BOOLEAN "
        f"RETURN EXISTS ({mine} AND r.role = 'steward')",

        f"CREATE OR REPLACE {kind} {f('mask_name')}(val STRING) RETURNS STRING "
        f"RETURN CASE WHEN {f('is_steward')}() OR val IS NULL THEN val ELSE '***' END",

        # keeps the last four characters and hides the rest; a value of four characters or fewer is hidden entirely
        f"CREATE OR REPLACE {kind} {f('mask_health_card')}(val STRING) RETURNS STRING "
        f"RETURN CASE WHEN {f('is_steward')}() OR val IS NULL THEN val "
        f"WHEN length(val) <= 4 THEN repeat('*', length(val)) "
        f"ELSE concat(repeat('*', length(val) - 4), right(val, 4)) END",

        # keeps the year only
        f"CREATE OR REPLACE {kind} {f('mask_dob')}(val DATE) RETURNS DATE "
        f"RETURN CASE WHEN {f('is_steward')}() OR val IS NULL THEN val "
        f"ELSE make_date(year(val), 1, 1) END",

        f"CREATE OR REPLACE {kind} {f(ROW_FILTER_FUNCTION)}(plan STRING) RETURNS BOOLEAN "
        f"RETURN EXISTS ({mine} AND (r.plan_scope = 'ALL' OR r.plan_scope = plan))",
    ]


def apply_statements(table: str, prefix: str = "") -> list[str]:
    stmts = [f"ALTER TABLE {table} ALTER COLUMN {m.column} SET MASK {prefix}{m.function}" for m in MASKS]
    stmts.append(f"ALTER TABLE {table} SET ROW FILTER {prefix}{ROW_FILTER_FUNCTION} ON ({ROW_FILTER_COLUMN})")
    return stmts


def remove_statements(table: str) -> list[str]:
    """Drop policies before dropping any function: a table whose policy function is gone becomes unreadable."""
    stmts = [f"ALTER TABLE {table} ALTER COLUMN {m.column} DROP MASK" for m in MASKS]
    stmts.append(f"ALTER TABLE {table} DROP ROW FILTER")
    return stmts


def set_role_statements(roles_table: str, email: str, role: str, plan_scope: str = "ALL") -> list[str]:
    """Replace this user's rows with a single role. A steward always gets scope ALL."""
    if role not in ROLES:
        raise ValueError(f"role must be one of {ROLES}, got {role!r}")
    scope = "ALL" if role == "steward" else plan_scope
    if not _SCOPE_PATTERN.match(scope):
        raise ValueError(f"plan_scope must be ALL or a plan like PLAN-A, got {scope!r}")
    if not email or "\n" in email:
        raise ValueError("email is required")
    return [
        f"DELETE FROM {roles_table} WHERE user_email = {_q(email)}",
        f"INSERT INTO {roles_table} VALUES ({_q(email)}, {_q(role)}, {_q(scope)})",
    ]


def clear_role_statement(roles_table: str, email: str) -> str:
    return f"DELETE FROM {roles_table} WHERE user_email = {_q(email)}"
