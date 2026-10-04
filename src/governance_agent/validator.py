"""sqlglot-based SQL validator -- the actual enforcement boundary.

Takes a raw SQL *string* and re-parses it independently, rather than
trusting a pre-built sqlglot expression from compiler.py. That's
deliberate: this is the second, independent check design decision #2 asks
for, and it's also the only way to meaningfully reject stacked statements
("SELECT 1; DROP TABLE x") -- by the time SQL exists as a single
exp.Select object, stacking has already been ruled out by construction.

Checks, in order: parses cleanly; exactly one statement; that statement is
a SELECT; no comments; no SELECT *; every referenced table is on the
role's allowlist; every SELECT-list and WHERE/HAVING column is allowed and
never PII; a LIMIT is present and within the role's max_rows.
"""

from __future__ import annotations

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.errors import SqlglotError

from src.governance_agent.policy import RolePolicy

DEFAULT_DIALECT = "duckdb"


class ValidationResult(BaseModel):
    allowed: bool
    violations: list[str] = []

    @property
    def reason(self) -> str:
        return "; ".join(self.violations)


def _blocked(violations: list[str]) -> ValidationResult:
    return ValidationResult(allowed=False, violations=violations)


# checking columns allowed based on role . hardcoded .
def _check_columns(
    columns: list[exp.Column], from_table: str, policy: RolePolicy, context: str
) -> list[str]:
    violations = []
    for column in columns:
        table = column.table or from_table
        if policy.is_pii_column(table, column.name):
            violations.append(
                f"PII column not allowed {context}: {table}.{column.name}"
            )
        elif context == "in SELECT" and not policy.is_allowed_column(
            table, column.name
        ):
            violations.append(
                f"Column not allowed for this role: {table}.{column.name}"
            )
    return violations


# NO STACKED QUEIRES ALLOWED   .
def validate_query(
    sql: str, policy: RolePolicy, dialect: str = DEFAULT_DIALECT
) -> ValidationResult:
    try:
        statements = [s for s in sqlglot.parse(sql, dialect=dialect) if s is not None]
    except SqlglotError as error:
        return _blocked([f"SQL failed to parse: {error}"])

    if len(statements) != 1:
        return _blocked(
            [
                f"Expected exactly one statement, found {len(statements)} "
                "(stacked statements are not allowed)"
            ]
        )

    statement = statements[0]
    if (
        not isinstance(statement, exp.Select)
        or "SELECT" not in policy.allowed_operations
    ):
        return _blocked(
            [f"Only SELECT statements are allowed, got {type(statement).__name__}"]
        )

    violations: list[str] = []

    if any(node.comments for node in statement.walk()):
        violations.append("Comments are not allowed in the query")

    if any(isinstance(e, exp.Star) for e in statement.selects):
        violations.append(
            "SELECT * is not allowed; the metric/dimension list must be explicit"
        )

    from_node = statement.find(exp.From)
    if from_node is None:
        return _blocked([*violations, "Query has no FROM clause"])
    from_table = from_node.this.name

    referenced_tables = {t.name for t in statement.find_all(exp.Table)}
    disallowed_tables = referenced_tables - set(policy.allowed_tables)
    if disallowed_tables:
        violations.append(
            f"Table(s) not allowed for this role: {sorted(disallowed_tables)}"
        )

    for projection in statement.selects:
        violations.extend(
            _check_columns(
                list(projection.find_all(exp.Column)), from_table, policy, "in SELECT"
            )
        )

    for clause_type in (exp.Where, exp.Having):
        clause = statement.find(clause_type)
        if clause is None:
            continue
        violations.extend(
            _check_columns(
                list(clause.find_all(exp.Column)), from_table, policy, "in a filter"
            )
        )

    limit_node = statement.find(exp.Limit)
    if limit_node is None:
        violations.append("Query has no LIMIT")
    else:
        limit_value = limit_node.expression
        if isinstance(limit_value, exp.Literal) and not limit_value.is_string:
            if int(limit_value.this) > policy.max_rows:
                violations.append(
                    f"LIMIT {limit_value.this} exceeds role's max_rows ({policy.max_rows})"
                )
        else:
            violations.append("LIMIT must be a literal number")

    return ValidationResult(allowed=not violations, violations=violations)
