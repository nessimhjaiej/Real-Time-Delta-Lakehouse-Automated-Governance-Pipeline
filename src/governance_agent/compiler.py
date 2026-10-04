"""Compiles a structured QueryIntent into SQL via the semantic layer.

No free-form LLM-written SQL: the LLM (Stage 6) only ever produces a
QueryIntent -- a metric name, dimension names, filters, and an optional time
range, all validated against the SemanticCatalog before any SQL is built.
Filter *values* are embedded as sqlglot literal nodes (exp.convert), never
string-interpolated, so this stays injection-safe even though the values
will eventually come from LLM output. sqlglot re-parses the result in
Stage 3's validator as a second, independent check.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel
from sqlglot import exp
from sqlglot.errors import ParseError

from src.governance_agent.exceptions import InvalidFilterError, NoJoinPathError
from src.governance_agent.semantic.models import Dimension, SemanticCatalog

FilterOperator = Literal["=", "!=", ">", "<", ">=", "<=", "IN"]
FilterValue = str | int | float | bool

_COMPARISON_OPERATORS: dict[str, type[exp.Binary]] = {
    "=": exp.EQ,
    "!=": exp.NEQ,
    ">": exp.GT,
    "<": exp.LT,
    ">=": exp.GTE,
    "<=": exp.LTE,
}

DEFAULT_ROW_LIMIT = 100


class Filter(BaseModel):
    dimension: str
    operator: FilterOperator
    value: FilterValue | list[FilterValue]


class TimeRange(BaseModel):
    start: date | None = None
    end: date | None = None


class QueryIntent(BaseModel):
    """What the LLM (Stage 6) produces as structured output."""

    metric: str
    dimensions: list[str] = []
    filters: list[Filter] = []
    time_range: TimeRange | None = None
    limit: int = DEFAULT_ROW_LIMIT


def _parse_sql_fragment(text: str) -> exp.Expr:
    """Parse a developer-authored SQL fragment from semantic/*.yml.

    A parse failure means the YAML itself is malformed, not that the LLM
    sent something bad (Stage 2 has no LLM involvement yet) -- surfaced as
    our own error type so callers don't need to know about sqlglot's.
    """
    try:
        return exp.maybe_parse(text, into=exp.Expr)
    except ParseError as error:
        raise InvalidFilterError(
            f"Could not parse SQL fragment {text!r}: {error}"
        ) from error


def _dimension_expr(dimension: Dimension) -> exp.Expr:
    parsed = _parse_sql_fragment(dimension.column)
    # Qualify a plain column ("country") with its table so it can't collide
    # with a same-named column on another joined table. Computed expressions
    # ("CAST(timestamp AS DATE)") are left as authored.
    if isinstance(parsed, exp.Column) and not parsed.table:
        return exp.column(parsed.name, table=dimension.table)
    return parsed


def _filter_condition(filter_: Filter, dimension: Dimension) -> exp.Expr:
    column_expr = _dimension_expr(dimension)
    if filter_.operator == "IN":
        if not isinstance(filter_.value, list):
            raise InvalidFilterError(
                f"Filter on {filter_.dimension!r} uses IN but value is not a list: {filter_.value!r}"
            )
        return exp.In(
            this=column_expr, expressions=[exp.convert(v) for v in filter_.value]
        )
    if isinstance(filter_.value, list):
        raise InvalidFilterError(
            f"Filter on {filter_.dimension!r} uses {filter_.operator!r} but value is a list: {filter_.value!r}"
        )
    operator_cls = _COMPARISON_OPERATORS[filter_.operator]
    return operator_cls(this=column_expr, expression=exp.convert(filter_.value))


def compile_query(intent: QueryIntent, catalog: SemanticCatalog) -> exp.Select:
    """Build a sqlglot Select expression from a QueryIntent.

    Returns a dialect-neutral sqlglot expression tree; callers render it for
    a specific engine with ``.sql(dialect=...)`` (e.g. "duckdb").
    """
    metric = catalog.get_metric(intent.metric)
    dimensions = [catalog.get_dimension(name) for name in intent.dimensions]
    filter_dimensions = [catalog.get_dimension(f.dimension) for f in intent.filters]

    select_exprs: list[exp.Expr] = [_dimension_expr(d).as_(d.name) for d in dimensions]
    select_exprs.append(_parse_sql_fragment(metric.expression).as_(metric.name))

    query = exp.select(*select_exprs).from_(metric.table)

    # A filter can reference a dimension that isn't also grouped by (e.g.
    # "revenue in France" with no country column in the output), so every
    # table either a select-list dimension OR a filter needs is joined here.
    joined_tables = {metric.table}
    for dimension in {d.table: d for d in [*dimensions, *filter_dimensions]}.values():
        if dimension.table in joined_tables:
            continue
        join = catalog.find_join(metric.table, dimension.table)
        if join is None:
            raise NoJoinPathError(metric.table, dimension.table)
        on = exp.condition(
            f"{join.left_table}.{join.left_key} = {join.right_table}.{join.right_key}"
        )
        query = query.join(dimension.table, on=on, join_type=join.join_type.upper())
        joined_tables.add(dimension.table)

    conditions: list[exp.Expr] = [
        _filter_condition(f, d) for f, d in zip(intent.filters, filter_dimensions)
    ]

    if intent.time_range is not None and metric.time_column is not None:
        time_col = exp.column(metric.time_column, table=metric.table)
        if intent.time_range.start is not None:
            conditions.append(
                exp.GTE(this=time_col, expression=exp.convert(intent.time_range.start))
            )
        if intent.time_range.end is not None:
            conditions.append(
                exp.LTE(this=time_col, expression=exp.convert(intent.time_range.end))
            )

    if conditions:
        query = query.where(exp.and_(*conditions))

    if dimensions:
        query = query.group_by(*[_dimension_expr(d) for d in dimensions])

    query = query.limit(intent.limit)
    return query
