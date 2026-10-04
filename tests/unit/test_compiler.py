from datetime import date

import pytest
import sqlglot

from src.governance_agent.compiler import Filter, QueryIntent, TimeRange, compile_query
from src.governance_agent.exceptions import (
    InvalidFilterError,
    NoJoinPathError,
    UnknownDimensionError,
    UnknownMetricError,
)
from src.governance_agent.semantic.loader import load_semantic_catalog
from src.governance_agent.semantic.models import (
    Dimension,
    Join,
    Metric,
    SemanticCatalog,
)


@pytest.fixture
def catalog() -> SemanticCatalog:
    """A small synthetic catalog, independent of the real semantic/ files,
    so these tests don't break if the real metric/dimension set changes."""
    return SemanticCatalog(
        metrics={
            "revenue": Metric(
                name="revenue",
                description="d",
                table="fact_orders",
                expression="SUM(total_amount)",
                time_column="timestamp",
            ),
        },
        dimensions={
            "country": Dimension(
                name="country", description="d", table="dim_customers", column="country"
            ),
            "category": Dimension(
                name="category",
                description="d",
                table="dim_products",
                column="category",
            ),
            "unreachable": Dimension(
                name="unreachable", description="d", table="orphan_table", column="x"
            ),
        },
        joins=[
            Join(
                left_table="fact_orders",
                left_key="customer_id",
                right_table="dim_customers",
                right_key="customer_id",
            ),
            Join(
                left_table="fact_orders",
                left_key="product_id",
                right_table="dim_products",
                right_key="product_id",
            ),
        ],
    )


def _sql(intent: QueryIntent, catalog: SemanticCatalog) -> str:
    query = compile_query(intent, catalog)
    rendered = query.sql(dialect="duckdb")
    sqlglot.parse_one(rendered, dialect="duckdb")  # must round-trip cleanly
    return rendered


def test_metric_only(catalog: SemanticCatalog) -> None:
    sql = _sql(QueryIntent(metric="revenue"), catalog)

    assert "SUM(total_amount) AS revenue" in sql
    assert "FROM fact_orders" in sql
    assert "JOIN" not in sql
    assert "LIMIT 100" in sql


def test_metric_with_one_dimension_joins_its_table(catalog: SemanticCatalog) -> None:
    sql = _sql(QueryIntent(metric="revenue", dimensions=["country"]), catalog)

    assert "dim_customers.country AS country" in sql
    assert (
        "LEFT JOIN dim_customers ON fact_orders.customer_id = dim_customers.customer_id"
        in sql
    )
    assert "GROUP BY dim_customers.country" in sql


def test_metric_with_two_dimensions_from_different_tables_joins_both(
    catalog: SemanticCatalog,
) -> None:
    sql = _sql(
        QueryIntent(metric="revenue", dimensions=["country", "category"]), catalog
    )

    assert "LEFT JOIN dim_customers" in sql
    assert "LEFT JOIN dim_products" in sql
    assert sql.count("JOIN") == 2


def test_filter_on_dimension_not_in_group_by_still_joins_its_table(
    catalog: SemanticCatalog,
) -> None:
    # Bug caught during Stage 2 smoke testing: filtering on a dimension that
    # isn't also being grouped by must still join that dimension's table.
    intent = QueryIntent(
        metric="revenue",
        filters=[Filter(dimension="country", operator="=", value="France")],
    )

    sql = _sql(intent, catalog)

    assert "LEFT JOIN dim_customers" in sql
    assert "WHERE dim_customers.country = 'France'" in sql
    assert "GROUP BY" not in sql  # no dimensions requested, so no group-by


def test_in_filter(catalog: SemanticCatalog) -> None:
    intent = QueryIntent(
        metric="revenue",
        filters=[
            Filter(dimension="country", operator="IN", value=["France", "Germany"])
        ],
    )

    sql = _sql(intent, catalog)

    assert "dim_customers.country IN ('France', 'Germany')" in sql


def test_in_filter_rejects_non_list_value(catalog: SemanticCatalog) -> None:
    intent = QueryIntent(
        metric="revenue",
        filters=[Filter(dimension="country", operator="IN", value="France")],
    )

    with pytest.raises(InvalidFilterError):
        compile_query(intent, catalog)


def test_comparison_filter_rejects_list_value(catalog: SemanticCatalog) -> None:
    intent = QueryIntent(
        metric="revenue",
        filters=[Filter(dimension="country", operator="=", value=["France"])],
    )

    with pytest.raises(InvalidFilterError):
        compile_query(intent, catalog)


def test_filter_value_with_quote_is_escaped_not_interpolated(
    catalog: SemanticCatalog,
) -> None:
    # Guards against SQL injection: a value containing a single quote must
    # come back as a properly escaped/parameterized literal, not raw text
    # that could break out of the string.
    intent = QueryIntent(
        metric="revenue",
        filters=[Filter(dimension="country", operator="=", value="France' OR '1'='1")],
    )

    sql = _sql(intent, catalog)

    parsed = sqlglot.parse_one(sql, dialect="duckdb")
    where = parsed.find(sqlglot.exp.Where)
    assert where is not None
    literal = where.find(sqlglot.exp.Literal)
    assert literal is not None
    assert literal.this == "France' OR '1'='1"
    # exactly one WHERE / one comparison -- the quote did not terminate the
    # string early and inject a second condition
    assert len(list(parsed.find_all(sqlglot.exp.Where))) == 1


def test_time_range_filters_on_the_metrics_time_column(
    catalog: SemanticCatalog,
) -> None:
    intent = QueryIntent(
        metric="revenue",
        time_range=TimeRange(start=date(2026, 1, 1), end=date(2026, 6, 30)),
    )

    sql = _sql(intent, catalog)

    assert "fact_orders.timestamp >= CAST('2026-01-01' AS DATE)" in sql
    assert "fact_orders.timestamp <= CAST('2026-06-30' AS DATE)" in sql


def test_time_range_is_ignored_when_metric_has_no_time_column(
    catalog: SemanticCatalog,
) -> None:
    catalog.metrics["no_time"] = Metric(
        name="no_time", description="d", table="fact_orders", expression="COUNT(*)"
    )
    intent = QueryIntent(metric="no_time", time_range=TimeRange(start=date(2026, 1, 1)))

    sql = _sql(intent, catalog)

    assert "WHERE" not in sql


def test_open_ended_time_range_only_applies_the_given_bound(
    catalog: SemanticCatalog,
) -> None:
    intent = QueryIntent(metric="revenue", time_range=TimeRange(start=date(2026, 1, 1)))

    sql = _sql(intent, catalog)

    assert ">=" in sql
    assert "<=" not in sql


def test_unknown_metric_raises(catalog: SemanticCatalog) -> None:
    with pytest.raises(UnknownMetricError):
        compile_query(QueryIntent(metric="does_not_exist"), catalog)


def test_unknown_dimension_raises(catalog: SemanticCatalog) -> None:
    with pytest.raises(UnknownDimensionError):
        compile_query(
            QueryIntent(metric="revenue", dimensions=["does_not_exist"]), catalog
        )


def test_unknown_filter_dimension_raises(catalog: SemanticCatalog) -> None:
    intent = QueryIntent(
        metric="revenue",
        filters=[Filter(dimension="does_not_exist", operator="=", value="x")],
    )

    with pytest.raises(UnknownDimensionError):
        compile_query(intent, catalog)


def test_dimension_with_no_join_path_raises(catalog: SemanticCatalog) -> None:
    intent = QueryIntent(metric="revenue", dimensions=["unreachable"])

    with pytest.raises(NoJoinPathError):
        compile_query(intent, catalog)


def test_default_and_custom_row_limit(catalog: SemanticCatalog) -> None:
    assert "LIMIT 100" in _sql(QueryIntent(metric="revenue"), catalog)
    assert "LIMIT 5" in _sql(QueryIntent(metric="revenue", limit=5), catalog)


def test_compiles_against_the_real_semantic_catalog() -> None:
    real_catalog = load_semantic_catalog()

    sql = _sql(
        QueryIntent(
            metric="total_revenue", dimensions=["customer_country", "product_category"]
        ),
        real_catalog,
    )

    assert "dim_customers.country AS customer_country" in sql
    assert "dim_products.category AS product_category" in sql
    assert "SUM(total_amount) AS total_revenue" in sql
