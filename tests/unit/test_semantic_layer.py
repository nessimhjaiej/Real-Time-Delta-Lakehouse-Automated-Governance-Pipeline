from pathlib import Path

import pytest
import yaml

from src.governance_agent.exceptions import (
    DuplicateDimensionError,
    DuplicateMetricError,
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


def _write_semantic_dir(
    tmp_path: Path,
    metrics: list[dict] | None = None,
    dimensions: list[dict] | None = None,
    joins: list[dict] | None = None,
) -> Path:
    (tmp_path / "metrics").mkdir()
    (tmp_path / "dimensions").mkdir()
    (tmp_path / "metrics" / "m.yml").write_text(
        yaml.dump({"metrics": metrics or []}), encoding="utf-8"
    )
    (tmp_path / "dimensions" / "d.yml").write_text(
        yaml.dump({"dimensions": dimensions or []}), encoding="utf-8"
    )
    if joins is not None:
        (tmp_path / "joins.yml").write_text(
            yaml.dump({"joins": joins}), encoding="utf-8"
        )
    return tmp_path


def test_loads_the_real_semantic_directory() -> None:
    catalog = load_semantic_catalog()

    assert set(catalog.metrics) == {
        "total_revenue",
        "order_count",
        "order_line_count",
        "total_quantity_sold",
        "average_order_value",
    }
    assert set(catalog.dimensions) == {
        "customer_country",
        "product_category",
        "product_title",
        "order_date",
    }
    assert len(catalog.joins) == 2
    assert all(m.table == "fact_orders" for m in catalog.metrics.values())


def test_no_pii_columns_are_exposed_as_dimensions() -> None:
    # dim_customers has full_name/email/ip_address/customer_id -- none of
    # those should ever be selectable as a group-by dimension (see Stage 1
    # PII inventory). Only country is safe.
    catalog = load_semantic_catalog()

    customer_dims = [
        d for d in catalog.dimensions.values() if d.table == "dim_customers"
    ]
    assert [d.column for d in customer_dims] == ["country"]


def test_loader_builds_catalog_from_multiple_files(tmp_path: Path) -> None:
    semantic_dir = _write_semantic_dir(
        tmp_path,
        metrics=[
            {"name": "m1", "description": "d", "table": "t", "expression": "SUM(x)"}
        ],
        dimensions=[{"name": "d1", "description": "d", "table": "t2", "column": "c"}],
        joins=[
            {
                "left_table": "t",
                "left_key": "id",
                "right_table": "t2",
                "right_key": "id",
            }
        ],
    )

    catalog = load_semantic_catalog(semantic_dir)

    assert catalog.get_metric("m1").expression == "SUM(x)"
    assert catalog.get_dimension("d1").column == "c"
    assert catalog.find_join("t", "t2") is not None
    assert catalog.find_join("t2", "t") is not None  # direction-agnostic lookup
    assert catalog.find_join("t", "unrelated") is None


def test_loader_rejects_duplicate_metric_names(tmp_path: Path) -> None:
    (tmp_path / "metrics").mkdir()
    (tmp_path / "dimensions").mkdir()
    metric = {"name": "dup", "description": "d", "table": "t", "expression": "SUM(x)"}
    (tmp_path / "metrics" / "a.yml").write_text(
        yaml.dump({"metrics": [metric]}), encoding="utf-8"
    )
    (tmp_path / "metrics" / "b.yml").write_text(
        yaml.dump({"metrics": [metric]}), encoding="utf-8"
    )
    (tmp_path / "dimensions" / "d.yml").write_text(
        yaml.dump({"dimensions": []}), encoding="utf-8"
    )

    with pytest.raises(DuplicateMetricError):
        load_semantic_catalog(tmp_path)


def test_loader_rejects_duplicate_dimension_names(tmp_path: Path) -> None:
    (tmp_path / "metrics").mkdir()
    (tmp_path / "dimensions").mkdir()
    dimension = {"name": "dup", "description": "d", "table": "t", "column": "c"}
    (tmp_path / "metrics" / "m.yml").write_text(
        yaml.dump({"metrics": []}), encoding="utf-8"
    )
    (tmp_path / "dimensions" / "a.yml").write_text(
        yaml.dump({"dimensions": [dimension]}), encoding="utf-8"
    )
    (tmp_path / "dimensions" / "b.yml").write_text(
        yaml.dump({"dimensions": [dimension]}), encoding="utf-8"
    )

    with pytest.raises(DuplicateDimensionError):
        load_semantic_catalog(tmp_path)


def test_loader_tolerates_missing_joins_file(tmp_path: Path) -> None:
    semantic_dir = _write_semantic_dir(tmp_path, joins=None)

    catalog = load_semantic_catalog(semantic_dir)

    assert catalog.joins == []


def test_catalog_lookup_errors_are_typed_and_carry_the_name() -> None:
    catalog = SemanticCatalog(metrics={}, dimensions={}, joins=[])

    with pytest.raises(UnknownMetricError) as excinfo:
        catalog.get_metric("nope")
    assert excinfo.value.name == "nope"

    with pytest.raises(UnknownDimensionError) as excinfo:
        catalog.get_dimension("nope")
    assert excinfo.value.name == "nope"


def test_models_are_plain_pydantic_and_round_trip() -> None:
    metric = Metric(name="m", description="d", table="t", expression="SUM(x)")
    dimension = Dimension(name="d", description="d", table="t2", column="c")
    join = Join(left_table="t", left_key="id", right_table="t2", right_key="id")

    assert metric.time_column is None
    assert join.join_type == "left"
    assert Metric.model_validate(metric.model_dump()) == metric
    assert Dimension.model_validate(dimension.model_dump()) == dimension
