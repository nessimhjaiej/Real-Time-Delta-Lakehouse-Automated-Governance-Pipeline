import pytest

from src.pipelines.bronze import ingest
from src.pipelines.gold import aggregate_counts


def test_bronze_requires_source_name() -> None:
    with pytest.raises(ValueError, match="source_name"):
        ingest(None, " ")  # type: ignore[arg-type]


def test_gold_requires_dimension() -> None:
    with pytest.raises(ValueError, match="dimension"):
        aggregate_counts(None, [])  # type: ignore[arg-type]
