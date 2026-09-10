"""Gold layer analytical aggregates."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def aggregate_counts(df: DataFrame, dimensions: Sequence[str]) -> DataFrame:
    """Produce record counts grouped by the requested business dimensions."""
    if not dimensions:
        raise ValueError("at least one aggregation dimension is required")
    missing = set(dimensions).difference(df.columns)
    if missing:
        raise ValueError(
            f"Unknown aggregation dimensions: {', '.join(sorted(missing))}"
        )
    return df.groupBy(*dimensions).agg(f.count(f.lit(1)).alias("record_count"))
