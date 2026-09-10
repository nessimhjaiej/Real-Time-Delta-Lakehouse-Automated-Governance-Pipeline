"""CSV batch extraction strategy."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .base import BaseExtractor


class CsvExtractor(BaseExtractor):
    """Load CSV data through Spark's batch reader."""

    def __init__(self, path: str | Path, **options: str) -> None:
        self.path = str(path)
        self.options = options

    def extract(self, spark: SparkSession) -> DataFrame:
        """Return CSV records from the configured path."""
        return spark.read.options(header=True, inferSchema=True, **self.options).csv(
            self.path
        )
