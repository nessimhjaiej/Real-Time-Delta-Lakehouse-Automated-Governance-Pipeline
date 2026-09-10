"""JSON API extraction strategy."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .base import BaseExtractor


class ApiExtractor(BaseExtractor):
    """Load JSON records exposed at an API-compatible path."""

    def __init__(self, path: str | Path, **options: str) -> None:
        self.path = str(path)
        self.options = options

    def extract(self, spark: SparkSession) -> DataFrame:
        """Return JSON records from the configured path."""
        return spark.read.options(**self.options).json(self.path)
