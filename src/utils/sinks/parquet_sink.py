"""Parquet sink strategy."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from .base import BaseSink


class ParquetSink(BaseSink):
    """Persist data as Parquet files."""

    def __init__(self, path: str | Path, mode: str = "append", **options: str) -> None:
        self.path = str(path)
        self.mode = mode
        self.options = options

    def write(self, df: DataFrame) -> None:
        """Write the DataFrame to the configured Parquet path."""
        df.write.mode(self.mode).options(**self.options).parquet(self.path)
