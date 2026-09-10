"""Base interface for Spark extraction strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class BaseExtractor(ABC):
    """Load records into a Spark DataFrame."""

    @abstractmethod
    def extract(self, spark: SparkSession) -> DataFrame:
        """Extract records using the supplied Spark session."""
