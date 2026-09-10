"""Base interface for Spark sink strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class BaseSink(ABC):
    """Persist a Spark DataFrame."""

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Persist the supplied DataFrame."""
