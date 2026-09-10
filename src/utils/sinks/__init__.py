"""Strategies for persisting Spark DataFrames."""

from .base import BaseSink
from .delta_sink import DeltaSink
from .factory import SinkFactory
from .parquet_sink import ParquetSink

# Retained for callers of the previous public API.
Sink = BaseSink

__all__ = ["BaseSink", "DeltaSink", "ParquetSink", "Sink", "SinkFactory"]
