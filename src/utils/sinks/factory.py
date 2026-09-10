"""Factory for selecting sink strategies."""

from __future__ import annotations

from .base import BaseSink
from .delta_sink import DeltaSink
from .parquet_sink import ParquetSink


class SinkFactory:
    """Create sinks from a target kind and destination details."""

    @staticmethod
    def create(kind: str, location_or_table: str, **options: str) -> BaseSink:
        """Create the sink matching ``kind``."""
        kind_lower = kind.lower()
        mode = options.pop("mode", "append")
        partition_by = options.pop("partition_by", None)

        if kind_lower == "parquet":
            return ParquetSink(location_or_table, mode=mode, **options)
        if kind_lower == "delta":
            return DeltaSink(
                location_or_table,
                mode=mode,
                partition_by=partition_by,
                **options,
            )
        raise ValueError(f"Unsupported sink type: {kind}")
