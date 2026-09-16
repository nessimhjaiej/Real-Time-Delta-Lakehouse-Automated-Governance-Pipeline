"""Delta Lake sink strategy."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from .base import BaseSink


class DeltaSink(BaseSink):
    """Persist data to a Delta Lake path or catalog table."""

    def __init__(
        self,
        path_or_table: str | Path,
        mode: str = "append",
        partition_by: str | list[str] | None = None,
        **options: str,
    ) -> None:
        self.target = str(path_or_table)
        self.mode = mode
        self.partition_by = partition_by or []
        self.options = options

    def write(self, df: DataFrame) -> None:
        """Write the DataFrame to the configured Delta target."""
        writer = df.write.format("delta").mode(self.mode).options(**self.options)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        if self._is_path_target():
            writer.save(self.target)
        else:
            writer.saveAsTable(self.target)

    def _is_path_target(self) -> bool:
        """Return whether the target identifies a filesystem path."""
        return self.target.startswith(("s3a://", "dbfs:/")) or "/" in self.target
