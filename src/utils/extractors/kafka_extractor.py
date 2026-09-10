"""Kafka streaming extraction strategy."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .base import BaseExtractor


class KafkaExtractor(BaseExtractor):
    """Read a Kafka topic into a structured streaming DataFrame."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        starting_offsets: str = "earliest",
        **options: str,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.starting_offsets = starting_offsets
        self.options = options

    def extract(self, spark: SparkSession) -> DataFrame:
        """Return the configured Kafka topic as a streaming DataFrame."""
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .options(**self.options)
            .load()
        )
