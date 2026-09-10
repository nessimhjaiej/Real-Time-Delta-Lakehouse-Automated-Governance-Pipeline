"""Factory for selecting extraction strategies."""

from __future__ import annotations

from .api_extractor import ApiExtractor
from .base import BaseExtractor
from .csv_extractor import CsvExtractor
from .kafka_extractor import KafkaExtractor


class ExtractorFactory:
    """Create extractors from a source kind and connection details."""

    @staticmethod
    def create(kind: str, location_or_server: str, **options: str) -> BaseExtractor:
        """Create the extractor matching ``kind``."""
        kind_lower = kind.lower()
        if kind_lower == "csv":
            return CsvExtractor(location_or_server, **options)
        if kind_lower == "api":
            return ApiExtractor(location_or_server, **options)
        if kind_lower == "kafka":
            topic = options.pop("topic", "ecommerce.orders.v1")
            return KafkaExtractor(
                bootstrap_servers=location_or_server,
                topic=topic,
                **options,
            )
        raise ValueError(f"Unsupported extractor type: {kind}")
