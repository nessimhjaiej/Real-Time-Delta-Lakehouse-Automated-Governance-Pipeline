"""Strategies for loading data into Spark DataFrames."""

from .api_extractor import ApiExtractor
from .base import BaseExtractor
from .csv_extractor import CsvExtractor
from .factory import ExtractorFactory
from .kafka_extractor import KafkaExtractor

# Retained for callers of the previous public API.
Extractor = BaseExtractor

__all__ = [
    "ApiExtractor",
    "BaseExtractor",
    "CsvExtractor",
    "Extractor",
    "ExtractorFactory",
    "KafkaExtractor",
]
