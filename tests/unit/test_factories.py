import pytest

from src.utils.extractors import BaseExtractor, CsvExtractor, ExtractorFactory
from src.utils.extractors.csv_extractor import CsvExtractor as CsvExtractorModule
from src.utils.sinks import BaseSink, ParquetSink, SinkFactory
from src.utils.sinks.parquet_sink import ParquetSink as ParquetSinkModule


def test_csv_extractor_factory_creates_csv_extractor() -> None:
    extractor = ExtractorFactory.create("csv", "incoming/events.csv")
    assert isinstance(extractor, CsvExtractor)
    assert extractor.path == "incoming/events.csv"
    assert isinstance(extractor, BaseExtractor)
    assert CsvExtractor is CsvExtractorModule


def test_parquet_sink_factory_respects_mode() -> None:
    sink = SinkFactory.create("parquet", "s3a://silver/events", mode="overwrite")
    assert isinstance(sink, ParquetSink)
    assert sink.mode == "overwrite"
    assert isinstance(sink, BaseSink)
    assert ParquetSink is ParquetSinkModule


@pytest.mark.parametrize("factory", [ExtractorFactory.create, SinkFactory.create])
def test_factory_rejects_unknown_strategy(factory: object) -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        factory("json", "events.json")  # type: ignore[operator]
