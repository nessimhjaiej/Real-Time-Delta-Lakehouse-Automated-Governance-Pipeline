"""Entrypoint for the always-on Kafka-to-Bronze stream."""

from src.pipelines.bronze import ingest_streaming_sources

if __name__ == "__main__":
    ingest_streaming_sources()
