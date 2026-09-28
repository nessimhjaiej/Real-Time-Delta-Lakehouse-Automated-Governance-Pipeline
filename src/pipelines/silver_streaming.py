"""Entrypoint for the always-on Bronze-to-Silver stream."""

from config.spark_config import get_spark_session
from src.pipelines.silver import start_streaming_silver

if __name__ == "__main__":
    spark = get_spark_session("SilverStreaming")
    query = start_streaming_silver(spark)
    query.awaitTermination()
