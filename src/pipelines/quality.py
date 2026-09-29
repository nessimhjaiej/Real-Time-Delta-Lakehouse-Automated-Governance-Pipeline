"""Great Expectations circuit breaker on the Silver-to-Gold boundary.

The expectations here encode what src/notebooks/02_explore_silver.py found by
hand: ``order_line_id`` is supposed to be a primary key in both orders tables,
but silver_orders_batch had one collision from a corrupted bronze invoice_id,
and silver_orders_stream had 33 -- because the dedup key in
``src.pipelines.silver.cleanse`` includes ``timestamp`` while
``order_line_id`` does not, so near-duplicate events with slightly different
timestamps survive dropDuplicates and then collide on the key Gold joins on.
This module stops that from reaching Gold silently.
"""

from __future__ import annotations

import logging

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import AbstractDataContext
from pyspark.sql import DataFrame

from config.spark_config import get_spark_session

logger = logging.getLogger(__name__)

SILVER_PATHS = {
    "silver_orders_batch": "s3a://lakehouse/silver/orders_batch",
    "silver_orders_stream": "s3a://lakehouse/silver/orders_stream",
    "silver_products_catalog": "s3a://lakehouse/silver/products_catalog",
    "silver_customers": "s3a://lakehouse/silver/customers",
}


class DataQualityError(RuntimeError):
    """Raised when a Silver table fails its Great Expectations suite."""


def _orders_suite(name: str) -> ExpectationSuite:
    """order_line_id must be a real primary key; core FKs/measures must be sane."""
    suite = gx.ExpectationSuite(name=name)
    for expectation in (
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_line_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_line_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id_hash"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1),
        gx.expectations.ExpectColumnValuesToBeBetween(column="unit_price", min_value=0),
    ):
        suite.add_expectation(expectation)
    return suite


def _products_suite(name: str) -> ExpectationSuite:
    suite = gx.ExpectationSuite(name=name)
    for expectation in (
        gx.expectations.ExpectColumnValuesToBeUnique(column="id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0),
    ):
        suite.add_expectation(expectation)
    return suite


def _customers_suite(name: str) -> ExpectationSuite:
    suite = gx.ExpectationSuite(name=name)
    for expectation in (
        gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="email"),
    ):
        suite.add_expectation(expectation)
    return suite


_SUITE_BUILDERS = {
    "silver_orders_batch": _orders_suite,
    "silver_orders_stream": _orders_suite,
    "silver_products_catalog": _products_suite,
    "silver_customers": _customers_suite,
}


def _validate_table(
    context: AbstractDataContext, table_name: str, df: DataFrame
) -> list[str]:
    """Validate one Silver table and return its failed-expectation descriptions."""
    data_source = context.data_sources.add_spark(name=f"{table_name}_source")
    data_asset = data_source.add_dataframe_asset(name=table_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{table_name}_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = _SUITE_BUILDERS[table_name](f"{table_name}_suite")
    result = batch.validate(suite)

    failures = []
    for expectation_result in result.results:
        if expectation_result.success:
            continue
        column = expectation_result.expectation_config.kwargs.get("column")
        failures.append(
            f"{table_name}.{column}: {expectation_result.expectation_config.type}"
        )
    return failures


def run_quality_gate() -> None:
    """Validate every Silver table; raise DataQualityError if any suite fails.

    Wired as the Airflow task between silver_layer and gold_layer so a data
    quality regression blocks Gold instead of silently propagating into the
    star schema.
    """
    spark = get_spark_session("SilverToGoldQualityGate")
    try:
        context = gx.get_context(mode="ephemeral")
        all_failures: list[str] = []

        for table_name, path in SILVER_PATHS.items():
            df = spark.read.format("delta").load(path)
            failures = _validate_table(context, table_name, df)
            if failures:
                all_failures.extend(failures)
            else:
                logger.info("Quality gate passed: %s", table_name)

        if all_failures:
            summary = "\n".join(f"  - {failure}" for failure in all_failures)
            raise DataQualityError(f"Silver-to-Gold quality gate failed:\n{summary}")

        logger.info("Silver-to-Gold quality gate passed for all tables.")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_quality_gate()
