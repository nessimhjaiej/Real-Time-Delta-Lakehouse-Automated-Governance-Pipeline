"""Run the repo's real Silver-to-Gold quality gate once and record every result.

    python -m toolkit.pages._quality_runner <out.json>

Runs in its own process (spawned by toolkit/pages/quality.py) so the Spark JVM
and the instrumentation below die with it. Nothing in src/ is copied or
modified: ``src.pipelines.quality.run_quality_gate`` runs as-is -- it builds
its own Spark session with config.spark_config.get_spark_session, reads every
table in SILVER_PATHS, validates it with the suites from _SUITE_BUILDERS and
stops Spark in its ``finally``. Two read-only wrappers record what it computes:

* ``Batch.validate``: the full GX ExpectationSuiteValidationResult per table
  (GX's default result_format SUMMARY, i.e. exactly what the gate computes);
* ``quality._validate_table``: the failure list the gate itself acts on.

Partial unexpected values are passed through ``mask_if_pii`` before anything
is written, so raw PII never reaches the snapshot.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from toolkit.common import load_env, mask_if_pii

# Spark settings the repo's builder does not set (so they survive getOrCreate):
# keep the laptop light. Recorded in the output so the page can say so.
SUBMIT_ARGS = (
    "--driver-memory 1g --conf spark.sql.shuffle.partitions=2 "
    "--conf spark.ui.enabled=false pyspark-shell"
)
# The gate's session has the OpenLineage listener; point it at a closed port so
# this inspection run is not recorded in Marquez as a real pipeline run.
LINEAGE_SINK = "http://127.0.0.1:9"
PARTIAL_LIMIT = 5


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _suite_static(quality: Any) -> list[dict[str, Any]]:
    """Expectations exactly as the repo's builders produce them (no Spark)."""
    import great_expectations as gx

    # Reading a suite's configs needs an active (throwaway) context; the gate
    # creates its own ephemeral context afterwards.
    gx.get_context(mode="ephemeral")
    out = []
    for table_name, path in quality.SILVER_PATHS.items():
        builder = quality._SUITE_BUILDERS[table_name]
        suite = builder(f"{table_name}_suite")
        lines, start = inspect.getsourcelines(builder)
        expectations = []
        for expectation in suite.expectations:
            config = expectation.configuration.to_json_dict()
            expectations.append(
                {"type": config["type"], "kwargs": _jsonable(config["kwargs"])}
            )
        out.append(
            {
                "gate_name": table_name,
                "path": path,
                "suite_name": suite.name,
                "builder": f"{builder.__module__}.{builder.__qualname__}",
                "builder_line": start,
                "builder_end": start + len(lines) - 1,
                "expectations": expectations,
            }
        )
    return out


def _result_rows(validation: Any) -> list[dict[str, Any]]:
    rows = []
    for item in validation.results:
        config = item.expectation_config
        kwargs = dict(config.kwargs) if config is not None else {}
        kwargs.pop("batch_id", None)
        column = kwargs.get("column")
        result = dict(item.result or {})
        partial = result.get("partial_unexpected_list") or []
        exception = item.exception_info or {}
        if hasattr(exception, "to_json_dict"):
            exception = exception.to_json_dict()
        raised = bool(exception.get("raised_exception")) if exception else False
        rows.append(
            {
                "type": config.type if config is not None else None,
                "column": column,
                "kwargs": _jsonable(kwargs),
                "success": bool(item.success),
                "element_count": result.get("element_count"),
                "unexpected_count": result.get("unexpected_count"),
                "unexpected_percent": result.get("unexpected_percent"),
                "missing_count": result.get("missing_count"),
                "partial_unexpected": [
                    _jsonable(mask_if_pii(str(column or ""), v))
                    for v in partial[:PARTIAL_LIMIT]
                ],
                "exception": (
                    str(exception.get("exception_message"))[:500] if raised else None
                ),
            }
        )
    return rows


def run(out_path: Path) -> None:
    load_env()
    os.environ["OPENLINEAGE_URL"] = LINEAGE_SINK
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

    import great_expectations as gx
    from great_expectations.datasource.fluent.interfaces import Batch

    from src.pipelines import quality

    payload: dict[str, Any] = {
        "gx_version": gx.__version__,
        "submit_args": SUBMIT_ARGS,
        "lineage_sink": LINEAGE_SINK,
        "suites": _suite_static(quality),
        "validations": [],
        "gate_failures": {},
        "log": [],
    }
    out_path.write_text(json.dumps(payload), encoding="utf-8")

    original_validate = Batch.validate
    original_validate_table = quality._validate_table
    spark_info: dict[str, Any] = {}

    def recording_validate(self: Any, expect: Any, *args: Any, **kwargs: Any) -> Any:
        result = original_validate(self, expect, *args, **kwargs)
        if not spark_info:
            from pyspark.sql import SparkSession

            session = SparkSession.getActiveSession()
            if session is not None:
                spark_info["version"] = session.version
                spark_info["app_name"] = session.sparkContext.appName
                spark_info["master"] = session.sparkContext.master
                spark_info["shuffle_partitions"] = session.conf.get(
                    "spark.sql.shuffle.partitions"
                )
        payload["validations"].append(
            {
                "asset": self.data_asset.name,
                "suite": getattr(expect, "name", None),
                "success": bool(getattr(result, "success", False)),
                "results": _result_rows(result),
            }
        )
        return result

    def recording_validate_table(context: Any, table_name: str, df: Any) -> Any:
        failures = original_validate_table(context, table_name, df)
        payload["gate_failures"][table_name] = list(failures)
        return failures

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            payload["log"].append(
                {"level": record.levelname, "message": record.getMessage()}
            )

    handler = _Capture(level=logging.INFO)
    gate_logger = logging.getLogger(quality.__name__)
    gate_logger.addHandler(handler)
    gate_logger.setLevel(logging.INFO)

    Batch.validate = recording_validate  # type: ignore[method-assign]
    quality._validate_table = recording_validate_table
    started = time.monotonic()
    outcome: dict[str, Any]
    try:
        quality.run_quality_gate()
        outcome = {"status": "PASS", "raised": None, "message": None}
    except quality.DataQualityError as error:
        outcome = {
            "status": "FAIL",
            "raised": type(error).__name__,
            "message": str(error),
        }
    except Exception as error:  # the Airflow task would fail on this too
        outcome = {
            "status": "ERROR",
            "raised": type(error).__name__,
            "message": str(error)[:1500],
        }
    finally:
        Batch.validate = original_validate  # type: ignore[method-assign]
        quality._validate_table = original_validate_table
        gate_logger.removeHandler(handler)
    outcome["duration_s"] = round(time.monotonic() - started, 1)
    outcome["spark"] = spark_info
    payload["outcome"] = outcome
    out_path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    run(Path(sys.argv[1]))
