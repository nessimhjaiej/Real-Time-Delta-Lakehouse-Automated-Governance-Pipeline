import logging
from types import SimpleNamespace

from src.pipelines.quality import DataQualityError
from src.utils.callbacks import log_quality_gate_failure


def test_log_quality_gate_failure_logs_run_details(caplog) -> None:
    context = {
        "task_instance": SimpleNamespace(
            dag_id="medallion_pipeline", task_id="quality_gate"
        ),
        "run_id": "manual__2026-09-29T00:00:00+00:00",
        "exception": DataQualityError(
            "Silver-to-Gold quality gate failed:\n"
            "  - silver_orders_batch.order_line_id: expect_column_values_to_be_unique"
        ),
    }

    with caplog.at_level(logging.ERROR):
        log_quality_gate_failure(context)

    assert len(caplog.records) == 1
    message = caplog.records[0].message
    assert "medallion_pipeline" in message
    assert "quality_gate" in message
    assert "manual__2026-09-29T00:00:00+00:00" in message
    assert "order_line_id: expect_column_values_to_be_unique" in message
