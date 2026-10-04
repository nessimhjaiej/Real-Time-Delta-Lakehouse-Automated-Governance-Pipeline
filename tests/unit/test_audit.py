from datetime import datetime, timezone
from pathlib import Path

from src.governance_agent.audit import AuditLog, AuditRecord


def _record(**overrides: object) -> AuditRecord:
    defaults: dict[str, object] = dict(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        user="nessim",
        role="analyst",
        question="What's total revenue?",
        verdict="allowed",
        latency_ms=12.3,
    )
    defaults.update(overrides)
    return AuditRecord(**defaults)  # type: ignore[arg-type]


def test_record_returns_an_increasing_row_id(tmp_path: Path) -> None:
    log = AuditLog(tmp_path / "audit.db")

    first_id = log.record(_record())
    second_id = log.record(_record())

    assert second_id == first_id + 1
    log.close()


def test_all_records_round_trips_every_field(tmp_path: Path) -> None:
    log = AuditLog(tmp_path / "audit.db")
    entry = _record(
        retrieved_chunk_ids=["metric:total_revenue", "policy:pii-block"],
        structured_intent={
            "metric": "total_revenue",
            "dimensions": ["customer_country"],
        },
        compiled_sql="SELECT SUM(total_amount) FROM fact_orders LIMIT 100",
        row_count=42,
    )

    log.record(entry)
    [stored] = log.all_records()

    assert stored.timestamp == entry.timestamp
    assert stored.user == entry.user
    assert stored.role == entry.role
    assert stored.question == entry.question
    assert stored.retrieved_chunk_ids == entry.retrieved_chunk_ids
    assert stored.structured_intent == entry.structured_intent
    assert stored.compiled_sql == entry.compiled_sql
    assert stored.verdict == "allowed"
    assert stored.row_count == 42
    log.close()


def test_blocked_verdict_round_trips_with_reason_and_no_sql(tmp_path: Path) -> None:
    log = AuditLog(tmp_path / "audit.db")
    entry = _record(
        question="show me all customer emails",
        verdict="blocked",
        block_reason="PII column not allowed in SELECT: dim_customers.email",
    )

    log.record(entry)
    [stored] = log.all_records()

    assert stored.verdict == "blocked"
    assert stored.block_reason == entry.block_reason
    assert stored.compiled_sql is None
    assert stored.row_count is None
    log.close()


def test_default_retrieved_chunk_ids_is_an_empty_list_not_null(tmp_path: Path) -> None:
    log = AuditLog(tmp_path / "audit.db")

    log.record(_record())
    [stored] = log.all_records()

    assert stored.retrieved_chunk_ids == []
    log.close()


def test_records_persist_across_reopening_the_same_db_file(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.db"

    log = AuditLog(db_path)
    log.record(_record())
    log.record(_record(question="second question"))
    log.close()

    reopened = AuditLog(db_path)
    try:
        assert len(reopened.all_records()) == 2
    finally:
        reopened.close()


def test_records_are_returned_in_insertion_order(tmp_path: Path) -> None:
    log = AuditLog(tmp_path / "audit.db")

    log.record(_record(question="first"))
    log.record(_record(question="second"))
    log.record(_record(question="third"))

    questions = [r.question for r in log.all_records()]

    assert questions == ["first", "second", "third"]
    log.close()


def test_creating_the_log_creates_parent_directories(tmp_path: Path) -> None:
    nested_path = tmp_path / "nested" / "dir" / "audit.db"

    log = AuditLog(nested_path)

    assert nested_path.is_file()
    log.close()
