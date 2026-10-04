"""Runs the full --mock eval set as a regular pytest test, so a regression
in the compile/validate/execute/narrate/block pipeline (or in
questions.yml itself) fails CI the same as any other test.

Every LLM call is mocked (see run_eval.py's module docstring), but
retrieval still hits the real local Qdrant -- so, like
test_ingest.py/test_retriever.py/test_executors.py, the tests that
actually run the graph are skipped automatically when Qdrant isn't
reachable (e.g. in CI, which doesn't run a Qdrant service) rather than
failing or hanging.
"""

from __future__ import annotations

import socket

import pytest

from tests.eval.run_eval import load_questions, run_eval


def _qdrant_is_up(
    host: str = "localhost", port: int = 6333, timeout: float = 0.5
) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


requires_qdrant = pytest.mark.skipif(
    not _qdrant_is_up(), reason="Qdrant is not running on localhost:6333"
)


def test_question_set_has_at_least_30_questions() -> None:
    # Pure YAML load -- no Qdrant/graph involved, so this always runs.
    assert len(load_questions()) >= 30


@requires_qdrant
def test_mock_eval_set_passes_every_question() -> None:
    report = run_eval(mode="mock")

    failed = [o for o in report.outcomes if not o.passed]
    assert not failed, "\n".join(f"{o.question_id}: {o.detail}" for o in failed)
    assert report.accuracy == 1.0


@requires_qdrant
def test_adversarial_questions_are_all_blocked() -> None:
    report = run_eval(mode="mock")

    assert report.adversarial_block_rate == 1.0


@requires_qdrant
def test_normal_questions_are_never_blocked() -> None:
    report = run_eval(mode="mock")

    normal = [o for o in report.outcomes if o.category == "normal"]
    assert normal
    assert all(o.verdict == "allowed" for o in normal)
