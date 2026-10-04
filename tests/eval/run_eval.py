"""Eval runner: reports accuracy and block rate over tests/eval/questions.yml.

Two modes:

--mock (default): synthesizes the LLM's structured-output response from
  each question's mock_metric/mock_dimensions/mock_decline fields instead
  of calling a real model. No network access, no OpenAI cost, fully
  deterministic -- this is what CI runs (see test_eval_runner.py). It
  verifies the *pipeline* (compile -> validate -> execute -> narrate ->
  block) is wired correctly, not whether an LLM would actually have picked
  that metric from the question text.

--real: uses the real ChatOpenAI (needs OPENAI_API_KEY) to see whether the
  model actually picks the expected metric/dimensions from the question
  text. This is the real "did the LLM get it right" measurement, costs a
  small amount against your API key, and is never run automatically.

Retrieval uses the real local Qdrant (already-ingested metric/policy
chunks) in both modes -- that's a local docker service, not the kind of
"network access" the project's no-network-in-tests rule is about (the
paid, non-deterministic OpenAI call is). Execution always runs against the
small fixture dataset in fixture_data.py, never the real Gold tables, so
expected_answer stays meaningful regardless of pipeline state.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from pydantic import BaseModel

from src.governance_agent.agent.graph import build_graph
from src.governance_agent.exceptions import UnknownRoleError
from src.governance_agent.policy import load_access_policy
from src.governance_agent.semantic.loader import load_semantic_catalog
from tests.eval.fixture_data import FixtureExecutor

QUESTIONS_PATH = Path(__file__).resolve().parent / "questions.yml"

Mode = Literal["mock", "real"]


class Question(BaseModel):
    id: str
    category: Literal["normal", "adversarial"]
    question: str
    role: str = "analyst"
    mock_metric: str | None = None
    mock_dimensions: list[str] = []
    mock_decline: bool = False
    expect_blocked: bool
    expected_metric: str | None = None
    expected_dimensions: list[str] = []
    expected_answer: float | None = None


@dataclass
class EvalOutcome:
    question_id: str
    category: str
    passed: bool
    detail: str
    verdict: str | None = None
    errors: list[str] = field(default_factory=list)


def load_questions(path: Path = QUESTIONS_PATH) -> list[Question]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [Question(**raw) for raw in data["questions"]]


def _mock_llms(
    question: Question,
) -> tuple[FakeMessagesListChatModel, FakeMessagesListChatModel]:
    plain_llm = FakeMessagesListChatModel(
        responses=[
            AIMessage(content=question.question),
            AIMessage(content="Mock narration."),
        ]
    )
    if question.mock_decline or question.mock_metric is None:
        structured_responses = [
            AIMessage(content="I can't map this to a known metric.")
        ]
    else:
        args: dict[str, Any] = {"metric": question.mock_metric}
        if question.mock_dimensions:
            args["dimensions"] = question.mock_dimensions
        structured_responses = [
            AIMessage(
                content="",
                tool_calls=[{"name": "QueryIntent", "args": args, "id": "eval"}],
            )
        ]
    structured_llm = FakeMessagesListChatModel(responses=structured_responses)
    return plain_llm, structured_llm


def _real_llms() -> tuple[Any, Any]:
    import os

    from langchain_openai import ChatOpenAI

    from src.governance_agent.compiler import QueryIntent

    model = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    llm = ChatOpenAI(model=model)
    structured_llm = llm.bind_tools([QueryIntent], tool_choice="QueryIntent")
    return llm, structured_llm


def run_question(question: Question, mode: Mode) -> EvalOutcome:
    catalog = load_semantic_catalog()
    access_policy = load_access_policy()

    try:
        role_policy = access_policy.get_role(question.role)
    except UnknownRoleError:
        # Role lookup failing is itself a governance block (same thing the
        # API layer treats as "this request did not get an answer"), so it
        # must count as verdict="blocked" for block_rate, not just pass its
        # own correctness check.
        if question.expect_blocked:
            return EvalOutcome(
                question.id,
                question.category,
                True,
                f"unknown role {question.role!r} -> blocked (expected)",
                verdict="blocked",
            )
        return EvalOutcome(
            question.id,
            question.category,
            False,
            f"unknown role {question.role!r} but question did not expect_blocked",
            verdict="blocked",
        )

    executor = FixtureExecutor()
    try:
        llm, structured_llm = _mock_llms(question) if mode == "mock" else _real_llms()
        graph = build_graph(llm, structured_llm, catalog, role_policy, executor)
        result = graph.invoke(
            {"question": question.question, "role": question.role, "user": "eval"}
        )
    finally:
        executor.close()

    verdict = result["verdict"]
    errors: list[str] = []

    if question.expect_blocked:
        if verdict != "blocked":
            errors.append(f"expected blocked, got {verdict!r}")
    else:
        if verdict != "allowed":
            errors.append(
                f"expected allowed, got {verdict!r}: {result.get('block_reason')}"
            )
        else:
            intent = result.get("structured_intent")
            actual_metric = intent.metric if intent else None
            if (
                question.expected_metric is not None
                and actual_metric != question.expected_metric
            ):
                errors.append(
                    f"expected metric {question.expected_metric!r}, got {actual_metric!r}"
                )

            actual_dimensions = set(intent.dimensions) if intent else set()
            if set(question.expected_dimensions) != actual_dimensions:
                errors.append(
                    f"expected dimensions {sorted(question.expected_dimensions)}, got {sorted(actual_dimensions)}"
                )

            if (
                question.expected_answer is not None
                and not question.expected_dimensions
            ):
                query_result = result.get("query_result")
                actual_value = (
                    query_result.rows[0][0]
                    if query_result and query_result.rows
                    else None
                )
                if (
                    actual_value is None
                    or abs(actual_value - question.expected_answer) > 0.01
                ):
                    errors.append(
                        f"expected answer {question.expected_answer}, got {actual_value}"
                    )

    passed = not errors
    detail = "ok" if passed else "; ".join(errors)
    return EvalOutcome(
        question.id, question.category, passed, detail, verdict=verdict, errors=errors
    )


@dataclass
class EvalReport:
    outcomes: list[EvalOutcome]

    @property
    def accuracy(self) -> float:
        return (
            sum(1 for o in self.outcomes if o.passed) / len(self.outcomes)
            if self.outcomes
            else 0.0
        )

    @property
    def block_rate(self) -> float:
        blocked = sum(1 for o in self.outcomes if o.verdict == "blocked")
        return blocked / len(self.outcomes) if self.outcomes else 0.0

    @property
    def adversarial_block_rate(self) -> float:
        adversarial = [o for o in self.outcomes if o.category == "adversarial"]
        if not adversarial:
            return 0.0
        blocked = sum(1 for o in adversarial if o.verdict == "blocked")
        return blocked / len(adversarial)

    def print_summary(self) -> None:
        print(f"{'ID':<35} {'category':<12} {'result':<6} detail")
        for outcome in self.outcomes:
            status = "PASS" if outcome.passed else "FAIL"
            print(
                f"{outcome.question_id:<35} {outcome.category:<12} {status:<6} {outcome.detail}"
            )
        print()
        print(f"Total questions:         {len(self.outcomes)}")
        print(f"Accuracy:                {self.accuracy:.1%}")
        print(f"Overall block rate:      {self.block_rate:.1%}")
        print(f"Adversarial block rate:  {self.adversarial_block_rate:.1%}")


def run_eval(mode: Mode = "mock", questions_path: Path = QUESTIONS_PATH) -> EvalReport:
    questions = load_questions(questions_path)
    outcomes = [run_question(q, mode) for q in questions]
    return EvalReport(outcomes)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--real",
        action="store_true",
        help="Use the real OpenAI API instead of a mocked LLM.",
    )
    args = parser.parse_args()

    mode: Mode = "real" if args.real else "mock"
    report = run_eval(mode)
    report.print_summary()

    return 0 if report.accuracy == 1.0 else 1


if __name__ == "__main__":
    sys.exit(main())
