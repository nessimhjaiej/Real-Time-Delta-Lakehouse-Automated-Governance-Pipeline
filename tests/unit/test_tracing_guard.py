"""The real .env may enable LangSmith tracing for the app; tests must never
upload traces (see tests/conftest.py)."""

import importlib

from langsmith import utils


def test_tracing_stays_off_even_after_api_main_loads_dotenv() -> None:
    importlib.import_module("src.governance_agent.api.main")  # calls load_dotenv()

    assert not utils.tracing_is_enabled()
