"""Keep LangSmith tracing off for the whole test suite.

The real .env may enable tracing for the running app, and load_dotenv() (run
when api.main is imported, and by get_embeddings) switches it on for any process
that loads it. load_dotenv() never overrides a variable that is already set, so
setting it here first stops pytest runs from uploading traces of fake-LLM tests
and from needing network access.
"""

import os

os.environ["LANGSMITH_TRACING"] = "false"
