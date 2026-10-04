"""Loads semantic/*.yml into a validated SemanticCatalog."""

from __future__ import annotations

from pathlib import Path

import yaml

from src.governance_agent.exceptions import (
    DuplicateDimensionError,
    DuplicateMetricError,
)
from src.governance_agent.semantic.models import (
    Dimension,
    Join,
    Metric,
    SemanticCatalog,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SEMANTIC_DIR = PROJECT_ROOT / "semantic"


def load_semantic_catalog(semantic_dir: Path = DEFAULT_SEMANTIC_DIR) -> SemanticCatalog:
    """Read every semantic/metrics/*.yml and semantic/dimensions/*.yml file plus
    semantic/joins.yml, and return the combined, validated catalog."""

    metrics: dict[str, Metric] = {}
    for path in sorted((semantic_dir / "metrics").glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for raw in data.get("metrics", []):
            metric = Metric(**raw)
            if metric.name in metrics:
                raise DuplicateMetricError(metric.name)
            metrics[metric.name] = metric

    dimensions: dict[str, Dimension] = {}
    for path in sorted((semantic_dir / "dimensions").glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for raw in data.get("dimensions", []):
            dimension = Dimension(**raw)
            if dimension.name in dimensions:
                raise DuplicateDimensionError(dimension.name)
            dimensions[dimension.name] = dimension

    joins_path = semantic_dir / "joins.yml"
    joins: list[Join] = []
    if joins_path.is_file():
        data = yaml.safe_load(joins_path.read_text(encoding="utf-8")) or {}
        joins = [Join(**raw) for raw in data.get("joins", [])]

    return SemanticCatalog(metrics=metrics, dimensions=dimensions, joins=joins)
