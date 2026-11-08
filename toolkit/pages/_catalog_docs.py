"""Descriptions the repo itself writes about tables and columns.

Every function returns records carrying ``file`` and ``line`` so the page can
cite them. Nothing here invents text: it only locates sentences in
semantic/*.yml, policies/access.yml (values and comments), policies/docs/*.md,
README.md and Python docstrings/comments.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from toolkit.common import LAYERS, REPO_ROOT, rel

SEMANTIC_DIR = REPO_ROOT / "semantic"
ACCESS_YML = REPO_ROOT / "policies" / "access.yml"
POLICY_DOCS = REPO_ROOT / "policies" / "docs"
README = REPO_ROOT / "README.md"

TOKEN_RE = re.compile(r"``([^`]+)``")


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def sentences(text: str) -> list[str]:
    """Split prose into sentences / bullet items, whitespace-normalised."""
    out: list[str] = []
    for para in re.split(r"\n\s*\n|\n\s*[-*]\s+", text):
        para = _norm(para)
        if not para:
            continue
        out.extend(s for s in re.split(r"(?<=[.!?])\s+(?=[A-Z`(])", para) if s)
    return out


def mentions(sentence: str, table: str, column: str) -> bool:
    """Does ``sentence`` name ``column`` of ``table``?

    Matches ````column```` or ````table.column```` (double-backtick tokens), and
    bare identifiers that contain an underscore (``order_line_id``), so plain
    words such as "price" or "country" never match by accident.
    """
    for token in TOKEN_RE.findall(sentence):
        token = token.strip()
        if token == column or token == f"{table}.{column}":
            return True
    if "_" in column.strip("_"):
        stripped = TOKEN_RE.sub(" ", sentence)
        for match in re.finditer(r"[A-Za-z_][\w.]*", stripped):
            word = match.group(0).rstrip(".")
            if word == column or word == f"{table}.{column}":
                return True
    return False


# ---------------------------------------------------------------- YAML helpers


def _compose(path: Path) -> yaml.Node | None:
    try:
        return yaml.compose(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None


def _mapping(node: yaml.Node | None) -> dict[str, yaml.Node]:
    if isinstance(node, yaml.MappingNode):
        return {str(k.value): v for k, v in node.value}
    return {}


def _scalar(node: yaml.Node | None) -> str | None:
    if isinstance(node, yaml.ScalarNode):
        return str(node.value)
    return None


def _sql_columns(expr: str) -> list[str]:
    """Column names referenced by a SQL expression (sqlglot), else the bare name."""
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(expr, dialect="duckdb")
    except Exception:  # sqlglot raises several parse error types
        return [expr] if re.fullmatch(r"\w+", expr) else []
    return sorted({c.name for c in parsed.find_all(exp.Column)})


def semantic_entries() -> list[dict[str, Any]]:
    """Dimensions and metrics from semantic/, with the columns each references."""
    out: list[dict[str, Any]] = []
    for path in sorted(SEMANTIC_DIR.rglob("*.yml")):
        root = _mapping(_compose(path))
        for kind in ("dimensions", "metrics"):
            seq = root.get(kind)
            if not isinstance(seq, yaml.SequenceNode):
                continue
            for item in seq.value:
                fields = _mapping(item)
                name = _scalar(fields.get("name"))
                table = _scalar(fields.get("table"))
                desc_node = fields.get("description")
                desc = _scalar(desc_node)
                expr = _scalar(fields.get("column")) or _scalar(
                    fields.get("expression")
                )
                if not (name and table and desc and desc_node is not None):
                    continue
                out.append(
                    {
                        "kind": kind[:-1],
                        "name": name,
                        "table": table,
                        "description": _norm(desc),
                        "expression": expr,
                        "columns": _sql_columns(expr) if expr else [],
                        "time_column": _scalar(fields.get("time_column")),
                        "file": rel(path),
                        "line": desc_node.start_mark.line + 1,
                        "item_line": item.start_mark.line + 1,
                    }
                )
    return out


def semantic_joins() -> list[dict[str, Any]]:
    path = SEMANTIC_DIR / "joins.yml"
    root = _mapping(_compose(path))
    seq = root.get("joins")
    out: list[dict[str, Any]] = []
    if not isinstance(seq, yaml.SequenceNode):
        return out
    for item in seq.value:
        fields = {k: _scalar(v) for k, v in _mapping(item).items()}
        out.append({**fields, "file": rel(path), "line": item.start_mark.line + 1})
    return out


def access_policy() -> dict[str, Any]:
    """Per-role allowed tables/columns and PII columns, with line numbers."""
    root = _mapping(_compose(ACCESS_YML))
    roles = _mapping(root.get("roles"))
    out: dict[str, Any] = {"file": rel(ACCESS_YML), "roles": {}}
    for role, node in roles.items():
        fields = _mapping(node)
        entry: dict[str, Any] = {"allowed": {}, "pii": {}, "tables": {}}
        tables = fields.get("allowed_tables")
        if isinstance(tables, yaml.SequenceNode):
            for t in tables.value:
                entry["tables"][str(t.value)] = t.start_mark.line + 1
        for key, target in (("allowed_columns", "allowed"), ("pii_columns", "pii")):
            for table, seq in _mapping(fields.get(key)).items():
                if isinstance(seq, yaml.SequenceNode):
                    entry[target][table] = {
                        str(c.value): c.start_mark.line + 1 for c in seq.value
                    }
        out["roles"][role] = entry
    return out


def yaml_comment_blocks(paths: list[Path]) -> list[dict[str, Any]]:
    """Contiguous ``#`` comment blocks in YAML files."""
    out: list[dict[str, Any]] = []
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        block: list[str] = []
        start = 0
        for i, line in enumerate(lines + [""], start=1):
            stripped = line.strip()
            if stripped.startswith("#"):
                if not block:
                    start = i
                block.append(stripped.lstrip("#").strip())
            elif block:
                out.append(
                    {"text": _norm(" ".join(block)), "file": rel(path), "line": start}
                )
                block = []
    return out


def markdown_lines() -> list[dict[str, Any]]:
    """Every non-empty prose line of policies/docs/*.md and README.md."""
    out: list[dict[str, Any]] = []
    paths = sorted(POLICY_DOCS.glob("*.md")) + ([README] if README.exists() else [])
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for i, line in enumerate(lines, start=1):
            if line.strip():
                out.append({"text": line.strip(), "file": rel(path), "line": i})
    return out


def markdown_files() -> list[str]:
    paths = sorted(POLICY_DOCS.glob("*.md")) + ([README] if README.exists() else [])
    return [rel(p) for p in paths]


def readme_layers() -> dict[str, dict[str, Any]]:
    """``- **Bronze** ...`` bullets under README's ``## Architecture`` heading."""
    out: dict[str, dict[str, Any]] = {}
    try:
        lines = README.read_text(encoding="utf-8").splitlines()
    except OSError:
        return out
    in_section = False
    for i, line in enumerate(lines, start=1):
        if line.startswith("## "):
            in_section = line.strip().lower() == "## architecture"
            continue
        if not in_section:
            continue
        match = re.match(r"\s*[-*]\s+\*\*(\w+)\*\*\s+(.*)", line)
        if match and match.group(1).lower() in LAYERS:
            out[match.group(1).lower()] = {
                "text": f"{match.group(1)} {match.group(2).strip()}",
                "file": rel(README),
                "line": i,
            }
    return out
