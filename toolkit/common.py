"""Shared building blocks for the toolkit pages.

Page contract -- every module in ``toolkit/pages/`` defines:

    SLUG: str        # output file name without .html, e.g. "lineage"
    TITLE: str       # page heading
    NAV_LABEL: str   # short label in the top navigation
    BLURB: str       # one sentence for the home page card
    LIVE: bool       # True if collect() reads running services (MinIO, Spark, docker)

    def collect() -> dict[str, Any]
        Gather every fact the page shows from the repo and/or the running stack.
        Must be JSON-serialisable and include:
          "summary": {label: value}   -- 2-4 headline numbers for the home page
          "sources": [str]            -- where the facts came from (files, commands, URIs)
        Raise SourceUnavailableError when a live source is down.

    def render(data: dict[str, Any]) -> str
        Return the page body HTML (placed inside <main>). Pure: no I/O.

Rules shared by all pages:
  * Facts are derived, never typed in by hand. If something cannot be derived,
    say so on the page instead of guessing.
  * Escape every value with ``esc`` (or build tables with ``table_html``).
  * Cross-page links use ``href(slug, focus_id)``; every page honours ``?focus=<id>``.
    Canonical ids: tables ``<layer>.<table>`` (``silver.orders_stream``),
    Kafka topics ``kafka.<topic>``, services ``service.<compose name>``,
    Python callables ``<dotted.module>.<qualname>``.
  * Never show raw PII in samples: pass values through ``mask_if_pii``.
"""

from __future__ import annotations

import html
import json
import os
import subprocess
import zlib
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
LAKE_BUCKET = "lakehouse"
LAYERS = ("bronze", "silver", "gold")

PAGE_SLUGS = {
    "architecture": "architecture",
    "lineage": "lineage",
    "call_graph": "call-graph",
    "data_model": "data-model",
    "quality": "quality",
    "catalog": "catalog",
}
"""Page module name -> output slug, in navigation order."""

# Raw personal data columns (Bronze holds them unhashed; Silver hashes them).
PII_COLUMNS = frozenset(
    {
        "email",
        "ip_address",
        "first_name",
        "last_name",
        "full_name",
        "customer_name",
        "phone",
        "address",
        "customer_id",
    }
)


class SourceUnavailableError(RuntimeError):
    """A live source (MinIO, Spark, docker, Marquez) could not be reached."""


class Raw(str):
    """A string that is already safe HTML; ``esc`` leaves it untouched."""


# ---------------------------------------------------------------- HTML helpers


def esc(value: Any) -> str:
    """Escape any value for HTML text or attribute context."""
    if isinstance(value, Raw):
        return str(value)
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def href(slug: str, focus_id: str | None = None) -> str:
    """Link to another toolkit page, optionally focused on one entity id."""
    target = f"{slug}.html"
    if focus_id:
        target += f"?focus={quote(focus_id, safe='')}"
    return target


def link(slug: str, focus_id: str, text: str | None = None) -> Raw:
    return Raw(f'<a href="{esc(href(slug, focus_id))}">{esc(text or focus_id)}</a>')


def badge(text: str, kind: str = "muted") -> Raw:
    """kind: ok | warn | fail | info | muted | pk | fk | pii"""
    return Raw(f'<span class="badge badge-{esc(kind)}">{esc(text)}</span>')


def table_html(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    filterable: bool = False,
    table_id: str | None = None,
    caption: str | None = None,
) -> Raw:
    """A data table; cells are escaped unless they are ``Raw``.

    ``filterable`` adds a search box that hides non-matching rows.
    """
    tid = table_id or f"t{zlib.crc32('|'.join(headers).encode()):08x}"
    parts = []
    if filterable:
        parts.append(
            f'<input class="table-filter" type="search" placeholder="Filter rows…" '
            f'data-filter-target="{esc(tid)}" aria-label="Filter table">'
        )
    parts.append(f'<div class="table-wrap"><table class="data" id="{esc(tid)}">')
    if caption:
        parts.append(f"<caption>{esc(caption)}</caption>")
    parts.append("<thead><tr>")
    parts.extend(f"<th>{esc(h)}</th>" for h in headers)
    parts.append("</tr></thead><tbody>")
    for row in rows:
        parts.append("<tr>")
        parts.extend(f"<td>{esc(cell)}</td>" for cell in row)
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
    return Raw("".join(parts))


def stat_cards(stats: dict[str, Any]) -> Raw:
    cards = "".join(
        f'<div class="stat"><div class="stat-value">{esc(v)}</div>'
        f'<div class="stat-label">{esc(k)}</div></div>'
        for k, v in stats.items()
    )
    return Raw(f'<div class="stats">{cards}</div>')


def json_script(element_id: str, data: Any) -> Raw:
    """Embed data for page JavaScript, safe against ``</script>`` injection."""
    payload = json.dumps(data, default=str).replace("</", "<\\/")
    return Raw(
        f'<script type="application/json" id="{esc(element_id)}">{payload}</script>'
    )


def graph_block(
    graph_id: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    *,
    groups: dict[str, str] | None = None,
    height: int = 620,
    rank_dir: str = "LR",
    up_label: str = "Upstream",
    down_label: str = "Downstream",
    transitive_default: bool = True,
) -> Raw:
    """Interactive graph (Cytoscape + dagre) rendered by assets/graph.js.

    nodes: {"id", "label", "group", optional "parent" (compound node id),
            "detail": {key: text}, "links": [{"label", "href"}]}
           A node whose id is used as another node's "parent" is drawn as a box.
    edges: {"source", "target", optional "label", optional "kind"}
           Direction is flow: source -> target (data flows / caller calls callee).
    groups: group key -> human label, in legend order (colours assigned in order).
    Clicking a node highlights everything upstream and downstream of it and lists
    them in the side panel; ``?focus=<node id>`` selects a node on load.
    """
    spec = {
        "nodes": nodes,
        "edges": edges,
        "groups": groups or {},
        "rankDir": rank_dir,
        "upLabel": up_label,
        "downLabel": down_label,
        "transitive": transitive_default,
    }
    checked = " checked" if transitive_default else ""
    return Raw(
        f'<div class="graph" data-graph="{esc(graph_id)}">'
        '<div class="graph-toolbar">'
        '<input type="search" data-role="search" placeholder="Find a node…" '
        'aria-label="Find a node">'
        f'<label class="check"><input type="checkbox" data-role="transitive"{checked}> '
        "Full chain (not just direct neighbours)</label>"
        '<button type="button" data-role="fit">Fit</button>'
        '<button type="button" data-role="clear">Clear</button>'
        "</div>"
        '<div class="graph-legend" data-role="legend"></div>'
        '<div class="graph-body">'
        f'<div class="graph-canvas" data-role="canvas" style="height:{int(height)}px"></div>'
        '<aside class="graph-panel" data-role="panel">'
        '<p class="muted">Click a node to see what feeds it and what it feeds.</p>'
        "</aside></div></div>" + json_script(f"graph-{graph_id}", spec)
    )


def section(title: str, body: str, *, section_id: str | None = None) -> Raw:
    sid = f' id="{esc(section_id)}"' if section_id else ""
    return Raw(f'<section class="block"{sid}><h2>{esc(title)}</h2>{body}</section>')


def sources_html(sources: Iterable[str]) -> Raw:
    items = "".join(f"<li><code>{esc(s)}</code></li>" for s in sources)
    return Raw(
        '<details class="sources"><summary>Where this page\'s facts come from</summary>'
        f"<ul>{items}</ul></details>"
    )


# ------------------------------------------------------------ repo + lake helpers


def rel(path: Path | str) -> str:
    """Repo-relative POSIX path, for citing sources."""
    p = Path(path).resolve()
    try:
        return p.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return p.as_posix()


def repo_revision() -> str:
    """Short git commit, with '+dirty' when the working tree has changes."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return commit + ("+dirty" if dirty else "")


def load_env() -> None:
    """Load .env into os.environ if python-dotenv is installed (never printed)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(REPO_ROOT / ".env", override=False)


def delta_uri(layer: str, table: str) -> str:
    """DuckDB-style URI of a Delta table on MinIO."""
    return f"s3://{LAKE_BUCKET}/{layer}/{table}"


def lake_connection() -> Any:
    """DuckDB connection that can ``delta_scan`` the lake on MinIO.

    Same settings as src/governance_agent/executors/duckdb_executor.py.
    Raises SourceUnavailableError if MinIO cannot be reached.
    """
    import duckdb

    load_env()
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    conn = duckdb.connect()
    for statement in ("INSTALL httpfs", "LOAD httpfs", "INSTALL delta", "LOAD delta"):
        conn.execute(statement)
    conn.execute(
        "CREATE SECRET minio_secret "
        "(TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, URL_STYLE 'path', USE_SSL false)",
        [
            os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            os.getenv("MINIO_SECRET_KEY", "minioadminpassword"),
            endpoint,
        ],
    )
    try:
        conn.execute(
            f"SELECT count(*) FROM glob('s3://{LAKE_BUCKET}/*/*/_delta_log/*.json')"
        ).fetchone()
    except Exception as error:  # duckdb raises several IO/HTTP error types
        conn.close()
        raise SourceUnavailableError(f"MinIO lake not reachable at {endpoint}: {error}")
    return conn


def list_lake_tables(conn: Any) -> list[str]:
    """Delta tables that actually exist on MinIO, as ``layer.table`` ids."""
    rows = conn.execute(
        f"SELECT DISTINCT file FROM glob('s3://{LAKE_BUCKET}/*/*/_delta_log/*.json')"
    ).fetchall()
    found = set()
    for (file,) in rows:
        parts = str(file).split("/")
        # s3://lakehouse/<layer>/<table>/_delta_log/<n>.json
        found.add(f"{parts[3]}.{parts[4]}")
    order = {layer: i for i, layer in enumerate(LAYERS)}
    return sorted(found, key=lambda t: (order.get(t.split(".")[0], 99), t))


def is_pii(column: str) -> bool:
    return column.lower() in PII_COLUMNS


def mask_value(value: Any) -> str:
    """Keep the shape of a value, hide its content: 'jane@x.com' -> 'j•••@x•••'."""
    if value is None:
        return ""
    text = str(value)
    if not text:
        return text
    if "@" in text:
        local, _, domain = text.partition("@")
        return f"{local[:1]}•••@{domain[:1]}•••"
    return text[:1] + "•" * min(len(text) - 1, 6)


def mask_if_pii(column: str, value: Any) -> Any:
    return mask_value(value) if is_pii(column) else value
