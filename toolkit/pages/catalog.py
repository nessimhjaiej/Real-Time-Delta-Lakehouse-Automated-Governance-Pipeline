"""Data catalog: every Delta table and column on MinIO, with samples and sourced descriptions.

collect() reads the lake with DuckDB (``_delta_log`` JSON + ``delta_scan``),
parses the repo's Python with ``ast`` to find who writes each table and how
each column is produced, and looks up descriptions the repo itself wrote
(semantic/*.yml, policies/, README.md, docstrings and comments). Samples are
masked before they are stored in the snapshot.
"""

from __future__ import annotations

from typing import Any

from toolkit.common import (
    LAKE_BUCKET,
    PII_COLUMNS,
    Raw,
    SourceUnavailableError,
    badge,
    esc,
    href,
    lake_connection,
    list_lake_tables,
    rel,
    sources_html,
    stat_cards,
    table_html,
)
from toolkit.pages import _catalog_lake as cl

SLUG = "catalog"
TITLE = "Data catalog"
NAV_LABEL = "Catalog"
BLURB = (
    "Every Delta table and column on MinIO: types, nulls, distinct counts, masked "
    "samples, the job that writes it and descriptions quoted from the repo."
)
LIVE = True


# ------------------------------------------------------------------- collect


def collect() -> dict[str, Any]:
    from toolkit.pages import _catalog_assemble as ca
    from toolkit.pages import _catalog_code as cc
    from toolkit.pages import _catalog_docs as cd

    try:
        conn = lake_connection()
    except SourceUnavailableError:
        raise
    except Exception as error:  # duckdb extension install / network errors
        raise SourceUnavailableError(
            f"DuckDB could not open the lake: {error}"
        ) from error
    profiles: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    try:
        table_ids = list_lake_tables(conn)
        logs = cl.read_delta_logs(conn)
        for tid in table_ids:
            layer, name = tid.split(".", 1)
            try:
                profiles[tid] = cl.table_profile(conn, layer, name)
            except Exception as error:  # one unreadable table must not hide the rest
                errors[tid] = f"{type(error).__name__}: {error}"
    finally:
        conn.close()

    catalog = ca.Catalog(table_ids, logs, profiles, errors)
    tables = catalog.build()
    code_only = catalog.code_only()

    n_cols = sum(len(t["columns"]) for t in tables)
    described = sum(t["described"] for t in tables)
    described_incl = sum(t["described_incl"] for t in tables)
    pii_cols = sum(1 for t in tables for c in t["columns"] if c["pii"])
    hashed_cols = sum(
        1 for t in tables for c in t["columns"] if c["hashed_from"] and not c["pii"]
    )
    pct = round(100 * described / n_cols) if n_cols else 0
    pct_incl = round(100 * described_incl / n_cols) if n_cols else 0

    sources = [
        f"toolkit.common.list_lake_tables: glob('s3://{LAKE_BUCKET}/*/*/_delta_log/*.json')",
        f"duckdb read_json('{cl.LOG_GLOB}') -- commitInfo / metaData / add / remove",
    ]
    sources += [
        f"duckdb delta_scan('s3://{LAKE_BUCKET}/{t['layer']}/{t['name']}')"
        for t in tables
    ]
    sources += sorted(rel(m.path) for m in catalog.mods.values())
    if catalog.compose_file:
        sources.append(catalog.compose_file)
    sources += sorted(rel(p) for p in (cd.SEMANTIC_DIR.rglob("*.yml"))) + [
        rel(cd.ACCESS_YML)
    ]
    sources += cd.markdown_files()
    sources += [
        f"{p} (header / keys only)" for p in sorted(set(catalog.raw_files_read))
    ]
    sources.append(f"toolkit.common.PII_COLUMNS ({', '.join(sorted(PII_COLUMNS))})")

    return {
        "summary": {
            "tables": len(tables),
            "columns": n_cols,
            "columns described": f"{pct}%",
            "PII columns": pii_cols,
        },
        "stats": {
            "tables": len(tables),
            "columns": n_cols,
            "described": described,
            "described_incl": described_incl,
            "pct": pct,
            "pct_incl": pct_incl,
            "pii": pii_cols,
            "hashed": hashed_cols,
        },
        "tables": tables,
        "code_only": code_only,
        "scan_dirs": list(cc.SCAN_DIRS),
        "sources": sources,
    }


# -------------------------------------------------------------------- render

STYLE = """
<style>
.cat-meta { display: grid; grid-template-columns: max-content minmax(0, 1fr); gap: 4px 14px; margin: 8px 0 14px; font-size: 14px; }
.cat-meta dt { color: var(--muted); }
.cat-meta dd { margin: 0; overflow-wrap: anywhere; }
details.cat-table > summary { font-size: 17px; display: flex; flex-wrap: wrap; gap: 6px 10px; align-items: baseline; }
details.cat-table > summary .muted { font-weight: 400; font-size: 14px; }
.cat-list { margin: 4px 0; padding-left: 18px; }
.cat-list li { margin: 2px 0; }
.cat-cite { color: var(--muted); font-size: 12px; white-space: nowrap; }
.cat-samples { display: flex; flex-wrap: wrap; gap: 4px; }
.cat-samples code { white-space: nowrap; max-width: 22ch; overflow: hidden; text-overflow: ellipsis; display: inline-block; vertical-align: bottom; }
table.data td.cat-wide { min-width: 260px; }
.cat-links { display: inline-flex; gap: 10px; flex-wrap: wrap; font-size: 14px; }
@media (max-width: 860px) { .cat-meta { grid-template-columns: 1fr; } .cat-meta dt { margin-top: 6px; } }
</style>
"""


def _fmt_int(value: Any) -> str:
    return f"{value:,}" if isinstance(value, int) else "not derivable"


def _fmt_size(value: Any) -> str:
    if not isinstance(value, int):
        return "not derivable"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1024
    return f"{value} B"


def _cite(text: str) -> str:
    return f'<span class="cat-cite">{esc(text)}</span>'


def _anchor(tid: str, text: str | None = None) -> str:
    return f'<a href="#{esc(tid)}">{esc(text or tid)}</a>'


def _col_link(ref: str, known: set[str]) -> str:
    table = ref.rsplit(".", 1)[0]
    return _anchor(table, ref) if table in known else f"<code>{esc(ref)}</code>"


def _view_links(tid: str) -> Raw:
    return Raw(
        '<span class="cat-links">'
        f'<a href="{esc(href("lineage", tid))}">lineage</a>'
        f'<a href="{esc(href("data-model", tid))}">data model</a>'
        f'<a href="{esc(href("quality", tid))}">quality</a>'
        "</span>"
    )


def _desc_items(items: list[dict[str, Any]], inherited: bool = False) -> str:
    parts = []
    for d in items:
        via = f" (inherited from {esc(d['via'])})" if inherited and d.get("via") else ""
        parts.append(
            f"<li>{esc(d['text'])} {_cite(d['cite'])}"
            f' <span class="muted small">{esc(d["label"])}{via}</span></li>'
        )
    return f'<ul class="cat-list">{"".join(parts)}</ul>' if parts else ""


def _column_desc_cell(col: dict[str, Any]) -> Raw:
    html = _desc_items(col["descriptions"])
    if col["inherited"]:
        html += '<div class="muted small">From upstream:</div>' + _desc_items(
            col["inherited"], inherited=True
        )
    if not html:
        html = '<span class="muted">No description in the repo</span>'
    return Raw(html)


def _produced_cell(col: dict[str, Any], known: set[str]) -> Raw:
    parts = []
    for p in col["produced"]:
        inputs = ""
        if p.get("inputs") and p["kind"] != "passthrough":
            inputs = " ← " + ", ".join(_col_link(i, known) for i in p["inputs"])
        if p["kind"] == "passthrough":
            text = "passed through unchanged from " + ", ".join(
                _col_link(i, known) for i in p.get("inputs", [])
            )
        else:
            text = f"<code>{esc(p['text'])}</code>"
        parts.append(f"<li>{text}{inputs} {_cite(p['cite'])}</li>")
    for g in col["governance"]:
        parts.append(f"<li>{badge(g['text'], g['kind'])} {_cite(g['cite'])}</li>")
    if not parts:
        return Raw('<span class="muted">not derivable from the code</span>')
    return Raw(f'<ul class="cat-list">{"".join(parts)}</ul>')


def _samples_cell(col: dict[str, Any]) -> Raw:
    if not col["samples"]:
        return Raw('<span class="muted">all null</span>')
    chips = "".join(f'<code title="{esc(v)}">{esc(v)}</code>' for v in col["samples"])
    return Raw(f'<span class="cat-samples">{chips}</span>')


def _name_cell(col: dict[str, Any]) -> Raw:
    html = f"<strong>{esc(col['name'])}</strong>"
    if col["pii"]:
        html += " " + badge("PII", "pii")
    elif col["hashed_from"]:
        html += " " + badge(f"hash of {', '.join(col['hashed_from'])}", "warn")
    if col["masked"]:
        reason = (
            f" (built from {', '.join(col['derived_from_masked'])})"
            if col.get("derived_from_masked")
            else ""
        )
        html += f'<div class="muted small">samples masked{esc(reason)}</div>'
    return Raw(html)


def _type_cell(col: dict[str, Any]) -> Raw:
    delta = col.get("delta_type") or "not in Delta schema"
    nullable = (
        ""
        if col.get("nullable") is None
        else (" · nullable" if col["nullable"] else " · not null")
    )
    return Raw(
        f"<code>{esc(col.get('duck_type') or '?')}</code>"
        f'<div class="muted small">Delta: {esc(delta)}{esc(nullable)}</div>'
    )


def _nulls_cell(col: dict[str, Any]) -> str:
    if col.get("nulls") is None:
        return "not derivable"
    return f"{col['nulls']:,} ({col['null_pct']}%)"


def _minmax(value: Any) -> str:
    return "" if value is None else str(value)


def _runs_html(writer: dict[str, Any]) -> str:
    items = []
    for chain in writer.get("runs", []):
        label = esc(chain["label"])
        if chain.get("service_id"):
            label = f'<a href="{esc(href("architecture", chain["service_id"]))}">{label}</a>'
        path = " → ".join(
            (
                f'<a href="{esc(href("call-graph", p))}">{esc(p)}</a>'
                if ":" not in p and not p.startswith("python ")
                else esc(p)
            )
            for p in chain["path"]
        )
        items.append(
            f"<li>{label} {_cite(chain['file'] + ':' + str(chain['line']))}<br>"
            f'<span class="small">{path}</span></li>'
        )
    if not items:
        return '<span class="muted">no entry point found in the scanned code</span>'
    return f'<ul class="cat-list">{"".join(items)}</ul>'


def _table_meta(t: dict[str, Any], known: set[str]) -> str:
    lc = t["last_commit"]
    last = (
        f"{esc(lc.get('timestamp'))} · <code>{esc(lc.get('operation'))}</code>"
        + (
            f" · {esc(', '.join(f'{k}={v}' for k, v in lc['parameters'].items()))}"
            if lc.get("parameters")
            else ""
        )
        if lc
        else "not derivable"
    )
    ops = ", ".join(f"{k} ×{v}" for k, v in sorted(t["operations"].items()))
    rows: list[tuple[str, str]] = [
        ("Delta path", f"<code>{esc(t['uri'])}</code>"),
        ("Layer", esc(t["layer"])),
        ("Rows", esc(_fmt_int(t["rows"]))),
        ("Columns", esc(len(t["columns"]))),
        (
            "Delta version",
            esc(t["version"])
            + f' <span class="muted small">({esc(t["commit_files"])} commit files'
            + (", complete from version 0" if t["log_complete"] else ", log incomplete")
            + ")</span>",
        ),
        ("Last commit", last),
        ("Commit history", esc(ops) if ops else "not derivable"),
        ("Table created", esc(t["created"] or "not derivable")),
        (
            "Size",
            esc(_fmt_size(t["size_bytes"]))
            + (
                f' <span class="muted small">in {esc(t["active_files"])} active data files</span>'
                if t["active_files"] is not None
                else ""
            ),
        ),
        (
            "Partitioned by",
            (
                esc(", ".join(t["partition_columns"]))
                if t["partition_columns"]
                else "none"
            ),
        ),
    ]
    writers = []
    for w in t["writers"]:
        fn = w["function"]
        fn_html = (
            f'<a href="{esc(href("call-graph", fn))}">{esc(fn)}</a>'
            if not w["module_level"]
            else esc(fn)
        )
        writers.append(
            f"<li>{fn_html} {_cite(w['file'] + ':' + str(w['line']))}"
            f' <span class="muted small">{esc(w["via"])} · format {esc(w["format"])}'
            f" · mode {esc(w['mode'])}</span>{_runs_html(w)}</li>"
        )
    rows.append(
        (
            "Writer job",
            (
                f'<ul class="cat-list">{"".join(writers)}</ul>'
                if writers
                else '<span class="muted">No writer found in the scanned code</span>'
            ),
        )
    )
    if t["dag_outlet_of"]:
        rows.append(
            (
                "Declared Airflow outlet",
                ", ".join(
                    f"{esc(d['dag_id'])}.{esc(d['task_id'])} {_cite(d['cite'])}"
                    for d in t["dag_outlet_of"]
                ),
            )
        )
    feeds = [_anchor(u) if u in known else esc(u) for u in t["upstream"]]
    for src in t["raw_sources"]:
        if src.get("kind") == "kafka":
            feeds.append(
                f'<a href="{esc(href("lineage", src.get("topic_id")))}">'
                f"Kafka topic {esc(src.get('topic'))}</a> {_cite(src['file'] + ':' + str(src['line']))}"
            )
        else:
            feeds.append(
                f"{esc(src.get('kind'))} file <code>{esc(', '.join(src.get('location') or []))}</code>"
                f" {_cite(src['file'] + ':' + str(src['line']))}"
            )
        gen = src.get("generator")
        if gen:
            feeds.append(
                f'<span class="small">generated by <a href="{esc(href("call-graph", gen["function"]))}">'
                f"{esc(gen['function'])}</a> {_cite(gen['file'] + ':' + str(gen['line']))}</span>"
            )
    rows.append(
        (
            "Reads from",
            " · ".join(feeds) if feeds else '<span class="muted">not derivable</span>',
        )
    )
    if t["table_ops"]:
        rows.append(
            (
                "Table-level steps",
                '<ul class="cat-list">'
                + "".join(
                    f"<li><code>{esc(o['op'])}({esc(o['args'])})</code>"
                    + (
                        f' <span class="muted small">{esc(o["condition"])}</span>'
                        if o.get("condition")
                        else ""
                    )
                    + f" {_cite(o['file'] + ':' + str(o['line']))}</li>"
                    for o in t["table_ops"]
                )
                + "</ul>",
            )
        )
    readers = [
        f"{esc(r['function'])} {_cite(r['file'] + ':' + str(r['line']))}"
        f' <span class="muted small">{esc(r["via"])}</span>'
        for r in t["readers"]
    ]
    rows.append(
        (
            "Read by",
            (
                '<ul class="cat-list">'
                + "".join(f"<li>{r}</li>" for r in readers)
                + "</ul>"
                if readers
                else '<span class="muted">no reader found in the scanned code</span>'
            ),
        )
    )
    if t["semantic"]:
        rows.append(
            (
                "Semantic layer",
                ", ".join(
                    f"{esc(s['kind'])} <code>{esc(s['name'])}</code> {_cite(s['cite'])}"
                    for s in t["semantic"]
                ),
            )
        )
    if t["joins"]:
        rows.append(
            (
                "Joins",
                ", ".join(
                    f"<code>{esc(j.get('left_table'))}.{esc(j.get('left_key'))} = "
                    f"{esc(j.get('right_table'))}.{esc(j.get('right_key'))}</code> "
                    f"({esc(j.get('join_type'))}) {_cite(j['cite'])}"
                    for j in t["joins"]
                ),
            )
        )
    if t["policy_roles"]:
        rows.append(
            (
                "Access policy",
                ", ".join(
                    f"allowed for role <code>{esc(p['role'])}</code> {_cite(p['cite'])}"
                    for p in t["policy_roles"]
                ),
            )
        )
    rows.append(("Other views", str(_view_links(t["id"]))))
    body = "".join(f"<dt>{esc(k)}</dt><dd>{v}</dd>" for k, v in rows)
    return f'<dl class="cat-meta">{body}</dl>'


def _table_block(t: dict[str, Any], known: set[str]) -> str:
    tid = t["id"]
    parts = [
        f'<details class="block cat-table" id="{esc(tid)}">',
        f"<summary><span>{esc(tid)}</span>"
        f'<span class="muted">{esc(_fmt_int(t["rows"]))} rows · {len(t["columns"])} columns'
        f" · {t['described']}/{len(t['columns'])} described</span></summary>",
    ]
    if t.get("profile_error"):
        parts.append(
            f"<p>{badge('profile failed', 'fail')} {esc(t['profile_error'])}</p>"
        )
    parts.append("<h3>Description</h3>")
    parts.append(
        _desc_items(t["descriptions"])
        or '<p class="muted">No description in the repo</p>'
    )
    parts.append(_table_meta(t, known))
    parts.append("<h3>Sample rows</h3>")
    if t["sample_rows"]:
        parts.append(
            table_html(
                t["sample_columns"],
                t["sample_rows"],
                table_id=f"rows-{tid}",
                caption="First 5 rows returned by delta_scan; PII and hashed-PII columns masked",
            )
        )
    else:
        parts.append('<p class="muted">no rows</p>')
    parts.append("<h3>Columns</h3>")
    rows = [
        [
            _name_cell(c),
            _type_cell(c),
            _nulls_cell(c),
            _fmt_int(c.get("distinct")),
            _minmax(c.get("min")),
            _minmax(c.get("max")),
            _samples_cell(c),
            _column_desc_cell(c),
            _produced_cell(c, known),
        ]
        for c in t["columns"]
    ]
    parts.append(
        table_html(
            [
                "Column",
                "Type",
                "Nulls",
                "Distinct",
                "Min",
                "Max",
                "Samples",
                "Description (source)",
                "How it is produced (evidence)",
            ],
            rows,
            filterable=True,
            table_id=f"cols-{tid}",
        )
    )
    parts.append("</details>")
    return "".join(parts)


def render(data: dict[str, Any]) -> str:
    stats = data["stats"]
    tables = data["tables"]
    known = {t["id"] for t in tables}
    out = [STYLE]
    out.append(
        '<p class="lead">Generated, not written: tables come from the Delta logs on '
        "MinIO, statistics and samples from DuckDB <code>delta_scan</code>, writers "
        f"and column logic from parsing <code>{esc(', '.join(data['scan_dirs']))}</code> "
        "with <code>ast</code>, and every description is quoted from a repo file shown "
        "next to it. Samples of PII columns and of hashes of PII are masked.</p>"
    )
    if data.get("_stale_reason"):
        out.append(
            f"<p>{badge('stale', 'warn')} live read failed: {esc(data['_stale_reason'])}</p>"
        )
    out.append(
        stat_cards(
            {
                "Delta tables": stats["tables"],
                "Columns": stats["columns"],
                "Columns described": f"{stats['pct']}% ({stats['pct_incl']}% incl. upstream)",
                "PII columns": f"{stats['pii']} (+{stats['hashed']} hashed)",
            }
        )
    )
    index_rows = []
    for t in tables:
        lc = t["last_commit"]
        writer = ", ".join(w["function"].rsplit(".", 1)[-1] for w in t["writers"])
        index_rows.append(
            [
                Raw(_anchor(t["id"])),
                t["layer"],
                _fmt_int(t["rows"]),
                len(t["columns"]),
                _fmt_size(t["size_bytes"]),
                t["version"],
                f"{lc.get('timestamp', '')} {lc.get('operation', '')}".strip()
                or "not derivable",
                writer or "none found",
                f"{t['described']}/{len(t['columns'])}",
                sum(1 for c in t["columns"] if c["pii"]),
                _view_links(t["id"]),
            ]
        )
    out.append(
        '<section class="block"><h2>Tables</h2>'
        + table_html(
            [
                "Table",
                "Layer",
                "Rows",
                "Cols",
                "Size",
                "Version",
                "Last commit",
                "Writer",
                "Described",
                "PII",
                "Views",
            ],
            index_rows,
            filterable=True,
            table_id="catalog-index",
        )
        + "</section>"
    )
    out.extend(_table_block(t, known) for t in tables)
    if data["code_only"]:
        out.append(
            '<section class="block"><h2>Named in code but not on MinIO</h2>'
            + table_html(
                ["Table id", "Where"],
                [[c["id"], "; ".join(c["where"])] for c in data["code_only"]],
            )
            + "</section>"
        )
    out.append(sources_html(data["sources"]))
    return "".join(out)
