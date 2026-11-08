"""Live half of the data-model page: what the Delta tables on MinIO actually hold.

DuckDB only (``delta_scan`` + ``read_ndjson_objects`` over ``_delta_log``); every
table is copied once into an in-memory temp table so the key checks below do
not re-read MinIO for every query. Only counts leave this module -- never values,
so no PII can reach the page from here.
"""

from __future__ import annotations

import json
import re
from typing import Any

from toolkit.common import LAKE_BUCKET, delta_uri

LOG_GLOB = f"s3://{LAKE_BUCKET}/*/*/_delta_log/*.json"
HEX64 = "^[0-9a-f]{64}$"


def qi(name: str) -> str:
    """Quote a SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def temp_name(table_id: str) -> str:
    return "t_" + table_id.replace(".", "__")


def load_tables(conn: Any, tables: list[str]) -> dict[str, dict[str, Any]]:
    """Materialise each Delta table and read its schema, row count and column stats."""
    out: dict[str, dict[str, Any]] = {}
    for table in tables:
        layer, name = table.split(".", 1)
        uri = delta_uri(layer, name)
        info: dict[str, Any] = {"uri": uri}
        try:
            conn.execute(
                f"CREATE OR REPLACE TEMP TABLE {qi(temp_name(table))} AS SELECT * FROM delta_scan(?)",
                [uri],
            )
        except Exception as error:  # duckdb raises several IO error types
            info["error"] = f"delta_scan failed: {type(error).__name__}: {error}"
            out[table] = info
            continue
        described = conn.execute(f"DESCRIBE {qi(temp_name(table))}").fetchall()
        info["columns"] = [
            {"name": str(r[0]), "duckdb_type": str(r[1])} for r in described
        ]
        info["rows"] = _scalar(conn, f"SELECT count(*) FROM {qi(temp_name(table))}")
        stats: dict[str, dict[str, Any]] = {}
        for col in info["columns"]:
            c = qi(col["name"])
            try:
                nulls, distinct = conn.execute(
                    f"SELECT count(*) FILTER (WHERE {c} IS NULL), count(DISTINCT {c}) FROM {qi(temp_name(table))}"
                ).fetchone()
                stats[col["name"]] = {"nulls": int(nulls), "distinct": int(distinct)}
            except Exception as error:
                stats[col["name"]] = {"error": f"{type(error).__name__}"}
        info["col_stats"] = stats
        out[table] = info
    return out


def _scalar(conn: Any, sql: str, params: list[Any] | None = None) -> int:
    row = conn.execute(sql, params or []).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def delta_logs(conn: Any) -> dict[str, dict[str, Any]]:
    """Latest metaData, partition columns, versions and schema history per table."""
    rows = conn.execute(
        "SELECT filename, json FROM read_ndjson_objects(?, filename=true)",
        [LOG_GLOB],
    ).fetchall()
    per_table: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    pattern = re.compile(rf"s3://{LAKE_BUCKET}/([^/]+)/([^/]+)/_delta_log/(\d+)\.json$")
    for filename, payload in rows:
        match = pattern.search(str(filename))
        if not match:
            continue
        table = f"{match.group(1)}.{match.group(2)}"
        per_table.setdefault(table, []).append(
            (int(match.group(3)), json.loads(payload))
        )
    out: dict[str, dict[str, Any]] = {}
    for table, actions in per_table.items():
        actions.sort(key=lambda item: item[0])
        versions = sorted({v for v, _ in actions})
        metas = [(v, a["metaData"]) for v, a in actions if "metaData" in a]
        commits = [(v, a["commitInfo"]) for v, a in actions if "commitInfo" in a]
        info: dict[str, Any] = {
            "versions": len(versions),
            "first_version": versions[0] if versions else None,
            "latest_version": versions[-1] if versions else None,
        }
        if metas:
            version, meta = metas[-1]
            schema = json.loads(meta.get("schemaString") or "{}")
            info["metadata_version"] = version
            info["partition_columns"] = list(meta.get("partitionColumns") or [])
            info["fields"] = [
                {
                    "name": f["name"],
                    "type": _delta_type(f.get("type")),
                    "nullable": bool(f.get("nullable", True)),
                }
                for f in schema.get("fields", [])
            ]
            info["schema_history"] = _schema_history(metas)
        if commits:
            version, commit = commits[-1]
            info["latest_operation"] = {
                "version": version,
                "operation": commit.get("operation"),
                "mode": (commit.get("operationParameters") or {}).get("mode"),
                "timestamp_ms": commit.get("timestamp"),
            }
        out[table] = info
    return out


def _delta_type(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and value.get("type") == "struct":
        inner = ", ".join(
            f"{f['name']}: {_delta_type(f.get('type'))}"
            for f in value.get("fields", [])
        )
        return f"struct<{inner}>"
    if isinstance(value, dict) and value.get("type") == "array":
        return f"array<{_delta_type(value.get('elementType'))}>"
    if isinstance(value, dict) and value.get("type") == "map":
        return f"map<{_delta_type(value.get('keyType'))}, {_delta_type(value.get('valueType'))}>"
    return json.dumps(value)


def _schema_history(metas: list[tuple[int, dict[str, Any]]]) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    previous: list[str] | None = None
    for version, meta in metas:
        schema = json.loads(meta.get("schemaString") or "{}")
        names = [f["name"] for f in schema.get("fields", [])]
        if previous is None:
            history.append(
                {"version": version, "added": names, "removed": [], "initial": True}
            )
        elif names != previous:
            history.append(
                {
                    "version": version,
                    "added": [n for n in names if n not in previous],
                    "removed": [n for n in previous if n not in names],
                    "reordered": sorted(set(names)) == sorted(set(previous)),
                    "initial": False,
                }
            )
        previous = names
    return history


def check_key(conn: Any, table: str, columns: list[str]) -> dict[str, Any]:
    """Uniqueness and null counts of a (possibly composite) key."""
    t = qi(temp_name(table))
    cols = ", ".join(qi(c) for c in columns)
    any_null = " OR ".join(f"{qi(c)} IS NULL" for c in columns)
    rows = _scalar(conn, f"SELECT count(*) FROM {t}")
    null_rows = _scalar(conn, f"SELECT count(*) FROM {t} WHERE {any_null}")
    dup = conn.execute(
        f"SELECT count(*), coalesce(sum(n), 0) FROM "
        f"(SELECT count(*) AS n FROM {t} WHERE NOT ({any_null}) GROUP BY {cols} HAVING count(*) > 1)"
    ).fetchone()
    distinct = _scalar(
        conn,
        f"SELECT count(*) FROM (SELECT DISTINCT {cols} FROM {t} WHERE NOT ({any_null}))",
    )
    dup_groups = int(dup[0]) if dup else 0
    dup_rows = int(dup[1]) if dup else 0
    return {
        "rows": rows,
        "null_rows": null_rows,
        "distinct": distinct,
        "duplicate_keys": dup_groups,
        "rows_in_duplicate_keys": dup_rows,
        "surplus_rows": dup_rows - dup_groups,
    }


def check_fk(
    conn: Any,
    child: str,
    child_col: str,
    parent: str,
    parent_col: str,
    types: tuple[str, str],
) -> dict[str, Any]:
    """Orphans, match rate and observed cardinality of child.col -> parent.col."""
    ct, pt = qi(temp_name(child)), qi(temp_name(parent))
    cc, pc = qi(child_col), qi(parent_col)
    as_text = types[0] != types[1]
    c_expr = f"CAST(c.{cc} AS VARCHAR)" if as_text else f"c.{cc}"
    p_expr = f"CAST(p.{pc} AS VARCHAR)" if as_text else f"p.{pc}"
    child_rows = _scalar(conn, f"SELECT count(*) FROM {ct}")
    null_fk = _scalar(conn, f"SELECT count(*) FROM {ct} WHERE {cc} IS NULL")
    orphan_rows = _scalar(
        conn,
        f"SELECT count(*) FROM {ct} c WHERE c.{cc} IS NOT NULL AND NOT EXISTS "
        f"(SELECT 1 FROM {pt} p WHERE {p_expr} = {c_expr})",
    )
    orphan_keys = _scalar(
        conn,
        f"SELECT count(DISTINCT c.{cc}) FROM {ct} c WHERE c.{cc} IS NOT NULL AND NOT EXISTS "
        f"(SELECT 1 FROM {pt} p WHERE {p_expr} = {c_expr})",
    )
    parent_rows = _scalar(conn, f"SELECT count(*) FROM {pt}")
    parent_keys = _scalar(conn, f"SELECT count(DISTINCT {pc}) FROM {pt}")
    parent_dupes = _scalar(
        conn,
        f"SELECT count(*) FROM (SELECT {pc} FROM {pt} WHERE {pc} IS NOT NULL GROUP BY {pc} HAVING count(*) > 1)",
    )
    referenced = _scalar(
        conn,
        f"SELECT count(DISTINCT {p_expr}) FROM {pt} p WHERE EXISTS "
        f"(SELECT 1 FROM {ct} c WHERE {c_expr} = {p_expr})",
    )
    max_children = _scalar(
        conn,
        f"SELECT coalesce(max(n), 0) FROM (SELECT count(*) AS n FROM {ct} c "
        f"WHERE c.{cc} IS NOT NULL GROUP BY c.{cc})",
    )
    child_distinct = _scalar(conn, f"SELECT count(DISTINCT {cc}) FROM {ct}")
    non_null = child_rows - null_fk
    matched = non_null - orphan_rows
    if parent_dupes:
        cardinality = "M:N (parent key repeats)"
    elif non_null == 0:
        cardinality = "no non-null child keys"
    elif child_distinct == non_null:
        cardinality = "1:1 (each parent at most one child row)"
    else:
        cardinality = "N:1 (many child rows per parent)"
    return {
        "child_rows": child_rows,
        "null_fk_rows": null_fk,
        "orphan_rows": orphan_rows,
        "orphan_keys": orphan_keys,
        "matched_rows": matched,
        "match_rate": round(matched / non_null, 4) if non_null else None,
        "parent_rows": parent_rows,
        "parent_keys": parent_keys,
        "parent_duplicate_keys": parent_dupes,
        "parents_referenced": referenced,
        "max_child_rows_per_parent": max_children,
        "cardinality": cardinality,
        "compared_as_text": as_text,
    }


def hex64_rate(conn: Any, table: str, column: str) -> dict[str, Any]:
    """How many non-null values look like a SHA-256 hex digest."""
    t, c = qi(temp_name(table)), qi(column)
    row = conn.execute(
        f"SELECT count({c}), count(*) FILTER (WHERE regexp_full_match(CAST({c} AS VARCHAR), ?)) FROM {t}",
        [HEX64],
    ).fetchone()
    non_null = int(row[0]) if row else 0
    hashed = int(row[1]) if row else 0
    return {
        "non_null": non_null,
        "hex64": hashed,
        "rate": round(hashed / non_null, 4) if non_null else None,
    }
