"""Live facts about the Delta tables on MinIO, read with DuckDB only.

* ``_delta_log/*.json`` (read_json, each action kept as raw JSON): version,
  last commit, Delta schema, active data files and their total size.
* ``delta_scan``: DuckDB column types, row count, per-column null/distinct/
  min/max aggregates, distinct sample values and five sample rows.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import uuid
from typing import Any

from toolkit.common import LAKE_BUCKET, delta_uri

NUMERIC_PREFIXES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "REAL",
    "DATE",
    "TIMESTAMP",
    "TIME",
)
LOG_GLOB = f"s3://{LAKE_BUCKET}/*/*/_delta_log/*.json"
SAMPLE_VALUES = 5
SAMPLE_ROWS = 5
LOCAL_COPY_MAX_ROWS = 200_000


def q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return str(value)


def _ms_to_iso(ms: Any) -> str | None:
    try:
        return (
            dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    except (TypeError, ValueError, OverflowError):
        return None


def delta_type_text(dtype: Any) -> str:
    """Compact text for a Delta schema type (``struct<count:long,rate:double>``)."""
    if isinstance(dtype, str):
        return dtype
    if isinstance(dtype, dict):
        kind = dtype.get("type")
        if kind == "struct":
            inner = ",".join(
                f"{f.get('name')}:{delta_type_text(f.get('type'))}"
                for f in dtype.get("fields", [])
            )
            return f"struct<{inner}>"
        if kind == "array":
            return f"array<{delta_type_text(dtype.get('elementType'))}>"
        if kind == "map":
            return (
                f"map<{delta_type_text(dtype.get('keyType'))},"
                f"{delta_type_text(dtype.get('valueType'))}>"
            )
    return json.dumps(dtype)


def read_delta_logs(conn: Any) -> dict[str, dict[str, Any]]:
    """Replay every table's JSON commit log -> per-table Delta metadata."""
    rows = conn.execute(
        'SELECT filename, commitInfo, metaData, "add", remove FROM read_json('
        f"{lit(LOG_GLOB)}, format='newline_delimited', filename=true, "
        "columns={'commitInfo': 'JSON', 'metaData': 'JSON', 'add': 'JSON', "
        "'remove': 'JSON'})"
    ).fetchall()
    per_table: dict[str, dict[int, list[tuple[Any, ...]]]] = {}
    for filename, commit, meta, add, remove in rows:
        parts = str(filename).split("/")
        tid = f"{parts[3]}.{parts[4]}"
        version = int(parts[-1].split(".")[0])
        per_table.setdefault(tid, {}).setdefault(version, []).append(
            (commit, meta, add, remove)
        )
    out: dict[str, dict[str, Any]] = {}
    for tid, versions in per_table.items():
        ordered = sorted(versions)
        contiguous = ordered == list(range(0, ordered[-1] + 1))
        active: dict[str, int] = {}
        schema: list[dict[str, Any]] | None = None
        partition_cols: list[str] = []
        created = None
        last_commit: dict[str, Any] = {}
        operations: dict[str, int] = {}
        for version in ordered:
            for commit, meta, add, remove in versions[version]:
                if commit:
                    info = json.loads(commit)
                    op = str(info.get("operation"))
                    operations[op] = operations.get(op, 0) + 1
                    last_commit = {
                        "version": version,
                        "timestamp": _ms_to_iso(info.get("timestamp")),
                        "operation": op,
                        "parameters": info.get("operationParameters") or {},
                        "metrics": info.get("operationMetrics") or {},
                        "engine": info.get("engineInfo"),
                    }
                if meta:
                    info = json.loads(meta)
                    try:
                        schema = json.loads(info.get("schemaString", "{}")).get(
                            "fields", []
                        )
                    except json.JSONDecodeError:
                        schema = None
                    partition_cols = list(info.get("partitionColumns") or [])
                    created = _ms_to_iso(info.get("createdTime"))
                if add:
                    info = json.loads(add)
                    active[str(info.get("path"))] = int(info.get("size") or 0)
                if remove:
                    info = json.loads(remove)
                    active.pop(str(info.get("path")), None)
        out[tid] = {
            "version": ordered[-1],
            "commit_files": len(ordered),
            "log_complete": contiguous,
            "last_commit": last_commit,
            "operations": operations,
            "created": created,
            "partition_columns": partition_cols,
            "delta_schema": (
                [
                    {
                        "name": f.get("name"),
                        "type": delta_type_text(f.get("type")),
                        "nullable": f.get("nullable"),
                    }
                    for f in schema
                ]
                if schema is not None
                else None
            ),
            "active_files": len(active) if contiguous else None,
            "size_bytes": sum(active.values()) if contiguous else None,
        }
    return out


def _is_orderable(duck_type: str) -> bool:
    return duck_type.upper().startswith(NUMERIC_PREFIXES)


def table_profile(conn: Any, layer: str, table: str) -> dict[str, Any]:
    """Row count, column types/aggregates, distinct samples and sample rows."""
    conn.execute("SET TimeZone = 'UTC'")  # timestamptz values rendered in UTC
    scan = f"delta_scan({lit(delta_uri(layer, table))})"
    total = int(conn.execute(f"SELECT count(*) FROM {scan}").fetchone()[0])
    src = scan
    if total <= LOCAL_COPY_MAX_ROWS:
        # small table: one scan into a temp table, then every per-column query is local
        conn.execute(f"CREATE OR REPLACE TEMP TABLE _catalog_t AS SELECT * FROM {scan}")
        src = "_catalog_t"
    described = conn.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    columns = [{"name": str(r[0]), "duck_type": str(r[1])} for r in described]
    selects = ["count(*)"]
    for col in columns:
        name = q(col["name"])
        selects.append(f"count({name})")
        selects.append(f"count(DISTINCT {name})")
        if _is_orderable(col["duck_type"]):
            selects.append(f"min({name})")
            selects.append(f"max({name})")
    agg = conn.execute(f"SELECT {', '.join(selects)} FROM {src}").fetchone()
    rows = int(agg[0])
    pos = 1
    for col in columns:
        non_null = int(agg[pos])
        col["nulls"] = rows - non_null
        col["null_pct"] = round(100.0 * (rows - non_null) / rows, 2) if rows else None
        col["distinct"] = int(agg[pos + 1])
        pos += 2
        if _is_orderable(col["duck_type"]):
            col["min"] = jsonable(agg[pos])
            col["max"] = jsonable(agg[pos + 1])
            pos += 2
        else:
            col["min"] = col["max"] = None
        sample = conn.execute(
            f"SELECT DISTINCT {q(col['name'])} AS v FROM {src} "
            f"WHERE {q(col['name'])} IS NOT NULL ORDER BY v LIMIT {SAMPLE_VALUES}"
        ).fetchall()
        col["samples"] = [jsonable(r[0]) for r in sample]
    result = conn.execute(f"SELECT * FROM {src} LIMIT {SAMPLE_ROWS}")
    sample_rows = [[jsonable(v) for v in r] for r in result.fetchall()]
    if src != scan:
        conn.execute("DROP TABLE IF EXISTS _catalog_t")
    return {
        "rows": rows,
        "columns": columns,
        "sample_rows": sample_rows,
        "query": f"SELECT count(*), count(c), count(DISTINCT c), min/max FROM {scan}",
    }
