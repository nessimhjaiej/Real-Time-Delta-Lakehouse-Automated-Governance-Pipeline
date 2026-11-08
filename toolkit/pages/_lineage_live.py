"""Read-only live checks for the lineage page: MinIO lake + Delta logs, local
files, the SQLite audit DB, and HTTP probes of Qdrant / Marquez. Nothing here
starts a service; every probe has a short timeout and degrades to a status."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from toolkit.common import (
    LAKE_BUCKET,
    REPO_ROOT,
    SourceUnavailableError,
    delta_uri,
    lake_connection,
    list_lake_tables,
)

LOG_GLOB = f"s3://{LAKE_BUCKET}/*/*/_delta_log/*.json"


def _iso(ms: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except (TypeError, ValueError, OSError):
        return ""


def _runs(versions: list[int]) -> str:
    """[0,1,2,5] -> 'v0-v2, v5'."""
    out: list[str] = []
    for v in sorted(versions):
        if out and out[-1].split("-")[-1] == f"v{v - 1}":
            out[-1] = f"{out[-1].split('-')[0]}-v{v}"
        else:
            out.append(f"v{v}")
    return ", ".join(out)


def _log_mode(operation: str, params: dict[str, Any]) -> str:
    """Delta commitInfo -> the same vocabulary as the code-derived write modes."""
    op = (operation or "").upper()
    if op == "STREAMING UPDATE":
        return f"stream-{str(params.get('outputMode', 'append')).lower()}"
    if op in ("WRITE", "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT"):
        return str(params.get("mode", "")).lower() or op.lower()
    return op.lower()


def lake_state() -> dict[str, Any]:
    """Delta tables on MinIO with row count and commit history from _delta_log."""
    try:
        conn = lake_connection()
    except SourceUnavailableError as error:
        return {"status": "unavailable", "reason": str(error), "tables": {}}
    try:
        conn.execute("SET enable_progress_bar = false")
        tables = list_lake_tables(conn)
        rows = conn.execute(
            "SELECT filename, commitInfo.timestamp, commitInfo.operation, "
            "CAST(commitInfo.operationParameters AS VARCHAR) "
            f"FROM read_json('{LOG_GLOB}', filename=true, format='newline_delimited', "
            "columns={commitInfo: 'STRUCT(timestamp BIGINT, operation VARCHAR, operationParameters JSON)'}) "
            "WHERE commitInfo IS NOT NULL"
        ).fetchall()
        commits: dict[str, list[dict[str, Any]]] = {}
        for filename, ts, operation, params_text in rows:
            parts = str(filename).split("/")
            tid = f"{parts[3]}.{parts[4]}"
            try:
                params = json.loads(params_text) if params_text else {}
            except json.JSONDecodeError:
                params = {}
            commits.setdefault(tid, []).append(
                {
                    "version": int(parts[-1].split(".")[0]),
                    "timestamp": _iso(ts),
                    "operation": operation,
                    "mode": _log_mode(
                        operation, params if isinstance(params, dict) else {}
                    ),
                }
            )
        out: dict[str, Any] = {}
        for tid in tables:
            layer, name = tid.split(".", 1)
            info: dict[str, Any] = {"uri": delta_uri(layer, name)}
            try:
                count = conn.execute(
                    f"SELECT count(*) FROM delta_scan('{delta_uri(layer, name)}')"
                ).fetchone()
                info["rows"] = int(count[0]) if count else None
            except Exception as error:  # duckdb raises several error types
                info["rows"] = None
                info["rows_error"] = f"{type(error).__name__}: {error}"[:200]
            history = sorted(commits.get(tid, []), key=lambda c: c["version"])
            info["commit_files"] = len(history)
            if history:
                last = history[-1]
                info.update(
                    version=last["version"],
                    last_commit=last["timestamp"],
                    last_operation=last["operation"],
                    last_mode=last["mode"],
                    first_version_in_log=history[0]["version"],
                    modes_seen=sorted({c["mode"] for c in history}),
                    mode_versions={
                        m: _runs([c["version"] for c in history if c["mode"] == m])
                        for m in sorted({c["mode"] for c in history})
                    },
                )
            out[tid] = info
        checkpoints = sorted(
            {
                "/".join(str(f).split("/")[:6])
                for (f,) in conn.execute(
                    f"SELECT file FROM glob('s3://{LAKE_BUCKET}/*/_checkpoints/*/*')"
                ).fetchall()
            }
        )
        prefixes = sorted(
            {
                "/".join(str(f).split("/")[3:5])
                for (f,) in conn.execute(
                    f"SELECT file FROM glob('s3://{LAKE_BUCKET}/**')"
                ).fetchall()
            }
        )
        return {
            "status": "ok",
            "tables": out,
            "checkpoints": [c.replace("s3://", "s3a://", 1) for c in checkpoints],
            "prefixes": prefixes,
            "queries": [
                f"list_lake_tables(): glob('s3://{LAKE_BUCKET}/*/*/_delta_log/*.json')",
                f"read_json('{LOG_GLOB}') -> commitInfo.timestamp/operation/operationParameters",
                "SELECT count(*) FROM delta_scan('s3://lakehouse/<layer>/<table>') per table",
                f"glob('s3://{LAKE_BUCKET}/*/_checkpoints/*/*'), glob('s3://{LAKE_BUCKET}/**')",
            ],
        }
    finally:
        conn.close()


def file_state(rel_paths: list[str]) -> dict[str, dict[str, Any]]:
    tracked = set(
        subprocess.run(
            ["git", "ls-files"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.split()
    )
    out: dict[str, dict[str, Any]] = {}
    for rel_path in rel_paths:
        path = REPO_ROOT / rel_path
        info: dict[str, Any] = {
            "exists": path.is_file(),
            "git_tracked": rel_path in tracked,
        }
        if path.is_file():
            info["bytes"] = path.stat().st_size
            info["modified"] = datetime.fromtimestamp(
                path.stat().st_mtime, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M UTC")
            if path.suffix == ".csv":
                with path.open(encoding="utf-8", errors="replace") as handle:
                    info["records"] = max(sum(1 for _ in handle) - 1, 0)
                info["records_how"] = "lines minus header"
            elif path.suffix == ".json":
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(payload, list):
                        info["records"] = len(payload)
                        info["records_how"] = "JSON array length"
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
        out[rel_path] = info
    return out


def sqlite_state(db_rel: str, tables: list[str]) -> dict[str, Any]:
    path = REPO_ROOT / db_rel
    if not path.is_file():
        return {"exists": False}
    info: dict[str, Any] = {"exists": True, "git_tracked": False, "rows": {}}
    try:
        conn = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True)
        try:
            for table in tables:
                if not table.isidentifier():
                    continue
                try:
                    row = conn.execute(
                        f"SELECT count(*) FROM {table}"
                    ).fetchone()  # noqa: S608 - identifier checked
                    info["rows"][table] = int(row[0]) if row else None
                except sqlite3.Error as error:
                    info["rows"][table] = f"error: {error}"
        finally:
            conn.close()
    except sqlite3.Error as error:
        info["error"] = str(error)
    return info


def http_json(url: str, timeout: float = 2.0) -> tuple[Any, str]:
    """GET a JSON URL; returns (payload or None, status text)."""
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(
            req, timeout=timeout
        ) as response:  # noqa: S310 - localhost probe
            body = response.read().decode("utf-8", errors="replace")
            return json.loads(body), f"HTTP {response.status}"
    except urllib.error.HTTPError as error:
        return None, f"HTTP {error.code}"
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        return (
            None,
            f"not responding ({type(error).__name__}: {getattr(error, 'reason', error)})",
        )
    except json.JSONDecodeError:
        return None, "responded, but not with JSON"


def marquez_lineage(base_url: str, namespace: str) -> dict[str, Any]:
    """Dataset-level input->output pairs per Marquez job, if the API answers."""
    payload, status = http_json(
        f"{base_url.rstrip('/')}/api/v1/namespaces/{namespace}/jobs?limit=500"
    )
    result: dict[str, Any] = {
        "url": base_url,
        "namespace": namespace,
        "status": status,
        "jobs": [],
    }
    if not isinstance(payload, dict):
        return result
    for job in payload.get("jobs", []):
        result["jobs"].append(
            {
                "name": job.get("name"),
                "inputs": [
                    f"{d.get('namespace')}|{d.get('name')}"
                    for d in job.get("inputs", [])
                ],
                "outputs": [
                    f"{d.get('namespace')}|{d.get('name')}"
                    for d in job.get("outputs", [])
                ],
            }
        )
    return result
