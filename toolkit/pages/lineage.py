"""Data lineage page: every dataset, the job that writes it and the jobs that read it.

Derived from the code (ast; see _lineage_extract), cross-checked against the
Airflow DAG's Dataset inlets/outlets and against the Delta tables on MinIO.
"""

from __future__ import annotations

import ast
from collections import deque
from pathlib import Path
from typing import Any

import yaml

from toolkit.common import (
    LAYERS,
    REPO_ROOT,
    Raw,
    badge,
    esc,
    graph_block,
    href,
    link,
    section,
    sources_html,
    stat_cards,
    table_html,
)
from toolkit.pages import _lineage_live as live
from toolkit.pages._lineage_extract import (
    Extractor,
    Func,
    Repo,
    Site,
    call_edges,
    dag_tasks,
    dotted,
    fastapi_routes,
)

SLUG = "lineage"
TITLE = "Data lineage"
NAV_LABEL = "Lineage"
BLURB = "Every table and the jobs between them, from source files and Kafka to Gold and the agent, derived from the code and checked against the DAG and MinIO."  # noqa: E501
LIVE = True

GROUPS = {
    "source": "Raw sources (files, HTTP)",
    "kafka": "Kafka topics",
    "bronze": "Bronze Delta",
    "silver": "Silver Delta",
    "gold": "Gold Delta",
    "serving": "DuckDB views + semantic layer",
    "agent": "Agent stores & config",
    "job_pipeline": "Job run by the DAG / a compose service",
    "job_other": "Job run by hand (CLI, notebook, API)",
}
BOXES = {
    "source": "Raw sources",
    "kafka": "Kafka",
    "bronze": "Bronze  ·  s3a://lakehouse/bronze",
    "silver": "Silver  ·  s3a://lakehouse/silver",
    "gold": "Gold  ·  s3a://lakehouse/gold",
    "serving": "Serving (governance agent)",
    "agent": "Agent stores & config",
}
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"


# ------------------------------------------------------------------ collect


def _merge_sites(sites: list[Site]) -> list[dict[str, Any]]:
    """One edge per (job, op, dataset, mode); evidence lines merged."""
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for s in sites:
        key = (s.job, s.op, s.target, s.mode)
        entry = merged.setdefault(
            key,
            {
                "job": s.job,
                "op": s.op,
                "kind": s.kind,
                "target": s.target,
                "mode": s.mode,
                "evidence": [],
                "via": [],
                "uri": s.uri,
                "extra": {},
            },
        )
        if s.evidence not in entry["evidence"]:
            entry["evidence"].append(s.evidence)
        if s.via and s.via not in entry["via"]:
            entry["via"].append(s.via)
        entry["extra"].update(s.extra)
    return list(merged.values())


def _reach(edges: dict[str, set[str]], start: str) -> dict[str, str | None]:
    """BFS from start: node -> predecessor on a shortest call path."""
    prev: dict[str, str | None] = {start: None}
    queue = deque([start])
    while queue:
        cur = queue.popleft()
        for nxt in sorted(edges.get(cur, ())):
            if nxt not in prev:
                prev[nxt] = cur
                queue.append(nxt)
    return prev


def _path(prev: dict[str, str | None], end: str) -> list[str]:
    out = [end]
    while prev.get(out[-1]) is not None:
        out.append(str(prev[out[-1]]))
    return list(reversed(out))


def _short(func_id: str) -> str:
    """'src.pipelines.bronze.ingest_batch_sources' -> 'bronze.ingest_batch_sources'."""
    parts = func_id.split(".")
    keep = 3 if len(parts) >= 3 and parts[-2][:1].isupper() else 2
    return ".".join(parts[-keep:])


def _compose_entries(repo: Repo) -> tuple[list[dict[str, str]], list[str]]:
    """Compose services whose command runs ``python -m <repo module>``."""
    out: list[dict[str, str]] = []
    notes: list[str] = []
    data = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8")) or {}
    for name, svc in (data.get("services") or {}).items():
        command = svc.get("command") if isinstance(svc, dict) else None
        words = (
            command.split()
            if isinstance(command, str)
            else [str(w) for w in command or []]
        )
        if "-m" in words and words.index("-m") + 1 < len(words):
            module = words[words.index("-m") + 1]
            fid = f"{module}.__main__"
            out.append(
                {
                    "id": f"service.{name}",
                    "label": f"service.{name} (python -m {module})",
                    "kind": "service",
                    "func": fid,
                    "evidence": "docker-compose.yml",
                }
            )
            if fid not in repo.funcs:
                notes.append(
                    f"service.{name} runs {module}, which has no __main__ block in the scanned code"
                )
        elif isinstance(command, str) and command.strip() == "scheduler":
            notes.append(
                f"service.{name} runs the Airflow scheduler (executes the DAG tasks below)"
            )
    return out, notes


def _config_value(repo: Repo, key: str) -> list[str]:
    """Values passed as ``.config(key, value)`` (Spark builder) anywhere in the code."""
    found: list[str] = []
    for fn in repo.funcs.values():
        for node in ast.walk(ast.Module(body=fn.body, type_ignores=[])):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "config"
                and len(node.args) == 2
            ):
                keys = [v for v, _ in repo.ev(node.args[0], fn.module, fn)]
                if key in keys:
                    found.extend(
                        str(v) for v, _ in repo.ev(node.args[1], fn.module, fn)
                    )
    return sorted(set(found))


def _client_urls(repo: Repo, class_suffix: str) -> list[str]:
    urls: list[str] = []
    for fn in repo.funcs.values():
        for node in ast.walk(ast.Module(body=fn.body, type_ignores=[])):
            if isinstance(node, ast.Call) and (dotted(node.func) or "").endswith(
                class_suffix
            ):
                for kw in node.keywords:
                    if kw.arg == "url":
                        urls.extend(
                            str(v)
                            for v, _ in repo.ev(kw.value, fn.module, fn)
                            if isinstance(v, str)
                        )
    return sorted(set(urls))


def _semantic(read_files: list[str]) -> dict[str, Any]:
    """Metrics/dimensions/roles from the YAML files the code reads (line numbers via yaml.compose)."""
    metrics: list[dict[str, Any]] = []
    dimensions: list[dict[str, Any]] = []
    roles: dict[str, list[str]] = {}
    for rel_path in read_files:
        if not rel_path.endswith((".yml", ".yaml")):
            continue
        text = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
        data = yaml.safe_load(text) or {}
        root = yaml.compose(text)
        if not isinstance(data, dict) or root is None:
            continue
        lines: dict[str, list[int]] = {}
        for key_node, value_node in getattr(root, "value", []):
            if isinstance(value_node, yaml.SequenceNode):
                lines[key_node.value] = [
                    item.start_mark.line + 1 for item in value_node.value
                ]
        for kind, bucket in (("metrics", metrics), ("dimensions", dimensions)):
            for i, item in enumerate(data.get(kind) or []):
                if isinstance(item, dict) and "table" in item:
                    line = lines.get(kind, [])[i] if i < len(lines.get(kind, [])) else 0
                    bucket.append(
                        {
                            "name": str(item.get("name")),
                            "table": str(item.get("table")),
                            "expression": str(
                                item.get("expression") or item.get("column") or ""
                            ),
                            "time_column": str(item.get("time_column") or ""),
                            "evidence": f"{rel_path}:{line}",
                            "file": rel_path,
                        }
                    )
        for role, spec in (data.get("roles") or {}).items():
            if isinstance(spec, dict):
                for table in spec.get("allowed_tables") or []:
                    roles.setdefault(str(table), []).append(f"{role} ({rel_path})")
    return {"metrics": metrics, "dimensions": dimensions, "roles": roles}


def collect() -> dict[str, Any]:
    repo = Repo()
    ex = Extractor(repo)
    ex.scan()
    repo.scope_fn, repo.scope_bind = None, {}
    io = _merge_sites(ex.sites)
    calls = call_edges(repo)

    # ---- entry points and what each one reaches
    entries: list[dict[str, str]] = []
    for fn in repo.funcs.values():
        if fn.qualname == "__main__":
            entries.append(
                {
                    "id": f"cli.{fn.module.name}",
                    "label": f"python -m {fn.module.name}",
                    "kind": "cli",
                    "func": fn.id,
                    "evidence": f"{fn.module.rel}:{fn.lineno}",
                }
            )
    job_ids = sorted({e["job"] for e in io})
    for jid in job_ids:
        fn = repo.funcs[jid]
        if fn.qualname == "<module>":
            entries.append(
                {
                    "id": f"script.{fn.module.name}",
                    "label": f"run {fn.module.rel} (module-level code)",
                    "kind": "script",
                    "func": jid,
                    "evidence": f"{fn.module.rel}:{fn.lineno}",
                }
            )
    tasks = dag_tasks(repo)
    for t in tasks:
        entries.append(
            {
                "id": f"dag.{t['dag_id']}.{t['task_id']}",
                "label": f"Airflow {t['dag_id']}.{t['task_id']}",
                "kind": "dag",
                "func": t["callable"],
                "evidence": t["evidence"],
            }
        )
    services, compose_notes = _compose_entries(repo)
    entries.extend(services)
    for r in fastapi_routes(repo):
        entries.append(
            {
                "id": f"http.{r['label']}",
                "label": r["label"],
                "kind": "http",
                "func": r["func"],
                "evidence": r["evidence"],
            }
        )
    reach = {e["id"]: _reach(calls, e["func"]) for e in entries}

    # ---- jobs
    jobs: dict[str, dict[str, Any]] = {}
    for jid in job_ids:
        fn: Func = repo.funcs[jid]
        run_by = []
        for e in entries:
            prev = reach[e["id"]]
            if jid in prev:
                run_by.append(
                    {
                        "entry": e["label"],
                        "kind": e["kind"],
                        "path": " -> ".join(_short(p) for p in _path(prev, jid)),
                        "evidence": e["evidence"],
                    }
                )
        jobs[jid] = {
            "id": jid,
            "label": _short(jid),
            "evidence": f"{fn.module.rel}:{fn.lineno}",
            "reads": sorted(
                {e["target"] for e in io if e["job"] == jid and e["op"] == "read"}
            ),
            "writes": sorted(
                {e["target"] for e in io if e["job"] == jid and e["op"] == "write"}
            ),
            "run_by": run_by,
            "pipeline": any(r["kind"] in ("dag", "service") for r in run_by),
        }

    # read-only job called (transitively) by a writing job -> its data flows into that job
    job_links: list[dict[str, Any]] = []
    for wid, w in jobs.items():
        if not w["writes"]:
            continue
        prev = _reach(calls, wid)
        for rid, r in jobs.items():
            if rid != wid and not r["writes"] and rid in prev:
                job_links.append(
                    {
                        "source": rid,
                        "target": wid,
                        "path": " -> ".join(_short(p) for p in _path(prev, rid)),
                    }
                )

    # ---- datasets
    datasets: dict[str, dict[str, Any]] = {}

    def ds(did: str, kind: str, uri: str = "") -> dict[str, Any]:
        if did not in datasets:
            datasets[did] = {
                "id": did,
                "kind": kind,
                "uri": uri,
                "writers": [],
                "readers": [],
                "dag_outlet_of": [],
                "dag_inlet_of": [],
                "extra": {},
            }
        elif uri and not datasets[did]["uri"]:
            datasets[did]["uri"] = uri
        return datasets[did]

    for e in io:
        d = ds(e["target"], e["kind"], e["uri"])
        (d["writers"] if e["op"] == "write" else d["readers"]).append(
            {
                "job": e["job"],
                "mode": e["mode"],
                "evidence": e["evidence"],
                "via": e["via"],
            }
        )
        if e["extra"].get("checkpoint"):
            d["extra"]["checkpoint"] = e["extra"]["checkpoint"]
    for t in tasks:
        for lid, origin in t["inlets"]:
            ds(lid, "delta", f"s3a://lakehouse/{lid.replace('.', '/', 1)}")[
                "dag_inlet_of"
            ].append({"task": t["task_id"], "evidence": origin})
        for lid, origin in t["outlets"]:
            ds(lid, "delta", f"s3a://lakehouse/{lid.replace('.', '/', 1)}")[
                "dag_outlet_of"
            ].append({"task": t["task_id"], "evidence": origin})

    # semantic layer: metrics / dimensions over the DuckDB views
    read_files = sorted(
        {d["uri"] for d in datasets.values() if d["kind"] == "file" and d["readers"]}
    )
    semantic = _semantic(read_files)
    views = {d["uri"]: did for did, d in datasets.items() if d["kind"] == "duckdb_view"}
    semantic_edges: list[dict[str, Any]] = []
    semantic_unresolved: list[str] = []
    for kind, items in (
        ("metric", semantic["metrics"]),
        ("dimension", semantic["dimensions"]),
    ):
        for item in items:
            nid = f"{kind}.{item['name']}"
            d = ds(nid, kind, item["evidence"])
            d["extra"].update(
                {
                    "expression": item["expression"],
                    "table": item["table"],
                    "time_column": item["time_column"],
                }
            )
            semantic_edges.append(
                {
                    "source": f"source.{item['file']}",
                    "target": nid,
                    "label": "defines",
                    "evidence": item["evidence"],
                }
            )
            if item["table"] in views:
                semantic_edges.append(
                    {
                        "source": views[item["table"]],
                        "target": nid,
                        "label": f"{kind} over",
                        "evidence": item["evidence"],
                    }
                )
            else:
                semantic_unresolved.append(
                    f"{nid} reads table {item['table']!r} ({item['evidence']}), but no DuckDB view of that name is created in the code"
                )
    for table, roles in semantic["roles"].items():
        if table in views:
            datasets[views[table]]["extra"]["allowed for roles"] = ", ".join(roles)

    # ---- live
    lake = live.lake_state()
    lake_tables: dict[str, Any] = lake.get("tables", {})
    for lid in lake_tables:
        ds(lid, "delta", f"s3a://lakehouse/{lid.replace('.', '/', 1)}")
    files = live.file_state(
        sorted(d["uri"] for d in datasets.values() if d["kind"] == "file")
    )
    sqlite_dbs: dict[str, list[str]] = {}
    for did, d in datasets.items():
        if d["kind"] == "sqlite3":
            sqlite_dbs.setdefault(d["uri"], []).append(did.split(".", 1)[1])
    sqlite_info = {
        db: live.sqlite_state(db, tables) for db, tables in sqlite_dbs.items() if db
    }
    qdrant_urls = _client_urls(repo, "QdrantClient")
    qdrant: dict[str, Any] = {"urls": qdrant_urls, "collections": {}}
    for did, d in datasets.items():
        if d["kind"] == "qdrant" and qdrant_urls:
            name = did.split(".", 1)[1]
            payload, status = live.http_json(
                f"{qdrant_urls[0].rstrip('/')}/collections/{name}"
            )
            points = (
                payload.get("result", {}).get("points_count")
                if isinstance(payload, dict)
                else None
            )
            qdrant["collections"][did] = {"status": status, "points": points}
    ol_urls = _config_value(repo, "spark.openlineage.transport.url")
    ol_ns = _config_value(repo, "spark.openlineage.namespace")
    marquez = (
        live.marquez_lineage(ol_urls[0], ol_ns[0])
        if ol_urls and ol_ns
        else {"status": "OpenLineage URL/namespace not found in code", "jobs": []}
    )
    marquez["derived_from"] = {
        "spark.openlineage.transport.url": ol_urls,
        "spark.openlineage.namespace": ol_ns,
    }

    # ---- per-dataset enrichment and group
    for did, d in datasets.items():
        kind = d["kind"]
        layer = did.split(".", 1)[0]
        if kind == "delta":
            d["group"] = layer if layer in LAYERS else "agent"
            d["lake"] = (
                lake_tables.get(did, {"exists": False})
                if lake["status"] == "ok"
                else {"status": "unavailable"}
            )
            if lake["status"] == "ok":
                d["lake"]["exists"] = did in lake_tables
        elif kind == "kafka":
            d["group"] = "kafka"
        elif kind == "http":
            d["group"] = "source"
        elif kind == "file":
            d["file"] = files.get(d["uri"], {})
            feeds_lake = any(
                t.split(".")[0] in LAYERS
                for r in d["readers"]
                for t in jobs[r["job"]]["writes"]
            )
            d["group"] = "source" if feeds_lake or d["writers"] else "agent"
        elif kind in ("duckdb_view", "metric", "dimension"):
            d["group"] = "serving"
        else:
            d["group"] = "agent"
        if kind == "sqlite3":
            info = sqlite_info.get(d["uri"], {})
            d["store"] = {
                "db": d["uri"],
                "exists": info.get("exists"),
                "rows": (info.get("rows") or {}).get(did.split(".", 1)[1]),
            }
        if kind == "qdrant":
            d["store"] = qdrant["collections"].get(
                did, {"status": "no client URL found in code"}
            )

    # ---- cross-check: DAG vs code
    dag_rows = []
    for t in tasks:
        prev = _reach(calls, t["callable"]) if t["callable"] else {}
        reached = [j for j in jobs if j in prev]
        code_reads = sorted(
            {x for j in reached for x in jobs[j]["reads"] if x.split(".")[0] in LAYERS}
        )
        code_writes = sorted(
            {x for j in reached for x in jobs[j]["writes"] if x.split(".")[0] in LAYERS}
        )
        inlets = sorted({lid for lid, _ in t["inlets"]})
        outlets = sorted({lid for lid, _ in t["outlets"]})
        dag_rows.append(
            {
                "task": t["task_id"],
                "callable": t["callable"],
                "evidence": t["evidence"],
                "upstream": t.get("upstream", []),
                "jobs": reached,
                "inlets": inlets,
                "code_reads": code_reads,
                "outlets": outlets,
                "code_writes": code_writes,
                "reads_not_declared": sorted(set(code_reads) - set(inlets)),
                "inlets_not_read": sorted(set(inlets) - set(code_reads)),
                "writes_not_declared": sorted(set(code_writes) - set(outlets)),
                "outlets_not_written": sorted(set(outlets) - set(code_writes)),
                "non_lake_reads": sorted(
                    {
                        x
                        for j in reached
                        for x in jobs[j]["reads"]
                        if x.split(".")[0] not in LAYERS
                    }
                ),
            }
        )
    all_outlets = {lid for t in tasks for lid, _ in t["outlets"]}
    lake_written = sorted(
        did for did, d in datasets.items() if d["kind"] == "delta" and d["writers"]
    )
    gap_map: dict[str, dict[str, Any]] = {}

    def gap(did: str, issue: str, evidence: list[str]) -> None:
        writers = datasets[did]["writers"]
        g = gap_map.setdefault(
            did,
            {
                "dataset": did,
                "issues": [],
                "writers": [w["job"] for w in writers],
                "run_by": sorted(
                    {r["entry"] for w in writers for r in jobs[w["job"]]["run_by"]}
                ),
                "evidence": [],
            },
        )
        g["issues"].append(issue)
        g["evidence"].extend(e for e in evidence if e not in g["evidence"])

    if tasks:
        for did in lake_written:
            if did not in all_outlets:
                gap(
                    did,
                    "written by code, but no DAG task declares it as an outlet",
                    sorted(
                        {ev for w in datasets[did]["writers"] for ev in w["evidence"]}
                    ),
                )
        for t in tasks:
            for lid, origin in t["inlets"]:
                if lid not in all_outlets:
                    gap(
                        lid,
                        f"inlet of DAG task {t['task_id']}, but no DAG task produces it",
                        [origin],
                    )
    dag_gaps = [{**g, "issue": "; ".join(g["issues"])} for g in gap_map.values()]

    # ---- cross-check: code vs lake
    lake_rows = []
    lake_ids = sorted(
        {did for did, d in datasets.items() if d["kind"] == "delta"},
        key=lambda x: (
            LAYERS.index(x.split(".")[0]) if x.split(".")[0] in LAYERS else 9,
            x,
        ),
    )
    for did in lake_ids:
        d = datasets[did]
        code_modes = sorted({w["mode"] for w in d["writers"]})
        info = lake_tables.get(did, {})
        seen = info.get("modes_seen", [])
        issues = []
        if lake["status"] == "ok":
            if not info:
                issues.append("declared/written in code but missing on MinIO")
            if info and not d["writers"]:
                issues.append("on MinIO but never written by the scanned code")
            if info and d["writers"] and set(seen) - set(code_modes):
                mv = info.get("mode_versions", {})
                issues.append(
                    "Delta log has write modes the current code does not use: "
                    + ", ".join(
                        f"{m} ({mv.get(m, '?')})"
                        for m in sorted(set(seen) - set(code_modes))
                    )
                    + " -- history from an earlier version of the code"
                    if info.get("last_mode") in code_modes
                    else "Delta log has write modes the code does not use: "
                    + ", ".join(
                        f"{m} ({mv.get(m, '?')})"
                        for m in sorted(set(seen) - set(code_modes))
                    )
                )
            if info and not d["readers"]:
                issues.append("never read by the scanned code")
        mode_versions = info.get("mode_versions", {})
        lake_rows.append(
            {
                "dataset": did,
                "code_modes": code_modes,
                "modes_seen": [f"{m} ({mode_versions.get(m, '?')})" for m in seen],
                "issues": issues,
                **{
                    k: info.get(k)
                    for k in (
                        "rows",
                        "version",
                        "last_commit",
                        "last_operation",
                        "commit_files",
                    )
                },
            }
        )
    ck_code = [
        {
            "checkpoint": c["checkpoint"],
            "target": c["target"],
            "job": c["job"],
            "evidence": c["evidence"],
        }
        for c in ex.checkpoints
    ]
    ck_lake = lake.get("checkpoints", [])

    # ---- graph edges
    edges: list[dict[str, Any]] = []
    for e in io:
        src, dst = (
            (e["target"], e["job"]) if e["op"] == "read" else (e["job"], e["target"])
        )
        edges.append(
            {
                "source": src,
                "target": dst,
                "op": e["op"],
                "label": e["mode"],
                "evidence": e["evidence"],
                "via": e["via"],
            }
        )
    for link_ in job_links:
        edges.append(
            {
                "source": link_["source"],
                "target": link_["target"],
                "op": "call",
                "label": "returns data",
                "evidence": [],
                "via": [link_["path"]],
            }
        )
    for se in semantic_edges:
        edges.append(
            {
                "source": se["source"],
                "target": se["target"],
                "op": "semantic",
                "label": se["label"],
                "evidence": [se["evidence"]],
                "via": [],
            }
        )

    disagreements = (
        len(dag_gaps)
        + sum(1 for r in lake_rows if r["issues"])
        + sum(
            1
            for r in dag_rows
            if r["reads_not_declared"]
            or r["inlets_not_read"]
            or r["writes_not_declared"]
            or r["outlets_not_written"]
        )
    )
    on_lake = sum(1 for r in lake_rows if r.get("version") is not None)
    summary = {
        "datasets": len(datasets),
        "jobs": len(jobs),
        "Delta tables on MinIO": on_lake if lake["status"] == "ok" else "MinIO down",
        "code vs DAG/lake findings": disagreements,
    }
    scanned = sorted(m.rel for m in repo.modules.values())
    sources = [
        "git ls-files '*.py' (excluding tests/ and toolkit/): " + ", ".join(scanned),
        "docker-compose.yml (services' python -m commands)",
        *[
            f"{f} (parsed: semantic metrics/dimensions/roles)"
            for f in read_files
            if f.endswith((".yml", ".yaml"))
        ],
        *[f"{f} (stat + record count)" for f in sorted(files)],
        *lake.get("queries", [f"MinIO: {lake.get('reason', '')}"]),
        *[f"sqlite (read-only): {db} SELECT count(*)" for db in sqlite_info],
        *[
            f"GET {qdrant_urls[0]}/collections/{c.split('.', 1)[1]}"
            for c in qdrant["collections"]
        ],
        f"GET {marquez.get('url', '?')}/api/v1/namespaces/{marquez.get('namespace', '?')}/jobs",
    ]
    return {
        "summary": summary,
        "sources": sources,
        "datasets": datasets,
        "jobs": jobs,
        "edges": edges,
        "entries": entries,
        "compose_notes": compose_notes,
        "dag": {"tasks": dag_rows, "gaps": dag_gaps},
        "lake": {
            "status": lake["status"],
            "reason": lake.get("reason", ""),
            "rows": lake_rows,
            "prefixes": lake.get("prefixes", []),
            "checkpoints_code": ck_code,
            "checkpoints_lake": ck_lake,
        },
        "marquez": marquez,
        "unresolved": [vars(u) for u in ex.unresolved]
        + [
            {
                "job": "",
                "evidence": "",
                "pattern": "semantic table",
                "expression": s,
                "reason": "no matching view",
            }
            for s in semantic_unresolved
        ],
        "generic": ex.generic,
        "parse_errors": repo.parse_errors,
        "scanned_files": scanned,
    }


# ------------------------------------------------------------------- render


def _label(d: dict[str, Any]) -> str:
    did, kind = d["id"], d["kind"]
    if kind == "delta":
        return did.split(".", 1)[1]
    if kind == "kafka":
        return did.split(".", 1)[1]
    if kind == "file":
        return Path(d["uri"]).name
    if kind == "duckdb_view":
        return f"{d['uri']}\n(DuckDB view)"
    if kind in ("metric", "dimension"):
        return f"{did.split('.', 1)[1]}\n({kind})"
    if kind == "sqlite3":
        return f"{did.split('.', 1)[1]}\n(SQLite)"
    if kind == "qdrant":
        return f"{did.split('.', 1)[1]}\n(Qdrant)"
    return did.split(".", 1)[1] if "." in did else did


def _fmt_int(v: Any) -> str:
    return f"{v:,}" if isinstance(v, int) else ("—" if v is None else str(v))


def _dataset_detail(d: dict[str, Any], jobs: dict[str, Any]) -> dict[str, str]:
    detail: dict[str, str] = {"id": d["id"], "kind": d["kind"]}
    if d["uri"]:
        detail["path / uri"] = d["uri"]
    detail["written by"] = (
        "; ".join(
            f"{_short(w['job'])} [{w['mode']}] {', '.join(w['evidence'])}"
            for w in d["writers"]
        )
        or "no writer in the scanned code"
    )
    detail["read by"] = (
        "; ".join(
            f"{_short(r['job'])} [{r['mode']}] {', '.join(r['evidence'])}"
            for r in d["readers"]
        )
        or "no reader in the scanned code"
    )
    if d["dag_outlet_of"] or d["dag_inlet_of"]:
        detail["DAG declares"] = "; ".join(
            [f"outlet of {o['task']} ({o['evidence']})" for o in d["dag_outlet_of"]]
            + [f"inlet of {i['task']} ({i['evidence']})" for i in d["dag_inlet_of"]]
        )
    lake = d.get("lake")
    if lake is not None:
        if lake.get("status") == "unavailable":
            detail["on MinIO"] = "MinIO not reachable at collect time"
        elif not lake.get("exists"):
            detail["on MinIO"] = "MISSING (no _delta_log)"
        else:
            detail["on MinIO"] = "yes"
            detail["rows"] = _fmt_int(lake.get("rows"))
            detail["Delta version"] = (
                f"{lake.get('version')} ({lake.get('commit_files')} commit JSON files)"
            )
            detail["last commit"] = (
                f"{lake.get('last_commit')} · {lake.get('last_operation')} ({lake.get('last_mode')})"
            )
    f = d.get("file")
    if f is not None:
        detail["file"] = (
            f"exists, {_fmt_int(f.get('bytes'))} bytes, modified {f.get('modified')}, git-tracked: {f.get('git_tracked')}"
            if f.get("exists")
            else "not present on disk"
        )
        if f.get("records") is not None:
            detail["records"] = f"{_fmt_int(f['records'])} ({f.get('records_how')})"
    store = d.get("store")
    if store is not None:
        detail["live"] = ", ".join(f"{k}: {v}" for k, v in store.items())
    for k, v in d["extra"].items():
        if v:
            detail[k] = str(v)
    return detail


def _dataset_links(d: dict[str, Any]) -> list[dict[str, str]]:
    if d["kind"] != "delta":
        return []
    return [
        {"label": "Catalog: columns + sample", "href": href("catalog", d["id"])},
        {
            "label": "Data model: keys + relationships",
            "href": href("data-model", d["id"]),
        },
        {"label": "Quality gates", "href": href("quality", d["id"])},
    ]


def _graph(data: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    datasets, jobs = data["datasets"], data["jobs"]
    nodes: list[dict[str, Any]] = []
    used_boxes = {d["group"] for d in datasets.values()}
    for key, label in BOXES.items():
        if key in used_boxes:
            nodes.append({"id": f"layer.{key}", "label": label})
    for d in datasets.values():
        nodes.append(
            {
                "id": d["id"],
                "label": _label(d),
                "group": d["group"],
                "parent": f"layer.{d['group']}",
                "detail": _dataset_detail(d, jobs),
                "links": _dataset_links(d),
            }
        )
    for j in jobs.values():
        nodes.append(
            {
                "id": j["id"],
                "label": j["label"],
                "group": "job_pipeline" if j["pipeline"] else "job_other",
                "detail": {
                    "callable": j["id"],
                    "defined at": j["evidence"],
                    "reads": ", ".join(j["reads"]) or "—",
                    "writes": ", ".join(j["writes"]) or "—",
                    "run by": "; ".join(
                        f"{r['entry']} via {r['path']}" for r in j["run_by"]
                    )
                    or "no entry point found by static call resolution",
                },
                "links": [
                    {
                        "label": "Call graph: who calls this",
                        "href": href("call-graph", j["id"]),
                    }
                ],
            }
        )
    edges = [
        {
            "source": e["source"],
            "target": e["target"],
            "label": e["label"],
            "kind": e["op"],
        }
        for e in data["edges"]
    ]
    return nodes, edges


def _ds_link(did: str, datasets: dict[str, Any]) -> Raw:
    return (
        Raw(f'<a href="#{esc(did)}">{esc(did)}</a>')
        if did in datasets
        else Raw(esc(did))
    )


def _neighbours(data: dict[str, Any], did: str) -> tuple[list[Raw], list[Raw]]:
    """Direct upstream / downstream datasets of a dataset, through the job between them."""
    datasets, edges = data["datasets"], data["edges"]
    into = [e for e in edges if e["target"] == did]
    out = [e for e in edges if e["source"] == did]
    ups: list[Raw] = []
    downs: list[Raw] = []
    for e in into:
        src = e["source"]
        if src in datasets:
            ups.append(
                Raw(
                    f"{_ds_link(src, datasets)} <span class='muted small'>({esc(e['label'])})</span>"
                )
            )
            continue
        feeders = [x for x in edges if x["target"] == src and x["source"] in datasets]
        job = link("call-graph", src, _short(src))
        if not feeders:
            ups.append(
                Raw(
                    f"{job} <span class='muted small'>[{esc(e['label'])}] (no dataset input)</span>"
                )
            )
        for x in feeders:
            ups.append(
                Raw(
                    f"{_ds_link(x['source'], datasets)} → {job} <span class='muted small'>[{esc(e['label'])}]</span>"
                )
            )
        for x in edges:
            if x["target"] == src and x["source"] in data["jobs"]:
                ups.append(
                    Raw(
                        f"{link('call-graph', x['source'], _short(x['source']))} → {job} <span class='muted small'>(returns data)</span>"
                    )
                )
    for e in out:
        dst = e["target"]
        if dst in datasets:
            downs.append(
                Raw(
                    f"{_ds_link(dst, datasets)} <span class='muted small'>({esc(e['label'])})</span>"
                )
            )
            continue
        job = link("call-graph", dst, _short(dst))
        targets = [x for x in edges if x["source"] == dst and x["target"] in datasets]
        chained = [
            x for x in edges if x["source"] == dst and x["target"] in data["jobs"]
        ]
        if not targets and not chained:
            downs.append(
                Raw(
                    f"{job} <span class='muted small'>[{esc(e['label'])}] (writes nothing)</span>"
                )
            )
        for x in targets:
            downs.append(
                Raw(
                    f"{job} → {_ds_link(x['target'], datasets)} <span class='muted small'>[{esc(x['label'])}]</span>"
                )
            )
        for x in chained:
            downs.append(
                Raw(
                    f"{job} → {link('call-graph', x['target'], _short(x['target']))} <span class='muted small'>(returns data)</span>"
                )
            )
    return ups, downs


def _ul(items: list[Raw]) -> str:
    if not items:
        return '<p class="muted small">none in the scanned code</p>'
    return "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>"


def _per_table(data: dict[str, Any]) -> str:
    datasets = data["datasets"]
    order = list(GROUPS)
    parts = []
    for d in sorted(
        datasets.values(), key=lambda x: (order.index(x["group"]), x["id"])
    ):
        ups, downs = _neighbours(data, d["id"])
        detail = _dataset_detail(d, data["jobs"])
        badges = [badge(GROUPS[d["group"]], "info")]
        lake = d.get("lake")
        if lake is not None and lake.get("status") != "unavailable":
            badges.append(
                badge("on MinIO", "ok")
                if lake.get("exists")
                else badge("missing on MinIO", "fail")
            )
        if d["kind"] == "delta" and not d["writers"]:
            badges.append(badge("no writer in code", "warn"))
        facts = "".join(
            f"<dt>{esc(k)}</dt><dd>{esc(v)}</dd>"
            for k, v in detail.items()
            if k not in ("id", "written by", "read by")
        )
        links = " · ".join(
            f'<a href="{esc(lnk["href"])}">{esc(lnk["label"])}</a>'
            for lnk in _dataset_links(d)
        )
        graph_link = f'<a href="{esc(href(SLUG, d["id"]))}">focus in graph</a>'
        open_attr = " open" if d["kind"] == "delta" else ""
        parts.append(
            f'<details id="{esc(d["id"])}"{open_attr}><summary><code>{esc(d["id"])}</code> {"".join(badges)}</summary>'
            f'<p class="small">{graph_link}{" · " + links if links else ""}</p>'
            f'<div style="overflow-wrap:anywhere"><h3>Upstream (direct)</h3>{_ul(ups)}<h3>Downstream (direct)</h3>{_ul(downs)}</div>'
            f'<dl class="small" style="overflow-wrap:anywhere">{facts}</dl></details>'
        )
    return "".join(parts)


def _list(items: list[str]) -> str:
    return ", ".join(items) if items else "—"


def render(data: dict[str, Any]) -> str:
    datasets = data["datasets"]
    nodes, edges = _graph(data)
    out: list[str] = [
        '<p class="lead">Each node below was found by parsing the repo\'s Python with <code>ast</code>: every Spark '
        "read/write chain, factory call (<code>ExtractorFactory</code>/<code>SinkFactory</code>, resolved through the "
        "strategy class it returns), Kafka producer, HTTP fetch, <code>open()</code>/<code>read_text()</code>, and SQL "
        "string run on a DuckDB/SQLite connection, with paths resolved from constants, dicts, loop variables and "
        "f-strings. Semantic metrics come from the YAML files the code loads. Live facts (rows, Delta versions) "
        "come from MinIO via DuckDB.</p>"
    ]
    if data.get("_stale_reason"):
        out.append(
            f'<p>{badge("stale", "warn")} live source failed: {esc(data["_stale_reason"])}</p>'
        )
    out.append(stat_cards(data["summary"]))
    if data["lake"]["status"] != "ok":
        out.append(
            f'<p>{badge("MinIO unavailable", "warn")} {esc(data["lake"]["reason"])}: lake checks below are not available.</p>'
        )

    graph_html = graph_block(
        "lineage",
        nodes,
        edges,
        groups=GROUPS,
        height=760,
        up_label="Upstream (feeds this)",
        down_label="Downstream (fed by this)",
    )
    edge_rows = [
        [
            e["source"],
            e["target"],
            e["op"],
            e["label"],
            ", ".join(e["evidence"]),
            "; ".join(e["via"]),
        ]
        for e in data["edges"]
    ]
    node_rows = []
    for n in nodes:
        if "group" not in n:
            continue
        det = n.get("detail", {})
        node_rows.append(
            [
                Raw(f'<a href="{esc(href(SLUG, n["id"]))}">{esc(n["id"])}</a>'),
                GROUPS.get(n["group"], n["group"]),
                det.get("path / uri", det.get("defined at", "")),
                det.get("on MinIO", ""),
            ]
        )
    out.append(
        section(
            "Lineage graph",
            '<p class="muted small">Datasets sit in their layer box; jobs (Python callables) sit between them. '
            "Edge labels are the write mode / read kind found in the code. Click a node for its full upstream and downstream chain.</p>"
            + graph_html
            + "<details><summary>Nodes as a table</summary>"
            + table_html(
                ["node", "type", "path / defined at", "on MinIO"],
                node_rows,
                filterable=True,
                table_id="lineage-nodes",
            )
            + "</details><details><summary>Edges as a table (with file:line evidence)</summary>"
            + table_html(
                ["from", "to", "op", "mode / label", "evidence", "resolved via"],
                edge_rows,
                filterable=True,
                table_id="lineage-edges",
            )
            + "</details>",
            section_id="graph",
        )
    )

    out.append(
        section(
            "Every dataset: direct upstream and downstream",
            '<p class="muted small">Each entry has the dataset id as its anchor, so <code>lineage.html?focus=&lt;id&gt;</code> lands here and on the graph node.</p>'  # noqa: E501
            + _per_table(data),
            section_id="datasets",
        )
    )

    # DAG cross-check
    dag = data["dag"]
    rows = []
    for t in dag["tasks"]:
        problems = [
            *(f"reads, not an inlet: {x}" for x in t["reads_not_declared"]),
            *(f"inlet, not read: {x}" for x in t["inlets_not_read"]),
            *(f"writes, not an outlet: {x}" for x in t["writes_not_declared"]),
            *(f"outlet, not written: {x}" for x in t["outlets_not_written"]),
        ]
        rows.append(
            [
                Raw(
                    f"<code>{esc(t['task'])}</code><br><span class='muted small'>{esc(t['evidence'])}</span>"
                ),
                link("call-graph", t["callable"], _short(t["callable"])),
                _list(t["inlets"]),
                _list(t["code_reads"]),
                _list(t["outlets"]),
                _list(t["code_writes"]),
                (
                    Raw(badge("agrees", "ok"))
                    if not problems
                    else Raw(
                        "<br>".join(
                            str(badge("differs", "fail")) + " " + esc(p)
                            for p in problems
                        )
                    )
                ),
            ]
        )
    gap_rows = [
        [
            _ds_link(g["dataset"], datasets),
            g["issue"],
            ", ".join(_short(w) for w in g["writers"]) or "—",
            "; ".join(g["run_by"]) or "—",
            ", ".join(g["evidence"]),
        ]
        for g in dag["gaps"]
    ]
    out.append(
        section(
            "Code vs the Airflow DAG's Dataset inlets/outlets",
            '<p class="muted small">Code reads/writes = lake datasets touched by every job statically reachable from the task\'s <code>python_callable</code>. Non-lake inputs (files, Kafka) are not declared as Datasets in the DAG and are listed separately.</p>'  # noqa: E501
            + table_html(
                [
                    "task",
                    "python_callable",
                    "declared inlets",
                    "code reads",
                    "declared outlets",
                    "code writes",
                    "verdict",
                ],
                rows,
                table_id="lineage-dag",
            )
            + "<h3>Datasets the DAG does not cover</h3>"
            + (
                table_html(
                    ["dataset", "finding", "written by", "run by", "evidence"],
                    gap_rows,
                    table_id="lineage-dag-gaps",
                )
                if gap_rows
                else '<p class="muted">none</p>'
            )
            + "<p class='small muted'>Non-lake inputs of DAG tasks: "
            + esc(
                "; ".join(
                    f"{t['task']}: {_list(t['non_lake_reads'])}" for t in dag["tasks"]
                )
            )
            + "</p>"
            + (
                "<p class='small muted'>"
                + esc("; ".join(data["compose_notes"]))
                + "</p>"
                if data["compose_notes"]
                else ""
            ),
            section_id="dag-check",
        )
    )

    # Lake cross-check
    lake = data["lake"]
    if lake["status"] == "ok":
        lrows = [
            [
                _ds_link(r["dataset"], datasets),
                _fmt_int(r["rows"]),
                _fmt_int(r["version"]),
                r["last_commit"] or "—",
                r["last_operation"] or "—",
                _list(r["code_modes"]),
                _list(r["modes_seen"]),
                (
                    Raw(
                        "<br>".join(
                            str(badge("check", "warn")) + " " + esc(i)
                            for i in r["issues"]
                        )
                    )
                    if r["issues"]
                    else Raw(badge("agrees", "ok"))
                ),
            ]
            for r in lake["rows"]
        ]
        ck_lake = set(lake["checkpoints_lake"])
        ck_rows = [
            [
                c["checkpoint"],
                c["target"],
                _short(c["job"]),
                c["evidence"],
                Raw(
                    badge("on MinIO", "ok")
                    if c["checkpoint"] in ck_lake
                    else badge("missing", "fail")
                ),
            ]
            for c in lake["checkpoints_code"]
        ]
        extra_ck = sorted(ck_lake - {c["checkpoint"] for c in lake["checkpoints_code"]})
        body = (
            table_html(
                [
                    "dataset",
                    "rows",
                    "Delta version",
                    "last commit",
                    "last operation",
                    "write modes in code",
                    "modes in Delta log",
                    "finding",
                ],
                lrows,
                table_id="lineage-lake",
            )
            + "<h3>Streaming checkpoints</h3>"
            + table_html(
                [
                    "checkpointLocation (code)",
                    "stream target",
                    "job",
                    "evidence",
                    "on MinIO",
                ],
                ck_rows,
                table_id="lineage-ckpt",
            )
            + (
                f"<p class='small'>{badge('check', 'warn')} checkpoints on MinIO no code writes: {esc(', '.join(extra_ck))}</p>"
                if extra_ck
                else ""
            )
            + "<p class='small muted'>Top-level prefixes in the bucket: "
            + esc(", ".join(lake["prefixes"]))
            + "</p>"
        )
    else:
        body = f"<p>{badge('unavailable', 'warn')} {esc(lake['reason'])}</p>"
    out.append(section("Code vs the lake on MinIO", body, section_id="lake-check"))

    # Marquez
    mq = data["marquez"]
    derived = mq.get("derived_from", {})
    mq_body = (
        f"<p>OpenLineage target derived from the Spark config in code: <code>{esc(_list(derived.get('spark.openlineage.transport.url', [])))}</code>, "  # noqa: E501
        f"namespace <code>{esc(_list(derived.get('spark.openlineage.namespace', [])))}</code>. "
        f"Probe of the Marquez API: <strong>{esc(mq.get('status'))}</strong>.</p>"
    )
    if mq.get("jobs"):
        mq_body += table_html(
            ["Marquez job", "inputs", "outputs"],
            [[j["name"], _list(j["inputs"]), _list(j["outputs"])] for j in mq["jobs"]],
            filterable=True,
            table_id="lineage-marquez",
        )
    else:
        mq_body += "<p class='muted small'>Not merged: Marquez lineage is only shown when its API already answers on localhost (this page never starts it).</p>"  # noqa: E501
    out.append(
        section("Marquez (OpenLineage) runtime lineage", mq_body, section_id="marquez")
    )

    # unresolved + generic
    un_rows = [
        [
            u["evidence"],
            _short(u["job"]) if u["job"] else "—",
            u["pattern"],
            u["expression"],
            u["reason"],
        ]
        for u in data["unresolved"]
    ]
    gen_rows = [
        [g["evidence"], _short(g["job"]), g["pattern"], g["expression"], g["format"]]
        for g in data["generic"]
    ]
    entry_rows = [
        [e["label"], e["kind"], _short(e["func"]) if e["func"] else "—", e["evidence"]]
        for e in data["entries"]
    ]
    out.append(
        section(
            "What the extractor could not resolve",
            "<p class='muted small'>Not derivable statically: these IO sites take their target at runtime, so they are not drawn.</p>"
            + (
                table_html(
                    ["where", "function", "pattern", "argument", "why"],
                    un_rows,
                    table_id="lineage-unresolved",
                )
                if un_rows
                else "<p>none</p>"
            )
            + "<h3>Generic IO strategies (resolved at their factory call sites)</h3>"
            + table_html(
                ["where", "method", "pattern", "argument", "format"],
                gen_rows,
                table_id="lineage-generic",
            )
            + "<h3>Entry points used for “run by”</h3>"
            + table_html(
                ["entry", "kind", "calls", "evidence"],
                entry_rows,
                filterable=True,
                table_id="lineage-entries",
            )
            + (
                f"<p>{badge('parse errors', 'fail')} {esc('; '.join(data['parse_errors']))}</p>"
                if data["parse_errors"]
                else ""
            ),
            section_id="unresolved",
        )
    )
    out.append(sources_html(data["sources"]))
    return "".join(out)
