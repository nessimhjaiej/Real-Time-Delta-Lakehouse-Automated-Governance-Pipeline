"""Data quality page: the Silver-to-Gold Great Expectations gate and its results.

collect() actually runs the gate (src.pipelines.quality.run_quality_gate) once,
in a child process, against the Silver Delta tables on MinIO, and records every
expectation result. The gate's wiring is read twice -- from the source with
ast (for file:line citations) and from the instantiated Airflow DAG (for the
values the scheduler would use) -- and the two are cross-checked. Every other
quality control in the pipeline code, the agent's query-side gates and the
tests that cover the gate are found by scanning the code.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from toolkit.common import (
    REPO_ROOT,
    Raw,
    badge,
    delta_uri,
    esc,
    graph_block,
    href,
    is_pii,
    lake_connection,
    link,
    list_lake_tables,
    load_env,
    rel,
    section,
    sources_html,
    stat_cards,
    table_html,
)
from toolkit.pages import _quality_scan as scan

SLUG = "quality"
TITLE = "Data quality"
NAV_LABEL = "Quality"
BLURB = (
    "The Silver-to-Gold Great Expectations gate, run live: every expectation's "
    "result, the gate's Airflow wiring, coverage gaps and the other controls."
)
LIVE = True

DAG_TIMEOUT_S = 300
GATE_TIMEOUT_S = 1800


# ------------------------------------------------------------------ collect


def _scrub(text: str) -> str:
    """Remove any secret-looking env value from text we might keep."""
    load_env()
    for key, value in os.environ.items():
        if value and len(value) >= 4 and re.search(r"KEY|SECRET|PASSWORD|TOKEN", key):
            text = text.replace(value, "***")
    return text


def _run_child(module: str, timeout: int, extra_env: dict[str, str]) -> Any:
    """Run ``python -m <module> <out.json>`` and return the parsed JSON."""
    with tempfile.TemporaryDirectory(prefix="toolkit-quality-") as tmp:
        out = Path(tmp) / "out.json"
        log = Path(tmp) / "child.log"
        env = {**os.environ, **extra_env, "PYTHONWARNINGS": "ignore"}
        with log.open("w", encoding="utf-8", errors="replace") as handle:
            proc = subprocess.run(
                [sys.executable, "-m", module, str(out)],
                cwd=REPO_ROOT,
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                check=False,
            )
        data = json.loads(out.read_text(encoding="utf-8")) if out.exists() else None
        if proc.returncode != 0 and (data is None or "outcome" not in data):
            tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-12:]
            raise RuntimeError(
                f"{module} exited {proc.returncode}: " + _scrub(" | ".join(tail))[:1500]
            )
        return data


def _lake_facts() -> dict[str, Any]:
    conn = lake_connection()  # SourceUnavailableError when MinIO is down
    try:
        tables = list_lake_tables(conn)
        schemas: dict[str, list[dict[str, str]]] = {}
        counts: dict[str, int] = {}
        for table_id in tables:
            layer, name = table_id.split(".", 1)
            uri = delta_uri(layer, name)
            rows = conn.execute(
                f"DESCRIBE SELECT * FROM delta_scan('{uri}')"
            ).fetchall()
            schemas[table_id] = [{"name": r[0], "type": r[1]} for r in rows]
            count = conn.execute(f"SELECT count(*) FROM delta_scan('{uri}')").fetchone()
            counts[table_id] = int(count[0]) if count else 0
    finally:
        conn.close()
    return {"tables": tables, "schemas": schemas, "counts": counts}


def _short_type(expectation_type: str) -> str:
    return expectation_type.removeprefix("expect_column_values_to_").removeprefix(
        "expect_"
    )


def collect() -> dict[str, Any]:
    lake = _lake_facts()  # raises SourceUnavailableError when MinIO is down

    with tempfile.TemporaryDirectory(prefix="toolkit-airflow-home-") as af_home:
        dag_runtime = _run_child(
            "toolkit.pages._quality_dag", DAG_TIMEOUT_S, {"AIRFLOW_HOME": af_home}
        )
    gate_run = _run_child("toolkit.pages._quality_runner", GATE_TIMEOUT_S, {})

    dag_src = scan.dag_ast()
    gate_src = scan.gate_code()
    writes = scan.lake_writes()
    compose = scan.compose_commands()
    controls, control_files = scan.scan_controls()
    absent, absent_files = scan.scan_absent()
    gate_names = [s["gate_name"] for s in gate_run["suites"]]
    tests = scan.gate_tests(gate_names)

    # ---------------- per-table results, joined to source lines and tests
    validations = {v["asset"]: v for v in gate_run.get("validations", [])}
    tables: list[dict[str, Any]] = []
    for suite in gate_run["suites"]:
        gate_name = suite["gate_name"]
        table_id = scan.lake_id(suite["path"]) or gate_name
        builder_fn = gate_src["builders"].get(gate_name, "")
        validation = validations.get(gate_name)
        results = validation["results"] if validation else []
        by_key = {(r["type"], r["column"]): r for r in results}
        rows = []
        for expectation in suite["expectations"]:
            column = expectation["kwargs"].get("column")
            kind = expectation["type"]
            result = by_key.get((kind, column))
            kwargs = {k: v for k, v in expectation["kwargs"].items() if k != "column"}
            rows.append(
                {
                    "column": column,
                    "type": kind,
                    "kwargs": kwargs,
                    "defined_at": gate_src["expectation_lines"].get(
                        f"{builder_fn}|{kind}|{column}"
                    ),
                    "result": result,
                    "tests_fail": [
                        t["test"]
                        for t in tests
                        if gate_name in t["tables"]
                        and f"{column}: {kind}" in t["asserts_fail"]
                    ],
                }
            )
        element_counts = {
            r["element_count"] for r in results if r.get("element_count") is not None
        }
        tables.append(
            {
                "id": table_id,
                "gate_name": gate_name,
                "path": suite["path"],
                "suite_name": suite["suite_name"],
                "builder": suite["builder"],
                "builder_at": f"{rel(scan.QUALITY_FILE)}:{suite['builder_line']}",
                "validated": validation is not None,
                "success": validation["success"] if validation else None,
                "element_count": sorted(element_counts),
                "duckdb_rows": lake["counts"].get(table_id),
                "schema": lake["schemas"].get(table_id),
                "expectations": rows,
                "gate_failures": gate_run.get("gate_failures", {}).get(gate_name),
                "tests_pass": [
                    t["test"] for t in tests if gate_name in t["asserts_pass"]
                ],
            }
        )

    # ---------------- wiring: runtime DAG + ast, cross-checked
    runtime_tasks = {
        (d["dag_id"], t["task_id"]): {**t, "dag_id": d["dag_id"], "file": d["file"]}
        for d in dag_runtime["dags"]
        for t in d["tasks"]
    }
    ast_tasks = {t["task_id"]: t for t in dag_src["tasks"]}
    gate_tasks = [
        t for t in runtime_tasks.values() if t["callable"] == scan.GATE_CALLABLE
    ]
    checks: list[dict[str, Any]] = []
    for task in runtime_tasks.values():
        source = ast_tasks.get(task["task_id"])
        checks.append(
            {
                "check": f"{task['task_id']}: callable in source == callable in loaded DAG",
                "ok": bool(source) and source["callable"] == task["callable"],
                "detail": f"{source and source['callable']} vs {task['callable']}",
            }
        )
    ast_edges = sorted((e["up"], e["down"]) for e in dag_src["edges"])
    rt_edges = sorted(
        (t["task_id"], d) for t in runtime_tasks.values() for d in t["downstream"]
    )
    checks.append(
        {
            "check": "task dependencies parsed from >> == upstream/downstream of loaded DAG",
            "ok": ast_edges == rt_edges,
            "detail": f"{len(ast_edges)} parsed, {len(rt_edges)} loaded",
        }
    )
    gate_reads = sorted(s["path"] for s in gate_run["suites"])
    for task in gate_tasks:
        checks.append(
            {
                "check": f"{task['task_id']} inlets == tables the gate reads (SILVER_PATHS)",
                "ok": sorted(task["inlets"]) == gate_reads,
                "detail": f"{len(task['inlets'])} inlets, {len(gate_reads)} read",
            }
        )
    for table in tables:
        executed = len([e for e in table["expectations"] if e["result"]])
        checks.append(
            {
                "check": f"{table['id']}: expectations built == expectations executed",
                "ok": executed == len(table["expectations"]),
                "detail": f"{len(table['expectations'])} built, {executed} executed",
            }
        )
        checks.append(
            {
                "check": f"{table['id']}: GX element_count == DuckDB count(*)",
                "ok": table["element_count"] == [table["duckdb_rows"]],
                "detail": f"{table['element_count']} vs {table['duckdb_rows']}",
            }
        )
        columns = {c["name"] for c in table["schema"] or []}
        missing = sorted(
            {e["column"] for e in table["expectations"] if e["column"] not in columns}
        )
        checks.append(
            {
                "check": f"{table['id']}: every expectation column exists in the Delta schema",
                "ok": not missing,
                "detail": ", ".join(missing) or "all present",
            }
        )
        failed = sorted(
            f"{table['gate_name']}.{e['column']}: {e['type']}"
            for e in table["expectations"]
            if e["result"] and not e["result"]["success"]
        )
        if table["gate_failures"] is not None:
            checks.append(
                {
                    "check": f"{table['id']}: gate's own failure list == failed GX results",
                    "ok": sorted(table["gate_failures"]) == failed,
                    "detail": f"{len(table['gate_failures'])} vs {len(failed)}",
                }
            )

    # ---------------- blind spots: who writes the gated / gold tables
    call_map: dict[str, list[str]] = writes["call_map"]
    entries: list[dict[str, Any]] = []
    for (dag_id, task_id), task in runtime_tasks.items():
        if task["callable"]:
            entries.append(
                {
                    "entry": f"DAG {dag_id} task {task_id}",
                    "kind": "dag_task",
                    "task_id": task_id,
                    "reach": sorted(scan.reach(call_map, [task["callable"]])),
                }
            )
    compose_by_module = {c["module"]: c for c in compose}
    for module_name, roots in writes["mains"].items():
        service = compose_by_module.get(module_name)
        entries.append(
            {
                "entry": f"python -m {module_name}"
                + (
                    f" (compose service {service['service']}, restart: {service['restart']})"
                    if service
                    else ""
                ),
                "kind": "service" if service else "__main__",
                "service": service["service"] if service else None,
                "service_at": service["at"] if service else None,
                "reach": sorted(
                    scan.reach(call_map, [*roots, f"{module_name}.__main__"])
                ),
            }
        )
    gate_task_ids = {t["task_id"] for t in gate_tasks}

    def ancestors(task_id: str) -> set[str]:
        seen: set[str] = set()
        todo = [task_id]
        while todo:
            current = todo.pop()
            for (_d, tid), task in runtime_tasks.items():
                if tid == current:
                    for up in task["upstream"]:
                        if up not in seen:
                            seen.add(up)
                            todo.append(up)
        return seen

    before_gate = (
        set().union(*(ancestors(t) for t in gate_task_ids)) if gate_task_ids else set()
    )
    after_gate = {tid for (_d, tid) in runtime_tasks if gate_task_ids & ancestors(tid)}
    gated_ids = {t["id"] for t in tables}
    write_rows = []
    for write in writes["writes"]:
        if not (write["table"] in gated_ids or write["table"].startswith("gold.")):
            continue
        for entry in entries:
            if write["function"] not in entry["reach"]:
                continue
            if entry["kind"] == "dag_task":
                if write["table"] in gated_ids:
                    status = (
                        "validated later in the same DAG run"
                        if entry["task_id"] in before_gate
                        else "written outside the gate's DAG path"
                    )
                else:
                    status = (
                        "only after the gate passed"
                        if entry["task_id"] in after_gate
                        else "NOT behind the gate"
                    )
            else:
                status = (
                    "outside the DAG: not validated until the next gate run"
                    if write["table"] in gated_ids
                    else "outside the DAG: bypasses the gate"
                )
            write_rows.append(
                {
                    **write,
                    "entry": entry["entry"],
                    "status": status,
                    "entry_kind": entry["kind"],
                    "service": entry.get("service"),
                    "service_at": entry.get("service_at"),
                }
            )

    # ---------------- totals
    all_results = [e for t in tables for e in t["expectations"]]
    passed = sum(1 for e in all_results if e["result"] and e["result"]["success"])
    failed_n = sum(1 for e in all_results if e["result"] and not e["result"]["success"])
    not_run = sum(1 for e in all_results if not e["result"])
    no_suite = [t for t in lake["tables"] if t not in gated_ids]
    outcome = gate_run.get("outcome") or {"status": "ERROR", "message": "no outcome"}

    sources = [
        rel(scan.QUALITY_FILE) + " (run: run_quality_gate, ast: suites/steps)",
        "python -m toolkit.pages._quality_runner (child process; one Spark session "
        "from config.spark_config.get_spark_session, stopped by the gate's finally)",
        "python -m toolkit.pages._quality_dag (imports dags/*.py with Airflow "
        f"{dag_runtime.get('airflow_version')})",
        *sorted({t["file"] for t in runtime_tasks.values()}),
        rel(scan.CALLBACKS_FILE),
        "config/spark_config.py",
        "docker-compose.yml (services with python -m commands)",
        "policies/access.yml",
        *[
            delta_uri(*t.split(".", 1)) + " (DuckDB delta_scan: schema, count)"
            for t in lake["tables"]
        ],
        "ast scan of " + ", ".join(control_files),
        "text search of " + ", ".join(absent_files),
        "ast scan of src/governance_agent/validator.py, compiler.py, executors/duckdb_executor.py",
        "ast scan of tests/**/*.py",
    ]

    return {
        "summary": {
            "gate verdict now": outcome["status"],
            "expectations passing": f"{passed}/{len(all_results)}",
            "Silver tables gated": len(tables),
            "lake tables with no suite": len(no_suite),
        },
        "outcome": outcome,
        "gx_version": gate_run.get("gx_version"),
        "run_settings": {
            "submit_args": gate_run.get("submit_args"),
            "lineage_sink": gate_run.get("lineage_sink"),
        },
        "gate_log": gate_run.get("log", []),
        "totals": {
            "expectations": len(all_results),
            "passed": passed,
            "failed": failed_n,
            "not_run": not_run,
            "rows_validated": sum(max(t["element_count"] or [0]) for t in tables),
        },
        "tables": tables,
        "lake_tables": lake["tables"],
        "lake_schemas": {k: v for k, v in lake["schemas"].items() if k in gated_ids},
        "no_suite": no_suite,
        "gate_code": gate_src,
        "dag_runtime": dag_runtime,
        "dag_source": dag_src,
        "gate_tasks": [t["task_id"] for t in gate_tasks],
        "callbacks": {
            t["task_id"]: scan.callback_code(t["on_failure_callback"])
            for t in gate_tasks
        },
        "after_gate": sorted(after_gate),
        "before_gate": sorted(before_gate),
        "checks": checks,
        "writes": write_rows,
        "controls": controls,
        "absent": absent,
        "agent_gates": scan.agent_gates(),
        "access_policy": scan.access_policy(),
        "tests": tests,
        "other_tests": scan.other_control_tests(),
        "sources": sources,
    }


# ------------------------------------------------------------------- render


def _ok(success: bool | None, yes: str = "pass", no: str = "fail") -> Raw:
    if success is None:
        return badge("not run", "warn")
    return badge(yes, "ok") if success else badge(no, "fail")


def _code(text: Any) -> Raw:
    return Raw(f"<code>{esc(text)}</code>")


def _table_links(table_id: str) -> Raw:
    return Raw(
        " · ".join(
            str(link(slug, table_id, label))
            for slug, label in (
                ("catalog", "catalog"),
                ("lineage", "lineage"),
                ("data-model", "data model"),
            )
        )
    )


def _pct(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):.2f}%"


def _verdict(data: dict[str, Any]) -> Raw:
    outcome = data["outcome"]
    status = outcome["status"]
    totals = data["totals"]
    kind = {"PASS": "ok", "FAIL": "fail"}.get(status, "warn")
    colour = {"ok": "var(--ok)", "fail": "var(--fail)"}.get(kind, "var(--warn)")
    gate = data["gate_code"]
    downstream = ", ".join(data["after_gate"]) or "no downstream task"
    if status == "PASS":
        meaning = (
            f"run_quality_gate() returned normally, so the Airflow task would succeed "
            f"and {downstream} would be allowed to run."
        )
    elif status == "FAIL":
        meaning = (
            f"run_quality_gate() raised {outcome.get('raised')}: the task fails "
            f"(retries=0) and {downstream} would not run."
        )
    else:
        meaning = (
            f"run_quality_gate() crashed with {outcome.get('raised')}: the Airflow task "
            f"would fail too, so {downstream} would not run."
        )
    spark = outcome.get("spark") or {}
    message = (
        f"<pre><code>{esc(outcome.get('message'))}</code></pre>"
        if outcome.get("message")
        else ""
    )
    return Raw(
        f'<section class="block" style="border-left:6px solid {colour}">'
        f"<h2>Gate verdict right now: {badge(status, kind)}</h2>"
        f'<p class="lead">{esc(meaning)}</p>'
        f"<p>{esc(totals['passed'])} of {esc(totals['expectations'])} expectations passed, "
        f"{esc(totals['failed'])} failed, {esc(totals['not_run'])} not run, over "
        f"{esc(totals['rows_validated'])} Silver rows. Gate run took "
        f"{esc(outcome.get('duration_s'))} s (Spark {esc(spark.get('version', '?'))}, "
        f"app {_code(spark.get('app_name', '?'))}, master {_code(spark.get('master', '?'))}, "
        f"GX {esc(data.get('gx_version'))}). Code: {_code(gate.get('run_at'))}.</p>"
        f"{message}</section>"
    )


def _wiring(data: dict[str, Any]) -> Raw:
    runtime = data["dag_runtime"]
    source = {t["task_id"]: t for t in data["dag_source"]["tasks"]}
    default_args = {d["key"]: d for d in data["dag_source"]["default_args"]}
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen: set[str] = set()
    table_status = {t["id"]: t for t in data["tables"]}

    def add_table(uri: str) -> str | None:
        table_id = scan.lake_id(uri)
        if not table_id:
            return None
        if table_id not in seen:
            seen.add(table_id)
            layer = table_id.split(".", 1)[0]
            status = table_status.get(table_id)
            if status:
                ok_n = sum(
                    1
                    for e in status["expectations"]
                    if e["result"] and e["result"]["success"]
                )
                label = f"{table_id}\nsuite {ok_n}/{len(status['expectations'])} pass"
                detail = {
                    "suite": status["suite_name"],
                    "result": "pass" if status["success"] else "fail",
                }
            else:
                label = f"{table_id}\nno suite"
                detail = {"suite": "none"}
            nodes.append(
                {
                    "id": table_id,
                    "label": label,
                    "group": layer,
                    "detail": {"uri": uri, **detail},
                    "links": [
                        (
                            {"label": "results on this page", "href": f"#{table_id}"}
                            if status
                            else {"label": "coverage", "href": "#coverage"}
                        ),
                        {"label": "catalog", "href": href("catalog", table_id)},
                        {"label": "lineage", "href": href("lineage", table_id)},
                    ],
                }
            )
        return table_id

    rows = []
    for dag in runtime["dags"]:
        for task in dag["tasks"]:
            tid = task["task_id"]
            node_id = f"airflow.{dag['dag_id']}.{tid}"
            src = source.get(tid, {})
            lines = src.get("kw_lines", {})
            retries_at = lines.get("retries") or default_args.get("retries", {}).get(
                "at"
            )
            is_gate = tid in data["gate_tasks"]
            nodes.append(
                {
                    "id": node_id,
                    "label": f"{tid}\nretries {task['retries']}",
                    "group": "gate" if is_gate else "task",
                    "detail": {
                        "callable": task["callable"],
                        "retries": f"{task['retries']} ({retries_at})",
                        "retry delay": f"{task['retry_delay_s']} s",
                        "trigger rule": task["trigger_rule"],
                        "on failure": task["on_failure_callback"] or "none",
                        "defined at": src.get("at"),
                    },
                    "links": (
                        [
                            {
                                "label": "call graph",
                                "href": href("call-graph", task["callable"]),
                            }
                        ]
                        if task["callable"]
                        else []
                    ),
                }
            )
            for down in task["downstream"]:
                edges.append(
                    {
                        "source": node_id,
                        "target": f"airflow.{dag['dag_id']}.{down}",
                        "label": "then",
                        "kind": "dependency",
                    }
                )
            for uri in task["inlets"]:
                table_id = add_table(uri)
                if table_id:
                    edges.append(
                        {
                            "source": table_id,
                            "target": node_id,
                            "label": "inlet",
                            "kind": "reads",
                        }
                    )
            for uri in task["outlets"]:
                table_id = add_table(uri)
                if table_id:
                    edges.append(
                        {
                            "source": node_id,
                            "target": table_id,
                            "label": "outlet",
                            "kind": "writes",
                        }
                    )
            if is_gate:
                callback = task["on_failure_callback"]
                if callback:
                    nodes.append(
                        {
                            "id": callback,
                            "label": callback.rsplit(".", 1)[-1],
                            "group": "callback",
                            "detail": {"callable": callback},
                            "links": [
                                {
                                    "label": "call graph",
                                    "href": href("call-graph", callback),
                                }
                            ],
                        }
                    )
                    edges.append(
                        {
                            "source": node_id,
                            "target": callback,
                            "label": "on_failure_callback",
                            "kind": "callback",
                        }
                    )
                raised = data["gate_code"].get("raises")
                if raised:
                    err_id = f"src.pipelines.quality.{raised}"
                    nodes.append(
                        {
                            "id": err_id,
                            "label": f"raise {raised}",
                            "group": "callback",
                            "detail": {
                                "raised at": next(
                                    (
                                        s["at"]
                                        for s in data["gate_code"]["steps"]
                                        if "raise" in s["step"]
                                    ),
                                    "?",
                                )
                            },
                            "links": [],
                        }
                    )
                    edges.append(
                        {
                            "source": node_id,
                            "target": err_id,
                            "label": "if any expectation fails",
                            "kind": "raises",
                        }
                    )
                    for down in task["downstream"]:
                        edges.append(
                            {
                                "source": err_id,
                                "target": f"airflow.{dag['dag_id']}.{down}",
                                "label": "blocks",
                                "kind": "blocks",
                            }
                        )
            rows.append(
                [
                    Raw(f"<strong>{esc(tid)}</strong>") if is_gate else tid,
                    _code(task["callable"]),
                    f"{task['retries']}",
                    retries_at or "Airflow default",
                    f"{task['retry_delay_s']} s",
                    task["trigger_rule"]
                    + (
                        "" if src.get("trigger_rule") else " (not set: Airflow default)"
                    ),
                    task["on_failure_callback"] or "—",
                    ", ".join(task["upstream"]) or "—",
                    ", ".join(task["downstream"]) or "—",
                    src.get("at", "?"),
                ]
            )
    for write in data["writes"]:
        if write["entry_kind"] == "service" and write["service"]:
            sid = f"service.{write['service']}"
            if sid not in seen:
                seen.add(sid)
                nodes.append(
                    {
                        "id": sid,
                        "label": f"{write['service']}\n(compose, outside DAG)",
                        "group": "service",
                        "detail": {
                            "entry": write["entry"],
                            "defined at": write["service_at"],
                        },
                        "links": [
                            {"label": "architecture", "href": href("architecture", sid)}
                        ],
                    }
                )
            table_id = add_table(scan.LAKE_PREFIX + write["table"].replace(".", "/", 1))
            if table_id:
                edges.append(
                    {
                        "source": sid,
                        "target": table_id,
                        "label": "writes (outside DAG)",
                        "kind": "writes",
                    }
                )

    groups = {
        "task": "Airflow task",
        "gate": "quality gate task",
        "bronze": "Bronze table",
        "silver": "Silver table",
        "gold": "Gold table",
        "callback": "failure path",
        "service": "compose service",
    }
    node_rows = [
        [n["id"], groups.get(n["group"], n["group"]), n["label"].replace("\n", " · ")]
        for n in nodes
    ]
    edge_rows = [[e["source"], e.get("label", ""), e["target"]] for e in edges]

    gate_code = data["gate_code"]
    steps = [[s["step"], s["at"]] for s in gate_code["steps"]]
    rule = gate_code.get("failure_rule")
    if rule:
        steps.append([rule["rule"], rule["at"]])
    vc = gate_code.get("validate_call")
    if vc:
        steps.append(
            [
                f"GX call {vc['code']} — result_format "
                + (
                    "passed explicitly"
                    if vc["result_format_passed"]
                    else "not passed (GX default SUMMARY)"
                ),
                vc["at"],
            ]
        )
    callbacks = []
    for task_id, cb in data["callbacks"].items():
        if not cb:
            callbacks.append([task_id, "no on_failure_callback", "—"])
            continue
        calls = ", ".join(
            c["call"] + " @" + c["at"].rsplit(":", 1)[1] for c in cb.get("calls", [])
        )
        callbacks.append(
            [
                task_id,
                Raw(f"{_code(cb['callable'])} makes these calls: {esc(calls)}"),
                cb.get("at", "?"),
            ]
        )

    checks = [
        [c["check"], _ok(c["ok"], "consistent", "MISMATCH"), c["detail"]]
        for c in data["checks"]
    ]

    return section(
        "Gate wiring",
        '<p class="muted small">Task values come from the DAG objects Airflow builds when it '
        "imports <code>dags/*.py</code> (default_args applied); file:line citations come from "
        "parsing the same files with <code>ast</code>. Click a node for upstream/downstream.</p>"
        + graph_block(
            "quality-wiring",
            nodes,
            edges,
            groups=groups,
            height=520,
            up_label="Feeds / runs before",
            down_label="Fed / runs after",
        )
        + "<h3>Tasks in the DAG that runs the gate</h3>"
        + table_html(
            [
                "task",
                "callable",
                "retries",
                "retries set at",
                "retry delay",
                "trigger rule",
                "on failure callback",
                "upstream",
                "downstream",
                "defined at",
            ],
            rows,
            table_id="q-tasks",
        )
        + "<h3>What the gate does, step by step (from the source)</h3>"
        + table_html(["step", "at"], steps, table_id="q-steps")
        + "<h3>On failure</h3>"
        + table_html(
            ["task", "callback", "defined at"], callbacks, table_id="q-callbacks"
        )
        + "<h3>Graph nodes and edges (text version)</h3>"
        + table_html(
            ["node id", "kind", "label"], node_rows, filterable=True, table_id="q-nodes"
        )
        + table_html(
            ["from", "edge", "to"], edge_rows, filterable=True, table_id="q-edges"
        )
        + "<h3>Consistency checks</h3>"
        + '<p class="muted small">Independent derivations of the same fact, compared at build time.</p>'
        + table_html(
            ["check", "result", "detail"], checks, filterable=True, table_id="q-checks"
        ),
        section_id="wiring",
    )


def _table_section(table: dict[str, Any]) -> Raw:
    rows = []
    for exp in table["expectations"]:
        result = exp["result"] or {}
        kwargs = ", ".join(f"{k}={v}" for k, v in exp["kwargs"].items()) or "—"
        partial = result.get("partial_unexpected") or []
        tests = exp["tests_fail"]
        rows.append(
            [
                (
                    badge(exp["column"], "pii")
                    if is_pii(str(exp["column"]))
                    else _code(exp["column"])
                ),
                exp["type"],
                kwargs,
                _ok(result.get("success") if exp["result"] else None),
                result.get("element_count", "—"),
                result.get("unexpected_count", "—"),
                _pct(result.get("unexpected_percent")),
                (
                    "—"
                    if result.get("missing_count") is None
                    else result.get("missing_count")
                ),
                ", ".join(str(v) for v in partial) or "—",
                (
                    Raw("<br>".join(str(_code(t.rsplit(".", 1)[-1])) for t in tests))
                    if tests
                    else badge("no test", "warn")
                ),
                exp["defined_at"] or "not derivable",
                result.get("exception") or "",
            ]
        )
    head = (
        f"<p>{_table_links(table['id'])} · read from {_code(table['path'])} · suite "
        f"{_code(table['suite_name'])} built by {_code(table['builder'])} "
        f"({esc(table['builder_at'])})</p>"
        f"<p>Result: {_ok(table['success'])} · rows validated (GX element_count): "
        f"{esc(', '.join(map(str, table['element_count'])) or '—')} · DuckDB count(*): "
        f"{esc(table['duckdb_rows'])}</p>"
    )
    failures = table.get("gate_failures")
    if failures:
        head += (
            "<p>Gate failure list: "
            + ", ".join(str(_code(f)) for f in failures)
            + "</p>"
        )
    if table["tests_pass"]:
        head += (
            '<p class="small muted">Asserted to pass on clean rows by: '
            + ", ".join(str(_code(t.rsplit(".", 1)[-1])) for t in table["tests_pass"])
            + "</p>"
        )
    else:
        head += '<p class="small muted">No test asserts this suite passes on clean rows.</p>'
    return Raw(
        f'<section class="block" id="{esc(table["id"])}"><h2>{esc(table["id"])} '
        f"{_ok(table['success'])}</h2>{head}"
        + table_html(
            [
                "column",
                "expectation",
                "kwargs",
                "result",
                "rows",
                "unexpected",
                "unexpected %",
                "missing (null)",
                "sample unexpected (masked)",
                "test asserting it fails",
                "defined at",
                "error",
            ],
            rows,
            table_id=f"q-results-{table['gate_name']}",
        )
        + "</section>"
    )


def _coverage(data: dict[str, Any]) -> Raw:
    types = sorted({e["type"] for t in data["tables"] for e in t["expectations"]})
    parts = [
        '<p class="muted small">Rows are the real Delta columns (DuckDB '
        "<code>DESCRIBE delta_scan</code>); columns are every expectation type any suite uses. "
        "A dash is a gap: that check is not run on that column.</p>"
    ]
    for table in data["tables"]:
        schema = table["schema"] or []
        by_col: dict[str, dict[str, Any]] = {}
        for exp in table["expectations"]:
            by_col.setdefault(str(exp["column"]), {})[exp["type"]] = exp
        names = [c["name"] for c in schema] + sorted(
            set(by_col) - {c["name"] for c in schema}
        )
        types_of = {c["name"]: c["type"] for c in schema}
        rows = []
        covered = 0
        for name in names:
            cells: list[Any] = []
            for kind in types:
                exp = by_col.get(name, {}).get(kind)
                if exp is None:
                    cells.append(Raw('<span class="muted">—</span>'))
                elif exp["result"] is None:
                    cells.append(badge("not run", "warn"))
                else:
                    cells.append(_ok(exp["result"]["success"]))
            if name in by_col:
                covered += 1
            rows.append(
                [
                    _code(name),
                    types_of.get(name, "NOT IN TABLE"),
                    badge("PII", "pii") if is_pii(name) else "",
                    *cells,
                ]
            )
        parts.append(
            f"<h3>{link(SLUG, table['id'])} — {covered} of {len(names)} columns checked</h3>"
        )
        parts.append(
            table_html(
                ["column", "type", "", *[_short_type(t) for t in types]],
                rows,
                table_id=f"q-cov-{table['gate_name']}",
            )
        )
    no_suite_rows = [
        [link("catalog", t), t.split(".", 1)[0], badge("no suite", "warn")]
        for t in data["no_suite"]
    ]
    parts.append(
        f"<h3>Lake tables the gate never checks ({len(data['no_suite'])} of "
        f"{len(data['lake_tables'])} Delta tables on MinIO)</h3>"
    )
    parts.append(
        table_html(["table", "layer", "suite"], no_suite_rows, table_id="q-nosuite")
    )
    return section("Coverage matrix", "".join(parts), section_id="coverage")


def _blind_spots(data: dict[str, Any]) -> Raw:
    rows = []
    for w in data["writes"]:
        bad = "outside" in w["status"] or "NOT" in w["status"]
        rows.append(
            [
                link("lineage", w["table"]),
                w["kind"],
                _code(w["function"]),
                w["at"],
                w["entry"],
                badge(w["status"], "warn" if bad else "ok"),
            ]
        )
    return section(
        "When the gate does not protect a table",
        '<p class="muted small">Every Delta write to a gated Silver table or a Gold table found by '
        "<code>ast</code> in <code>src/</code>, joined to each entry point that reaches the writing "
        "function (DAG task callables, <code>__main__</code> blocks, compose "
        "<code>python -m</code> services). Status compares the entry point with the gate task's "
        "position in the loaded DAG.</p>"
        + table_html(
            ["table", "write", "function", "at", "entry point", "status"],
            rows,
            filterable=True,
            table_id="q-writes",
        ),
        section_id="blind-spots",
    )


def _controls(data: dict[str, Any]) -> Raw:
    rows = [
        [
            c["category"],
            _code(c["construct"]),
            c["meaning"],
            c["detail"],
            _code(c["function"]),
            c["area"],
            c["at"],
        ]
        for c in data["controls"]
    ]
    counts: dict[str, int] = {}
    for c in data["controls"]:
        counts[c["category"]] = counts.get(c["category"], 0) + 1
    absent_rows = [
        [
            a["control"],
            _code(a["pattern"]),
            badge("not found", "warn") if not a["hits"] else ", ".join(a["hits"]),
        ]
        for a in data["absent"]
    ]
    rule_rows = [[k, v[0], v[1]] for k, v in sorted(scan.CONTROL_RULES.items())] + [
        [f"option {k}", v[0], v[1]] for k, v in sorted(scan.OPTION_RULES.items())
    ]
    return section(
        "Other quality controls in the pipeline code",
        stat_cards(dict(sorted(counts.items(), key=lambda kv: -kv[1])))
        + table_html(
            ["category", "construct", "what it does", "code", "function", "area", "at"],
            rows,
            filterable=True,
            table_id="q-controls",
        )
        + "<h3>Looked for, by text search</h3>"
        + table_html(["control", "pattern", "hits"], absent_rows, table_id="q-absent")
        + "<details><summary>Rules used to classify constructs</summary>"
        + table_html(
            ["construct", "category", "meaning"], rule_rows, table_id="q-rules"
        )
        + "</details>",
        section_id="other-controls",
    )


def _agent(data: dict[str, Any]) -> Raw:
    rows = [[g["check"], _code(g["function"]), g["at"]] for g in data["agent_gates"]]
    policy = data["access_policy"]
    policy_rows = [
        [r["role"], r["tables"], r["pii_columns"], r["max_rows"]]
        for r in policy.get("roles", [])
    ]
    return section(
        "Other gates: the analytics agent's query checks",
        '<p class="muted small">Not data quality but the other enforcement boundary in the repo: '
        "each SQL rejection the validator can produce, the compiler/executor guards, and where "
        "the validator is called. Messages are the literal strings from the code.</p>"
        + table_html(
            ["check / rejection", "function", "at"],
            rows,
            filterable=True,
            table_id="q-agent",
        )
        + f"<h3>Roles in {esc(policy.get('file', 'policies/access.yml'))}</h3>"
        + table_html(
            ["role", "allowed tables", "PII columns blocked", "max rows"],
            policy_rows,
            table_id="q-roles",
        ),
        section_id="agent-gates",
    )


def _tests(data: dict[str, Any]) -> Raw:
    rows = []
    for t in data["tests"]:
        rows.append(
            [
                _code(t["test"].rsplit(".", 1)[-1]),
                ", ".join(t["targets"]),
                ", ".join(t["tables"]) or "—",
                ", ".join(t["asserts_fail"]) or "—",
                ", ".join(t["asserts_pass"]) or "—",
                t["at"],
            ]
        )
    expectations = [e for tb in data["tables"] for e in tb["expectations"]]
    untested = [
        f"{tb['gate_name']}.{e['column']}: {_short_type(e['type'])}"
        for tb in data["tables"]
        for e in tb["expectations"]
        if not e["tests_fail"]
    ]
    other = [
        [o["file"], ", ".join(o["covers"]), o["tests"], o["first_at"]]
        for o in data["other_tests"]
    ]
    return section(
        "Tests that cover the gate",
        f"<p>{len(expectations) - len(untested)} of {len(expectations)} expectations have a unit test "
        "that feeds bad rows and asserts that exact failure. Tests are listed from the source; "
        "they are not run by this page.</p>"
        + (
            "<p>Without such a test: "
            + ", ".join(str(_code(u)) for u in untested)
            + "</p>"
            if untested
            else ""
        )
        + table_html(
            [
                "test",
                "imports",
                "tables validated",
                "asserts failure of",
                "asserts pass for",
                "at",
            ],
            rows,
            table_id="q-tests",
        )
        + "<h3>Tests of the other controls</h3>"
        + table_html(
            ["file", "imports module(s) of", "test functions", "first test"],
            other,
            table_id="q-othertests",
        ),
        section_id="tests",
    )


def render(data: dict[str, Any]) -> str:
    totals = data["totals"]
    stale = data.get("_stale_reason")
    parts: list[str] = []
    if stale:
        parts.append(
            f'<p class="badge badge-warn">Live run failed ({esc(stale)}); showing the last snapshot.</p>'
        )
    parts.append(
        '<p class="lead">Generated by running the repo\'s own gate '
        "(<code>src.pipelines.quality.run_quality_gate</code>) against the Silver Delta tables on "
        "MinIO and recording what Great Expectations computed. Nothing on this page is typed in.</p>"
    )
    parts.append(_verdict(data))
    parts.append(
        stat_cards(
            {
                "expectations": totals["expectations"],
                "passed": totals["passed"],
                "failed": totals["failed"],
                "Silver tables gated": len(data["tables"]),
                "rows validated": totals["rows_validated"],
                "lake tables with no suite": len(data["no_suite"]),
                "other controls found": len(data["controls"]),
            }
        )
    )
    toc = " · ".join(
        f'<a href="#{esc(i)}">{esc(t)}</a>'
        for i, t in [
            ("wiring", "wiring"),
            *[(t["id"], t["id"]) for t in data["tables"]],
            ("coverage", "coverage"),
            ("blind-spots", "blind spots"),
            ("other-controls", "other controls"),
            ("agent-gates", "agent gates"),
            ("tests", "tests"),
        ]
    )
    parts.append(f'<p class="small">{toc}</p>')
    parts.append(_wiring(data))
    parts.extend(_table_section(t) for t in data["tables"])
    parts.append(_coverage(data))
    parts.append(_blind_spots(data))
    parts.append(_controls(data))
    parts.append(_agent(data))
    parts.append(_tests(data))
    log_rows = [[r["level"], r["message"]] for r in data.get("gate_log", [])]
    settings = data.get("run_settings", {})
    parts.append(
        section(
            "How this run was made",
            "<p>The gate ran unmodified in a child process. Two read-only wrappers recorded "
            "the GX result of each <code>batch.validate(suite)</code> call and the failure list "
            "<code>_validate_table</code> returned. Spark options the repo's session builder does "
            f"not set were added for a light laptop run: {_code(settings.get('submit_args'))}; "
            f"OpenLineage events were sent to {_code(settings.get('lineage_sink'))} (closed port) "
            "so this inspection is not recorded as a pipeline run. Sample unexpected values pass "
            "through <code>mask_if_pii</code> before they are saved.</p>"
            + "<h3>Gate log output</h3>"
            + table_html(["level", "message"], log_rows, table_id="q-log"),
            section_id="method",
        )
    )
    parts.append(sources_html(data["sources"]))
    return "".join(parts)
