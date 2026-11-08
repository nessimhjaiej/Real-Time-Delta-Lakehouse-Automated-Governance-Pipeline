"""Call graph page: who calls what in the first-party Python code.

Static analysis only (``ast``); nothing is imported or executed. See
``_call_graph_ast`` for how call sites are resolved and
``_call_graph_entries`` for the entry points parsed from compose, Dockerfiles,
README and CI.
"""

from __future__ import annotations

import ast
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from toolkit.common import (
    REPO_ROOT,
    Raw,
    badge,
    esc,
    graph_block,
    href,
    section,
    sources_html,
    stat_cards,
    table_html,
)
from toolkit.pages._call_graph_ast import Analyzer, Def, first_doc_line, is_stdlib
from toolkit.pages._call_graph_entries import (
    ci_commands,
    compose_services,
    dockerfiles,
    documented_commands,
)

SLUG = "call-graph"
TITLE = "Call graph"
NAV_LABEL = "Call graph"
BLURB = (
    "Who calls what: every function, method and class in src/, dags/, config/ "
    "and tests/, resolved from the AST, with the entry points that start them."
)
LIVE = False

ANALYSED_ROOTS = ("src", "dags", "config", "tests")
SKIP_DIRS = frozenset(
    {
        "venv",
        ".venv",
        ".git",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
        "node_modules",
        "reports",
        "logs",
    }
)
ENTRY_GROUP = "entry"


# ------------------------------------------------------------------ discovery


def _discover(repo: Path) -> tuple[dict[str, tuple[str, str]], list[dict[str, str]]]:
    """Analysed modules (name -> (rel path, text)) and the .py files left out."""
    files: dict[str, tuple[str, str]] = {}
    excluded: list[dict[str, str]] = []
    for path in sorted(repo.rglob("*.py")):
        rel_parts = path.relative_to(repo).parts
        if any(part in SKIP_DIRS for part in rel_parts[:-1]):
            continue
        rel = "/".join(rel_parts)
        top = rel_parts[0]
        if top == "toolkit":
            excluded.append({"file": rel, "reason": "the toolkit itself (excluded)"})
            continue
        if len(rel_parts) > 1 and top not in ANALYSED_ROOTS:
            excluded.append(
                {
                    "file": rel,
                    "reason": "outside src/, dags/, config/, tests/ and top-level scripts",
                }
            )
            continue
        parts = list(rel_parts)
        parts[-1] = parts[-1][:-3]
        if parts[-1] == "__init__":
            parts = parts[:-1]
        module = ".".join(parts)
        files[module] = (rel, path.read_text(encoding="utf-8"))
    return files, excluded


def _group(module: str, is_package: bool) -> str:
    parts = module.split(".")
    if parts[0] == "tests":
        return "tests"
    if parts[0] in ("dags", "config"):
        return parts[0]
    if parts[0] == "src":
        if len(parts) >= 2 and parts[1] == "governance_agent":
            if len(parts) >= 4 or (len(parts) == 3 and is_package):
                return f"governance_agent.{parts[2]}"
            return "governance_agent"
        if len(parts) >= 2:
            return parts[1]
        return "src"
    return "scripts"


def _signature(definition: Def) -> str:
    node = definition.node
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        prefix = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
        text = f"{prefix} {node.name}({ast.unparse(node.args)})"
        if node.returns is not None:
            text += f" -> {ast.unparse(node.returns)}"
        return text
    if isinstance(node, ast.ClassDef):
        bases = ", ".join(ast.unparse(b) for b in node.bases)
        return f"class {node.name}({bases})" if bases else f"class {node.name}"
    if definition.kind == "main":
        return 'if __name__ == "__main__":'
    return "module-level statements"


# -------------------------------------------------------------------- collect


def collect() -> dict[str, Any]:
    files, excluded = _discover(REPO_ROOT)
    analyzer = Analyzer(files)
    analyzer.run()
    modules = analyzer.modules
    defs = analyzer.defs

    services, compose_file = compose_services(REPO_ROOT)
    docker = dockerfiles(REPO_ROOT)
    docs = documented_commands(REPO_ROOT)
    ci = ci_commands(REPO_ROOT)

    entries: list[dict[str, Any]] = [dict(e) for e in analyzer.entries]
    edge_map = analyzer.edges

    def add_edge(source: str, target: str, label: str, location: str) -> None:
        analyzer.add_edge(source, target, "registers", label, location)

    module_sites = Counter(site.def_id for site in analyzer.call_sites)

    def module_entry_targets(module: str) -> list[str]:
        """What `python -m <module>` runs: module-level code (if it calls
        anything) and the __main__ block."""
        targets = []
        if module_sites[f"{module}.<module>"] or f"{module}.__main__" not in defs:
            targets.append(f"{module}.<module>")
        if f"{module}.__main__" in defs:
            targets.append(f"{module}.__main__")
        return targets

    def first_party_module(name: str) -> bool:
        return name in modules

    # Compose services that start first-party Python.
    service_rows: list[dict[str, Any]] = []
    for service in services:
        targets: list[str] = []
        notes: list[str] = []
        for launch in service["launches"]:
            module = launch.get("module", "")
            if launch["how"] == "python -m" and first_party_module(module):
                for target in module_entry_targets(module):
                    targets.append(target)
                    add_edge(
                        f"service.{service['name']}",
                        target,
                        f"command: {service['command']}",
                        service["location"],
                    )
        if service["command"].strip() == "scheduler":
            for volume in service["volumes"]:
                host = volume.split(":")[0]
                host_dir = host.removeprefix("./").strip("/")
                if not host_dir.endswith("dags"):
                    continue
                for module, info in modules.items():
                    if info.path.startswith(host_dir + "/"):
                        target = f"{module}.<module>"
                        targets.append(target)
                        note = f"scheduler parses DAG files mounted from {volume}"
                        if service["airflow_executor"]:
                            note += (
                                f"; AIRFLOW__CORE__EXECUTOR="
                                f"{service['airflow_executor']} runs the tasks"
                            )
                        notes.append(note)
                        add_edge(
                            f"service.{service['name']}",
                            target,
                            "parses DAG file",
                            service["location"],
                        )
        service_rows.append({**service, "targets": targets, "notes": notes})
        if targets:
            entries.append(
                {
                    "id": f"service.{service['name']}",
                    "kind": "compose-service",
                    "label": f"service {service['name']}",
                    "location": service["location"],
                    "targets": targets,
                    "detail": service["command"] or service["entrypoint"],
                }
            )
            for target in targets:
                defs[target].entry.append(
                    f"started by compose service {service['name']}"
                )

    # __main__ blocks and notebooks.
    for definition in defs.values():
        if definition.kind == "main":
            entries.append(
                {
                    "id": definition.id,
                    "kind": "__main__",
                    "label": f"python -m {definition.module}",
                    "location": f"{modules[definition.module].path}:{definition.line}",
                    "targets": [definition.id],
                }
            )
        if definition.kind == "module" and modules[definition.module].is_notebook:
            definition.entry.append("notebook (# %% cells)")
            entries.append(
                {
                    "id": definition.id,
                    "kind": "notebook",
                    "label": modules[definition.module].path,
                    "location": f"{modules[definition.module].path}:1",
                    "targets": [definition.id],
                }
            )

    # Documented (README) and CI launch commands.
    pytest_sources: list[str] = []
    for command in [*docs, *ci]:
        cmd_id = f"cmd:{command['location']}"
        targets = []
        if command["how"] == "pytest":
            targets = ["pytest"]
            pytest_sources.append(f"{command['location']} ({command['text']})")
        elif command["how"] == "python -m" and first_party_module(command["module"]):
            targets = module_entry_targets(command["module"])
        elif command["how"] == "uvicorn" and first_party_module(command["module"]):
            targets = [
                str(e["id"])
                for e in analyzer.entries
                if e["kind"] == "api-route"
                and e.get("module") == command["module"]
                and e.get("app") == command["attr"]
            ]
            if module_sites[f"{command['module']}.<module>"]:
                targets.append(f"{command['module']}.<module>")
        if not targets:
            continue
        for target in targets:
            add_edge(cmd_id, target, "launches", command["location"])
        entries.append(
            {
                "id": cmd_id,
                "kind": (
                    "ci-command"
                    if command["location"].startswith(".github")
                    else "documented-command"
                ),
                "label": f"$ {command['text']}",
                "location": command["location"],
                "targets": targets,
            }
        )

    # pytest collects tests and fixtures.
    pytest_targets = sorted(d.id for d in defs.values() if d.is_test or d.is_fixture)
    for target in pytest_targets:
        label = "fixture" if defs[target].is_fixture else "collects test"
        location = f"{modules[defs[target].module].path}:{defs[target].line}"
        add_edge("pytest", target, label, location)
    if pytest_targets:
        entries.append(
            {
                "id": "pytest",
                "kind": "pytest",
                "label": "pytest",
                "location": "; ".join(pytest_sources) or "test_*.py files",
                "targets": pytest_targets,
            }
        )

    entry_ids = {str(e["id"]) for e in entries}
    synthetic = {str(e["id"]): e for e in entries if str(e["id"]) not in defs}

    # ---------------------------------------------------------------- edges
    edges: list[dict[str, Any]] = []
    for (source, target, kind), info in sorted(edge_map.items()):
        edges.append(
            {
                "source": source,
                "target": target,
                "kind": kind,
                "labels": sorted(info.labels),
                "count": len(info.lines),
                "lines": info.lines,
            }
        )
    fan_in: dict[str, set[str]] = defaultdict(set)
    fan_out: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        fan_out[edge["source"]].add(edge["target"])
        fan_in[edge["target"]].add(edge["source"])

    # Sites per def for the detail panel.
    site_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for site in analyzer.call_sites:
        site_counts[site.def_id][site.category] += 1

    # ---------------------------------------------------------------- nodes
    nodes: list[dict[str, Any]] = []
    for definition in sorted(defs.values(), key=lambda d: (d.module, d.line, d.id)):
        info = modules[definition.module]
        if definition.kind == "module" and not (
            fan_in[definition.id] or fan_out[definition.id] or definition.entry
        ):
            continue
        is_entry = definition.kind == "main" or definition.id in entry_ids
        if definition.kind == "module" and info.is_notebook:
            is_entry = True
        doc = first_doc_line(definition.node)
        if definition.kind == "main":
            doc = ""
        externals = sorted(definition.externals)
        nodes.append(
            {
                "id": definition.id,
                "module": definition.module,
                "qualname": definition.qualname,
                "kind": definition.kind,
                "file": info.path,
                "line": definition.line,
                "end_line": definition.end_line,
                "signature": _signature(definition),
                "doc": doc,
                "group": (
                    ENTRY_GROUP
                    if is_entry and definition.kind in ("main", "module")
                    else _group(definition.module, info.is_package)
                ),
                "package_group": _group(definition.module, info.is_package),
                "externals": externals,
                "externals_stdlib": [p for p in externals if is_stdlib(p)],
                "entry": definition.entry
                + (["__main__ block"] if definition.kind == "main" else []),
                "is_test": definition.is_test,
                "is_fixture": definition.is_fixture,
                "is_abstract": definition.is_abstract,
                "is_property": definition.is_property,
                "decorators": definition.decorators,
                "fan_in": len(fan_in[definition.id]),
                "fan_out": len(fan_out[definition.id]),
                "sites": dict(site_counts[definition.id]),
            }
        )
    for entry_id, entry in sorted(synthetic.items()):
        module = str(entry.get("module", ""))
        nodes.append(
            {
                "id": entry_id,
                "module": module,
                "qualname": "",
                "kind": str(entry["kind"]),
                "file": str(entry["location"]).rsplit(":", 1)[0],
                "line": 0,
                "end_line": 0,
                "signature": str(entry.get("detail", entry["label"])),
                "doc": "",
                "group": ENTRY_GROUP,
                "package_group": ENTRY_GROUP,
                "externals": [],
                "externals_stdlib": [],
                "entry": [str(entry["kind"])],
                "is_test": False,
                "is_fixture": False,
                "is_abstract": False,
                "is_property": False,
                "decorators": [],
                "fan_in": len(fan_in[entry_id]),
                "fan_out": len(fan_out[entry_id]),
                "sites": {},
                "label": str(entry["label"]),
                "location": str(entry["location"]),
            }
        )
    node_ids = {n["id"] for n in nodes}
    edges = [e for e in edges if e["source"] in node_ids and e["target"] in node_ids]

    # ------------------------------------------------------- graph partitions
    test_modules = {m for m in modules if m.split(".")[0] == "tests"}
    harness = {m for m in test_modules if f"{m}.__main__" in defs}
    changed = True
    while changed:
        changed = False
        for edge in edges:
            src_mod = _module_of(edge["source"], defs)
            dst_mod = _module_of(edge["target"], defs)
            if (
                src_mod in harness
                and dst_mod in test_modules
                and dst_mod not in harness
            ):
                harness.add(dst_mod)
                changed = True
    test_only = test_modules - harness
    app_nodes = [
        n["id"]
        for n in nodes
        if n["module"] not in test_only
        and n["id"] != "pytest"
        and not (
            n["kind"] in ("ci-command", "documented-command")
            and n["id"] in _pytest_cmds(entries)
        )
    ]
    test_seed = [
        n["id"]
        for n in nodes
        if n["module"] in test_only
        or n["id"] == "pytest"
        or n["id"] in _pytest_cmds(entries)
    ]
    seed = set(test_seed)
    test_nodes = set(seed)
    for edge in edges:
        if edge["source"] in seed:
            test_nodes.add(edge["target"])

    # --------------------------------------------------- module-level summary
    # synthetic DAG-task / route nodes belong to the module that declares them
    node_module = {str(n["id"]): str(n["module"]) for n in nodes}
    module_edges: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for edge in edges:
        src_mod = node_module.get(edge["source"], "")
        dst_mod = node_module.get(edge["target"], "")
        if src_mod and dst_mod and src_mod != dst_mod:
            module_edges[(src_mod, dst_mod)][edge["kind"]] += edge["count"]
    module_rows = []
    for module, info in sorted(modules.items()):
        members = [
            d for d in defs.values() if d.module == module and d.kind not in ("module",)
        ]
        module_rows.append(
            {
                "id": module,
                "file": info.path,
                "group": _group(module, info.is_package),
                "lines": len(info.text.splitlines()),
                "defs": len(members),
                "doc": first_doc_line(info.tree),
                "is_test": module in test_only,
                "out_modules": sorted({b for (a, b) in module_edges if a == module}),
                "in_modules": sorted({a for (a, b) in module_edges if b == module}),
            }
        )
    module_edge_rows = [
        {"source": a, "target": b, "counts": dict(c), "total": sum(c.values())}
        for (a, b), c in sorted(module_edges.items())
    ]

    # ------------------------------------------------------- resolution stats
    categories = Counter(site.category for site in analyzer.call_sites)
    categories_app = Counter(
        site.category
        for site in analyzer.call_sites
        if _module_of(site.def_id, defs) not in test_only
    )
    unresolved = [
        {"where": s.location, "in": s.def_id, "call": s.expr, "note": s.detail}
        for s in analyzer.call_sites
        if s.category == "unresolved"
    ]
    dynamic_reasons: dict[str, list[dict[str, str]]] = defaultdict(list)
    for site in analyzer.call_sites:
        if site.category == "dynamic":
            dynamic_reasons[site.detail].append(
                {"where": site.location, "in": site.def_id, "call": site.expr}
            )
    externals_usage: dict[str, dict[str, Any]] = {}
    for site in analyzer.call_sites:
        if site.category != "external":
            continue
        package = site.detail.split(".")[0]
        row = externals_usage.setdefault(
            package,
            {
                "package": package,
                "stdlib": is_stdlib(package),
                "sites": 0,
                "nodes": set(),
            },
        )
        row["sites"] += 1
        row["nodes"].add(site.def_id)
    externals_rows = [
        {**row, "nodes": len(row["nodes"])}
        for row in sorted(
            externals_usage.values(), key=lambda r: (-r["sites"], r["package"])
        )
    ]

    function_count = sum(
        1 for d in defs.values() if d.kind in ("function", "method", "closure")
    )
    class_count = sum(1 for d in defs.values() if d.kind == "class")
    first_party_edges = len(edges)
    entry_count = len(entries)

    sources = sorted(info.path for info in modules.values())
    sources = [f"ast.parse: {p}" for p in sources]
    if compose_file:
        sources.append(f"yaml.safe_load: {compose_file}")
    sources += [f"Dockerfile scan: {d['file']}" for d in docker]
    if docs:
        sources.append("README.md (fenced code blocks)")
    sources += sorted({f"CI run steps: {c['location'].rsplit(':', 1)[0]}" for c in ci})
    sources.append(
        f"Python {sys.version.split()[0]} ast module, sys.stdlib_module_names"
    )

    return {
        "summary": {
            "functions & methods": function_count,
            "call-graph edges": first_party_edges,
            "entry points": entry_count,
            "unresolved calls": categories.get("unresolved", 0),
        },
        "sources": sources,
        "stats": {
            "modules": len(modules),
            "functions": function_count,
            "classes": class_count,
            "call_sites": sum(categories.values()),
            "categories": dict(categories),
            "categories_app": dict(categories_app),
            "edge_kinds": dict(Counter(e["kind"] for e in edges)),
            "parse_errors": analyzer.parse_errors,
        },
        "nodes": nodes,
        "edges": edges,
        "entries": sorted(entries, key=lambda e: (str(e["kind"]), str(e["id"]))),
        "app_nodes": sorted(app_nodes),
        "test_nodes": sorted(test_nodes),
        "test_only_modules": sorted(test_only),
        "harness_modules": sorted(harness),
        "modules": module_rows,
        "module_edges": module_edge_rows,
        "unresolved": unresolved,
        "dynamic": {k: v for k, v in sorted(dynamic_reasons.items())},
        "externals": externals_rows,
        "services": service_rows,
        "dockerfiles": docker,
        "excluded_files": excluded,
    }


def _module_of(node_id: str, defs: dict[str, Def]) -> str:
    if node_id in defs:
        return defs[node_id].module
    return ""


def _pytest_cmds(entries: list[dict[str, Any]]) -> set[str]:
    return {
        str(e["id"])
        for e in entries
        if e["kind"] in ("ci-command", "documented-command")
        and e["targets"] == ["pytest"]
    }


# --------------------------------------------------------------------- render

GROUP_ORDER = (
    ENTRY_GROUP,
    "dags",
    "pipelines",
    "config",
    "utils",
    "notebooks",
    "governance_agent",
    "governance_agent.agent",
    "governance_agent.api",
    "governance_agent.executors",
    "governance_agent.rag",
    "governance_agent.semantic",
    "scripts",
    "tests",
)


def _groups(present: set[str]) -> dict[str, str]:
    ordered = [g for g in GROUP_ORDER if g in present]
    ordered += sorted(present - set(ordered))
    labels = {ENTRY_GROUP: "entry points"}
    return {g: labels.get(g, g.replace(".", "/")) for g in ordered}


def _node_label(node: dict[str, Any]) -> str:
    if node.get("label"):
        return str(node["label"])
    if node["kind"] == "module":
        return "<module code>"
    return str(node["qualname"])


def _edge_display(edge: dict[str, Any]) -> str:
    kind = edge["kind"]
    if kind == "calls":
        if "constructs" in edge["labels"]:
            return "new"
        if "property" in edge["labels"]:
            return "property"
        if any(label.startswith("fixture") for label in edge["labels"]):
            return "fixture"
        return f"×{edge['count']}" if edge["count"] > 1 else ""
    if kind == "dispatch":
        return "override"
    if kind == "returns":
        return "returns"
    label = edge["labels"][0] if edge["labels"] else kind
    if label.startswith("argument to "):
        return (
            "passed to "
            + label.removeprefix("argument to ").split("(")[0].split(".")[-1]
        )
    if "=" in label:
        return label.split("=")[0]
    return label


def _links(node: dict[str, Any]) -> list[dict[str, str]]:
    links = []
    if node["id"].startswith("service."):
        links.append(
            {
                "label": "Open this service on the architecture page",
                "href": href("architecture", node["id"]),
            }
        )
    for entry in node["entry"]:
        if entry.startswith("started by compose service "):
            name = entry.removeprefix("started by compose service ")
            links.append(
                {
                    "label": f"Container: service.{name} (architecture)",
                    "href": href("architecture", f"service.{name}"),
                }
            )
    return links


def _graph_nodes(
    nodes: list[dict[str, Any]], ids: set[str], modules: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    used_modules: set[str] = set()
    for node in nodes:
        if node["id"] not in ids:
            continue
        detail: dict[str, str] = {}
        if node["line"]:
            detail["file"] = f"{node['file']}:{node['line']}"
        elif node.get("location"):
            detail["source"] = str(node["location"])
        detail["kind"] = node["kind"]
        detail["signature"] = node["signature"]
        if node["doc"]:
            detail["docstring"] = node["doc"]
        detail["callers"] = str(node["fan_in"])
        detail["callees"] = str(node["fan_out"])
        if node["externals"]:
            detail["externals"] = ", ".join(
                p + (" (stdlib)" if p in node["externals_stdlib"] else "")
                for p in node["externals"]
            )
        if node["entry"]:
            detail["entry point"] = "; ".join(node["entry"])
        sites = node["sites"]
        if sites:
            detail["call sites"] = ", ".join(
                f"{k} {v}" for k, v in sorted(sites.items())
            )
        graph_node: dict[str, Any] = {
            "id": node["id"],
            "label": _node_label(node),
            "group": node["group"],
            "detail": detail,
            "links": _links(node),
        }
        module = node["module"]
        if module and module in modules and node["kind"] not in ("compose-service",):
            graph_node["parent"] = module
            used_modules.add(module)
        out.append(graph_node)
    for module in sorted(used_modules):
        info = modules[module]
        out.append(
            {
                "id": module,
                "label": info["file"],
                "group": info["group"],
                "detail": {"file": info["file"], "kind": "module"},
            }
        )
    return out


def _graph_edges(edges: list[dict[str, Any]], ids: set[str]) -> list[dict[str, Any]]:
    return [
        {
            "source": e["source"],
            "target": e["target"],
            "kind": e["kind"],
            "label": _edge_display(e),
        }
        for e in edges
        if e["source"] in ids and e["target"] in ids
    ]


def _where(lines: list[str]) -> str:
    shown = ", ".join(lines[:3])
    if len(lines) > 3:
        shown += f" (+{len(lines) - 3} more)"
    return shown


def _edge_table(edges: list[dict[str, Any]], ids: set[str], table_id: str) -> Raw:
    rows = [
        (
            e["source"],
            e["kind"],
            e["target"],
            "; ".join(e["labels"]),
            e["count"],
            _where(e["lines"]),
        )
        for e in edges
        if e["source"] in ids and e["target"] in ids
    ]
    return table_html(
        ["From", "Kind", "To", "How", "Sites", "Where"],
        rows,
        filterable=True,
        table_id=table_id,
    )


def _node_table(nodes: list[dict[str, Any]], ids: set[str], table_id: str) -> Raw:
    rows = []
    for node in nodes:
        if node["id"] not in ids:
            continue
        where = (
            f"{node['file']}:{node['line']}"
            if node["line"]
            else node.get("location", "")
        )
        rows.append(
            (
                node["id"],
                node["kind"],
                node["group"],
                where,
                node["fan_in"],
                node["fan_out"],
            )
        )
    return table_html(
        ["Node", "Kind", "Group", "Where", "Callers", "Callees"],
        rows,
        filterable=True,
        table_id=table_id,
    )


def render(data: dict[str, Any]) -> str:
    nodes: list[dict[str, Any]] = data["nodes"]
    edges: list[dict[str, Any]] = data["edges"]
    modules = {m["id"]: m for m in data["modules"]}
    stats = data["stats"]
    cats = stats["categories"]
    parts: list[str] = []

    parts.append(
        '<p class="lead">Generated by parsing every first-party Python file with '
        "<code>ast</code> (nothing is imported or run). Each call site is resolved "
        "through imports, aliases, <code>self</code>/<code>super()</code>, type "
        "annotations and simple local inference; functions handed to a framework "
        "(Airflow <code>python_callable</code>, FastAPI routes, LangGraph "
        "<code>add_node</code>, default arguments, closures returned by factories) "
        "become <em>registers</em> edges. Entry points come from "
        "<code>__main__</code> blocks, the DAG, route decorators, "
        "<code>docker-compose.yml</code>, README code blocks and CI steps.</p>"
    )
    parts.append(
        stat_cards(
            {
                "modules parsed": stats["modules"],
                "functions & methods": stats["functions"],
                "classes": stats["classes"],
                "call-graph edges": len(edges),
                "entry points": len(data["entries"]),
                "call sites": stats["call_sites"],
                "unresolved calls": cats.get("unresolved", 0),
            }
        )
    )
    if stats["parse_errors"]:
        parts.append(
            section(
                "Files that failed to parse",
                table_html(["Error"], [(e,) for e in stats["parse_errors"]]),
            )
        )

    kinds = ", ".join(f"{k}: {v}" for k, v in sorted(stats["edge_kinds"].items()))
    legend = (
        '<p class="muted small">Arrows point from caller to callee. Edge kinds: '
        "<strong>calls</strong> (a resolved call; <em>new</em> = constructor, "
        "<em>property</em> = property access), <strong>registers</strong> (a function "
        "passed as a value to something that will call it), <strong>returns</strong> "
        "(a factory returning a closure), <strong>dispatch</strong> (a base-class "
        f"method to a first-party override). Counts here: {esc(kinds)}. Boxes are "
        "files; colours are packages; toggle a package in the legend.</p>"
    )

    app_ids = set(data["app_nodes"])
    parts.append(
        section(
            "Function call graph (application code)",
            legend
            + graph_block(
                "calls",
                _graph_nodes(nodes, app_ids, modules),
                _graph_edges(edges, app_ids),
                groups=_groups({n["group"] for n in nodes if n["id"] in app_ids}),
                height=780,
                up_label="Called by (upstream)",
                down_label="Calls (downstream)",
                transitive_default=False,
            )
            + '<p class="muted small">Includes the eval harness '
            + esc(", ".join(data["harness_modules"]) or "(none)")
            + ", which has its own <code>__main__</code>. The pytest suite is in "
            "its own graph below.</p>"
            + "<details><summary>Nodes of this graph (table)</summary>"
            + _node_table(nodes, app_ids, "calls-nodes")
            + "</details><details><summary>Edges of this graph (table)</summary>"
            + _edge_table(edges, app_ids, "calls-edges")
            + "</details>",
            section_id="function-graph",
        )
    )

    # Entry points.
    entry_rows = []
    for entry in data["entries"]:
        targets = entry["targets"]
        shown = ", ".join(targets[:4]) + (
            f" (+{len(targets) - 4} more)" if len(targets) > 4 else ""
        )
        link: Any = ""
        if str(entry["id"]).startswith("service."):
            link = Raw(
                f'<a href="{esc(href("architecture", entry["id"]))}">architecture</a>'
            )
        entry_rows.append(
            (
                badge(str(entry["kind"]), "info"),
                entry["label"],
                entry["location"],
                shown,
                link,
            )
        )
    parts.append(
        section(
            "Entry points",
            '<p class="muted small">Where execution starts. Services come from '
            "<code>docker-compose.yml</code> commands; tasks from "
            "<code>python_callable=</code> in the DAG; routes from FastAPI "
            "decorators; commands from README code fences and CI <code>run:</code> "
            "steps.</p>"
            + table_html(
                ["Kind", "Entry", "Declared at", "Starts", "Link"],
                entry_rows,
                filterable=True,
                table_id="entries",
            ),
            section_id="entry-points",
        )
    )

    # Containers.
    service_rows = []
    for service in data["services"]:
        if service["targets"]:
            runs: Any = ", ".join(service["targets"])
        elif service["image"] and not service["dockerfile"]:
            runs = Raw(
                f'<span class="muted">image {esc(service["image"])}; no first-party Python</span>'
            )
        else:
            runs = Raw(
                '<span class="muted">no first-party Python module found in '
                f'command "{esc(service["command"] or service["entrypoint"] or "(none)")}"</span>'
            )
        service_rows.append(
            (
                Raw(
                    f'<a href="{esc(href("architecture", "service." + service["name"]))}">'
                    f'{esc(service["name"])}</a>'
                ),
                service["dockerfile"] or service["image"],
                service["command"] or service["entrypoint"],
                runs,
                "; ".join(service["notes"]),
                service["location"],
            )
        )
    docker_rows = []
    for docker in data["dockerfiles"]:
        if docker["instructions"]:
            for instruction in docker["instructions"]:
                docker_rows.append(
                    (
                        docker["file"],
                        docker["from"],
                        instruction["keyword"],
                        instruction["value"],
                        instruction["location"],
                    )
                )
        else:
            docker_rows.append(
                (
                    docker["file"],
                    docker["from"],
                    "none",
                    "no CMD/ENTRYPOINT: the base image's default applies (not derivable from the repo)",
                    docker["file"],
                )
            )
    parts.append(
        section(
            "Which code each container runs",
            table_html(
                ["Service", "Built from", "Command", "Runs", "Notes", "Declared at"],
                service_rows,
                filterable=True,
                table_id="services",
            )
            + "<h3>Dockerfiles</h3>"
            + table_html(["File", "FROM", "Instruction", "Value", "At"], docker_rows),
            section_id="containers",
        )
    )

    # Module summary graph.
    module_ids = {
        m["id"]
        for m in data["modules"]
        if m["defs"] or m["in_modules"] or m["out_modules"]
    }
    module_nodes = [
        {
            "id": m["id"],
            "label": m["file"],
            "group": m["group"],
            "detail": {
                "file": m["file"],
                "lines": str(m["lines"]),
                "definitions": str(m["defs"]),
                "docstring": m["doc"],
                "calls into": ", ".join(m["out_modules"]) or "none",
                "called from": ", ".join(m["in_modules"]) or "none",
            },
        }
        for m in data["modules"]
        if m["id"] in module_ids
    ]
    module_graph_edges = [
        {
            "source": e["source"],
            "target": e["target"],
            "label": str(e["total"]),
            "kind": "calls",
        }
        for e in data["module_edges"]
        if e["source"] in module_ids and e["target"] in module_ids
    ]
    module_edge_rows = [
        (
            e["source"],
            e["target"],
            e["total"],
            ", ".join(f"{k} {v}" for k, v in sorted(e["counts"].items())),
        )
        for e in data["module_edges"]
    ]
    parts.append(
        section(
            "Module summary",
            '<p class="muted small">One node per file; an edge label is the number '
            "of call sites (all edge kinds) from one file into another. The "
            "<em>tests</em> group can be toggled in the legend.</p>"
            + graph_block(
                "modules",
                module_nodes,
                module_graph_edges,
                groups=_groups({m["group"] for m in module_nodes}),
                height=640,
                up_label="Called from modules",
                down_label="Calls into modules",
                transitive_default=False,
            )
            + "<details><summary>Module-to-module edges (table)</summary>"
            + table_html(
                ["From module", "To module", "Call sites", "By kind"],
                module_edge_rows,
                filterable=True,
                table_id="module-edges",
            )
            + "</details>",
            section_id="module-graph",
        )
    )

    # Tests graph.
    test_ids = set(data["test_nodes"])
    parts.append(
        section(
            "Test suite: what each test calls",
            '<p class="muted small">pytest-collected tests and fixtures '
            "(<code>test_*.py</code>, <code>conftest.py</code>) with the "
            "first-party code they call directly. Toggle groups in the legend.</p>"
            + graph_block(
                "tests",
                _graph_nodes(nodes, test_ids, modules),
                _graph_edges(edges, test_ids),
                groups=_groups({n["group"] for n in nodes if n["id"] in test_ids}),
                height=720,
                up_label="Called by",
                down_label="Calls",
                transitive_default=False,
            )
            + "<details><summary>Nodes of this graph (table)</summary>"
            + _node_table(nodes, test_ids, "tests-nodes")
            + "</details><details><summary>Edges of this graph (table)</summary>"
            + _edge_table(edges, test_ids, "tests-edges")
            + "</details>",
            section_id="tests-graph",
        )
    )

    # All functions with fan-in / fan-out.
    fn_rows = []
    for node in sorted(nodes, key=lambda n: (-n["fan_in"], n["id"])):
        if node["kind"] not in (
            "function",
            "method",
            "class",
            "closure",
            "main",
            "module",
        ):
            continue
        flags = []
        if node["entry"]:
            flags.append(str(badge("entry", "info")))
        if node["is_test"]:
            flags.append(str(badge("test", "muted")))
        if node["is_fixture"]:
            flags.append(str(badge("fixture", "muted")))
        if node["is_abstract"]:
            flags.append(str(badge("abstract", "warn")))
        if node["is_property"]:
            flags.append(str(badge("property", "muted")))
        unresolved = node["sites"].get("unresolved", 0)
        fn_rows.append(
            (
                node["id"],
                node["kind"],
                f"{node['file']}:{node['line']}",
                node["fan_in"],
                node["fan_out"],
                ", ".join(node["externals"]),
                unresolved,
                Raw(" ".join(flags)),
            )
        )
    parts.append(
        section(
            "Every function, method and class",
            '<p class="muted small">Callers / callees count distinct nodes over all '
            "edge kinds. Externals are the packages a node calls into (calls on "
            "objects returned by a library are attributed to that library).</p>"
            + table_html(
                [
                    "Node id",
                    "Kind",
                    "Defined at",
                    "Callers",
                    "Callees",
                    "Externals",
                    "Unresolved",
                    "Flags",
                ],
                fn_rows,
                filterable=True,
                table_id="functions",
            ),
            section_id="functions",
        )
    )

    # Resolution report.
    app_cats = stats["categories_app"]
    cat_rows = [
        (
            cat,
            app_cats.get(cat, 0),
            cats.get(cat, 0) - app_cats.get(cat, 0),
            cats.get(cat, 0),
        )
        for cat in ("first-party", "external", "builtin", "dynamic", "unresolved")
    ]
    dyn_rows = []
    for reason, sites in data["dynamic"].items():
        examples = "; ".join(f"{s['call']} @ {s['where']}" for s in sites[:3])
        dyn_rows.append((reason, len(sites), examples))
    unresolved_rows = [
        (u["where"], u["in"], u["call"], u["note"]) for u in data["unresolved"]
    ]
    ext_rows = [
        (
            r["package"],
            badge("stdlib", "muted") if r["stdlib"] else badge("third-party", "info"),
            r["sites"],
            r["nodes"],
        )
        for r in data["externals"]
    ]
    parts.append(
        section(
            "How well the calls resolved",
            '<p class="muted small">Every call expression is classified. '
            "<em>dynamic</em> means the target is chosen at run time (an untyped or "
            "callable parameter, an item pulled from a container); "
            "<em>unresolved</em> means the analyser could not tell what is being "
            "called.</p>"
            + table_html(
                ["Category", "Application code", "pytest suite", "Total"], cat_rows
            )
            + "<h3>Dynamic calls by reason</h3>"
            + table_html(
                ["Reason", "Sites", "Examples"],
                dyn_rows,
                filterable=True,
                table_id="dynamic",
            )
            + f"<h3>Unresolved calls ({len(unresolved_rows)})</h3>"
            + (
                table_html(
                    ["Where", "In", "Call", "Note"],
                    unresolved_rows,
                    filterable=True,
                    table_id="unresolved",
                )
                if unresolved_rows
                else '<p class="muted">None.</p>'
            )
            + "<h3>External packages called</h3>"
            + table_html(
                ["Package", "Kind", "Call sites", "Nodes"],
                ext_rows,
                filterable=True,
                table_id="externals",
            ),
            section_id="resolution",
        )
    )

    excluded = data["excluded_files"]
    toolkit_count = sum(1 for e in excluded if e["reason"].startswith("the toolkit"))
    other = [e for e in excluded if not e["reason"].startswith("the toolkit")]
    parts.append(
        section(
            "Scope",
            f'<p class="muted small">{esc(stats["modules"])} files parsed. '
            f"{esc(toolkit_count)} toolkit files excluded by design.</p>"
            + (
                table_html(
                    ["Python file not analysed", "Why"],
                    [(e["file"], e["reason"]) for e in other],
                )
                if other
                else ""
            ),
            section_id="scope",
        )
    )
    parts.append(sources_html(data["sources"]))
    return "".join(parts)
