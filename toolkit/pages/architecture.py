"""Architecture page: the system diagram derived from what runs.

Sources, all read-only:
  * the resolved compose config (``docker compose config --format json``) and the
    Dockerfiles it builds from,
  * live container state (``docker compose ps -a --format json``), when docker is up,
  * the Python code (ast): entry points (``__main__`` guards, ASGI apps, scripts),
    what each one reaches, and the env vars / literals that name endpoints,
  * the Airflow DAG file (ast),
  * the Delta tables that exist on MinIO (for links to the lineage page).
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from collections.abc import Iterable
from typing import Any

from toolkit.common import (
    REPO_ROOT,
    Raw,
    SourceUnavailableError,
    badge,
    esc,
    graph_block,
    href,
    lake_connection,
    link,
    list_lake_tables,
    rel,
    section,
    sources_html,
    stat_cards,
    table_html,
)
from toolkit.pages import _architecture_compose as compose
from toolkit.pages._architecture_code import (
    CodeIndex,
    _dotted,
    asgi_apps,
    is_secret_key,
    main_calls,
    parse_endpoints,
    strings_in,
)
from toolkit.pages._architecture_dag import lake_id, parse_dags

SLUG = "architecture"
TITLE = "Architecture"
NAV_LABEL = "Architecture"
BLURB = (
    "The running system drawn from the resolved compose config, live container "
    "state and the code's own entry points and connection settings."
)
LIVE = True

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1"}
_LAKE_STR = re.compile(r"s3a?://[\w-]+/[\w./-]+")
_STDLIB = set(sys.stdlib_module_names)


# =================================================================== collect


def _dotenv_keys() -> list[str]:
    """Key NAMES in .env (values are never read into the snapshot)."""
    path = REPO_ROOT / ".env"
    if not path.exists():
        return []
    keys = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            keys.append(line.split("=", 1)[0].removeprefix("export ").strip())
    return sorted(set(keys))


def _tracked_text_files() -> list[str]:
    try:
        out = subprocess.run(
            ["git", "ls-files"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        return []
    keep = []
    for item in out:
        name = item.rsplit("/", 1)[-1]
        if item.startswith(("toolkit/", "venv/", ".")):
            continue
        if name.endswith((".md", ".yml", ".yaml", ".toml", ".sh", ".cfg")) or name in (
            "Dockerfile",
            "Makefile",
            "Procfile",
        ):
            keep.append(item)
    return keep


def _documented_urls(
    routes: list[dict[str, Any]], files: list[str]
) -> list[dict[str, str]]:
    """URLs in tracked Markdown docs whose path is one of the app's routes (not
    '/'). Config files are skipped: their URLs belong to the services they
    configure (e.g. a compose healthcheck's /health)."""
    paths = {r["path"] for r in routes if r["path"] not in ("/", "?")}
    url_re = re.compile(r"https?://([\w.-]+):(\d{2,5})(/[\w/{}.-]*)")
    found: list[dict[str, str]] = []
    for item in files:
        if not item.endswith(".md"):
            continue
        try:
            lines = (REPO_ROOT / item).read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for number, line in enumerate(lines, 1):
            for match in url_re.finditer(line):
                if match.group(3).rstrip("/") in paths:
                    found.append(
                        {
                            "address": f"{match.group(1)}:{match.group(2)}",
                            "path": match.group(3),
                            "where": f"{item}:{number}",
                        }
                    )
    return found


def _launch_commands(
    module: str, attr: str | None, files: list[str], secrets: set[str]
) -> list[str]:
    """Lines in tracked docs/config that launch a module (``-m mod``, ``path.py``
    or ``uvicorn mod:attr``), as 'file:line: text'."""
    path = module.replace(".", "/") + ".py"
    patterns = [
        rf"-m\s+{re.escape(module)}(?![\w.])",
        rf"python[0-9.]*\s+(?:-\S+\s+)*(?:\./)?{re.escape(path)}\b",
    ]
    if attr:
        patterns.append(rf"{re.escape(module)}:{re.escape(attr)}\b")
    regex = re.compile("|".join(patterns))
    hits = []
    for item in files:
        if item == "docker-compose.yml":
            continue  # compose commands are shown on the service itself
        try:
            lines = (REPO_ROOT / item).read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError):
            continue
        for number, line in enumerate(lines, 1):
            if regex.search(line):
                text = compose.redact_command(line.strip().strip("`"), secrets)
                hits.append(f"{item}:{number}: {text}")
    return hits


def _tag(context: str | None) -> str:
    if not context:
        return ""
    first = context.split(", ")[0]
    if re.fullmatch(r"[\w.-]+", first) and "." in first:
        parts = first.split(".")
        while parts and parts[0] in ("spark", "hadoop", "fs"):
            parts = parts[1:]
        return parts[0] if parts else first
    return first.split("[")[0]


def _dedupe_facts(index: CodeIndex, units: Iterable[str]) -> list[dict[str, Any]]:
    best: dict[tuple[Any, ...], dict[str, Any]] = {}
    for uid in units:
        for fact in index.units[uid].facts:
            key = (
                fact["kind"],
                fact.get("key"),
                fact.get("value"),
                fact["file"],
                fact["line"],
            )
            current = best.get(key)
            if current is None or (not current.get("context") and fact.get("context")):
                best[key] = fact
    return sorted(best.values(), key=lambda f: (f["file"], f["line"]))


def _libraries(index: CodeIndex, units: Iterable[str]) -> list[str]:
    libs = set()
    for uid in units:
        for name in index.units[uid].third_party:
            top = name.split(".")[0]
            if top not in _STDLIB:
                libs.add(top)
    return sorted(libs)


def _imported_modules(index: CodeIndex) -> set[str]:
    out: set[str] = set()
    for mod in index.modules.values():
        for target in mod.imports.values():
            parts = target.split(".")
            for cut in range(len(parts), 0, -1):
                candidate = ".".join(parts[:cut])
                if candidate in index.modules and candidate != mod.name:
                    out.add(candidate)
                    break
    for unit in index.units.values():
        for ref in unit.refs:
            owner = index.units.get(ref)
            if owner and owner.module != unit.module:
                out.add(owner.module)
            elif ref in index.class_methods:
                out.add(ref.rpartition(".")[0])
    return out


class _Resolver:
    """Turns code facts into edges to compose services / external hosts."""

    def __init__(self, config: dict[str, Any], dotenv_keys: list[str]) -> None:
        self.config = config
        self.services = config.get("services", {})
        self.hostnames = compose._hostnames(config)
        self.published = compose.published_ports(config)
        self.dotenv_keys = set(dotenv_keys)

    def connections(
        self,
        index: CodeIndex,
        entries: list[str],
        service: str | None,
    ) -> dict[str, Any]:
        reached = index.reachable(entries)
        facts = _dedupe_facts(index, reached)
        libs = _libraries(index, reached)
        loads_dotenv = any(
            n.startswith("dotenv.") for u in reached for n in index.units[u].third_party
        )
        svc = self.services.get(service) if service else None
        edges: list[dict[str, Any]] = []
        env_reads: list[dict[str, Any]] = []
        warnings: list[str] = []
        for fact in facts:
            origin_file = f"{fact['file']}:{fact['line']}"
            if fact["kind"] == "env":
                key = fact["key"]
                value = None
                origin = f"default of os.getenv({key!r}) at {origin_file}"
                if svc is not None:
                    from_env = compose.env_endpoint(svc, key)
                    if from_env is not None:
                        value = from_env
                        origin = f"{service} compose environment {key} (read at {origin_file})"
                    elif key in (svc.get("environment") or {}):
                        origin = (
                            f"{service} compose environment {key} (secret, not shown)"
                        )
                if value is None:
                    value = fact.get("default")
                override = None
                if svc is None and loads_dotenv and key in self.dotenv_keys:
                    override = ".env defines this key (value not read); it overrides the default"
                env_reads.append(
                    {
                        "key": key,
                        "secret": fact.get("secret", False) or is_secret_key(key),
                        "where": origin_file,
                        "context": fact.get("context"),
                        "source": (
                            "compose environment"
                            if (svc and key in (svc.get("environment") or {}))
                            else "code default"
                        ),
                        "override": override,
                    }
                )
                if fact.get("secret") or is_secret_key(key) or not value:
                    continue
                eps = parse_endpoints(value)
            else:
                origin = f"string literal at {origin_file}"
                eps = fact.get("endpoints", [])
            for ep in eps:
                if ep["scheme"] in ("s3", "s3a"):
                    continue  # bucket paths, listed as lake tables instead
                edge = self._target(ep, service)
                if edge is None:
                    continue
                target, note = edge
                if note:
                    warnings.append(f"{note} ({origin})")
                if target is None:
                    continue
                host_port = f"{ep['host']}:{ep['port']}" if ep["port"] else ep["host"]
                tag = _tag(fact.get("context"))
                edges.append(
                    {
                        "target": target,
                        "label": host_port + (f" · {tag}" if tag else ""),
                        "endpoint": host_port,
                        "scheme": ep["scheme"],
                        "context": fact.get("context"),
                        "evidence": origin,
                    }
                )
        lake = strings_in(index, reached, _LAKE_STR)
        tables = sorted({t for t in (lake_id(h["match"]) for h in lake) if t})
        other_paths = sorted({h["match"] for h in lake if not lake_id(h["match"])})
        topics = sorted({t["topic"] for u in reached for t in index.units[u].topics})
        return {
            "reached_units": len(reached),
            "edges": _merge_edges(edges),
            "env_reads": _merge_env(env_reads),
            "libraries": libs,
            "tables": tables,
            "other_lake_paths": other_paths,
            "topics": topics,
            "warnings": sorted(set(warnings)),
            "loads_dotenv": loads_dotenv,
        }

    def _target(
        self, ep: dict[str, Any], service: str | None
    ) -> tuple[str | None, str | None] | None:
        host, port = ep["host"], ep["port"]
        if host in _LOCAL_HOSTS:
            owner = self.published.get(port) if port else None
            if service is not None:
                if owner == service:
                    return None
                return (
                    None,
                    f"{host}:{port} is used inside container '{service}', where localhost "
                    f"is the container itself"
                    + (f" (host port {port} belongs to '{owner}')" if owner else ""),
                )
            if owner:
                return (f"service.{owner}", None)
            return (
                "external." + f"{host}:{port}",
                f"{host}:{port}: no compose service publishes port {port}",
            )
        if host in self.hostnames:
            target = self.hostnames[host]
            if service is None:
                return (
                    f"service.{target}",
                    f"{host} is a compose-network hostname; it resolves only inside the docker network",
                )
            if target == service:
                return None
            return (f"service.{target}", None)
        return (f"external.{host}", None)


def _merge_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for edge in edges:
        key = (edge["target"], edge["label"])
        if key in merged:
            if edge["evidence"] not in merged[key]["evidence"]:
                merged[key]["evidence"] += "; " + edge["evidence"]
        else:
            merged[key] = dict(edge)
    return sorted(merged.values(), key=lambda e: (e["target"], e["label"]))


def _merge_env(reads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for read in reads:
        current = merged.setdefault(read["key"], {**read, "where": [read["where"]]})
        if read["where"] not in current["where"]:
            current["where"].append(read["where"])
        if read.get("context") and not current.get("context"):
            current["context"] = read["context"]
    return sorted(merged.values(), key=lambda r: r["key"])


def _group_of(svc: dict[str, Any]) -> tuple[str, str]:
    build = svc.get("build")
    if build:
        dockerfile = (
            build.get("dockerfile", "Dockerfile")
            if isinstance(build, dict)
            else "Dockerfile"
        )
        return f"build:{dockerfile}", f"built from {dockerfile}"
    image = str(svc.get("image", "?"))
    repo = image.split(":")[0]
    namespace = repo.split("/")[0] if "/" in repo else repo
    return f"image:{namespace}", (
        f"image {namespace}/*" if "/" in repo else f"image {namespace}"
    )


def _ports(svc: dict[str, Any]) -> list[str]:
    out = []
    for p in svc.get("ports") or []:
        if isinstance(p, dict):
            published = p.get("published")
            host_ip = p.get("host_ip")
            prefix = f"{host_ip}:" if host_ip else ""
            out.append(
                f"{prefix}{published}->{p.get('target')}/{p.get('protocol', 'tcp')}"
                if published
                else f"{p.get('target')}/{p.get('protocol', 'tcp')}"
            )
        else:
            out.append(str(p))
    return out


def _cmd_text(value: Any, secrets: set[str]) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        if not value:
            return "[] (cleared)"
        text = " ".join(str(v) for v in value)
    else:
        text = str(value)
    return compose.redact_command(" ".join(text.split()), secrets)


def _spark_config(index: CodeIndex) -> list[dict[str, Any]]:
    """``.config(key, value)`` calls on a ``SparkSession.builder`` chain."""
    rows: list[dict[str, Any]] = []
    for uid, unit in index.units.items():
        mod = index.modules[unit.module]
        local_env: dict[str, tuple[str, Any]] = {}
        for top in unit.nodes:
            for node in ast.walk(top):
                if (
                    isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and isinstance(node.value, ast.Call)
                    and (_dotted(node.value.func) or "")
                    in ("os.getenv", "os.environ.get")
                    and node.value.args
                    and isinstance(node.value.args[0], ast.Constant)
                ):
                    default = (
                        node.value.args[1].value
                        if len(node.value.args) > 1
                        and isinstance(node.value.args[1], ast.Constant)
                        else None
                    )
                    local_env[node.targets[0].id] = (
                        str(node.value.args[0].value),
                        default,
                    )
        for top in unit.nodes:
            for node in ast.walk(top):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "config"
                    and len(node.args) == 2
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                ):
                    continue
                base: ast.AST = node.func.value
                while isinstance(base, (ast.Call, ast.Attribute)):
                    if isinstance(base, ast.Attribute) and _dotted(base):
                        break
                    base = base.func if isinstance(base, ast.Call) else base.value
                dotted = _dotted(base) or ""
                full = CodeIndex._full_name(mod.imports, dotted)
                if not re.search(r"SparkSession\.builder(\.|$)", full):
                    continue
                key = node.args[0].value
                value_node = node.args[1]
                value = _spark_value(key, value_node, local_env)
                rows.append(
                    {
                        "key": key,
                        "value": value,
                        "unit": uid,
                        "where": f"{rel(mod.path)}:{node.args[0].lineno}",
                        "_line": node.args[0].lineno,
                    }
                )
    rows.sort(key=lambda r: (r["unit"], r.pop("_line")))
    return rows


def _spark_value(key: str, node: ast.AST, local_env: dict[str, tuple[str, Any]]) -> str:
    secret_key = bool(re.search(r"secret|password|access\.key|token", key, re.I))
    env: tuple[str, Any] | None = None
    if isinstance(node, ast.Name) and node.id in local_env:
        env = local_env[node.id]
    elif (
        isinstance(node, ast.Call)
        and (_dotted(node.func) or "") in ("os.getenv", "os.environ.get")
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ):
        default = (
            node.args[1].value
            if len(node.args) > 1 and isinstance(node.args[1], ast.Constant)
            else None
        )
        env = (str(node.args[0].value), default)
    if env is not None:
        name, default = env
        if secret_key or is_secret_key(name) or default is None:
            return f"env {name}" + (
                " (default hidden: secret)" if default is not None else ""
            )
        return f"env {name} (default {default})"
    if secret_key:
        return compose.MASK
    if isinstance(node, ast.Constant):
        return str(node.value)
    return ast.unparse(node)


def _lake_tables() -> tuple[list[str] | None, str]:
    try:
        conn = lake_connection()
    except SourceUnavailableError as error:
        return None, str(error)
    except Exception as error:  # duckdb extension install/network errors
        return None, f"{type(error).__name__}: {error}"
    try:
        return list_lake_tables(conn), ""
    finally:
        conn.close()


def collect() -> dict[str, Any]:
    sources: list[str] = []
    config, how, config_source = compose.resolved_config()
    sources.append(config_source)
    secrets = compose.secret_values(config)
    states, state_error = compose.container_states()
    sources.append(
        "docker compose ps -a --format json"
        + ("" if states is not None else f" (unavailable: {state_error})")
    )

    index = CodeIndex()
    sources.append(
        f"ast of {len(index.modules)} git-tracked Python modules (git ls-files '*.py')"
    )
    dotenv_keys = _dotenv_keys()
    if dotenv_keys:
        sources.append(".env (key names only)")
    resolver = _Resolver(config, dotenv_keys)
    text_files = _tracked_text_files()
    sources.append("launch commands searched in: " + ", ".join(text_files))

    services_cfg: dict[str, Any] = config.get("services", {})
    networks_cfg: dict[str, Any] = config.get("networks", {}) or {}

    # ------------------------------------------------------------- services
    services: list[dict[str, Any]] = []
    dockerfiles: dict[str, dict[str, Any]] = {}
    module_runs: dict[str, str] = {}
    for name, svc in services_cfg.items():
        group, group_label = _group_of(svc)
        build = svc.get("build")
        build_info = None
        if build:
            context = (
                build.get("context", ".") if isinstance(build, dict) else str(build)
            )
            dockerfile = (
                build.get("dockerfile", "Dockerfile")
                if isinstance(build, dict)
                else "Dockerfile"
            )
            ctx_rel = rel(context) if context else "."
            df_path = (
                REPO_ROOT / ctx_rel / dockerfile
                if ctx_rel != context
                else REPO_ROOT / dockerfile
            )
            df_rel = rel(df_path)
            if df_rel not in dockerfiles:
                dockerfiles[df_rel] = compose.dockerfile_facts(df_path)
                sources.append(df_rel)
            build_info = {
                "context": "./" if ctx_rel == "." else ctx_rel,
                "dockerfile": df_rel,
            }
        runs = compose.python_module_of(svc)
        if runs:
            module_runs[runs] = name
        health = svc.get("healthcheck") or {}
        services.append(
            {
                "name": name,
                "id": f"service.{name}",
                "group": group,
                "group_label": group_label,
                "container_name": svc.get("container_name"),
                "image": svc.get("image"),
                "build": build_info,
                "entrypoint": _cmd_text(svc.get("entrypoint"), secrets),
                "command": _cmd_text(svc.get("command"), secrets),
                "ports": _ports(svc),
                "expose": [str(e) for e in svc.get("expose") or []],
                "depends_on": [
                    {
                        "service": d,
                        "condition": (spec or {}).get("condition", "service_started"),
                    }
                    for d, spec in (svc.get("depends_on") or {}).items()
                ],
                "healthcheck": (
                    {
                        "test": _cmd_text(
                            (
                                [
                                    t
                                    for t in health.get("test", [])
                                    if t not in ("CMD", "CMD-SHELL")
                                ]
                                if isinstance(health.get("test"), list)
                                else health.get("test")
                            ),
                            secrets,
                        ),
                        **{
                            k: health[k]
                            for k in ("interval", "timeout", "retries", "start_period")
                            if k in health
                        },
                    }
                    if health
                    else None
                ),
                "volumes": compose.bind_mounts(svc),
                "networks": [
                    (networks_cfg.get(n) or {}).get("name", n)
                    for n in (svc.get("networks") or {"default": None})
                ],
                "profiles": svc.get("profiles") or [],
                "restart": svc.get("restart"),
                "user": svc.get("user"),
                "working_dir": svc.get("working_dir"),
                "env_keys": sorted((svc.get("environment") or {}).keys()),
                "runs_module": runs,
                "state": (states or {}).get(name) if states is not None else None,
            }
        )

    compose_edges = compose.service_edges(config)

    # ------------------------------------------------- code run by services
    for svc_info in services:
        runs = svc_info["runs_module"]
        if not runs:
            continue
        entry = index.main_entry(runs)
        if entry is None:
            svc_info["code"] = {"error": f"module {runs} not found in the repo"}
            continue
        conn = resolver.connections(index, [entry], svc_info["name"])
        conn["entry"] = entry
        conn["entry_where"] = index.unit_file_line(entry)
        conn["calls"] = main_calls(index, entry)
        svc_info["code"] = conn

    # ---------------------------------------------------------------- DAGs
    backed = {
        (e["source"], e["target"]) for e in compose_edges if e["kind"] != "depends_on"
    }
    for svc_info in services:
        for conn in (svc_info.get("code") or {}).get("edges", []):
            backed.add((svc_info["name"], str(conn["target"]).removeprefix("service.")))
    unbacked = [
        {"source": e["source"], "target": e["target"], "condition": e["evidence"]}
        for e in compose_edges
        if e["kind"] == "depends_on" and (e["source"], e["target"]) not in backed
    ]

    dags = parse_dags(index)
    for dag in dags:
        sources.append(dag["file"] + " (ast)")
        dag_dir = dag["file"].rsplit("/", 1)[0]
        mounts = [
            s["name"]
            for s in services
            if any(
                v["type"] == "bind" and v["source"].rstrip("/") == f"./{dag_dir}"
                for v in s["volumes"]
            )
        ]
        dag["mounted_by"] = mounts
        dag["mount_targets"] = sorted(
            {
                v["target"]
                for s in services
                for v in s["volumes"]
                if v["type"] == "bind" and v["source"].rstrip("/") == f"./{dag_dir}"
            }
        )
        for task in dag["tasks"]:
            entries = [task["callable"]] if task["callable"] in index.units else []
            entries += [c for c in task["callbacks"].values() if c in index.units]
            per_service = {}
            for svc_name in mounts or [None]:  # type: ignore[list-item]
                per_service[svc_name or "(no mounting service)"] = resolver.connections(
                    index, entries, svc_name
                )
            first = next(iter(per_service.values()))
            same = all(c["edges"] == first["edges"] for c in per_service.values())
            task["code"] = first
            task["code_same_for_all_mounting_services"] = same
            task["callable_where"] = (
                index.unit_file_line(task["callable"])
                if task["callable"] in index.units
                else None
            )
            declared = set(task["inlets"]) | set(task["outlets"])
            task["tables_in_code_not_declared"] = sorted(
                set(first["tables"]) - declared
            )
            task["declared_not_in_code"] = sorted(declared - set(first["tables"]))

    # ------------------------------------------- processes outside compose
    imported = _imported_modules(index)
    apps = asgi_apps(index)
    app_modules = {a["module"] for a in apps}
    dag_modules = {d["file"] for d in dags}
    processes: list[dict[str, Any]] = []
    for app in apps:
        conn = resolver.connections(index, app["handlers"], None)
        processes.append(
            {
                "id": app["id"],
                "kind": f"ASGI app ({app['class']})",
                "module": app["module"],
                "where": f"{app['file']}:{app['line']}",
                "launch": _launch_commands(
                    app["module"], app["attr"], text_files, secrets
                ),
                "routes": app["routes"],
                "documented_at": _documented_urls(app["routes"], text_files),
                "calls": app["handlers"],
                "code": conn,
            }
        )
    for mod_name, mod in sorted(index.modules.items()):
        if mod_name in app_modules or rel(mod.path) in dag_modules:
            continue
        main_uid = f"{mod_name}.__main__"
        top_uid = f"{mod_name}.<module>"
        if main_uid in index.units:
            kind, entry = "script (__main__ guard)", main_uid
        elif (
            top_uid in index.units
            and mod_name not in imported
            and any(
                r in index.units or r in index.class_methods
                for r in index.units[top_uid].refs
            )
        ):
            kind, entry = "top-level script (no __main__ guard)", top_uid
        else:
            continue
        run_by = module_runs.get(mod_name)
        svc_code = next((s.get("code") for s in services if s["name"] == run_by), None)
        if run_by and svc_code and "edges" in svc_code:
            conn = svc_code  # shown with the environment of the service that runs it
        else:
            conn = resolver.connections(index, [entry], None)
        processes.append(
            {
                "id": entry,
                "kind": kind,
                "module": mod_name,
                "where": index.unit_file_line(entry),
                "launch": _launch_commands(mod_name, None, text_files, secrets),
                "also_run_by_service": run_by,
                "calls": main_calls(index, entry),
                "code": conn,
            }
        )

    # ------------------------------------------------------ spark + lake
    spark_conf = _spark_config(index)
    tables, lake_error = _lake_tables()
    sources.append(
        "s3://lakehouse/*/*/_delta_log/*.json via DuckDB (toolkit.common.lake_connection)"
        + ("" if tables is not None else f" (unavailable: {lake_error})")
    )

    host_processes = [p for p in processes if not p.get("also_run_by_service")]
    running = (
        sum(1 for s in services if (s["state"] or {}).get("state") == "running")
        if states is not None
        else None
    )
    data: dict[str, Any] = {
        "project": config.get("name"),
        "config_resolved_by": how,
        "docker_state_error": state_error if states is None else None,
        "services": services,
        "networks": {k: (v or {}).get("name", k) for k, v in networks_cfg.items()},
        "named_volumes": sorted((config.get("volumes") or {}).keys()),
        "compose_edges": compose_edges,
        "unbacked_depends_on": unbacked,
        "dockerfiles": dockerfiles,
        "dags": dags,
        "processes": processes,
        "spark_config": spark_conf,
        "lake_tables": tables,
        "lake_error": lake_error or None,
        "dotenv_keys": dotenv_keys,
        "index_errors": index.errors,
        "summary": {
            "compose services": len(services),
            "running containers": (
                f"{running}/{len(services)}" if running is not None else "docker down"
            ),
            "processes outside compose": len(host_processes),
            "DAG tasks": sum(len(d["tasks"]) for d in dags),
        },
        "sources": sources,
    }
    return compose.scrub(data, secrets)  # type: ignore[no-any-return]


# ==================================================================== render


def _state_badge(state: dict[str, Any] | None) -> Raw:
    if not state:
        return badge("no container", "muted")
    value = state.get("state") or "?"
    kind = {
        "running": "ok",
        "exited": "muted",
        "restarting": "warn",
        "dead": "fail",
    }.get(value, "info")
    if value == "exited" and state.get("exit_code") not in (0, None):
        kind = "warn"
    text = value + (f" ({state['health']})" if state.get("health") else "")
    return badge(text, kind)


def _state_text(state: dict[str, Any] | None, docker_error: str | None) -> str:
    if docker_error:
        return "unknown (docker not reachable)"
    if not state:
        return "no container created"
    return f"{state.get('state')} — {state.get('status')}"


def _graph(
    data: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    groups: dict[str, str] = {}
    docker_error = data.get("docker_state_error")
    networks_used: dict[str, str] = {}
    for svc in data["services"]:
        groups.setdefault(svc["group"], svc["group_label"])
        if svc["networks"]:
            networks_used.setdefault(svc["networks"][0], f"net.{svc['networks'][0]}")
    groups["dag"] = "Airflow DAG"
    groups["process"] = "process outside compose"
    groups["external"] = "external endpoint"

    for net_name, net_id in networks_used.items():
        nodes.append(
            {"id": net_id, "label": f"docker network {net_name}", "group": "network"}
        )
    nodes.append({"id": "host", "label": "not started by compose", "group": "network"})

    for svc in data["services"]:
        state = svc["state"]
        image = svc["image"] or (
            f"build {svc['build']['dockerfile']}" if svc["build"] else "?"
        )
        detail = {
            "image": image,
            "container": svc["container_name"] or "",
            "state": _state_text(state, docker_error),
            "ports": ", ".join(svc["ports"]) or "none published",
            "entrypoint": svc["entrypoint"] or "image default",
            "command": svc["command"] or "image default",
            "depends_on": ", ".join(
                f"{d['service']} ({d['condition']})" for d in svc["depends_on"]
            )
            or "none",
            "healthcheck": (svc["healthcheck"] or {}).get("test") or "none",
            "volumes": "; ".join(
                f"{v['source']} → {v['target']}{' (ro)' if v['read_only'] else ''}"
                for v in svc["volumes"]
            )
            or "none",
            "restart": svc["restart"] or "no",
            "profiles": ", ".join(svc["profiles"]) or "none (always started)",
            "env keys": ", ".join(svc["env_keys"]) or "none",
        }
        links = []
        code = svc.get("code") or {}
        if svc["runs_module"]:
            detail["runs"] = f"python -m {svc['runs_module']}"
            for callee in code.get("calls", []):
                links.append(
                    {
                        "label": f"call graph: {callee}",
                        "href": href("call-graph", callee),
                    }
                )
            if code.get("topics"):
                detail["kafka topics"] = ", ".join(code["topics"])
            if code.get("tables"):
                detail["lake tables in code"] = ", ".join(code["tables"])
                links += [
                    {"label": f"lineage: {t}", "href": href("lineage", t)}
                    for t in code["tables"]
                ]
            if code.get("warnings"):
                detail["warnings"] = " | ".join(code["warnings"])
        if svc["name"] == _minio_service(data):
            links.append(
                {"label": "lineage of every lake table", "href": href("lineage")}
            )
            for table in data.get("lake_tables") or []:
                links.append(
                    {"label": f"lineage: {table}", "href": href("lineage", table)}
                )
            detail["lake tables (live)"] = (
                str(len(data["lake_tables"]))
                if data.get("lake_tables") is not None
                else f"not derivable: {data.get('lake_error')}"
            )
        topic_users = _topics_by_service(data).get(svc["name"])
        if topic_users:
            detail["topics used by code"] = ", ".join(sorted(topic_users))
            links += [
                {"label": f"lineage: kafka.{t}", "href": href("lineage", f"kafka.{t}")}
                for t in sorted(topic_users)
            ]
        state_mark = ""
        if state and state.get("state") == "running":
            state_mark = " ●"
        nodes.append(
            {
                "id": svc["id"],
                "label": f"{svc['name']}{state_mark}\n{image}",
                "group": svc["group"],
                "parent": f"net.{svc['networks'][0]}" if svc["networks"] else None,
                "detail": detail,
                "links": links,
            }
        )

    for edge in data["compose_edges"]:
        edges.append(
            {
                "source": f"service.{edge['source']}",
                "target": f"service.{edge['target']}",
                "label": edge["label"],
                "kind": edge["kind"],
                "evidence": edge["evidence"],
            }
        )
    for svc in data["services"]:
        for conn in (svc.get("code") or {}).get("edges", []):
            edges.append(
                {
                    "source": svc["id"],
                    "target": conn["target"],
                    "label": conn["label"],
                    "kind": "code",
                    "evidence": conn["evidence"],
                }
            )

    externals: dict[str, str] = {}
    for dag in data["dags"]:
        parent = None
        mounted = [s for s in data["services"] if s["name"] in dag["mounted_by"]]
        if mounted and mounted[0]["networks"]:
            parent = f"net.{mounted[0]['networks'][0]}"
        nodes.append(
            {
                "id": dag["id"],
                "label": f"DAG {dag['dag_id']}\n{len(dag['tasks'])} tasks",
                "group": "dag",
                "parent": parent,
                "detail": {
                    "file": f"{dag['file']}:{dag['line']}",
                    **{k: v for k, v in dag["attrs"].items() if k != "default_args"},
                    "tasks": " → ".join(t["task_id"] for t in dag["tasks"]),
                    "mounted by": ", ".join(dag["mounted_by"]) or "no compose service",
                },
                "links": [{"label": "DAG task graph (below)", "href": "#airflow-dag"}],
            }
        )
        for svc_name in dag["mounted_by"]:
            edges.append(
                {
                    "source": f"service.{svc_name}",
                    "target": dag["id"],
                    "label": "mounts " + dag["file"].rsplit("/", 1)[0],
                    "kind": "mount",
                    "evidence": f"volumes of {svc_name}",
                }
            )
        seen: set[tuple[str, str]] = set()
        for task in dag["tasks"]:
            for conn in task["code"]["edges"]:
                key = (conn["target"], conn["label"])
                if key in seen:
                    continue
                seen.add(key)
                edges.append(
                    {
                        "source": dag["id"],
                        "target": conn["target"],
                        "label": conn["label"],
                        "kind": "code",
                        "evidence": f"task {task['task_id']}: {conn['evidence']}",
                    }
                )

    for proc in data["processes"]:
        if proc.get("also_run_by_service"):
            continue
        code = proc["code"]
        detail = {
            "kind": proc["kind"],
            "entry": proc["where"],
            "launch": " | ".join(proc["launch"])
            or "not derivable: no launch command in tracked docs/config",
            "libraries": ", ".join(code["libraries"]) or "none",
            "env read": ", ".join(r["key"] for r in code["env_reads"]) or "none",
        }
        if proc.get("routes"):
            detail["routes"] = ", ".join(
                f"{r['method']} {r['path']}" for r in proc["routes"]
            )
        if proc.get("documented_at"):
            detail["docs call it at"] = ", ".join(
                f"{d['address']}{d['path']} ({d['where']})"
                for d in proc["documented_at"]
            )
        if code["topics"]:
            detail["kafka topics"] = ", ".join(code["topics"])
        if code["tables"]:
            detail["lake tables in code"] = ", ".join(code["tables"])
        if code["warnings"]:
            detail["warnings"] = " | ".join(code["warnings"])
        if code["loads_dotenv"]:
            detail[".env"] = (
                "loaded via python-dotenv (key names: "
                + (", ".join(data["dotenv_keys"]) or "none")
                + ")"
            )
        links = [
            {"label": f"call graph: {c}", "href": href("call-graph", c)}
            for c in proc["calls"]
        ]
        links += [
            {"label": f"lineage: {t}", "href": href("lineage", t)}
            for t in code["tables"]
        ]
        links += [
            {"label": f"lineage: kafka.{t}", "href": href("lineage", f"kafka.{t}")}
            for t in code["topics"]
        ]
        nodes.append(
            {
                "id": proc["id"],
                "label": proc["module"].rsplit(".", 1)[-1]
                + ("\nASGI app" if proc.get("routes") else ""),
                "group": "process",
                "parent": "host",
                "detail": detail,
                "links": links,
            }
        )
        for conn in code["edges"]:
            edges.append(
                {
                    "source": proc["id"],
                    "target": conn["target"],
                    "label": conn["label"],
                    "kind": "code",
                    "evidence": conn["evidence"],
                }
            )

    for edge in edges:
        if edge["target"].startswith("external."):
            externals[edge["target"]] = edge["target"].removeprefix("external.")
    for ext_id, host in sorted(externals.items()):
        nodes.append(
            {
                "id": ext_id,
                "label": host,
                "group": "external",
                "detail": {
                    "host": host,
                    "found in": "string literal / env default in code",
                },
            }
        )
    groups["network"] = "boundary"
    return nodes, edges, groups


def _minio_service(data: dict[str, Any]) -> str | None:
    """The service that S3/S3A endpoints in the code resolve to."""
    for svc in data["services"]:
        for conn in (svc.get("code") or {}).get("edges", []):
            if "s3a" in conn["label"]:
                return str(conn["target"]).removeprefix("service.")
    for proc in data["processes"]:
        for conn in proc["code"]["edges"]:
            if "s3a" in conn["label"]:
                return str(conn["target"]).removeprefix("service.")
    return None


def _topics_by_service(data: dict[str, Any]) -> dict[str, set[str]]:
    """Kafka topics, attributed to the service whose endpoint the same code uses
    as a Kafka bootstrap (context 'kafka' or 'bootstrap_servers')."""
    out: dict[str, set[str]] = {}
    codes = [s.get("code") or {} for s in data["services"]] + [
        p["code"] for p in data["processes"]
    ]
    for code in codes:
        if not code.get("topics"):
            continue
        for conn in code.get("edges", []):
            ctx = conn.get("context") or ""
            if "kafka" in ctx or "bootstrap" in ctx:
                out.setdefault(
                    str(conn["target"]).removeprefix("service."), set()
                ).update(code["topics"])
    return out


def _codes(items: Iterable[str]) -> Raw:
    return Raw(
        ", ".join(f"<code>{esc(i)}</code>" for i in items)
        or '<span class="muted">none</span>'
    )


def _services_table(data: dict[str, Any]) -> Raw:
    docker_error = data.get("docker_state_error")
    rows = []
    for svc in data["services"]:
        image = svc["image"] or ""
        if svc["build"]:
            image = Raw(
                f"build <code>{esc(svc['build']['dockerfile'])}</code> (context <code>{esc(svc['build']['context'])}</code>)"
            )
        cmd_parts = []
        if svc["entrypoint"]:
            cmd_parts.append(
                f"<span class='muted small'>entrypoint</span> <code>{esc(svc['entrypoint'])}</code>"
            )
        if svc["command"]:
            cmd_parts.append(
                f"<span class='muted small'>command</span> <code>{esc(svc['command'])}</code>"
            )
        health = svc["healthcheck"]
        rows.append(
            [
                Raw(
                    f'<span id="row-{esc(svc["id"])}"><strong>{esc(svc["name"])}</strong></span>'
                    f'<br><span class="muted small">{esc(svc["container_name"] or "")}</span>'
                ),
                image,
                Raw(
                    "<br>".join(cmd_parts) or '<span class="muted">image default</span>'
                ),
                _codes(svc["ports"]),
                Raw(
                    "<br>".join(
                        f"{esc(d['service'])} <span class='muted small'>{esc(d['condition'])}</span>"
                        for d in svc["depends_on"]
                    )
                    or '<span class="muted">none</span>'
                ),
                Raw(
                    f"<code>{esc(health['test'])}</code><br><span class='muted small'>"
                    + esc(
                        ", ".join(f"{k} {v}" for k, v in health.items() if k != "test")
                    )
                    + "</span>"
                    if health
                    else '<span class="muted">none</span>'
                ),
                Raw(
                    "<br>".join(
                        f"<code>{esc(v['source'])}</code> → <code>{esc(v['target'])}</code>"
                        + (
                            " <span class='muted small'>ro</span>"
                            if v["read_only"]
                            else ""
                        )
                        for v in svc["volumes"]
                    )
                    or '<span class="muted">none</span>'
                ),
                ", ".join(svc["networks"]),
                ", ".join(svc["profiles"]) or "—",
                svc["restart"] or "no",
                _codes(svc["env_keys"]),
                (
                    Raw(
                        f"{_state_badge(svc['state'])}<br><span class='muted small'>{esc(_state_text(svc['state'], docker_error))}</span>"
                    )
                    if not docker_error
                    else "unknown"
                ),
            ]
        )
    return table_html(
        [
            "service",
            "image / build",
            "entrypoint / command",
            "ports (host→container)",
            "depends_on",
            "healthcheck",
            "volumes",
            "networks",
            "profiles",
            "restart",
            "env keys (names only)",
            "state",
        ],
        rows,
        filterable=True,
        table_id="arch-services",
    )


def _edges_table(
    nodes: list[dict[str, Any]], edges: list[dict[str, Any]], table_id: str
) -> Raw:
    labels = {n["id"]: n.get("label", n["id"]).replace("\n", " ") for n in nodes}
    rows = [
        [
            Raw(
                f'<a href="?focus={esc(e["source"])}">{esc(labels.get(e["source"], e["source"]))}</a>'
            ),
            Raw(
                f'<a href="?focus={esc(e["target"])}">{esc(labels.get(e["target"], e["target"]))}</a>'
            ),
            e.get("kind", ""),
            e.get("label", ""),
            e.get("evidence", ""),
        ]
        for e in edges
    ]
    return table_html(
        ["from", "to", "kind", "label", "evidence"],
        rows,
        filterable=True,
        table_id=table_id,
    )


def _nodes_table(
    nodes: list[dict[str, Any]], groups: dict[str, str], table_id: str
) -> Raw:
    rows = []
    for n in nodes:
        if n.get("group") == "network":
            continue
        detail = n.get("detail") or {}
        rows.append(
            [
                Raw(f'<a href="?focus={esc(n["id"])}"><code>{esc(n["id"])}</code></a>'),
                n.get("label", "").replace("\n", " — "),
                groups.get(n.get("group", ""), n.get("group", "")),
                Raw(
                    "<br>".join(
                        f"<span class='muted small'>{esc(k)}</span> {esc(v)}"
                        for k, v in detail.items()
                    )
                ),
            ]
        )
    return table_html(
        ["id", "node", "type", "detail"], rows, filterable=True, table_id=table_id
    )


def _process_table(data: dict[str, Any]) -> Raw:
    rows = []
    for proc in data["processes"]:
        code = proc["code"]
        run_by = proc.get("also_run_by_service")
        conns = (
            "<br>".join(
                f"→ <strong>{esc(c['target'])}</strong> <code>{esc(c['label'])}</code>"
                f"<br><span class='muted small'>{esc(c['evidence'])}</span>"
                for c in code["edges"]
            )
            or '<span class="muted">no endpoint found in reachable code</span>'
        )
        if code["warnings"]:
            conns += "<br>" + "<br>".join(
                f"{badge('warn', 'warn')} {esc(w)}" for w in code["warnings"]
            )
        launch = "<br>".join(f"<code>{esc(line)}</code>" for line in proc["launch"])
        if run_by:
            launch = (
                f"compose service {link(SLUG, f'service.{run_by}', run_by)}<br>"
                + launch
            )
        if not launch:
            launch = '<span class="muted">not derivable (no launch command in tracked docs/config)</span>'
        calls = ", ".join(str(link("call-graph", c)) for c in proc["calls"]) or "—"
        extras = []
        if proc.get("routes"):
            extras.append(
                "routes: "
                + ", ".join(
                    f"<code>{esc(r['method'])} {esc(r['path'])}</code>"
                    for r in proc["routes"]
                )
            )
        for doc in proc.get("documented_at", []):
            extras.append(
                f"docs call it at <code>{esc(doc['address'])}{esc(doc['path'])}</code> "
                f"<span class='muted small'>({esc(doc['where'])})</span>"
            )
        if code["tables"]:
            extras.append(
                "tables: " + ", ".join(str(link("lineage", t)) for t in code["tables"])
            )
        if code["topics"]:
            extras.append(
                "topics: "
                + ", ".join(
                    str(link("lineage", f"kafka.{t}", t)) for t in code["topics"]
                )
            )
        extras.append(
            "libraries: "
            + (", ".join(f"<code>{esc(x)}</code>" for x in code["libraries"]) or "none")
        )
        extras.append(
            "env: "
            + (
                ", ".join(
                    f"<code>{esc(r['key'])}</code>"
                    + (
                        " <span class='muted small'>(.env overrides)</span>"
                        if r.get("override")
                        else ""
                    )
                    for r in code["env_reads"]
                )
                or "none"
            )
        )
        rows.append(
            [
                Raw(
                    f'<span id="row-{esc(proc["id"])}"><code>{esc(proc["module"])}</code></span>'
                    f'<br><span class="muted small">{esc(proc["kind"])} · {esc(proc["where"])}</span>'
                ),
                Raw(launch),
                Raw(calls),
                Raw(conns),
                Raw("<br>".join(extras)),
            ]
        )
    return table_html(
        ["process", "how it is launched", "calls (entry)", "connects to", "uses"],
        rows,
        filterable=True,
        table_id="arch-processes",
    )


def _dag_section(dag: dict[str, Any]) -> Raw:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    groups = {
        "task": "task",
        "bronze": "bronze dataset",
        "silver": "silver dataset",
        "gold": "gold dataset",
        "dataset": "dataset",
        "service": "service reached by the task's code",
    }
    for ds_id, ds in sorted(dag["datasets"].items()):
        layer = ds_id.split(".")[0] if "." in ds_id else "dataset"
        nodes.append(
            {
                "id": ds_id,
                "label": ds_id,
                "group": layer if layer in groups else "dataset",
                "detail": {
                    "uri": ds["uri"],
                    "declared at": f"{dag['file']}:{ds['line']}",
                },
                "links": [
                    {"label": f"lineage: {ds_id}", "href": href("lineage", ds_id)}
                ],
            }
        )
    service_ids: set[str] = set()
    for task in dag["tasks"]:
        code = task["code"]
        detail = {
            "operator": task["operator"],
            "python_callable": task["callable"] or "none",
            "callable at": task["callable_where"] or "not in repo",
            "retries": (
                f"{task['retries']} (from {task['retries_from']})"
                if task["retries"]
                else "not set"
            ),
            "retry_delay": task["retry_delay"] or "not set",
            "callbacks": ", ".join(f"{k}: {v}" for k, v in task["callbacks"].items())
            or "none",
            "defined at": f"{dag['file']}:{task['line']}",
            "connects to": ", ".join(
                f"{c['target']} ({c['label']})" for c in code["edges"]
            )
            or "none",
        }
        if task["tables_in_code_not_declared"]:
            detail["tables in code, not declared as inlet/outlet"] = ", ".join(
                task["tables_in_code_not_declared"]
            )
        if task["declared_not_in_code"]:
            detail["declared, not found in code"] = ", ".join(
                task["declared_not_in_code"]
            )
        links = []
        if task["callable"]:
            links.append(
                {
                    "label": f"call graph: {task['callable']}",
                    "href": href("call-graph", task["callable"]),
                }
            )
        for cb in task["callbacks"].values():
            if cb:
                links.append(
                    {"label": f"call graph: {cb}", "href": href("call-graph", cb)}
                )
        nodes.append(
            {
                "id": task["id"],
                "label": task["task_id"],
                "group": "task",
                "detail": detail,
                "links": links,
            }
        )
        for ds in task["inlets"]:
            edges.append(
                {
                    "source": ds,
                    "target": task["id"],
                    "label": "inlet",
                    "kind": "inlet",
                    "evidence": f"inlets of {task['task_id']} ({dag['file']}:{task['line']})",
                }
            )
        for ds in task["outlets"]:
            edges.append(
                {
                    "source": task["id"],
                    "target": ds,
                    "label": "outlet",
                    "kind": "outlet",
                    "evidence": f"outlets of {task['task_id']} ({dag['file']}:{task['line']})",
                }
            )
        for conn in code["edges"]:
            service_ids.add(conn["target"])
            edges.append(
                {
                    "source": task["id"],
                    "target": conn["target"],
                    "label": conn["label"],
                    "kind": "code",
                    "evidence": conn["evidence"],
                }
            )
    for e in dag["edges"]:
        edges.append(
            {
                "source": e["source"],
                "target": e["target"],
                "label": e["how"],
                "kind": "order",
                "evidence": f"{dag['file']}:{e['line']}",
            }
        )
    for sid in sorted(service_ids):
        nodes.append(
            {
                "id": sid,
                "label": sid.removeprefix("service.").removeprefix("external."),
                "group": "service",
                "links": [
                    {"label": "service details (main diagram)", "href": f"?focus={sid}"}
                ],
            }
        )

    rows = []
    order = {t["id"]: i for i, t in enumerate(dag["tasks"])}
    for task in sorted(dag["tasks"], key=lambda t: order[t["id"]]):
        ups = [
            e["source"].rsplit(".", 1)[-1]
            for e in dag["edges"]
            if e["target"] == task["id"]
        ]
        rows.append(
            [
                Raw(
                    f'<span id="row-{esc(task["id"])}"><strong>{esc(task["task_id"])}</strong></span>'
                ),
                Raw(
                    str(link("call-graph", task["callable"]))
                    if task["callable"]
                    else "—"
                ),
                (
                    f"{task['retries']} ({task['retries_from']})"
                    if task["retries"]
                    else "—"
                ),
                task["retry_delay"] or "—",
                Raw(", ".join(str(link("lineage", d)) for d in task["inlets"]) or "—"),
                Raw(", ".join(str(link("lineage", d)) for d in task["outlets"]) or "—"),
                ", ".join(ups) or "— (first)",
                Raw(
                    ", ".join(
                        f"<code>{esc(c['label'])}</code>" for c in task["code"]["edges"]
                    )
                    or "—"
                ),
                Raw(
                    (
                        ", ".join(
                            str(link("lineage", t))
                            for t in task["tables_in_code_not_declared"]
                        )
                        or "—"
                    )
                ),
            ]
        )
    attrs = {k: v for k, v in dag["attrs"].items() if k != "default_args"}
    attr_html = "".join(
        f"<li><span class='muted'>{esc(k)}</span> <code>{esc(v)}</code></li>"
        for k, v in attrs.items()
    )
    da = "".join(
        f"<li><span class='muted'>{esc(k)}</span> <code>{esc(v)}</code></li>"
        for k, v in dag["default_args"].items()
    )
    mounted = (
        ", ".join(str(link(SLUG, f"service.{s}", s)) for s in dag["mounted_by"])
        or "no compose service"
    )
    body = (
        f"<p class='small'><code>{esc(dag['dag_id'])}</code> defined at <code>{esc(dag['file'])}:{esc(dag['line'])}</code>; "
        f"the DAG folder is bind-mounted into {mounted} at "
        + ", ".join(f"<code>{esc(t)}</code>" for t in dag["mount_targets"])
        + ". Each task's connections are resolved with the environment of those services"
        + (
            ""
            if all(t["code_same_for_all_mounting_services"] for t in dag["tasks"])
            else " (they differ between services; the first is shown)"
        )
        + ".</p>"
        f"<ul class='small'>{attr_html}</ul><p class='small muted'>default_args</p><ul class='small'>{da}</ul>"
        + graph_block(
            f"dag-{dag['dag_id']}",
            nodes,
            edges,
            groups=groups,
            height=480,
            up_label="Runs before / feeds it",
            down_label="Runs after / it feeds",
        )
        + "<h3>Tasks</h3>"
        + table_html(
            [
                "task",
                "python_callable",
                "retries",
                "retry_delay",
                "inlets",
                "outlets",
                "upstream tasks",
                "endpoints its code uses",
                "lake tables in code but not declared",
            ],
            rows,
            filterable=True,
            table_id=f"dag-tasks-{dag['dag_id']}",
        )
        + "<details><summary>Task graph edges</summary>"
        + _edges_table(nodes, edges, f"dag-edges-{dag['dag_id']}")
        + "</details>"
    )
    return Raw(body)


def _spark_table(data: dict[str, Any]) -> Raw:
    rows = [
        [
            Raw(str(link("call-graph", r["unit"]))),
            Raw(f"<code>{esc(r['key'])}</code>"),
            Raw(f"<code>{esc(r['value'])}</code>"),
            r["where"],
        ]
        for r in data["spark_config"]
    ]
    return table_html(
        ["built in", "spark config key", "value (env names, never secrets)", "where"],
        rows,
        filterable=True,
        table_id="arch-spark",
    )


def _dockerfile_table(data: dict[str, Any]) -> Raw:
    rows = []
    for path, facts in sorted(data["dockerfiles"].items()):
        if facts.get("error"):
            rows.append([path, facts["error"], "", "", ""])
            continue
        users = [
            s["name"]
            for s in data["services"]
            if s["build"] and s["build"]["dockerfile"] == path
        ]
        rows.append(
            [
                Raw(f"<code>{esc(path)}</code>"),
                _codes(facts["from"]),
                _codes(facts["pip"]),
                _codes(facts["apt"]),
                ", ".join(users),
            ]
        )
    return table_html(
        ["Dockerfile", "FROM", "pip install", "apt-get install", "used by"],
        rows,
        table_id="arch-dockerfiles",
    )


def _not_derivable(data: dict[str, Any]) -> list[str]:
    items: list[str] = []
    for proc in data["processes"]:
        if not proc["launch"] and not proc.get("also_run_by_service"):
            items.append(
                f"How {proc['module']} is launched: no command in tracked docs/config "
                f"(entry at {proc['where']})."
            )
        if proc.get("routes"):
            port_given = any(
                re.search(r"--port\b|--bind\b|-b\b", line) for line in proc["launch"]
            )
            if not port_given:
                documented = sorted(
                    {d["address"] for d in proc.get("documented_at", [])}
                )
                items.append(
                    f"The port {proc['module']} listens on: its launch command does not set one"
                    + (
                        f"; tracked docs call its routes at {', '.join(documented)} "
                        "(that is a documented client URL, not configuration)."
                        if documented
                        else ", and the repo does not configure it elsewhere."
                    )
                )
    libs = sorted({lib for p in data["processes"] for lib in p["code"]["libraries"]})
    if libs:
        items.append(
            "Endpoints chosen inside third-party libraries when the repo passes none "
            f"(libraries used by processes: {', '.join(libs)}). Only endpoints written in this repo "
            "(env defaults, compose environment, literals) are drawn."
        )
    if data.get("docker_state_error"):
        items.append(
            f"Container state: docker not reachable ({data['docker_state_error']})."
        )
    if data.get("lake_tables") is None:
        items.append(
            f"Which lake tables exist: MinIO not reachable ({data.get('lake_error')})."
        )
    for dag in data["dags"]:
        if len(dag["mounted_by"]) > 1:
            items.append(
                f"Which of {', '.join(dag['mounted_by'])} executes the tasks of {dag['dag_id']}: all of them "
                "mount the DAG folder; the executor choice lives in Airflow, not in the repo's files."
            )
    return items


def render(data: dict[str, Any]) -> str:
    nodes, edges, groups = _graph(data)
    out: list[str] = []
    out.append(
        '<p class="lead">Every box and arrow below is computed: services, images, ports, '
        "depends_on and healthchecks from <code>docker compose config</code>; container state from "
        "<code>docker compose ps</code>; processes outside compose from the code's own entry points "
        "(<code>__main__</code> guards, the ASGI app, top-level scripts); and each connection from the "
        "env var or literal the reachable code actually reads, resolved against the compose environment "
        "or the code's default. Env values are never shown, only key names.</p>"
    )
    if data.get("_stale_reason"):
        out.append(f'<p>{badge("stale", "warn")} {esc(data["_stale_reason"])}</p>')
    if data.get("config_resolved_by") != "docker compose config":
        out.append(
            f'<p>{badge("fallback", "warn")} compose config {esc(data.get("config_resolved_by"))}</p>'
        )
    code_edges = sum(1 for e in edges if e["kind"] == "code")
    out.append(
        stat_cards(
            {
                **data["summary"],
                "connections found in code": code_edges,
                "compose edges": len(data["compose_edges"]),
            }
        )
    )
    out.append(
        section(
            "System diagram",
            "<p class='small muted'>Arrows point from the client to what it uses. "
            "<code>depends_on</code> edges come from compose; labelled <code>host:port · key</code> edges come "
            "from env vars and literals in the code (the key is the Spark config key, keyword argument or "
            "factory argument the value flows into). ● marks a running container. Click a node for its "
            "details and its full upstream/downstream chain.</p>"
            + graph_block(
                "architecture",
                nodes,
                edges,
                groups=groups,
                height=720,
                up_label="Uses it / depends on it",
                down_label="It uses / depends on",
            )
            + "<details><summary>Diagram nodes (accessible list)</summary>"
            + _nodes_table(nodes, groups, "arch-nodes")
            + "</details><details><summary>Diagram edges (accessible list)</summary>"
            + _edges_table(nodes, edges, "arch-edges")
            + "</details>",
            section_id="system-diagram",
        )
    )
    out.append(
        section(
            "Compose services",
            f"<p class='small muted'>Project <code>{esc(data.get('project'))}</code>; networks "
            + ", ".join(f"<code>{esc(v)}</code>" for v in data["networks"].values())
            + "; named volumes "
            + ", ".join(f"<code>{esc(v)}</code>" for v in data["named_volumes"])
            + ". Commands are shown with secret values masked (•••).</p>"
            + _services_table(data)
            + "<h3>depends_on with no connection behind it</h3>"
            + "<p class='small muted'>depends_on edges where neither the service's environment/command "
            "nor the code it runs names the target (start-up ordering only).</p>"
            + table_html(
                ["service", "depends on", "condition"],
                [
                    [
                        link(SLUG, f"service.{u['source']}", u["source"]),
                        link(SLUG, f"service.{u['target']}", u["target"]),
                        u["condition"],
                    ]
                    for u in data.get("unbacked_depends_on", [])
                ],
                table_id="arch-unbacked",
            )
            + "<h3>Images built from the repo</h3>"
            + _dockerfile_table(data),
            section_id="services",
        )
    )
    out.append(
        section(
            "Processes outside compose",
            "<p class='small muted'>Modules with a <code>__main__</code> guard, ASGI apps (an app object with "
            "route decorators) and top-level scripts that nothing imports. Connections are what the code "
            "reachable from the entry point reads; outside docker, env defaults apply unless the process "
            "loads <code>.env</code> and <code>.env</code> defines the key.</p>"
            + _process_table(data),
            section_id="processes",
        )
    )
    for dag in data["dags"]:
        out.append(
            section(
                f"Airflow DAG · {dag['dag_id']}",
                _dag_section(dag),
                section_id="airflow-dag",
            )
        )
    out.append(
        section(
            "Spark session configuration",
            "<p class='small muted'><code>.config(key, value)</code> calls on a "
            "<code>SparkSession.builder</code> chain. This is where Spark is pointed at MinIO (S3A) and at "
            "the OpenLineage endpoint.</p>" + _spark_table(data),
            section_id="spark-config",
        )
    )
    nd = _not_derivable(data)
    if data.get("index_errors"):
        nd += [f"Could not parse: {e}" for e in data["index_errors"]]
    out.append(
        section(
            "Not derivable",
            (
                "<ul>" + "".join(f"<li>{esc(i)}</li>" for i in nd) + "</ul>"
                if nd
                else "<p>Nothing.</p>"
            ),
            section_id="not-derivable",
        )
    )
    out.append(
        section(
            "How this page is generated",
            "<p class='small'>Python files: <code>git ls-files '*.py'</code> minus dot-directories, "
            "<code>toolkit/</code> and pytest modules. A unit is a function, a method, a module's import-time "
            "code or its <code>__main__</code> block; a unit reaches the repo callables and classes it names "
            "(a class pulls in all its methods). For every reached unit, <code>os.getenv</code>/"
            "<code>os.environ</code> reads and endpoint-like strings are collected. Inside a compose service the "
            "service's environment wins over the code default. Values of keys that look secret are dropped "
            "before anything is stored.</p>",
            section_id="how",
        )
    )
    out.append(sources_html(data["sources"]))
    return "".join(str(x) for x in out)
