"""Docker Compose facts for the architecture page.

* ``docker compose config --format json`` (read-only) gives the resolved config;
  if the docker CLI is missing, docker-compose.yml is resolved with PyYAML
  (anchors and ``<<`` merges) and the page says so.
* ``docker compose ps -a --format json`` (read-only) gives each container's state.
* Environment *values* are only used in memory: to find references to other
  services (hostname[:port]) and to collect secret values for redaction. They
  are never returned.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any

from toolkit.common import REPO_ROOT, rel
from toolkit.pages._architecture_code import is_secret_key

COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
MASK = "•••"


def _run(cmd: list[str], timeout: int = 60) -> tuple[str | None, str]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return None, f"{type(error).__name__}: {error}"
    if proc.returncode != 0:
        lines = (proc.stderr or proc.stdout or "").strip().splitlines()
        return None, lines[-1] if lines else f"exit {proc.returncode}"
    return proc.stdout, ""


def resolved_config() -> tuple[dict[str, Any], str, str]:
    """(config, how it was resolved, source string)."""
    out, err = _run(["docker", "compose", "config", "--format", "json"])
    if out:
        return (
            json.loads(out),
            "docker compose config",
            "docker compose config --format json",
        )
    import yaml

    raw = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))
    raw.setdefault("name", REPO_ROOT.name)
    for svc in raw.get("services", {}).values():
        deps = svc.get("depends_on")
        if isinstance(deps, list):
            svc["depends_on"] = {d: {"condition": "service_started"} for d in deps}
        env = svc.get("environment")
        if isinstance(env, list):
            svc["environment"] = dict(
                item.split("=", 1) if "=" in item else (item, "") for item in env
            )
    note = f"PyYAML fallback (docker compose config failed: {err})"
    return raw, note, f"{rel(COMPOSE_FILE)} (parsed with PyYAML)"


def container_states() -> tuple[dict[str, dict[str, Any]] | None, str]:
    out, err = _run(["docker", "compose", "ps", "-a", "--format", "json"], timeout=30)
    if out is None:
        return None, err
    items: list[dict[str, Any]] = []
    text = out.strip()
    if text.startswith("["):
        items = json.loads(text)
    else:
        for line in text.splitlines():
            line = line.strip()
            if line:
                parsed = json.loads(line)
                items.extend(parsed if isinstance(parsed, list) else [parsed])
    states: dict[str, dict[str, Any]] = {}
    for item in items:
        states[item.get("Service", "?")] = {
            "container": item.get("Name"),
            "state": item.get("State"),
            "status": item.get("Status"),
            "health": item.get("Health") or None,
            "exit_code": item.get("ExitCode"),
        }
    return states, ""


# ---------------------------------------------------------------- secrets


def secret_values(config: dict[str, Any]) -> set[str]:
    """Values that must never appear on the page: secret-named env values in the
    compose config and in .env / .env.example, and URL passwords."""
    secrets: set[str] = set()
    for svc in config.get("services", {}).values():
        for key, value in (svc.get("environment") or {}).items():
            if value is None:
                continue
            text = str(value)
            if is_secret_key(key):
                secrets.add(text)
            for match in re.finditer(r"://[^:/@\s]+:([^@/\s]+)@", text):
                secrets.add(match.group(1))
    try:
        from dotenv import dotenv_values

        for env_file in (REPO_ROOT / ".env", REPO_ROOT / ".env.example"):
            if env_file.exists():
                for key, value in dotenv_values(env_file).items():
                    if value and is_secret_key(key):
                        secrets.add(value)
    except ImportError:
        pass
    return {s for s in secrets if s}


def redact_command(text: str, secrets: set[str]) -> str:
    """Mask secret tokens in a shell command, keeping each segment's program name
    (so ``airflow db migrate`` stays readable while ``--password airflow`` does not)."""
    if not text:
        return text
    ordered = sorted((s for s in secrets if len(s) >= 4), key=len, reverse=True)
    pieces = re.split(r"(\s+)", text)
    out = []
    first = True
    mask_next = False
    for piece in pieces:
        if not piece or piece.isspace():
            out.append(piece)
            continue
        core = piece.strip("'\"();")
        clean = piece
        if mask_next or (not first and core in secrets):
            clean = piece.replace(core, MASK) if core else MASK
        elif not first:
            for secret in ordered:
                clean = clean.replace(secret, MASK)
        mask_next = bool(re.fullmatch(r"--?(password|passwd|secret|token)", core, re.I))
        out.append(clean)
        first = (
            core in ("&&", "||", "|", "-c", "do", "then", "")
            or piece.endswith(";")
            or piece.startswith("(")
        )
    return "".join(out)


def scrub(obj: Any, secrets: set[str]) -> Any:
    """Last line of defence over the whole snapshot: replace any long secret value."""
    long = sorted((s for s in secrets if len(s) >= 8), key=len, reverse=True)
    if isinstance(obj, str):
        for secret in long:
            obj = obj.replace(secret, MASK)
        return obj
    if isinstance(obj, list):
        return [scrub(x, secrets) for x in obj]
    if isinstance(obj, dict):
        return {k: scrub(v, secrets) for k, v in obj.items()}
    return obj


# -------------------------------------------------------- service references


def _hostnames(config: dict[str, Any]) -> dict[str, str]:
    """hostname -> service (service names and container_names)."""
    names: dict[str, str] = {}
    for name, svc in config.get("services", {}).items():
        names[name] = name
        if svc.get("container_name"):
            names[str(svc["container_name"])] = name
    return names


def find_service_refs(
    text: str, hostnames: dict[str, str]
) -> list[tuple[str, str, int | None]]:
    """(service, hostname, port) mentioned in a string as a hostname."""
    found: list[tuple[str, str, int | None]] = []
    for host in sorted(hostnames, key=len, reverse=True):
        pattern = re.compile(rf"(?<![\w.-]){re.escape(host)}(?![\w-])(?::(\d{{2,5}}))?")
        for match in pattern.finditer(text):
            start = match.start()
            if (
                start > 0
                and text[start - 1] == "/"
                and text[max(0, start - 2) : start] != "//"
            ):
                continue  # a path segment like /usr/bin/mc, not a hostname
            port = int(match.group(1)) if match.group(1) else None
            item = (hostnames[host], host, port)
            if item not in found:
                found.append(item)
    return found


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(v) for v in value)
    return str(value)


def _container_ports(svc: dict[str, Any]) -> set[int]:
    ports = set()
    for p in svc.get("ports") or []:
        if isinstance(p, dict) and p.get("target"):
            ports.add(int(p["target"]))
        elif isinstance(p, str):
            ports.add(int(p.split(":")[-1].split("/")[0]))
    for e in svc.get("expose") or []:
        ports.add(int(str(e).split("/")[0]))
    return ports


def published_ports(config: dict[str, Any]) -> dict[int, str]:
    """host port -> service."""
    out: dict[int, str] = {}
    for name, svc in config.get("services", {}).items():
        for p in svc.get("ports") or []:
            if isinstance(p, dict) and p.get("published"):
                out[int(p["published"])] = name
            elif isinstance(p, str) and ":" in p:
                out[int(p.split(":")[-2])] = name
    return out


def service_edges(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Edges between compose services: depends_on, and hostname references in
    environment values / command / entrypoint (values are not returned)."""
    services = config.get("services", {})
    hostnames = _hostnames(config)
    edges: list[dict[str, Any]] = []
    for name, svc in services.items():
        for dep, spec in (svc.get("depends_on") or {}).items():
            cond = (spec or {}).get("condition", "service_started")
            edges.append(
                {
                    "source": name,
                    "target": dep,
                    "kind": "depends_on",
                    "label": "depends_on"
                    + ("" if cond == "service_started" else f" ({cond})"),
                    "evidence": f"depends_on.{dep}.condition = {cond}",
                }
            )
        places: list[tuple[str, str]] = []
        for key, value in (svc.get("environment") or {}).items():
            if not is_secret_key(key):
                places.append((f"environment {key}", _as_text(value)))
        places.append(("command", _as_text(svc.get("command"))))
        places.append(("entrypoint", _as_text(svc.get("entrypoint"))))
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for where, text in places:
            for target, host, port in find_service_refs(text, hostnames):
                if target == name:
                    continue
                note = ""
                if port is not None and port not in _container_ports(services[target]):
                    declared = _env_keys_mentioning_port(services[target], port)
                    note = f" (port {port} is not in {target}'s ports/expose" + (
                        f"; it is a listener in {target}'s environment {', '.join(declared)})"
                        if declared
                        else ")"
                    )
                label = f"{host}:{port}" if port else host
                evidence = f"{where} references {label}{note}"
                key = (target, label)
                if key in merged:
                    merged[key]["evidence"] += "; " + evidence
                else:
                    merged[key] = {
                        "source": name,
                        "target": target,
                        "kind": "env reference",
                        "label": label,
                        "evidence": evidence,
                    }
        edges.extend(merged.values())
    return edges


def _env_keys_mentioning_port(svc: dict[str, Any], port: int) -> list[str]:
    keys = []
    for key, value in (svc.get("environment") or {}).items():
        if not is_secret_key(key) and re.search(rf":{port}(?!\d)", _as_text(value)):
            keys.append(key)
    return sorted(keys)


def env_endpoint(svc: dict[str, Any], key: str) -> str | None:
    """Value of a non-secret env key of a service, for endpoint parsing only."""
    if is_secret_key(key):
        return None
    env = svc.get("environment") or {}
    if key not in env or env[key] is None:
        return None
    return str(env[key])


def python_module_of(svc: dict[str, Any]) -> str | None:
    """Repo module a service runs: ``python[3] -m X`` or ``python[3] path.py``."""
    argv: list[str] = []
    for part in ("entrypoint", "command"):
        value = svc.get(part)
        if isinstance(value, list):
            argv.extend(str(v) for v in value)
        elif isinstance(value, str):
            argv.extend(shlex.split(value))
    for i, tok in enumerate(argv):
        if re.fullmatch(r"(.*/)?python[0-9.]*", tok):
            rest = argv[i + 1 :]
            if len(rest) >= 2 and rest[0] == "-m":
                return rest[1]
            if rest and rest[0].endswith(".py"):
                return rest[0][:-3].replace("/", ".")
    return None


def bind_mounts(svc: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for vol in svc.get("volumes") or []:
        if isinstance(vol, dict):
            source = vol.get("source", "")
            kind = vol.get("type", "volume")
            target = vol.get("target", "")
            ro = bool(vol.get("read_only"))
        else:
            parts = str(vol).split(":")
            source, target = parts[0], parts[1] if len(parts) > 1 else ""
            kind = "bind" if source.startswith((".", "/")) else "volume"
            ro = len(parts) > 2 and parts[2] == "ro"
        shown = source
        if kind == "bind":
            path = Path(source)
            if not path.is_absolute():
                path = REPO_ROOT / path
            shown = "./" + rel(path) if rel(path) != path.as_posix() else source
            if rel(path) == ".":
                shown = "./"
        out.append({"type": kind, "source": shown, "target": target, "read_only": ro})
    return out


def dockerfile_facts(dockerfile: Path) -> dict[str, Any]:
    """FROM images, ENV keys and pip/apt packages from a Dockerfile."""
    if not dockerfile.exists():
        return {"error": f"{rel(dockerfile)} not found"}
    text = dockerfile.read_text(encoding="utf-8").replace("\\\n", " ")
    base, env_keys, pip, apt = [], [], [], []
    for line in text.splitlines():
        stripped = line.strip()
        upper = stripped.upper()
        if upper.startswith("FROM "):
            base.append(stripped.split()[1])
        elif upper.startswith("ENV "):
            body = stripped[4:].strip()
            env_keys.extend(k.split("=")[0] for k in body.split() if "=" in k)
        elif upper.startswith("RUN "):
            for seg in re.split(r"&&|;", stripped[4:]):
                words = shlex.split(seg) if seg.strip() else []
                if "pip" in words and "install" in words:
                    i = words.index("install")
                    pip.extend(w for w in words[i + 1 :] if not w.startswith("-"))
                if "apt-get" in words and "install" in words:
                    i = words.index("install")
                    apt.extend(w for w in words[i + 1 :] if not w.startswith("-"))
    return {"from": base, "env_keys": env_keys, "pip": pip, "apt": apt}
