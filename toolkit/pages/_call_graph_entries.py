"""Entry points that live outside Python: compose services, Dockerfiles,
documented commands (README) and CI steps (private helper of the call-graph page).

Everything here is parsed from the files; nothing is typed in by hand.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

PY_MODULE_RE = re.compile(r"\bpython(?:3(?:\.\d+)?)?\s+-m\s+([A-Za-z_][\w.]*)")
PY_FILE_RE = re.compile(r"\bpython(?:3(?:\.\d+)?)?\s+([\w./-]+\.py)\b")
UVICORN_RE = re.compile(r"\buvicorn\s+([A-Za-z_][\w.]*):([A-Za-z_]\w*)")
PYTEST_RE = re.compile(r"(?:^|\s|`)(pytest)(?:\s+([\w./-]+))?")


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(v) for v in value)
    return str(value)


def commands_in(text: str) -> list[dict[str, str]]:
    """Python launches found in one command string."""
    found: list[dict[str, str]] = []
    for match in PY_MODULE_RE.finditer(text):
        found.append({"how": "python -m", "module": match.group(1)})
    for match in PY_FILE_RE.finditer(text):
        found.append({"how": "python file", "file": match.group(1)})
    for match in UVICORN_RE.finditer(text):
        found.append(
            {"how": "uvicorn", "module": match.group(1), "attr": match.group(2)}
        )
    return found


def compose_services(repo: Path) -> tuple[list[dict[str, Any]], str]:
    path = repo / "docker-compose.yml"
    if not path.is_file():
        return [], ""
    text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    lines = text.splitlines()
    services: list[dict[str, Any]] = []
    for name, spec in (data.get("services") or {}).items():
        spec = spec or {}
        line = next(
            (
                i + 1
                for i, raw in enumerate(lines)
                if re.match(rf"^\s{{2}}{re.escape(name)}:\s*$", raw)
            ),
            0,
        )
        command = _as_text(spec.get("command"))
        entrypoint = _as_text(spec.get("entrypoint"))
        build = spec.get("build")
        dockerfile = ""
        if isinstance(build, dict):
            dockerfile = str(build.get("dockerfile") or "")
        elif isinstance(build, str):
            dockerfile = f"{build}/Dockerfile"
        volumes = [str(v) for v in spec.get("volumes") or []]
        environment = spec.get("environment") or {}
        env_keys = (
            sorted(environment)
            if isinstance(environment, dict)
            else sorted(str(e).split("=")[0] for e in environment)
        )
        executor = (
            str(environment.get("AIRFLOW__CORE__EXECUTOR", ""))
            if isinstance(environment, dict)
            else ""
        )
        services.append(
            {
                "name": name,
                "location": f"docker-compose.yml:{line}",
                "image": str(spec.get("image") or ""),
                "dockerfile": dockerfile,
                "command": command,
                "entrypoint": entrypoint,
                "volumes": volumes,
                "env_keys": env_keys,
                "airflow_executor": executor,
                "launches": commands_in(f"{entrypoint} {command}"),
            }
        )
    return services, "docker-compose.yml"


def dockerfiles(repo: Path) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    docker_dir = repo / "docker"
    candidates = sorted(docker_dir.rglob("Dockerfile*")) if docker_dir.is_dir() else []
    candidates += sorted(repo.glob("Dockerfile*"))
    for path in candidates:
        rel = path.relative_to(repo).as_posix()
        base = ""
        instructions: list[dict[str, str]] = []
        for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = raw.strip()
            upper = line.upper()
            if upper.startswith("FROM "):
                base = line.split(None, 1)[1]
            for keyword in ("CMD", "ENTRYPOINT"):
                if upper.startswith(keyword + " ") or upper.startswith(keyword + "["):
                    instructions.append(
                        {
                            "keyword": keyword,
                            "value": line[len(keyword) :].strip(),
                            "location": f"{rel}:{number}",
                        }
                    )
        result.append({"file": rel, "from": base, "instructions": instructions})
    return result


def documented_commands(repo: Path) -> list[dict[str, Any]]:
    """Launch commands inside fenced code blocks of README.md."""
    path = repo / "README.md"
    if not path.is_file():
        return []
    found: list[dict[str, Any]] = []
    in_fence = False
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if raw.strip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            continue
        line = raw.split("#", 1)[0].strip()
        for launch in commands_in(line):
            found.append({**launch, "text": line, "location": f"README.md:{number}"})
        if PYTEST_RE.search(line) and line.split()[0] == "pytest":
            found.append(
                {"how": "pytest", "text": line, "location": f"README.md:{number}"}
            )
    return found


def ci_commands(repo: Path) -> list[dict[str, Any]]:
    """``run:`` steps of GitHub Actions workflows that launch first-party Python."""
    found: list[dict[str, Any]] = []
    workflows = repo / ".github" / "workflows"
    if not workflows.is_dir():
        return found
    for path in sorted([*workflows.glob("*.yml"), *workflows.glob("*.yaml")]):
        rel = path.relative_to(repo).as_posix()
        for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = raw.strip()
            if line.startswith("#"):
                continue
            if line.startswith("pytest") or " pytest " in f" {line} ":
                if line.split()[0] == "pytest":
                    found.append(
                        {"how": "pytest", "text": line, "location": f"{rel}:{number}"}
                    )
            for launch in commands_in(line):
                if launch.get("module") == "pip":
                    continue
                found.append({**launch, "text": line, "location": f"{rel}:{number}"})
    return found
