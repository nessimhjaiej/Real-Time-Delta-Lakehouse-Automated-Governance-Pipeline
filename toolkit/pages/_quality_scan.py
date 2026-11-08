"""Static (ast/yaml) analysis behind the quality page.

Everything here is read from the repo's source text; nothing is hand-listed
except the *rules* that say which Spark/Python constructs count as a quality
control (``CONTROL_RULES``) and which text patterns mean "a control we looked
for" (``ABSENT_PATTERNS``) -- both are shown on the page as the method.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from toolkit.common import REPO_ROOT, rel

QUALITY_FILE = REPO_ROOT / "src" / "pipelines" / "quality.py"
CALLBACKS_FILE = REPO_ROOT / "src" / "utils" / "callbacks.py"
GATE_CALLABLE = "src.pipelines.quality.run_quality_gate"
LAKE_PREFIX = "s3a://lakehouse/"

# construct -> (category, what it does). Method rules match ``<x>.<name>(...)``.
CONTROL_RULES: dict[str, tuple[str, str]] = {
    "dropDuplicates": ("dedupe", "drops duplicate rows on the listed key"),
    "dropDuplicatesWithinWatermark": ("dedupe", "streaming dedupe within watermark"),
    "distinct": ("dedupe", "drops fully identical rows"),
    "withWatermark": ("late data", "bounds streaming state by event time"),
    "filter": ("row filter", "keeps only rows matching a predicate"),
    "where": ("row filter", "keeps only rows matching a predicate"),
    "dropna": ("null handling", "drops rows with nulls"),
    "fillna": ("null handling", "replaces nulls"),
    "cast": ("type enforcement", "casts a column to a declared type"),
    "from_json": ("schema enforcement", "parses JSON against a declared schema"),
    "StructType": ("schema enforcement", "declares an explicit schema"),
    "sha2": ("PII protection", "SHA-2 hash pseudonymises a value"),
    "select": ("column contract", "explicit output column list"),
}
OPTION_RULES: dict[str, tuple[str, str]] = {
    "overwriteSchema": (
        "schema drift allowed",
        "write replaces the table schema instead of rejecting a mismatch",
    ),
    "mergeSchema": ("schema drift allowed", "write merges new columns in"),
    "inferSchema": ("schema inferred", "column types are guessed from the data"),
    "badRecordsPath": ("quarantine", "bad records are diverted to a path"),
    "mode": ("parse mode", "reader mode for malformed records"),
}
ABSENT_PATTERNS: dict[str, str] = {
    "quarantine / dead-letter table": r"quarantine|dead[_-]?letter|\bdlq\b",
    "Spark bad-record handling": r"badRecordsPath|columnNameOfCorruptRecord|_corrupt_record|FAILFAST|DROPMALFORMED",
    "Great Expectations outside quality.py": (
        r"import great_expectations|from great_expectations|\bgx\."
    ),
    "Spark ANSI mode (strict casts)": r"spark\.sql\.ansi",
    "Delta table constraints / invariants": (
        r"ADD CONSTRAINT|addConstraint|delta\.constraints|delta\.invariants"
    ),
}


# ------------------------------------------------------------------ parsing


@dataclass
class Module:
    path: Path
    source: str
    tree: ast.Module
    imports: dict[str, str] = field(default_factory=dict)
    constants: dict[str, Any] = field(default_factory=dict)
    local_strings: dict[str, list[str]] = field(default_factory=dict)

    @property
    def dotted(self) -> str:
        return ".".join(Path(rel(self.path)).with_suffix("").parts)

    def line(self, lineno: int) -> str:
        lines = self.source.splitlines()
        return lines[lineno - 1].strip() if 0 < lineno <= len(lines) else ""


_CACHE: dict[Path, Module] = {}


def load(path: Path) -> Module:
    path = path.resolve()
    if path not in _CACHE:
        source = path.read_text(encoding="utf-8")
        module = Module(path, source, ast.parse(source, filename=str(path)))
        for node in module.tree.body:
            if isinstance(node, ast.ImportFrom) and node.module:
                for alias in node.names:
                    module.imports[alias.asname or alias.name] = (
                        f"{node.module}.{alias.name}"
                    )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    module.imports[alias.asname or alias.name] = alias.name
            elif isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    try:
                        module.constants[target.id] = ast.literal_eval(node.value)
                    except (ValueError, SyntaxError, TypeError):
                        module.constants[target.id] = node.value
        for node in ast.walk(module.tree):
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
            ):
                module.local_strings.setdefault(node.targets[0].id, []).append(
                    node.value.value
                )
        _CACHE[path] = module
    return _CACHE[path]


def cite(module: Module, lineno: int) -> str:
    return f"{rel(module.path)}:{lineno}"


def _walk_with_scope(tree: ast.AST) -> Iterator[tuple[ast.AST, str]]:
    """Yield (node, enclosing qualname or '<module>')."""

    def visit(node: ast.AST, scope: list[str]) -> Iterator[tuple[ast.AST, str]]:
        for child in ast.iter_child_nodes(node):
            yield child, ".".join(scope) or "<module>"
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                yield from visit(child, [*scope, child.name])
            elif _is_main_guard(child):
                yield from visit(child, [*scope, "__main__"])
            else:
                yield from visit(child, scope)

    yield from visit(tree, [])


def _is_main_guard(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
    )


def _func_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    if isinstance(call.func, ast.Name):
        return call.func.id
    return None


def _call_line(call: ast.Call) -> int:
    """Line where the called name appears (matters for chained calls)."""
    return int(getattr(call.func, "end_lineno", None) or call.lineno)


def _resolve_str(module: Module, node: ast.AST | None) -> list[str]:
    """String value(s) an expression can take, via module-level constants."""
    if node is None:
        return []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Name):
        value = module.constants.get(node.id)
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            return [v for v in value.values() if isinstance(v, str)]
        return list(module.local_strings.get(node.id, []))
    if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name):
        value = module.constants.get(node.value.id)
        if isinstance(value, dict):
            try:
                key = ast.literal_eval(node.slice)
            except (ValueError, SyntaxError):
                return [v for v in value.values() if isinstance(v, str)]
            item = value.get(key)
            return [item] if isinstance(item, str) else []
    return []


def lake_id(uri: str) -> str | None:
    """'s3a://lakehouse/silver/orders_batch' -> 'silver.orders_batch'."""
    for prefix in (LAKE_PREFIX, "s3://lakehouse/"):
        if uri.startswith(prefix):
            parts = uri[len(prefix) :].strip("/").split("/")
            if len(parts) >= 2 and not parts[1].startswith("_"):
                return f"{parts[0]}.{parts[1]}"
    return None


# --------------------------------------------------------------- DAG wiring


def dag_ast() -> dict[str, Any]:
    """Operators, default_args and >> edges as written in dags/*.py, with lines."""
    tasks: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    default_args: list[dict[str, Any]] = []
    for file in sorted((REPO_ROOT / "dags").glob("*.py")):
        module = load(file)
        var_to_task: dict[str, str] = {}
        for node, _scope in _walk_with_scope(module.tree):
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                name = node.targets[0].id
                value = node.value
                if name == "default_args" and isinstance(value, ast.Dict):
                    for key, val in zip(value.keys, value.values, strict=False):
                        if isinstance(key, ast.Constant):
                            default_args.append(
                                {
                                    "key": key.value,
                                    "value": ast.get_source_segment(module.source, val),
                                    "at": cite(module, val.lineno),
                                }
                            )
                if not (isinstance(value, ast.Call) and _func_name(value)):
                    continue
                op = _func_name(value) or ""
                if not op.endswith(("Operator", "Sensor")):
                    continue
                kwargs = {k.arg: k.value for k in value.keywords if k.arg}
                task_id_node = kwargs.get("task_id")
                task_id = (
                    task_id_node.value
                    if isinstance(task_id_node, ast.Constant)
                    else name
                )
                var_to_task[name] = str(task_id)

                def resolved(key: str) -> str | None:
                    node_ = kwargs.get(key)
                    if node_ is None:
                        return None
                    if isinstance(node_, ast.Name) and node_.id in module.imports:
                        return module.imports[node_.id]
                    return ast.get_source_segment(module.source, node_)

                tasks.append(
                    {
                        "task_id": task_id,
                        "var": name,
                        "operator": op,
                        "at": cite(module, value.lineno),
                        "callable": resolved("python_callable"),
                        "retries": resolved("retries"),
                        "retry_delay": resolved("retry_delay"),
                        "trigger_rule": resolved("trigger_rule"),
                        "on_failure_callback": resolved("on_failure_callback"),
                        "kw_lines": {
                            k: cite(module, v.lineno) for k, v in kwargs.items()
                        },
                    }
                )
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp):
                for up, down in _flatten_shift(node.value):
                    for u in up:
                        for d in down:
                            edges.append(
                                {
                                    "up": var_to_task.get(u, u),
                                    "down": var_to_task.get(d, d),
                                    "at": cite(module, node.lineno),
                                }
                            )
    return {"tasks": tasks, "edges": edges, "default_args": default_args}


def _names(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [n.id for n in node.elts if isinstance(n, ast.Name)]
    return []


def _flatten_shift(node: ast.BinOp) -> list[tuple[list[str], list[str]]]:
    """``a >> b >> [c, d]`` -> [([a],[b]), ([b],[c,d])]; ``<<`` reversed."""
    pairs: list[tuple[list[str], list[str]]] = []

    def rightmost(n: ast.AST) -> list[str]:
        return rightmost(n.right) if isinstance(n, ast.BinOp) else _names(n)

    def visit(n: ast.AST) -> None:
        if not isinstance(n, ast.BinOp) or not isinstance(
            n.op, (ast.RShift, ast.LShift)
        ):
            return
        visit(n.left)
        left = rightmost(n.left)
        right = _names(n.right)
        if isinstance(n.op, ast.RShift):
            pairs.append((left, right))
        else:
            pairs.append((right, left))

    visit(node)
    return pairs


# ------------------------------------------------------- the gate's own code


def _function(module: Module, name: str) -> ast.FunctionDef | None:
    for node in ast.walk(module.tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def gate_code() -> dict[str, Any]:
    """Where SILVER_PATHS/_SUITE_BUILDERS/the failure rule/the raise live."""
    module = load(QUALITY_FILE)
    facts: dict[str, Any] = {"file": rel(QUALITY_FILE)}
    for node in module.tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            facts[f"{node.targets[0].id}_at"] = cite(module, node.lineno)
        if isinstance(node, ast.ClassDef):
            facts.setdefault("classes", []).append(
                {
                    "name": node.name,
                    "bases": [ast.unparse(b) for b in node.bases],
                    "at": cite(module, node.lineno),
                }
            )

    builders: dict[str, str] = {}
    suites_node = next(
        (
            n
            for n in module.tree.body
            if isinstance(n, ast.Assign)
            and isinstance(n.targets[0], ast.Name)
            and n.targets[0].id == "_SUITE_BUILDERS"
        ),
        None,
    )
    if suites_node is not None and isinstance(suites_node.value, ast.Dict):
        for key, val in zip(
            suites_node.value.keys, suites_node.value.values, strict=False
        ):
            if isinstance(key, ast.Constant) and isinstance(val, ast.Name):
                builders[str(key.value)] = val.id
    facts["builders"] = builders

    # (builder function, snake_case expectation type, column) -> line
    expectation_lines: dict[str, str] = {}
    for builder in set(builders.values()):
        func = _function(module, builder)
        if func is None:
            continue
        for node in ast.walk(func):
            if isinstance(node, ast.Call):
                cls = _func_name(node) or ""
                if not cls.startswith("Expect"):
                    continue
                snake = re.sub(r"(?<!^)(?=[A-Z])", "_", cls).lower()
                column = next(
                    (
                        k.value.value
                        for k in node.keywords
                        if k.arg == "column" and isinstance(k.value, ast.Constant)
                    ),
                    None,
                )
                expectation_lines[f"{builder}|{snake}|{column}"] = cite(
                    module, node.lineno
                )
    facts["expectation_lines"] = expectation_lines

    run = _function(module, "run_quality_gate")
    steps: list[dict[str, str]] = []
    if run is not None:
        facts["run_at"] = cite(module, run.lineno)
        for node in ast.walk(run):
            if isinstance(node, ast.Call):
                name = _func_name(node)
                if name == "get_spark_session":
                    args = [ast.unparse(a) for a in node.args]
                    steps.append(
                        {
                            "step": f"starts Spark: get_spark_session({', '.join(args)})",
                            "at": cite(module, node.lineno),
                        }
                    )
                elif name == "get_context":
                    kw = ", ".join(ast.unparse(k) for k in node.keywords)
                    steps.append(
                        {
                            "step": f"GX context: gx.get_context({kw})",
                            "at": cite(module, node.lineno),
                        }
                    )
                elif name == "load":
                    steps.append(
                        {
                            "step": "reads each Silver Delta table: "
                            + ast.unparse(node),
                            "at": cite(module, node.lineno),
                        }
                    )
                elif name == "_validate_table":
                    steps.append(
                        {
                            "step": "validates it: " + ast.unparse(node),
                            "at": cite(module, node.lineno),
                        }
                    )
            elif isinstance(node, ast.For):
                steps.append(
                    {
                        "step": f"loops: for {ast.unparse(node.target)} in {ast.unparse(node.iter)}",
                        "at": cite(module, node.lineno),
                    }
                )
            elif isinstance(node, ast.If) and any(
                isinstance(n, ast.Raise) for n in node.body
            ):
                raise_node = next(n for n in node.body if isinstance(n, ast.Raise))
                exc = raise_node.exc
                exc_name = (
                    _func_name(exc)
                    if isinstance(exc, ast.Call)
                    else ast.unparse(exc or node)
                )
                steps.append(
                    {
                        "step": f"if {ast.unparse(node.test)}: raise {exc_name}(...)",
                        "at": cite(module, raise_node.lineno),
                    }
                )
                facts["raises"] = exc_name
            elif isinstance(node, ast.Try) and node.finalbody:
                calls = [ast.unparse(n) for n in node.finalbody]
                steps.append(
                    {
                        "step": "finally: " + "; ".join(calls),
                        "at": cite(module, node.finalbody[0].lineno),
                    }
                )
    steps.sort(key=lambda s: int(s["at"].rsplit(":", 1)[1]))
    facts["steps"] = steps

    validate = _function(module, "_validate_table")
    if validate is not None:
        facts["validate_at"] = cite(module, validate.lineno)
        for node in ast.walk(validate):
            if isinstance(node, ast.If) and any(
                isinstance(n, ast.Continue) for n in node.body
            ):
                facts["failure_rule"] = {
                    "rule": f"an expectation fails the table unless `{ast.unparse(node.test)}`",
                    "at": cite(module, node.lineno),
                }
            if isinstance(node, ast.Call) and _func_name(node) == "validate":
                facts["validate_call"] = {
                    "code": ast.unparse(node),
                    "result_format_passed": any(
                        k.arg == "result_format" for k in node.keywords
                    ),
                    "at": cite(module, node.lineno),
                }
    return facts


def callback_code(dotted: str | None) -> dict[str, Any] | None:
    """What the on_failure_callback actually does (every call it makes)."""
    if not dotted:
        return None
    module_name, _, func_name = dotted.rpartition(".")
    path = REPO_ROOT / Path(*module_name.split(".")).with_suffix(".py")
    if not path.exists():
        return {"callable": dotted, "error": "source file not found"}
    module = load(path)
    func = _function(module, func_name)
    if func is None:
        return {"callable": dotted, "error": "function not found"}
    calls = []
    for node in ast.walk(func):
        if isinstance(node, ast.Call):
            calls.append(
                {"call": ast.unparse(node.func), "at": cite(module, node.lineno)}
            )
    calls.sort(key=lambda c: int(c["at"].rsplit(":", 1)[1]))
    return {"callable": dotted, "at": cite(module, func.lineno), "calls": calls}


# ----------------------------------------------- writes, entry points, reach


def _py_files(*roots: str) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        base = REPO_ROOT / root
        files.extend(sorted(base.rglob("*.py")) if base.is_dir() else [base])
    return [f for f in files if "__pycache__" not in f.parts]


def _module_path(dotted: str) -> Path | None:
    path = REPO_ROOT / Path(*dotted.split("."))
    if path.with_suffix(".py").exists():
        return path.with_suffix(".py")
    if (path / "__init__.py").exists():
        return path / "__init__.py"
    return None


def _calls_in(module: Module, node: ast.AST) -> set[str]:
    """Dotted names of src.* functions called directly inside ``node``."""
    out: set[str] = set()
    local_defs = {n.name for n in module.tree.body if isinstance(n, ast.FunctionDef)}
    for child in ast.walk(node):
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name):
            name = child.func.id
            if name in local_defs:
                out.add(f"{module.dotted}.{name}")
            elif name in module.imports and module.imports[name].startswith("src."):
                out.add(module.imports[name])
    return out


def lake_writes() -> dict[str, Any]:
    """Every Delta write to the lake in src/, the function that does it, and
    every entry point (DAG task / compose service / __main__) that reaches it."""
    writes: list[dict[str, Any]] = []
    call_map: dict[str, set[str]] = {}
    mains: dict[str, set[str]] = {}
    for file in _py_files("src/pipelines", "src/utils"):
        module = load(file)
        for node in module.tree.body:
            if isinstance(node, ast.FunctionDef):
                call_map[f"{module.dotted}.{node.name}"] = _calls_in(module, node)
            elif _is_main_guard(node):
                mains[module.dotted] = _calls_in(module, node)
        for node, scope in _walk_with_scope(module.tree):
            if not isinstance(node, ast.Call):
                continue
            name = _func_name(node)
            target: ast.AST | None = None
            if name in {"save", "start"} and node.args:
                target = node.args[0]
            elif name == "create" and len(node.args) >= 2:
                if ast.unparse(node.func).endswith("SinkFactory.create"):
                    target = node.args[1]
            for uri in _resolve_str(module, target):
                table = lake_id(uri)
                if table:
                    writes.append(
                        {
                            "table": table,
                            "function": f"{module.dotted}.{scope}",
                            "kind": "stream" if name == "start" else "batch",
                            "at": cite(module, _call_line(node)),
                        }
                    )

    return {
        "writes": writes,
        "mains": {k: sorted(v) for k, v in sorted(mains.items())},
        "call_map": {k: sorted(v) for k, v in sorted(call_map.items())},
    }


def reach(call_map: dict[str, list[str]], roots: list[str]) -> set[str]:
    """Every function transitively called from ``roots`` (roots included)."""
    seen: set[str] = set()
    todo = list(roots)
    while todo:
        fn = todo.pop()
        if fn in seen:
            continue
        seen.add(fn)
        todo.extend(call_map.get(fn, ()))
    return seen


def compose_commands() -> list[dict[str, str]]:
    """Compose services that run a ``python -m <module>`` command."""
    path = REPO_ROOT / "docker-compose.yml"
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    text_lines = path.read_text(encoding="utf-8").splitlines()
    out = []
    for name, service in (data.get("services") or {}).items():
        command = (service or {}).get("command")
        if isinstance(command, list):
            command = " ".join(str(c) for c in command)
        match = re.search(r"python3?\s+-m\s+([\w.]+)", str(command or ""))
        if not match:
            continue
        line = next(
            (i + 1 for i, text in enumerate(text_lines) if match.group(0) in text),
            0,
        )
        out.append(
            {
                "service": str(name),
                "module": match.group(1),
                "restart": str((service or {}).get("restart", "")),
                "at": f"docker-compose.yml:{line}",
            }
        )
    return out


# ------------------------------------------------------------ other controls


def scan_controls() -> tuple[list[dict[str, Any]], list[str]]:
    """Quality-relevant constructs in the pipeline/util code (not the gate)."""
    files = [
        f
        for f in _py_files("src/pipelines", "src/utils")
        if f.resolve() not in {QUALITY_FILE.resolve(), CALLBACKS_FILE.resolve()}
    ]
    found: list[dict[str, Any]] = []
    for file in files:
        module = load(file)
        area = "pipeline" if "pipelines" in file.parts else "utility"
        for node, scope in _walk_with_scope(module.tree):
            if isinstance(node, ast.Call):
                name = _func_name(node) or ""
                if name in CONTROL_RULES:
                    if name == "select" and not (
                        len(node.args) >= 3
                        and all(
                            isinstance(a, ast.Constant) and isinstance(a.value, str)
                            for a in node.args
                        )
                    ):
                        continue
                    if name in {"filter", "where"} and not node.args:
                        continue
                    category, meaning = CONTROL_RULES[name]
                    detail = _control_detail(module, node, name)
                    found.append(
                        {
                            "category": category,
                            "construct": name,
                            "meaning": meaning,
                            "detail": detail,
                            "function": f"{module.dotted}.{scope}",
                            "area": area,
                            "at": cite(module, _call_line(node)),
                        }
                    )
                if name in {"option", "options"}:
                    for key, value in _option_pairs(node):
                        if key in OPTION_RULES:
                            category, meaning = OPTION_RULES[key]
                            found.append(
                                {
                                    "category": category,
                                    "construct": f"{name}({key}={value})",
                                    "meaning": meaning,
                                    "detail": module.line(_call_line(node)),
                                    "function": f"{module.dotted}.{scope}",
                                    "area": area,
                                    "at": cite(module, _call_line(node)),
                                }
                            )
            elif isinstance(node, ast.Raise) and node.exc is not None:
                found.append(
                    {
                        "category": "explicit error",
                        "construct": "raise",
                        "meaning": "stops on an invalid input/state",
                        "detail": ast.unparse(node.exc)[:160],
                        "function": f"{module.dotted}.{scope}",
                        "area": area,
                        "at": cite(module, node.lineno),
                    }
                )
            elif isinstance(node, ast.Assert):
                found.append(
                    {
                        "category": "explicit error",
                        "construct": "assert",
                        "meaning": "stops when the condition is false",
                        "detail": ast.unparse(node.test)[:160],
                        "function": f"{module.dotted}.{scope}",
                        "area": area,
                        "at": cite(module, node.lineno),
                    }
                )
    found.sort(
        key=lambda c: (c["at"].rsplit(":", 1)[0], int(c["at"].rsplit(":", 1)[1]))
    )
    return found, [rel(f) for f in files]


def _control_detail(module: Module, node: ast.Call, name: str) -> str:
    if name in {"dropDuplicates", "dropDuplicatesWithinWatermark"} and node.args:
        arg = node.args[0]
        if isinstance(arg, ast.Name):
            # resolve a local list variable assigned in the same function
            for other in ast.walk(module.tree):
                if (
                    isinstance(other, ast.Assign)
                    and isinstance(other.targets[0], ast.Name)
                    and other.targets[0].id == arg.id
                ):
                    return f"key {arg.id} = {ast.unparse(other.value)}"
        return f"key {ast.unparse(arg)}"
    if name == "cast" and isinstance(node.func, ast.Attribute):
        target = ast.unparse(node.func.value)
        return f"{target} -> {', '.join(ast.unparse(a) for a in node.args)}"
    if name == "select":
        return f"{len(node.args)} columns: " + ", ".join(
            str(a.value) for a in node.args if isinstance(a, ast.Constant)
        )
    if name == "StructType":
        return module.line(node.lineno)
    return module.line(_call_line(node))[:160]


def _option_pairs(node: ast.Call) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    if (
        _func_name(node) == "option"
        and len(node.args) == 2
        and isinstance(node.args[0], ast.Constant)
    ):
        pairs.append((str(node.args[0].value), ast.unparse(node.args[1])))
    for kw in node.keywords:
        if kw.arg:
            pairs.append((kw.arg, ast.unparse(kw.value)))
    return pairs


def scan_absent() -> tuple[list[dict[str, Any]], list[str]]:
    """Text search for controls a lakehouse often has; hits are cited."""
    files = _py_files("src", "dags", "config") + [REPO_ROOT / "docker-compose.yml"]
    files = [f for f in files if f.exists()]
    results = []
    for label, pattern in ABSENT_PATTERNS.items():
        regex = re.compile(pattern)
        hits = []
        for file in files:
            if (
                label.startswith("Great Expectations")
                and file.resolve() == QUALITY_FILE.resolve()
            ):
                continue
            for i, text in enumerate(file.read_text(encoding="utf-8").splitlines(), 1):
                if regex.search(text):
                    hits.append(f"{rel(file)}:{i}")
        results.append({"control": label, "pattern": pattern, "hits": hits})
    return results, [rel(f) for f in files]


# ----------------------------------------------------- agent's other gates


def agent_gates() -> list[dict[str, Any]]:
    """The NL agent's query-side checks: validator violations, guards, raises."""
    out: list[dict[str, Any]] = []
    targets = [
        REPO_ROOT / "src" / "governance_agent" / "validator.py",
        REPO_ROOT / "src" / "governance_agent" / "compiler.py",
        REPO_ROOT / "src" / "governance_agent" / "executors" / "duckdb_executor.py",
    ]
    for file in targets:
        if not file.exists():
            continue
        module = load(file)
        for node, scope in _walk_with_scope(module.tree):
            message: str | None = None
            if isinstance(node, ast.Call):
                name = _func_name(node)
                if name == "append" and ast.unparse(node.func).startswith("violations"):
                    message = _message(node.args[0]) if node.args else None
                elif (
                    name == "_blocked"
                    and node.args
                    and isinstance(node.args[0], ast.List)
                ):
                    for elt in node.args[0].elts:
                        if not isinstance(elt, ast.Starred):
                            out.append(
                                {
                                    "check": _message(elt),
                                    "function": f"{module.dotted}.{scope}",
                                    "at": cite(module, elt.lineno),
                                }
                            )
                    continue
            elif isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
                exc = _func_name(node.exc) or "?"
                args = node.exc.args
                text = _message(args[0]) if args else ""
                message = f"raise {exc}" + (f": {text}" if text else "")
            if message:
                out.append(
                    {
                        "check": message,
                        "function": f"{module.dotted}.{scope}",
                        "at": cite(module, node.lineno),
                    }
                )
    # where the validator is enforced
    for file in _py_files("src/governance_agent"):
        module = load(file)
        for node, scope in _walk_with_scope(module.tree):
            if isinstance(node, ast.Call) and _func_name(node) == "validate_query":
                out.append(
                    {
                        "check": "validate_query(...) is enforced here",
                        "function": f"{module.dotted}.{scope}",
                        "at": cite(module, node.lineno),
                    }
                )
    return out


def _message(node: ast.AST) -> str:
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            else:
                parts.append("{" + ast.unparse(getattr(value, "value", value)) + "}")
        return "".join(parts)
    return ast.unparse(node)


def access_policy() -> dict[str, Any]:
    path = REPO_ROOT / "policies" / "access.yml"
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    roles = []
    for name, role in (data.get("roles") or {}).items():
        role = role or {}
        roles.append(
            {
                "role": name,
                "tables": len(role.get("allowed_tables") or []),
                "pii_columns": sum(
                    len(v or []) for v in (role.get("pii_columns") or {}).values()
                ),
                "max_rows": role.get("max_rows"),
            }
        )
    return {"file": rel(path), "roles": roles}


# ----------------------------------------------------------------- tests


GATE_MODULES = ("src.pipelines.quality", "src.utils.callbacks")


def gate_tests(table_names: list[str]) -> list[dict[str, Any]]:
    """Test functions that import the gate or its callback, and what they assert.

    For calls ``_validate_table(ctx, "<table>", df)`` the asserted
    ``"<column>: <expectation_type>"`` strings are mapped back to expectations;
    ``== []`` comparisons mean "this table's suite must pass".
    """
    out: list[dict[str, Any]] = []
    for file in _py_files("tests"):
        module = load(file)
        imported = {v.rsplit(".", 1)[0] for v in module.imports.values()}
        targets = sorted(m for m in imported if m in GATE_MODULES)
        if not targets:
            continue
        for node in module.tree.body:
            if not (
                isinstance(node, ast.FunctionDef) and node.name.startswith("test_")
            ):
                continue
            tables: list[str] = []
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and _func_name(call) == "_validate_table":
                    for arg in call.args:
                        if isinstance(arg, ast.Constant) and arg.value in table_names:
                            tables.append(str(arg.value))
            asserts_fail: list[str] = []
            asserts_pass: list[str] = []
            for stmt in ast.walk(node):
                if not isinstance(stmt, ast.Assert):
                    continue
                for const in ast.walk(stmt.test):
                    if isinstance(const, ast.Constant) and isinstance(const.value, str):
                        match = re.search(r"(\w+): (expect_\w+)", const.value)
                        if match:
                            asserts_fail.append(f"{match.group(1)}: {match.group(2)}")
                test = stmt.test
                if (
                    isinstance(test, ast.Compare)
                    and isinstance(test.left, ast.Call)
                    and _func_name(test.left) == "_validate_table"
                    and isinstance(test.comparators[0], ast.List)
                    and not test.comparators[0].elts
                ):
                    for arg in test.left.args:
                        if isinstance(arg, ast.Constant) and arg.value in table_names:
                            asserts_pass.append(str(arg.value))
            out.append(
                {
                    "test": f"{module.dotted}.{node.name}",
                    "at": cite(module, node.lineno),
                    "targets": targets,
                    "tables": sorted(set(tables)),
                    "asserts_fail": asserts_fail,
                    "asserts_pass": sorted(set(asserts_pass)),
                }
            )
    return out


def other_control_tests() -> list[dict[str, Any]]:
    """Tests of the Silver/Bronze controls and the agent's validator/policy."""
    watched = {
        "src.pipelines.silver": "Silver cleansing",
        "src.pipelines.bronze": "Bronze ingestion",
        "src.pipelines.gold": "Gold build",
        "src.governance_agent.validator": "agent SQL validator",
        "src.governance_agent.policy": "agent access policy",
    }
    out = []
    for file in _py_files("tests"):
        module = load(file)
        imported = {v.rsplit(".", 1)[0] for v in module.imports.values()}
        hit = sorted(watched[m] for m in imported if m in watched)
        if not hit:
            continue
        tests = [
            n
            for n in module.tree.body
            if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")
        ]
        if not tests:
            continue
        out.append(
            {
                "file": rel(file),
                "covers": hit,
                "tests": len(tests),
                "first_at": cite(module, tests[0].lineno) if tests else rel(file),
            }
        )
    return out
