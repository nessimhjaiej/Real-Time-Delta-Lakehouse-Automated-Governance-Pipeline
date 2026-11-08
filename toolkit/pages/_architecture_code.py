"""Static analysis for the architecture page: which code runs, and what it connects to.

Everything here is parsed from the repo with ``ast``:

* a module index of every git-tracked ``.py`` file (minus pytest files and the
  toolkit itself),
* "units" -- top-level functions, class methods, module top-level code and
  ``if __name__ == "__main__"`` blocks -- with the repo callables, module
  constants and third-party names each one references,
* "facts" found in each unit: ``os.getenv``/``os.environ`` reads (key + default)
  and string literals that look like endpoints (``scheme://host:port`` or
  ``host:port``), each with the call/keyword it flows into ("context"),
* Kafka topics (string passed as ``topic=`` or as the first argument of ``.send``),
* reachability from an entry unit (BFS over references; referencing a class
  pulls in all its methods; nested functions belong to their enclosing unit).

Values of secret-looking env keys are never read or stored.
"""

from __future__ import annotations

import ast
import re
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from toolkit.common import REPO_ROOT, rel

SECRET_KEY_RE = re.compile(
    r"(PASSWORD|PASSWD|SECRET|TOKEN|API_KEY|ACCESS_KEY|PRIVATE_KEY|CREDENTIAL)",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"([a-z][a-z0-9+.-]*)://([^/\s'\",;]+)", re.IGNORECASE)
_HOSTPORT_RE = re.compile(r"^([A-Za-z0-9_.-]*[A-Za-z][A-Za-z0-9_.-]*):(\d{2,5})$")
_ENV_FUNCS = {"os.getenv", "os.environ.get"}
_ROUTE_METHODS = {"get", "post", "put", "patch", "delete", "api_route", "websocket"}


def is_secret_key(key: str) -> bool:
    return bool(SECRET_KEY_RE.search(key))


def parse_endpoints(value: str) -> list[dict[str, Any]]:
    """Endpoints in a string: URLs and comma-separated host:port lists.

    Returns [{"scheme", "host", "port"}]; userinfo (user:password@) is dropped.
    """
    out: list[dict[str, Any]] = []
    for match in _URL_RE.finditer(value):
        scheme = match.group(1).lower()
        try:
            parts = urlsplit(f"{scheme}://{match.group(2)}")
            host, port = parts.hostname, parts.port
        except ValueError:
            continue
        if host:
            out.append({"scheme": scheme, "host": host, "port": port})
    if not out:
        for item in value.split(","):
            hp = _HOSTPORT_RE.match(item.strip())
            if hp:
                out.append(
                    {"scheme": None, "host": hp.group(1), "port": int(hp.group(2))}
                )
    return out


# ------------------------------------------------------------------ indexing


@dataclass
class Module:
    name: str
    path: Path
    tree: ast.Module
    is_package: bool
    imports: dict[str, str] = field(default_factory=dict)
    defs: dict[str, ast.AST] = field(default_factory=dict)  # functions + classes
    consts: dict[str, ast.AST] = field(default_factory=dict)  # module-level assigns
    parents: dict[int, ast.AST] = field(default_factory=dict)


@dataclass
class Unit:
    id: str
    module: str
    kind: str  # function | method | module | main
    nodes: list[ast.AST]
    line: int
    cls: str | None = None
    refs: set[str] = field(default_factory=set)
    const_refs: set[tuple[str, str]] = field(default_factory=set)
    third_party: set[str] = field(default_factory=set)
    constructed: set[str] = field(default_factory=set)
    facts: list[dict[str, Any]] = field(default_factory=list)
    topics: list[dict[str, Any]] = field(default_factory=list)


def tracked_python_files() -> list[Path]:
    """Git-tracked .py files that make up the running code base.

    Excluded (and said so on the page): dot-directories (.claude, .github),
    toolkit/ (this generator), and pytest modules (test_*.py per pyproject,
    conftest.py) which are not processes.
    """
    try:
        listed = subprocess.run(
            ["git", "ls-files", "*.py"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        listed = [rel(p) for p in REPO_ROOT.rglob("*.py") if "venv" not in p.parts]
    files = []
    for item in listed:
        parts = Path(item).parts
        name = parts[-1]
        if parts[0].startswith(".") or parts[0] in ("toolkit", "venv"):
            continue
        if name.startswith("test_") or name == "conftest.py":
            continue
        files.append(REPO_ROOT / item)
    return sorted(files)


def _module_name(path: Path) -> tuple[str, bool]:
    parts = list(path.relative_to(REPO_ROOT).with_suffix("").parts)
    is_pkg = parts[-1] == "__init__"
    if is_pkg:
        parts = parts[:-1]
    return ".".join(parts), is_pkg


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else None
    return None


def _is_main_guard(node: ast.AST) -> bool:
    if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
        return False
    test = node.test
    sides = [test.left, *test.comparators]
    names = {s.id for s in sides if isinstance(s, ast.Name)}
    consts = {s.value for s in sides if isinstance(s, ast.Constant)}
    return "__name__" in names and "__main__" in consts


def _import_map(mod_name: str, is_pkg: bool, nodes: list[ast.AST]) -> dict[str, str]:
    out: dict[str, str] = {}
    package = mod_name if is_pkg else mod_name.rpartition(".")[0]
    for top in nodes:
        for node in ast.walk(top):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.asname:
                        out[alias.asname] = alias.name
                    else:
                        head = alias.name.split(".")[0]
                        out[head] = head
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    base_parts = package.split(".") if package else []
                    if node.level > 1:
                        base_parts = base_parts[: len(base_parts) - (node.level - 1)]
                    base = ".".join(base_parts)
                    src = f"{base}.{node.module}" if node.module else base
                else:
                    src = node.module or ""
                for alias in node.names:
                    out[alias.asname or alias.name] = f"{src}.{alias.name}"
    return out


class CodeIndex:
    def __init__(self, files: list[Path] | None = None) -> None:
        self.modules: dict[str, Module] = {}
        self.units: dict[str, Unit] = {}
        self.class_methods: dict[str, list[str]] = {}
        self.class_bases: dict[str, list[str]] = {}
        self.errors: list[str] = []
        for path in files if files is not None else tracked_python_files():
            self._add_module(path)
        for mod in self.modules.values():
            self._build_units(mod)
        for unit in self.units.values():
            self._analyse(unit)

    # ---------------------------------------------------------------- build
    def _add_module(self, path: Path) -> None:
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (SyntaxError, UnicodeDecodeError) as error:
            self.errors.append(f"{rel(path)}: {error}")
            return
        name, is_pkg = _module_name(path)
        mod = Module(name=name, path=path, tree=tree, is_package=is_pkg)
        mod.imports = _import_map(name, is_pkg, list(tree.body))
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                mod.parents[id(child)] = node
        for stmt in tree.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                mod.defs[stmt.name] = stmt
            elif isinstance(stmt, (ast.Assign, ast.AnnAssign)) and stmt.value:
                targets = (
                    stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
                )
                for target in targets:
                    if isinstance(target, ast.Name):
                        mod.consts[target.id] = stmt.value
        self.modules[name] = mod

    def _build_units(self, mod: Module) -> None:
        top: list[ast.AST] = []
        for stmt in mod.tree.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                uid = f"{mod.name}.{stmt.name}"
                self.units[uid] = Unit(uid, mod.name, "function", [stmt], stmt.lineno)
            elif isinstance(stmt, ast.ClassDef):
                cid = f"{mod.name}.{stmt.name}"
                self.class_methods[cid] = []
                self.class_bases[cid] = [b for b in map(_dotted, stmt.bases) if b]
                for item in stmt.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        uid = f"{cid}.{item.name}"
                        self.units[uid] = Unit(
                            uid, mod.name, "method", [item], item.lineno, cls=cid
                        )
                        self.class_methods[cid].append(uid)
            elif _is_main_guard(stmt):
                uid = f"{mod.name}.__main__"
                self.units[uid] = Unit(uid, mod.name, "main", [stmt], stmt.lineno)
            elif not isinstance(
                stmt, (ast.Assign, ast.AnnAssign, ast.Import, ast.ImportFrom)
            ):
                if not (
                    isinstance(stmt, ast.Expr)
                    and isinstance(stmt.value, ast.Constant)
                    and isinstance(stmt.value.value, str)
                ):
                    top.append(stmt)
            elif isinstance(stmt, (ast.Assign, ast.AnnAssign)) and isinstance(
                stmt.value, ast.Call
            ):
                # Module-level calls run on import (e.g. ``spark = get_spark_session()``).
                top.append(stmt)
        if top:
            uid = f"{mod.name}.<module>"
            line = getattr(top[0], "lineno", 1)
            self.units[uid] = Unit(uid, mod.name, "module", top, line)

    # ------------------------------------------------------------ resolution
    def resolve(self, dotted: str, depth: int = 0) -> tuple[str, str] | None:
        """Resolve a fully-qualified dotted name.

        Returns ("unit"|"class"|"const"|"module"|"third", target) or None.
        """
        if depth > 8:
            return None
        parts = dotted.split(".")
        for cut in range(len(parts), 0, -1):
            mod_name = ".".join(parts[:cut])
            mod = self.modules.get(mod_name)
            if mod is None:
                continue
            rest = parts[cut:]
            if not rest:
                return ("module", mod_name)
            head = rest[0]
            if head in mod.defs:
                target = f"{mod_name}.{head}"
                node = mod.defs[head]
                if isinstance(node, ast.ClassDef):
                    if len(rest) > 1:
                        method = f"{target}.{rest[1]}"
                        if method in self.units:
                            return ("unit", method)
                    return ("class", target)
                return ("unit", target)
            if head in mod.imports:
                return self.resolve(".".join([mod.imports[head], *rest[1:]]), depth + 1)
            if head in mod.consts:
                return ("const", f"{mod_name}.{head}")
            submodule = f"{mod_name}.{head}"
            if submodule in self.modules:
                continue
            return None
        top = parts[0]
        if top in self.modules or any(m.startswith(top + ".") for m in self.modules):
            return None
        return ("third", dotted)

    def _resolve_in(
        self, unit: Unit, imports: dict[str, str], expr: ast.AST
    ) -> tuple[str, str] | None:
        dotted = _dotted(expr)
        if not dotted:
            return None
        mod = self.modules[unit.module]
        head, _, tail = dotted.partition(".")
        if head in ("self", "cls") and unit.cls:
            if tail:
                method = f"{unit.cls}.{tail.split('.')[0]}"
                if method in self.units:
                    return ("unit", method)
            return None
        if head in imports:
            full = imports[head] + (f".{tail}" if tail else "")
        elif head in mod.defs or head in mod.consts:
            full = f"{mod.name}.{dotted}"
        else:
            return None
        return self.resolve(full)

    # -------------------------------------------------------------- analysis
    def _walk(self, unit: Unit) -> Iterator[ast.AST]:
        for top in unit.nodes:
            yield from ast.walk(top)

    def _docstring_ids(self, unit: Unit) -> set[int]:
        out = set()
        for node in self._walk(unit):
            body = getattr(node, "body", None)
            if isinstance(body, list) and body:
                first = body[0]
                if (
                    isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)
                ):
                    out.add(id(first.value))
        return out

    def _analyse(self, unit: Unit) -> None:
        mod = self.modules[unit.module]
        imports = dict(mod.imports)
        imports.update(_import_map(mod.name, mod.is_package, unit.nodes))
        skip = self._docstring_ids(unit)
        env_defaults: set[int] = set()
        local_names = self._local_names(unit)
        for node in self._walk(unit):
            # references
            if isinstance(node, (ast.Name, ast.Attribute)) and isinstance(
                getattr(node, "ctx", None), ast.Load
            ):
                parent = mod.parents.get(id(node))
                if isinstance(parent, ast.Attribute):
                    continue  # handled at the outermost attribute
                if isinstance(node, ast.Name) and node.id in local_names:
                    continue
                resolved = self._resolve_in(unit, imports, node)
                if resolved:
                    kind, target = resolved
                    if kind == "unit":
                        unit.refs.add(target)
                    elif kind == "class":
                        unit.refs.add(target)
                    elif kind == "const":
                        m, _, n = target.rpartition(".")
                        unit.const_refs.add((m, n))
                        self._const_site(unit, m, n, node)
                    elif kind == "third":
                        unit.third_party.add(target)
                        call = mod.parents.get(id(node))
                        last = target.rsplit(".", 1)[-1]
                        if (
                            isinstance(call, ast.Call)
                            and call.func is node
                            and last[:1].isupper()
                        ):
                            unit.constructed.add(target)
            # env reads
            if isinstance(node, ast.Call):
                fname = _dotted(node.func)
                full = self._full_name(imports, fname) if fname else None
                if full in _ENV_FUNCS and node.args:
                    key = node.args[0]
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        default = None
                        if len(node.args) > 1 and isinstance(
                            node.args[1], ast.Constant
                        ):
                            default = node.args[1].value
                            env_defaults.add(id(node.args[1]))
                        self._add_env_fact(unit, key.value, default, node)
                # Kafka topics
                topic = self._topic(unit, node)
                if topic:
                    unit.topics.append(
                        {"topic": topic, "file": rel(mod.path), "line": node.lineno}
                    )
            if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
                base = _dotted(node.value)
                if base and self._full_name(imports, base) == "os.environ":
                    sl = node.slice
                    if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
                        self._add_env_fact(unit, sl.value, None, node)
            # endpoint-looking literals
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in skip
                and id(node) not in env_defaults
            ):
                eps = parse_endpoints(node.value) if len(node.value) < 300 else []
                if eps:
                    unit.facts.append(
                        {
                            "kind": "literal",
                            "key": None,
                            "value": node.value,
                            "endpoints": eps,
                            "context": self._context(unit, node),
                            "file": rel(mod.path),
                            "line": node.lineno,
                        }
                    )

    def _local_names(self, unit: Unit) -> set[str]:
        """Names bound inside a function unit (params, assignments), which shadow
        module-level names."""
        names: set[str] = set()
        if unit.kind not in ("function", "method"):
            return names
        for node in self._walk(unit):
            if isinstance(node, ast.arg):
                names.add(node.arg)
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                names.add(node.id)
        return names

    @staticmethod
    def _full_name(imports: dict[str, str], dotted: str) -> str:
        head, _, tail = dotted.partition(".")
        if head in imports:
            return imports[head] + (f".{tail}" if tail else "")
        return dotted

    def _add_env_fact(self, unit: Unit, key: str, default: Any, node: ast.AST) -> None:
        mod = self.modules[unit.module]
        secret = is_secret_key(key)
        default_text = None if (secret or default is None) else str(default)
        unit.facts.append(
            {
                "kind": "env",
                "key": key,
                "secret": secret,
                "default": default_text,
                "endpoints": parse_endpoints(default_text) if default_text else [],
                "context": self._context(unit, node),
                "file": rel(mod.path),
                "line": getattr(node, "lineno", 0),
            }
        )

    def _str_value(self, unit: Unit, node: ast.AST) -> str | None:
        """A string constant, or a module constant (possibly imported) holding one."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            mod = self.modules[unit.module]
            value = mod.consts.get(node.id)
            if value is None and node.id in mod.imports:
                resolved = self.resolve(mod.imports[node.id])
                if resolved and resolved[0] == "const":
                    m, _, n = resolved[1].rpartition(".")
                    value = self.modules[m].consts.get(n)
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value
        return None

    def _topic(self, unit: Unit, call: ast.Call) -> str | None:
        for kw in call.keywords:
            if kw.arg == "topic":
                return self._str_value(unit, kw.value)
        if (
            isinstance(call.func, ast.Attribute)
            and call.func.attr == "send"
            and call.args
        ):
            return self._str_value(unit, call.args[0])
        return None

    # --------------------------------------------------------------- context
    def _context(self, unit: Unit, node: ast.AST, depth: int = 0) -> str | None:
        """Where a value flows: a config key, a keyword, or 'callee[arg i]'."""
        mod = self.modules[unit.module]
        cur = node
        for _ in range(12):
            parent = mod.parents.get(id(cur))
            if parent is None:
                return None
            if isinstance(
                parent,
                (ast.IfExp, ast.BoolOp, ast.BinOp, ast.List, ast.Tuple),
            ):
                cur = parent
                continue
            if isinstance(parent, (ast.JoinedStr, ast.FormattedValue)):
                return None  # formatted into a message, not passed to a client
            if isinstance(parent, ast.Attribute):
                call = mod.parents.get(id(parent))
                if isinstance(call, ast.Call) and call.func is parent:
                    cur = call
                    continue
                return None
            if isinstance(parent, ast.keyword):
                return parent.arg
            if isinstance(parent, ast.Call) and cur in parent.args:
                index = parent.args.index(cur)
                first = parent.args[0]
                if (
                    index > 0
                    and isinstance(first, ast.Constant)
                    and isinstance(first.value, str)
                    and re.fullmatch(r"[\w.-]+", first.value)
                ):
                    return first.value
                if isinstance(parent.func, ast.Attribute):
                    callee = parent.func.attr
                else:
                    callee = _dotted(parent.func) or "call"
                return f"{callee.rsplit('.', 1)[-1]}[{index}]"
            if isinstance(parent, ast.arguments):
                return self._param_for_default(parent, cur)
            if isinstance(parent, ast.Dict):
                for k, v in zip(parent.keys, parent.values):
                    if v is cur and isinstance(k, ast.Constant):
                        return str(k.value)
                return None
            if isinstance(parent, (ast.Assign, ast.AnnAssign)):
                targets = (
                    parent.targets
                    if isinstance(parent, ast.Assign)
                    else [parent.target]
                )
                if (
                    depth < 3
                    and len(targets) == 1
                    and isinstance(targets[0], ast.Name)
                    and unit.kind in ("function", "method")
                ):
                    return self._var_context(unit, targets[0].id, parent, depth)
                return None
            return None
        return None

    def _var_context(
        self, unit: Unit, var: str, assign: ast.AST, depth: int
    ) -> str | None:
        seen: list[str] = []
        for node in self._walk(unit):
            if (
                isinstance(node, ast.Name)
                and node.id == var
                and isinstance(node.ctx, ast.Load)
                and getattr(node, "lineno", 0) >= getattr(assign, "lineno", 0)
            ):
                ctx = self._context(unit, node, depth + 1)
                if ctx and ctx not in seen:
                    seen.append(ctx)
        return ", ".join(seen) if seen else None

    @staticmethod
    def _param_for_default(args: ast.arguments, default: ast.AST) -> str | None:
        positional = [*args.posonlyargs, *args.args]
        if default in args.defaults:
            offset = len(positional) - len(args.defaults)
            return positional[offset + args.defaults.index(default)].arg
        for arg, kw_default in zip(args.kwonlyargs, args.kw_defaults):
            if kw_default is default:
                return arg.arg
        return None

    def _const_site(self, unit: Unit, mod_name: str, name: str, site: ast.AST) -> None:
        """Facts carried by a module constant, attributed to the referencing unit
        with the context of the reference site."""
        owner = self.modules.get(mod_name)
        if owner is None:
            return
        value = owner.consts.get(name)
        if value is None:
            return
        ctx = self._context(unit, site)
        owner_imports = owner.imports
        defaults: set[int] = set()
        for node in ast.walk(value):
            if isinstance(node, ast.Call) and len(node.args) > 1:
                defaults.add(id(node.args[1]))
        for node in ast.walk(value):
            if isinstance(node, ast.Call):
                fname = _dotted(node.func)
                full = self._full_name(owner_imports, fname) if fname else None
                if full in _ENV_FUNCS and node.args:
                    key = node.args[0]
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        default = None
                        if len(node.args) > 1 and isinstance(
                            node.args[1], ast.Constant
                        ):
                            default = node.args[1].value
                        secret = is_secret_key(key.value)
                        text = None if (secret or default is None) else str(default)
                        unit.facts.append(
                            {
                                "kind": "env",
                                "key": key.value,
                                "secret": secret,
                                "default": text,
                                "endpoints": parse_endpoints(text) if text else [],
                                "context": ctx,
                                "file": rel(owner.path),
                                "line": node.lineno,
                                "via_const": f"{mod_name}.{name}",
                            }
                        )
            elif (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in defaults
            ):
                eps = parse_endpoints(node.value) if len(node.value) < 300 else []
                if eps:
                    unit.facts.append(
                        {
                            "kind": "literal",
                            "key": None,
                            "value": node.value,
                            "endpoints": eps,
                            "context": ctx,
                            "file": rel(owner.path),
                            "line": node.lineno,
                            "via_const": f"{mod_name}.{name}",
                        }
                    )

    # ---------------------------------------------------------- reachability
    def reachable(self, entries: list[str]) -> list[str]:
        """Units reachable from the entry units (plus the import-time code of
        every module they live in)."""
        seen: list[str] = []
        queue = list(entries)
        visited: set[str] = set()
        while queue:
            uid = queue.pop(0)
            if uid in visited:
                continue
            visited.add(uid)
            if uid in self.class_methods:
                queue.extend(self.class_methods[uid])
                for base in self.class_bases.get(uid, []):
                    owner = uid.rpartition(".")[0]
                    resolved = self.resolve(f"{owner}.{base}")
                    if resolved and resolved[0] == "class":
                        queue.append(resolved[1])
                continue
            unit = self.units.get(uid)
            if unit is None:
                continue
            seen.append(uid)
            queue.extend(sorted(unit.refs))
            top = f"{unit.module}.<module>"
            if top in self.units and top not in visited:
                queue.append(top)
        return seen

    def main_entry(self, module: str) -> str | None:
        uid = f"{module}.__main__"
        if uid in self.units:
            return uid
        top = f"{module}.<module>"
        return top if top in self.units else None

    def unit_file_line(self, uid: str) -> str:
        unit = self.units[uid]
        return f"{rel(self.modules[unit.module].path)}:{unit.line}"


def strings_in(
    index: CodeIndex, uids: list[str], pattern: re.Pattern[str]
) -> list[dict[str, Any]]:
    """String constants matching ``pattern`` in the given units (and in module
    constants they reference), with file:line."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int]] = set()

    def visit(node: ast.AST, path: Path) -> None:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                for match in pattern.finditer(sub.value):
                    key = (match.group(0), rel(path), sub.lineno)
                    if key not in seen:
                        seen.add(key)
                        out.append(
                            {
                                "match": match.group(0),
                                "file": rel(path),
                                "line": sub.lineno,
                            }
                        )

    for uid in uids:
        unit = index.units[uid]
        mod = index.modules[unit.module]
        for top in unit.nodes:
            visit(top, mod.path)
        for m, n in unit.const_refs:
            owner = index.modules.get(m)
            if owner and n in owner.consts:
                visit(owner.consts[n], owner.path)
    return out


def asgi_apps(index: CodeIndex) -> list[dict[str, Any]]:
    """Module-level ``app = <ThirdParty>(...)`` objects that have route decorators."""
    apps = []
    for mod in index.modules.values():
        for name, value in mod.consts.items():
            if not isinstance(value, ast.Call):
                continue
            fname = _dotted(value.func)
            if not fname:
                continue
            full = CodeIndex._full_name(mod.imports, fname)
            resolved = index.resolve(full)
            if not resolved or resolved[0] != "third":
                continue
            routes = []
            handlers = []
            for stmt in mod.tree.body:
                if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for deco in stmt.decorator_list:
                    if (
                        isinstance(deco, ast.Call)
                        and isinstance(deco.func, ast.Attribute)
                        and isinstance(deco.func.value, ast.Name)
                        and deco.func.value.id == name
                        and deco.func.attr in _ROUTE_METHODS
                    ):
                        path = (
                            deco.args[0].value
                            if deco.args and isinstance(deco.args[0], ast.Constant)
                            else "?"
                        )
                        routes.append(
                            {
                                "method": deco.func.attr.upper(),
                                "path": str(path),
                                "handler": f"{mod.name}.{stmt.name}",
                                "line": stmt.lineno,
                            }
                        )
                        handlers.append(f"{mod.name}.{stmt.name}")
            if routes:
                apps.append(
                    {
                        "id": f"{mod.name}.{name}",
                        "module": mod.name,
                        "attr": name,
                        "class": resolved[1],
                        "file": rel(mod.path),
                        "line": value.lineno,
                        "routes": routes,
                        "handlers": handlers,
                    }
                )
    return apps


def main_calls(index: CodeIndex, uid: str) -> list[str]:
    """Repo callables referenced directly by an entry unit."""
    unit = index.units.get(uid)
    return sorted(r for r in unit.refs if r in index.units) if unit else []
