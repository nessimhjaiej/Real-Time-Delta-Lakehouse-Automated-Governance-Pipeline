"""Static table-level lineage extraction for the lineage page (ast, no imports).

Every read/write site is found by walking the AST of the repo's tracked Python
files and resolving the path/topic/table argument through a small constant
evaluator: module constants, dict constants (``SILVER_PATHS["x"]``), loop
variables over constant lists/dicts, f-strings, ``Path(__file__).parents[n] /
"..."`` chains, function-parameter defaults, ``os.getenv(name, default)`` and
cross-module ``from x import NAME``. Factory call sites
(``ExtractorFactory.create("csv", path)``) are resolved by binding the call's
arguments into the factory's ``create`` body, following the ``if kind == ...``
branch to the strategy class, and resolving the strategy's own generic IO site
(``spark.read...csv(self.path)``) through its ``__init__``. Anything that
cannot be resolved is reported, never guessed.
"""

from __future__ import annotations

import ast
import itertools
import re
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from toolkit.common import LAKE_BUCKET, REPO_ROOT

# Directories not scanned: tests build in-memory DuckDB fixtures, toolkit is this site.
SCAN_EXCLUDE = ("tests/", "toolkit/")

SPARK_READ_TERMINALS = {"load", "csv", "json", "parquet", "table", "text", "orc"}
SPARK_WRITE_TERMINALS = {
    "save",
    "start",
    "saveAsTable",
    "toTable",
    "insertInto",
    "parquet",
    "csv",
    "json",
}
SQL_EXEC_METHODS = {"execute", "executemany", "sql"}
HTTP_CALLS = {
    "urllib.request.Request",
    "urllib.request.urlopen",
    "requests.get",
    "requests.post",
    "httpx.get",
}

Val = tuple[Any, str]
"""(value, origin "file:line" of the literal that produced it)."""


@dataclass(frozen=True)
class PathV:
    path: Path


@dataclass(frozen=True)
class GlobV:
    base: Path
    pattern: str


@dataclass(frozen=True)
class ParentsV:
    path: Path


@dataclass(frozen=True)
class KwargsV:
    """A ``**kwargs`` dict bound from a call site: key -> candidate values."""

    items: tuple[tuple[str, tuple[Val, ...]], ...]

    def get(self, key: str) -> tuple[Val, ...] | None:
        for k, v in self.items:
            if k == key:
                return v
        return None


@dataclass
class Func:
    qualname: str  # module-relative, e.g. "DuckDBExecutor.__init__", "__main__"
    module: Module
    body: list[ast.stmt]
    lineno: int
    node: ast.AST | None = None
    class_name: str | None = None
    params: dict[str, ast.expr | None] = field(default_factory=dict)
    param_order: list[str] = field(default_factory=list)
    kwargs_param: str | None = None
    annotations: dict[str, ast.expr] = field(default_factory=dict)
    returns: ast.expr | None = None
    locals: dict[str, list[ast.expr]] = field(default_factory=dict)
    loops: dict[str, list[tuple[ast.expr, str]]] = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.module.name}.{self.qualname}"


@dataclass
class Module:
    name: str
    path: Path
    rel: str
    tree: ast.Module
    assigns: dict[str, list[ast.expr]] = field(default_factory=dict)
    imports: dict[str, tuple[str, str | None]] = field(default_factory=dict)
    funcs: dict[str, Func] = field(default_factory=dict)
    classes: dict[str, ast.ClassDef] = field(default_factory=dict)


@dataclass
class Site:
    """One resolved (or unresolved) read/write of a dataset."""

    job: str  # function id
    op: str  # read | write
    kind: str  # delta | kafka | file | http | duckdb_view | sqlite | qdrant
    target: str  # dataset id ("" if unresolved)
    mode: str  # overwrite / append / stream-append / batch / create / ...
    evidence: str  # file:line of the call
    via: str = ""  # how it was resolved
    uri: str = ""
    extra: dict[str, str] = field(default_factory=dict)


@dataclass
class Unresolved:
    job: str
    evidence: str
    pattern: str
    expression: str
    reason: str


# ----------------------------------------------------------------- utilities


def tracked_python_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return [REPO_ROOT / p for p in out if not p.startswith(SCAN_EXCLUDE)]


def module_name(rel_path: str) -> str:
    parts = rel_path[:-3].split("/")
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def dotted(expr: ast.expr) -> str | None:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        base = dotted(expr.value)
        return f"{base}.{expr.attr}" if base else None
    return None


def lake_id(uri: str) -> str | None:
    """s3a://lakehouse/<layer>/<table>[/...] -> '<layer>.<table>'."""
    m = re.match(r"^s3a?://([^/]+)/([^/]+)/([^/]+)/?$", uri)
    if not m or m.group(1) != LAKE_BUCKET:
        return None
    return f"{m.group(2)}.{m.group(3)}"


# ------------------------------------------------------------------ the model


class Repo:
    def __init__(self, files: list[Path] | None = None) -> None:
        self.modules: dict[str, Module] = {}
        self.parse_errors: list[str] = []
        for path in files if files is not None else tracked_python_files():
            rel_path = path.relative_to(REPO_ROOT).as_posix()
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel_path)
            except (SyntaxError, UnicodeDecodeError) as error:
                self.parse_errors.append(f"{rel_path}: {error}")
                continue
            mod = Module(module_name(rel_path), path, rel_path, tree)
            self._index(mod)
            self.modules[mod.name] = mod
        self.funcs: dict[str, Func] = {
            f.id: f for m in self.modules.values() for f in m.funcs.values()
        }
        self._depth = 0
        # Loop variables bound by the for-loops enclosing the call being scanned
        # (so two loops reusing one name do not mix their iterables).
        self.scope_fn: Func | None = None
        self.scope_bind: dict[str, tuple[Val, ...]] = {}

    # ---- indexing

    def _index(self, mod: Module) -> None:
        loose: list[ast.stmt] = []
        for stmt in mod.tree.body:
            if isinstance(stmt, (ast.Import, ast.ImportFrom)):
                self._index_import(mod, stmt)
            elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                mod.funcs[stmt.name] = self._func(mod, stmt, stmt.name)
            elif isinstance(stmt, ast.ClassDef):
                mod.classes[stmt.name] = stmt
                for item in stmt.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        qn = f"{stmt.name}.{item.name}"
                        mod.funcs[qn] = self._func(mod, item, qn, stmt.name)
            elif _is_main_guard(stmt):
                assert isinstance(stmt, ast.If)
                main = Func("__main__", mod, stmt.body, stmt.lineno, stmt)
                _collect_locals(main, stmt.body)
                mod.funcs["__main__"] = main
            else:
                if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
                    for name, value in _assigned_names(stmt):
                        mod.assigns.setdefault(name, []).append(value)
                loose.append(stmt)
        # Module-level statements that do more than define constants (notebooks).
        active = [s for s in loose if _has_call(s)]
        if active:
            pseudo = Func("<module>", mod, loose, active[0].lineno, mod.tree)
            mod.funcs["<module>"] = pseudo

    def _index_import(self, mod: Module, stmt: ast.Import | ast.ImportFrom) -> None:
        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                mod.imports[alias.asname or alias.name.split(".")[0]] = (
                    alias.name if alias.asname else alias.name.split(".")[0],
                    None,
                )
            return
        base = stmt.module or ""
        if stmt.level:
            pkg = mod.name.split(".")
            if not mod.rel.endswith("__init__.py"):
                pkg = pkg[:-1]
            pkg = pkg[: len(pkg) - (stmt.level - 1)] if stmt.level > 1 else pkg
            base = ".".join([*pkg, base] if base else pkg)
        for alias in stmt.names:
            mod.imports[alias.asname or alias.name] = (base, alias.name)

    def _func(
        self,
        mod: Module,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualname: str,
        class_name: str | None = None,
    ) -> Func:
        fn = Func(
            qualname,
            mod,
            node.body,
            node.lineno,
            node,
            class_name,
            returns=node.returns,
        )
        args = node.args
        positional = [*args.posonlyargs, *args.args]
        defaults: list[ast.expr | None] = [None] * (
            len(positional) - len(args.defaults)
        ) + list(args.defaults)
        for arg, default in zip(positional, defaults):
            fn.params[arg.arg] = default
            fn.param_order.append(arg.arg)
            if arg.annotation is not None:
                fn.annotations[arg.arg] = arg.annotation
        for arg, kw_default in zip(args.kwonlyargs, args.kw_defaults):
            fn.params[arg.arg] = kw_default
            if arg.annotation is not None:
                fn.annotations[arg.arg] = arg.annotation
        if args.kwarg is not None:
            fn.kwargs_param = args.kwarg.arg
        _collect_locals(fn, node.body)
        return fn

    # ---- name resolution

    def resolve_import(self, mod: Module, name: str) -> tuple[Module, str] | None:
        """``from a.b import NAME`` -> (module a.b, "NAME") if a.b is in the repo."""
        if name not in mod.imports:
            return None
        target, attr = mod.imports[name]
        if attr is None:
            return None
        if target in self.modules:
            return self.modules[target], attr
        sub = f"{target}.{attr}"
        if sub in self.modules:
            return self.modules[sub], ""
        return None

    def find_class(self, mod: Module, name: str) -> tuple[Module, ast.ClassDef] | None:
        if name in mod.classes:
            return mod, mod.classes[name]
        hit = self.resolve_import(mod, name)
        if hit and hit[1]:
            return self.find_class(hit[0], hit[1])
        return None

    def find_func(self, mod: Module, name: str) -> Func | None:
        if name in mod.funcs:
            return mod.funcs[name]
        hit = self.resolve_import(mod, name)
        if hit and hit[1]:
            return self.find_func(hit[0], hit[1])
        return None

    def method(self, mod: Module, cls: ast.ClassDef, name: str) -> Func | None:
        fn = mod.funcs.get(f"{cls.name}.{name}")
        if fn is not None:
            return fn
        for base in cls.bases:
            if isinstance(base, ast.Name):
                found = self.find_class(mod, base.id)
                if found:
                    inherited = self.method(found[0], found[1], name)
                    if inherited is not None:
                        return inherited
        return None

    def external_origin(self, mod: Module, expr: ast.expr) -> str | None:
        """Fully dotted origin of a name imported from outside the repo."""
        name = dotted(expr)
        if not name:
            return None
        head, _, rest = name.partition(".")
        if head not in mod.imports:
            return name
        target, attr = mod.imports[head]
        full = f"{target}.{attr}" if attr else target
        return f"{full}.{rest}" if rest else full

    # ---- evaluation

    def ev(
        self,
        e: ast.expr | None,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]] | None = None,
    ) -> list[Val]:
        if e is None:
            return []
        self._depth += 1
        try:
            if self._depth > 40:
                return []
            return _dedupe(self._ev(e, mod, fn, bind or {}))
        finally:
            self._depth -= 1

    def _ev(
        self,
        e: ast.expr,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]],
    ) -> list[Val]:
        where = f"{mod.rel}:{getattr(e, 'lineno', 0)}"
        if isinstance(e, ast.Constant):
            if isinstance(e.value, (str, int)) and not isinstance(e.value, bool):
                return [(e.value, where)]
            return []
        if isinstance(e, ast.JoinedStr):
            return self._ev_fstring(e, mod, fn, bind, where)
        if isinstance(e, ast.Name):
            return self._ev_name(e.id, mod, fn, bind)
        if isinstance(e, ast.Attribute):
            if isinstance(e.value, ast.Name) and e.value.id == "self" and fn:
                return self._ev_self_attr(e.attr, mod, fn, bind)
            if isinstance(e.value, ast.Name) and e.value.id in mod.imports:
                target, attr = mod.imports[e.value.id]
                full = f"{target}.{attr}" if attr else target
                if full in self.modules:
                    return self._ev_name(e.attr, self.modules[full], None, {})
            out: list[Val] = []
            for value, origin in self.ev(e.value, mod, fn, bind):
                if isinstance(value, PathV):
                    if e.attr == "parent":
                        out.append((PathV(value.path.parent), origin))
                    elif e.attr == "parents":
                        out.append((ParentsV(value.path), origin))
                    elif e.attr in ("name", "stem"):
                        out.append((getattr(value.path, e.attr), origin))
            return out
        if isinstance(e, ast.Subscript):
            return self._ev_subscript(e, mod, fn, bind)
        if isinstance(e, ast.Call):
            return self._ev_call(e, mod, fn, bind)
        if isinstance(e, ast.BinOp):
            lefts = self.ev(e.left, mod, fn, bind)
            rights = self.ev(e.right, mod, fn, bind)
            out = []
            for (lv, lo), (rv, ro) in itertools.product(lefts, rights):
                if isinstance(e.op, ast.Div) and isinstance(lv, PathV):
                    if isinstance(rv, str):
                        out.append((PathV(lv.path / rv), ro))
                elif isinstance(e.op, ast.Add) and isinstance(lv, str):
                    if isinstance(rv, str):
                        out.append((lv + rv, lo))
            return out
        if isinstance(e, (ast.List, ast.Tuple, ast.Set)):
            return [v for elt in e.elts for v in self.ev(elt, mod, fn, bind)]
        if isinstance(e, ast.IfExp):
            return self.ev(e.body, mod, fn, bind) + self.ev(e.orelse, mod, fn, bind)
        if isinstance(e, ast.BoolOp):
            return [v for x in e.values for v in self.ev(x, mod, fn, bind)]
        return []

    def _ev_fstring(
        self,
        e: ast.JoinedStr,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]],
        where: str,
    ) -> list[Val]:
        # Bind each distinct variable once, so f"{t} ... {t}" stays consistent.
        names: list[str] = []
        for part in e.values:
            if isinstance(part, ast.FormattedValue) and isinstance(
                part.value, ast.Name
            ):
                if part.value.id not in names:
                    names.append(part.value.id)
        choices = {n: self.ev(ast.Name(id=n), mod, fn, bind) for n in names}
        others = {
            id(p): self.ev(p.value, mod, fn, bind)
            for p in e.values
            if isinstance(p, ast.FormattedValue) and not isinstance(p.value, ast.Name)
        }
        combos = itertools.islice(itertools.product(*[choices[n] for n in names]), 64)
        out: list[Val] = []
        for combo in combos:
            env = dict(zip(names, combo))
            pieces: list[list[str]] = []
            for part in e.values:
                if isinstance(part, ast.Constant):
                    pieces.append([str(part.value)])
                elif isinstance(part, ast.FormattedValue):
                    vals = (
                        [env[part.value.id]]
                        if isinstance(part.value, ast.Name)
                        else others[id(part)]
                    )
                    rendered = [_as_text(v) for v, _ in vals]
                    pieces.append([r for r in rendered if r is not None])
            for combo_str in itertools.islice(itertools.product(*pieces), 64):
                out.append(("".join(combo_str), where))
        return out

    def _ev_name(
        self,
        name: str,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]],
    ) -> list[Val]:
        if name in bind:
            return list(bind[name])
        if fn is not None and fn is self.scope_fn and name in self.scope_bind:
            return list(self.scope_bind[name])
        if fn is not None:
            if name in fn.loops:
                return [
                    v
                    for it, how in fn.loops[name]
                    for v in self._ev_iter(it, how, mod, fn, bind)
                ]
            if name in fn.locals:
                return [
                    v for expr in fn.locals[name] for v in self.ev(expr, mod, fn, bind)
                ]
            if name in fn.params:
                default = fn.params[name]
                return self.ev(default, mod, None) if default is not None else []
        if name == "__file__":
            return [(PathV(mod.path), f"{mod.rel}:1")]
        if name in mod.assigns:
            return [v for expr in mod.assigns[name] for v in self.ev(expr, mod, None)]
        hit = self.resolve_import(mod, name)
        if hit and hit[1]:
            return self._ev_name(hit[1], hit[0], None, {})
        return []

    def _ev_self_attr(
        self,
        attr: str,
        mod: Module,
        fn: Func,
        bind: dict[str, tuple[Val, ...]],
    ) -> list[Val]:
        if fn.class_name is None:
            return []
        init = mod.funcs.get(f"{fn.class_name}.__init__")
        if init is None:
            return []
        out: list[Val] = []
        for expr in _self_assigns(init, attr):
            out.extend(self.ev(expr, mod, init, bind))
        return out

    def _ev_iter(
        self,
        it: ast.expr,
        how: str,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]],
    ) -> list[Val]:
        # for k, v in D.items() / D.keys() / D.values()
        if isinstance(it, ast.Call) and isinstance(it.func, ast.Attribute):
            meth = it.func.attr
            if meth in ("items", "keys", "values"):
                pairs = self.dict_pairs(it.func.value, mod, fn)
                if pairs is None:
                    return []
                want = {"items": how, "keys": "key", "values": "value"}[meth]
                if meth == "items" and how not in ("key", "value"):
                    return []
                out: list[Val] = []
                for key_vals, value_expr, ctx_mod, ctx_fn in pairs:
                    if want == "key":
                        out.extend(key_vals)
                    else:
                        out.extend(self.ev(value_expr, ctx_mod, ctx_fn))
                return out
        if how != "elem":
            return []
        return self.ev(it, mod, fn, bind)

    def dict_pairs(
        self, e: ast.expr, mod: Module, fn: Func | None
    ) -> list[tuple[list[Val], ast.expr, Module, Func | None]] | None:
        """Resolve an expression to a dict literal's (keys, value expr) pairs."""
        node, ctx_mod, ctx_fn = self._dict_literal(e, mod, fn)
        if node is None:
            return None
        pairs = []
        for k, v in zip(node.keys, node.values):
            if k is None:
                continue
            pairs.append((self.ev(k, ctx_mod, ctx_fn), v, ctx_mod, ctx_fn))
        return pairs

    def _dict_literal(
        self, e: ast.expr, mod: Module, fn: Func | None
    ) -> tuple[ast.Dict | None, Module, Func | None]:
        if isinstance(e, ast.Dict):
            return e, mod, fn
        if isinstance(e, ast.Name):
            if fn is not None and e.id in fn.locals:
                for expr in fn.locals[e.id]:
                    if isinstance(expr, ast.Dict):
                        return expr, mod, fn
            for expr in mod.assigns.get(e.id, []):
                if isinstance(expr, ast.Dict):
                    return expr, mod, None
            hit = self.resolve_import(mod, e.id)
            if hit and hit[1]:
                return self._dict_literal(ast.Name(id=hit[1]), hit[0], None)
        return None, mod, fn

    def _ev_subscript(
        self,
        e: ast.Subscript,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]],
    ) -> list[Val]:
        base = self.ev(e.value, mod, fn, bind)
        keys = self.ev(e.slice, mod, fn, bind)
        out: list[Val] = []
        for value, origin in base:
            if isinstance(value, ParentsV):
                for k, _ in keys:
                    if isinstance(k, int) and k < len(value.path.parents):
                        out.append((PathV(value.path.parents[k]), origin))
        if out:
            return out
        pairs = self.dict_pairs(e.value, mod, fn)
        if pairs is None:
            return []
        wanted = {k for k, _ in keys}
        for key_vals, value_expr, ctx_mod, ctx_fn in pairs:
            if not wanted or wanted & {k for k, _ in key_vals}:
                out.extend(self.ev(value_expr, ctx_mod, ctx_fn))
        return out

    def _ev_call(
        self,
        e: ast.Call,
        mod: Module,
        fn: Func | None,
        bind: dict[str, tuple[Val, ...]],
    ) -> list[Val]:
        func = e.func
        first = e.args[0] if e.args else None
        if isinstance(func, ast.Name):
            if func.id in ("Path", "PurePath", "PurePosixPath"):
                return [
                    (PathV(Path(v)), o)
                    for v, o in self.ev(first, mod, fn, bind)
                    if isinstance(v, str)
                ] + [
                    (v, o)
                    for v, o in self.ev(first, mod, fn, bind)
                    if isinstance(v, PathV)
                ]
            if func.id in ("str", "sorted", "list", "tuple", "set", "Dataset"):
                return self.ev(first, mod, fn, bind)
            return []
        if not isinstance(func, ast.Attribute):
            return []
        origin = self.external_origin(mod, func)
        if origin in ("os.getenv", "os.environ.get") and len(e.args) >= 2:
            return self.ev(e.args[1], mod, fn, bind)
        meth = func.attr
        if meth in ("pop", "get") and isinstance(func.value, ast.Name) and e.args:
            for kw_val, _ in self.ev(func.value, mod, fn, bind):
                if isinstance(kw_val, KwargsV):
                    for key, _o in self.ev(e.args[0], mod, fn, bind):
                        hit = kw_val.get(str(key))
                        if hit is not None:
                            return list(hit)
            return self.ev(e.args[1], mod, fn, bind) if len(e.args) > 1 else []
        base = self.ev(func.value, mod, fn, bind)
        if meth in ("resolve", "absolute", "expanduser"):
            return base
        if meth in ("glob", "rglob") and first is not None:
            pats = self.ev(first, mod, fn, bind)
            return [
                (GlobV(b.path, str(p)), po)
                for (b, _), (p, po) in itertools.product(base, pats)
                if isinstance(b, PathV)
            ]
        if meth in ("lower", "upper", "strip"):
            return [
                (getattr(v, meth)(), o) if isinstance(v, str) else (v, o)
                for v, o in base
            ]
        if meth in ("split", "removeprefix", "removesuffix"):
            return [(v, o) for v, o in base if isinstance(v, str)]
        return []


# ------------------------------------------------------------- AST helpers


def _is_main_guard(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.If) or not isinstance(stmt.test, ast.Compare):
        return False
    t = stmt.test
    return (
        isinstance(t.left, ast.Name)
        and t.left.id == "__name__"
        and len(t.comparators) == 1
        and isinstance(t.comparators[0], ast.Constant)
        and t.comparators[0].value == "__main__"
    )


def _has_call(stmt: ast.stmt) -> bool:
    if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
        value = stmt.value
        return value is not None and any(
            isinstance(n, ast.Call)
            and not (isinstance(n.func, ast.Name) and n.func.id in ("Dataset", "Path"))
            and not (dotted(n.func) or "").endswith(("resolve", "getLogger"))
            for n in ast.walk(value)
        )
    return any(isinstance(n, ast.Call) for n in ast.walk(stmt))


def _assigned_names(stmt: ast.Assign | ast.AnnAssign) -> list[tuple[str, ast.expr]]:
    if isinstance(stmt, ast.AnnAssign):
        if isinstance(stmt.target, ast.Name) and stmt.value is not None:
            return [(stmt.target.id, stmt.value)]
        return []
    return [(t.id, stmt.value) for t in stmt.targets if isinstance(t, ast.Name)]


def _walk_no_defs(nodes: list[ast.stmt]) -> Iterator[ast.AST]:
    """ast.walk over statements, descending into nested defs too (closures run
    inside the enclosing function) but not into class bodies."""
    stack: list[ast.AST] = list(reversed(nodes))
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, ast.ClassDef):
            continue
        stack.extend(reversed(list(ast.iter_child_nodes(node))))


def _collect_locals(fn: Func, body: list[ast.stmt]) -> None:
    for node in _walk_no_defs(body):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            for name, value in _assigned_names(node):
                fn.locals.setdefault(name, []).append(value)
        elif isinstance(node, (ast.For, ast.comprehension)):
            target, it = node.target, node.iter
            if isinstance(target, ast.Name):
                fn.loops.setdefault(target.id, []).append((it, "elem"))
            elif isinstance(target, ast.Tuple) and len(target.elts) == 2:
                for elt, how in zip(target.elts, ("key", "value")):
                    if isinstance(elt, ast.Name):
                        fn.loops.setdefault(elt.id, []).append((it, how))
        elif isinstance(node, ast.withitem) and isinstance(
            node.optional_vars, ast.Name
        ):
            fn.locals.setdefault(node.optional_vars.id, []).append(node.context_expr)


def _self_assigns(init: Func, attr: str) -> list[ast.expr]:
    out = []
    for node in _walk_no_defs(init.body):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if (
                    isinstance(t, ast.Attribute)
                    and isinstance(t.value, ast.Name)
                    and t.value.id == "self"
                    and t.attr == attr
                ):
                    out.append(node.value)
    return out


def _dedupe(vals: list[Val]) -> list[Val]:
    seen: set[Any] = set()
    out = []
    for v, o in vals:
        key = (repr(v), o)
        if key not in seen:
            seen.add(key)
            out.append((v, o))
    return out


def _as_text(v: Any) -> str | None:
    if isinstance(v, (str, int)):
        return str(v)
    if isinstance(v, PathV):
        return str(v.path)
    return None


def _flatten(call: ast.Call) -> tuple[ast.expr, list[tuple[str, ast.Call | None]]]:
    """``a.b(x).c.d(y)`` -> (Name a, [("b", call), ("c", None), ("d", call)])."""
    segs: list[tuple[str, ast.Call | None]] = []
    node: ast.expr = call
    while True:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            segs.append((node.func.attr, node))
            node = node.func.value
        elif isinstance(node, ast.Attribute):
            segs.append((node.attr, None))
            node = node.value
        else:
            break
    segs.reverse()
    return node, segs


def _arg(
    call: ast.Call | None, index: int, keyword: str | None = None
) -> ast.expr | None:
    if call is None:
        return None
    if keyword:
        for kw in call.keywords:
            if kw.arg == keyword:
                return kw.value
    return call.args[index] if len(call.args) > index else None


# ---------------------------------------------------------------- detectors


@dataclass
class SparkIO:
    op: str
    fmt: str
    path_expr: ast.expr | None
    mode: str  # resolved mode text, "" when Spark's default applies
    mode_expr: ast.expr | None
    stream: bool
    checkpoint_expr: ast.expr | None
    terminal: str


class Extractor:
    def __init__(self, repo: Repo) -> None:
        self.repo = repo
        self.sites: list[Site] = []
        self.unresolved: list[Unresolved] = []
        self.generic: list[dict[str, str]] = []
        self.checkpoints: list[dict[str, str]] = []
        self.qdrant_constructions: list[tuple[str, str]] = []  # (collection, evidence)
        self._strategy_cache: dict[str, list[tuple[SparkIO, Func]]] = {}

    # ---- spark chains

    def spark_io(
        self, call: ast.Call, mod: Module, fn: Func, bind: dict[str, tuple[Val, ...]]
    ) -> SparkIO | None:
        root, segs = _flatten(call)
        if not segs:
            return None
        if isinstance(root, ast.Name) and root.id in fn.locals:
            for expr in fn.locals[root.id]:
                if isinstance(expr, (ast.Call, ast.Attribute)):
                    inner = expr if isinstance(expr, ast.Call) else None
                    if inner is not None:
                        _, pre = _flatten(inner)
                    else:
                        pre = (
                            [(expr.attr, None)]
                            if isinstance(expr, ast.Attribute)
                            else []
                        )
                    names = {n for n, _ in pre}
                    if names & {"read", "readStream", "write", "writeStream"}:
                        segs = pre + segs
                        break
        names = [n for n, _ in segs]
        terminal, tcall = segs[-1]
        if tcall is not call:
            return None
        seg = dict((n, c) for n, c in segs)
        fmt_vals = self.repo.ev(_arg(seg.get("format"), 0), mod, fn, bind)
        fmt = str(fmt_vals[0][0]) if fmt_vals else ""
        if (
            "read" in names or "readStream" in names
        ) and terminal in SPARK_READ_TERMINALS:
            if terminal in ("csv", "json", "parquet", "text", "orc"):
                fmt = terminal
            path_expr = _arg(tcall, 0, "path")
            if fmt == "kafka":
                for n, c in segs:
                    if n == "option" and c is not None:
                        key = self.repo.ev(_arg(c, 0), mod, fn, bind)
                        if key and key[0][0] in (
                            "subscribe",
                            "subscribePattern",
                            "assign",
                        ):
                            path_expr = _arg(c, 1)
            return SparkIO(
                "read", fmt, path_expr, "", None, "readStream" in names, None, terminal
            )
        if (
            "write" in names or "writeStream" in names
        ) and terminal in SPARK_WRITE_TERMINALS:
            if terminal in ("csv", "json", "parquet"):
                fmt = terminal
            stream = "writeStream" in names
            mode_call = seg.get("outputMode") if stream else seg.get("mode")
            mode_expr = _arg(mode_call, 0)
            mode_vals = self.repo.ev(mode_expr, mod, fn, bind)
            mode = str(mode_vals[0][0]).lower() if mode_vals else ""
            ckpt = None
            for n, c in segs:
                if n == "option" and c is not None:
                    key = self.repo.ev(_arg(c, 0), mod, fn, bind)
                    if key and key[0][0] == "checkpointLocation":
                        ckpt = _arg(c, 1)
            return SparkIO(
                "write",
                fmt,
                _arg(tcall, 0, "path"),
                mode,
                mode_expr,
                stream,
                ckpt,
                terminal,
            )
        return None

    def _spark_mode_label(self, io: SparkIO) -> str:
        if io.op == "read":
            return "stream" if io.stream else "batch"
        if io.stream:
            return f"stream-{io.mode or 'append'}"
        return io.mode or "errorifexists (Spark default)"

    # ---- strategies behind factories

    def strategy_io(self, mod: Module, cls: ast.ClassDef) -> list[tuple[SparkIO, Func]]:
        key = f"{mod.name}.{cls.name}"
        if key not in self._strategy_cache:
            found: list[tuple[SparkIO, Func]] = []
            for item in cls.body:
                if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                meth = mod.funcs.get(f"{cls.name}.{item.name}")
                if meth is None:
                    continue
                for node in _walk_no_defs(meth.body):
                    if isinstance(node, ast.Call):
                        io = self.spark_io(node, mod, meth, {})
                        if io is not None:
                            found.append((io, meth))
            self._strategy_cache[key] = found
        return self._strategy_cache[key]

    def bind_call(
        self,
        callee: Func,
        call: ast.Call,
        mod: Module,
        fn: Func,
        bind: dict[str, tuple[Val, ...]],
        skip_self: bool,
    ) -> dict[str, tuple[Val, ...]]:
        order = callee.param_order[1:] if skip_self else list(callee.param_order)
        out: dict[str, tuple[Val, ...]] = {}
        for name, arg in zip(order, call.args):
            out[name] = tuple(self.repo.ev(arg, mod, fn, bind))
        extra: list[tuple[str, tuple[Val, ...]]] = []
        for kw in call.keywords:
            if kw.arg is None:
                # **options forwarded: pass the caller's kwargs through.
                for v, _ in self.repo.ev(kw.value, mod, fn, bind):
                    if isinstance(v, KwargsV):
                        extra.extend((k, vals) for k, vals in v.items if k not in out)
                continue
            vals = tuple(self.repo.ev(kw.value, mod, fn, bind))
            if kw.arg in callee.params:
                out[kw.arg] = vals
            else:
                extra.append((kw.arg, vals))
        if callee.kwargs_param:
            out[callee.kwargs_param] = ((KwargsV(tuple(extra)), ""),)
        # parameters left to their defaults
        for name, default in callee.params.items():
            if name not in out and default is not None and name != callee.kwargs_param:
                out[name] = tuple(self.repo.ev(default, callee.module, None))
        return out

    def factory_sites(
        self, call: ast.Call, mod: Module, fn: Func, bind: dict[str, tuple[Val, ...]]
    ) -> list[tuple[SparkIO, Func, dict[str, tuple[Val, ...]], str]] | None:
        """``Factory.create(kind, ...)`` -> the strategy's IO with bound args."""
        if not (
            isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
        ):
            return None
        found = self.repo.find_class(mod, call.func.value.id)
        if found is None:
            return None
        fmod, fcls = found
        create = self.repo.method(fmod, fcls, call.func.attr)
        if create is None:
            return None
        cbind = self.bind_call(create, call, mod, fn, bind, skip_self=False)
        results = []
        for node in _walk_no_defs(create.body):
            if not isinstance(node, ast.If) or not isinstance(node.test, ast.Compare):
                continue
            test = node.test
            if len(test.comparators) != 1 or not isinstance(test.ops[0], ast.Eq):
                continue
            lefts = {v for v, _ in self.repo.ev(test.left, fmod, create, cbind)}
            rights = {
                v for v, _ in self.repo.ev(test.comparators[0], fmod, create, cbind)
            }
            if not lefts & rights:
                continue
            for stmt in node.body:
                if not (
                    isinstance(stmt, ast.Return) and isinstance(stmt.value, ast.Call)
                ):
                    continue
                ctor = stmt.value
                if not isinstance(ctor.func, ast.Name):
                    continue
                scls = self.repo.find_class(fmod, ctor.func.id)
                if scls is None:
                    continue
                smod, sclass = scls
                init = self.repo.method(smod, sclass, "__init__")
                if init is None:
                    continue
                ibind = self.bind_call(init, ctor, fmod, create, cbind, skip_self=True)
                via = (
                    f"{fcls.name}.{call.func.attr} ({fmod.rel}:{node.lineno}) -> "
                    f"{sclass.name} ({smod.rel}:{sclass.lineno})"
                )
                for io, meth in self.strategy_io(smod, sclass):
                    results.append(
                        (
                            io,
                            init,
                            ibind,
                            f"{via} -> {meth.qualname} ({smod.rel}:{meth.lineno})",
                        )
                    )
        return results

    # ---- dataset classification

    def classify(self, value: Any, fmt: str) -> tuple[str, str, str] | None:
        """value -> (kind, dataset id, uri)."""
        if isinstance(value, str):
            if fmt == "kafka":
                return "kafka", f"kafka.{value}", value
            lid = lake_id(value)
            if lid:
                return "delta" if fmt in ("delta", "") else fmt, lid, value
            if value.startswith(("http://", "https://")):
                return "http", "source." + re.sub(r"^https?://", "", value), value
            if value.startswith(("s3a://", "s3://")):
                return "lake_other", "lake." + value, value
            return None
        if isinstance(value, PathV):
            try:
                rel_path = value.path.resolve().relative_to(REPO_ROOT).as_posix()
            except ValueError:
                return "file", "file." + value.path.as_posix(), value.path.as_posix()
            return "file", f"source.{rel_path}", rel_path
        if isinstance(value, GlobV):
            files = sorted(value.base.glob(value.pattern))
            if not files:
                return None
            return (
                "glob",
                "",
                "|".join(f.resolve().relative_to(REPO_ROOT).as_posix() for f in files),
            )
        return None

    def emit(
        self,
        fn: Func,
        op: str,
        fmt: str,
        values: list[Val],
        mode: str,
        evidence: str,
        expr_text: str,
        pattern: str,
        via: str = "",
        extra: dict[str, str] | None = None,
    ) -> None:
        resolved = 0
        for value, origin in values:
            hit = self.classify(value, fmt)
            if hit is None:
                continue
            kind, did, uri = hit
            note = via or (f"literal at {origin}" if origin else "")
            if kind == "glob":
                for one in uri.split("|"):
                    self.sites.append(
                        Site(
                            fn.id,
                            op,
                            "file",
                            f"source.{one}",
                            mode,
                            evidence,
                            f"glob {expr_text} ({origin})",
                            one,
                            dict(extra or {}),
                        )
                    )
                    resolved += 1
                continue
            self.sites.append(
                Site(fn.id, op, kind, did, mode, evidence, note, uri, dict(extra or {}))
            )
            resolved += 1
        if not resolved:
            self.unresolved.append(
                Unresolved(
                    fn.id,
                    evidence,
                    pattern,
                    expr_text,
                    (
                        "argument is not a compile-time constant"
                        if values == []
                        else "resolved to values that are not datasets: "
                        + ", ".join(sorted({repr(v)[:60] for v, _ in values}))
                    ),
                )
            )

    # ---- per-function scan

    def scan(self) -> None:
        for fn in self.repo.funcs.values():
            self.scan_func(fn)
        self._resolve_qdrant()

    def scan_func(self, fn: Func) -> None:
        mod = fn.module
        parents: dict[int, ast.AST] = {}
        for outer in _walk_no_defs(fn.body):
            for child in ast.iter_child_nodes(outer):
                parents[id(child)] = outer
        for node in _walk_no_defs(fn.body):
            if not isinstance(node, ast.Call):
                continue
            self._bind_enclosing_loops(fn, node, parents)
            ev = f"{mod.rel}:{node.lineno}"
            text = ast.unparse(node)
            # Spark read/write chains
            io = self.spark_io(node, mod, fn, {})
            if io is not None:
                self._emit_spark(fn, io, {}, mod, fn, ev, "")
                continue
            # factories (ExtractorFactory.create / SinkFactory.create)
            fsites = self.factory_sites(node, mod, fn, {})
            if fsites:
                for sio, init, ibind, via in fsites:
                    self._emit_spark(
                        fn, sio, ibind, init.module, init, ev, via, strategy=True
                    )
                continue
            origin = self.repo.external_origin(mod, node.func) or ""
            func_name = dotted(node.func) or ""
            # Kafka producer
            if isinstance(node.func, ast.Attribute) and node.func.attr == "send":
                recv = node.func.value
                if isinstance(recv, ast.Name) and self._is_instance_of(
                    recv.id, fn, mod, "kafka.KafkaProducer"
                ):
                    vals = self.repo.ev(_arg(node, 0, "topic"), mod, fn)
                    self.emit(
                        fn,
                        "write",
                        "kafka",
                        vals,
                        "produce",
                        ev,
                        ast.unparse(_arg(node, 0, "topic") or node),
                        "KafkaProducer.send",
                    )
                    continue
            # HTTP
            if origin in HTTP_CALLS:
                vals = [
                    (v, o)
                    for v, o in self.repo.ev(_arg(node, 0, "url"), mod, fn)
                    if isinstance(v, str)
                ]
                if vals or origin.endswith("Request"):
                    self.emit(
                        fn,
                        "read",
                        "http",
                        vals,
                        "GET",
                        ev,
                        ast.unparse(_arg(node, 0) or node),
                        origin,
                    )
                continue
            # local files
            if func_name == "open":
                path_expr = _arg(node, 0, "file")
                mode_vals = self.repo.ev(_arg(node, 1, "mode"), mod, fn)
                fmode = str(mode_vals[0][0]) if mode_vals else "r"
                op = "write" if any(c in fmode for c in "wax+") else "read"
                label = {"w": "overwrite", "a": "append", "x": "create"}.get(
                    fmode[0], "read"
                )
                self.emit(
                    fn,
                    op,
                    "file",
                    self.repo.ev(path_expr, mod, fn),
                    label,
                    ev,
                    ast.unparse(path_expr) if path_expr else text,
                    "open()",
                )
                continue
            if isinstance(node.func, ast.Attribute) and node.func.attr in (
                "read_text",
                "read_bytes",
                "write_text",
                "write_bytes",
            ):
                vals = [
                    (v, o)
                    for v, o in self.repo.ev(node.func.value, mod, fn)
                    if isinstance(v, (PathV, GlobV))
                ]
                if vals:
                    op = "write" if node.func.attr.startswith("write") else "read"
                    self.emit(
                        fn,
                        op,
                        "file",
                        vals,
                        "overwrite" if op == "write" else "read",
                        ev,
                        ast.unparse(node.func.value),
                        f"Path.{node.func.attr}()",
                    )
                else:
                    self.unresolved.append(
                        Unresolved(
                            fn.id,
                            ev,
                            f"Path.{node.func.attr}()",
                            ast.unparse(node.func.value),
                            "file path is not a compile-time constant (e.g. a CLI argument)",
                        )
                    )
                continue
            # SQL on DuckDB / SQLite connections
            if (
                isinstance(node.func, ast.Attribute)
                and node.func.attr in SQL_EXEC_METHODS
            ):
                engine = self._connection_engine(node.func.value, mod, fn)
                if engine is not None:
                    self._emit_sql(fn, node, engine, mod, ev)
                elif (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id in fn.annotations
                ):
                    ann = fn.annotations[node.func.value.id]
                    cls = (
                        self.repo.find_class(mod, ann.id)
                        if isinstance(ann, ast.Name)
                        else None
                    )
                    if cls is not None:
                        impls = sorted(
                            f.id
                            for f in self.repo.funcs.values()
                            if f.qualname.endswith(f".{node.func.attr}")
                            and f.class_name
                            and any(
                                isinstance(b, ast.Name) and b.id == cls[1].name
                                for b in f.module.classes[f.class_name].bases
                            )
                        )
                        self.unresolved.append(
                            Unresolved(
                                fn.id,
                                ev,
                                f"{cls[1].name}.{node.func.attr}()",
                                ast.unparse(node),
                                "dynamic dispatch on an interface; SQL is compiled at runtime. Implementations: "
                                + (", ".join(impls) or "none found"),
                            )
                        )
                continue
            # Qdrant
            if func_name.endswith("QdrantVectorStore") or origin.endswith(
                "QdrantVectorStore"
            ):
                for v, _o in self.repo.ev(_arg(node, 99, "collection_name"), mod, fn):
                    self.qdrant_constructions.append((str(v), ev))
                continue
            if isinstance(node.func, ast.Attribute):
                attr = node.func.attr
                if attr in ("add_documents", "add_texts"):
                    self.sites.append(
                        Site(
                            fn.id,
                            "write",
                            "qdrant",
                            "",
                            "upsert",
                            ev,
                            f"{attr}() on a vector store",
                        )
                    )
                elif attr.startswith("similarity_search"):
                    self.sites.append(
                        Site(
                            fn.id,
                            "read",
                            "qdrant",
                            "",
                            "search",
                            ev,
                            f"{attr}() on a vector store",
                        )
                    )
                elif attr in (
                    "create_collection",
                    "delete_collection",
                    "recreate_collection",
                ):
                    vals = self.repo.ev(_arg(node, 0, "collection_name"), mod, fn)
                    for v, o in vals:
                        self.sites.append(
                            Site(
                                fn.id,
                                "write",
                                "qdrant",
                                f"qdrant.{v}",
                                "drop" if attr == "delete_collection" else "create",
                                ev,
                                f"{attr}({ast.unparse(_arg(node, 0) or node)}) literal at {o}",
                            )
                        )
                    if not vals:
                        self.unresolved.append(
                            Unresolved(
                                fn.id, ev, attr, text, "collection name not constant"
                            )
                        )

    def _bind_enclosing_loops(
        self, fn: Func, node: ast.AST, parents: dict[int, ast.AST]
    ) -> None:
        self.repo.scope_fn, self.repo.scope_bind = None, {}
        bind: dict[str, tuple[Val, ...]] = {}
        cur = parents.get(id(node))
        while cur is not None:
            if isinstance(cur, ast.For):
                targets: list[tuple[ast.expr, str]] = []
                if isinstance(cur.target, ast.Name):
                    targets = [(cur.target, "elem")]
                elif isinstance(cur.target, ast.Tuple) and len(cur.target.elts) == 2:
                    targets = list(zip(cur.target.elts, ("key", "value")))
                for target, how in targets:
                    if isinstance(target, ast.Name) and target.id not in bind:
                        bind[target.id] = tuple(
                            self.repo._ev_iter(cur.iter, how, fn.module, fn, {})
                        )
            cur = parents.get(id(cur))
        self.repo.scope_fn, self.repo.scope_bind = fn, bind

    def _emit_spark(
        self,
        fn: Func,
        io: SparkIO,
        bind: dict[str, tuple[Val, ...]],
        ctx_mod: Module,
        ctx_fn: Func,
        ev: str,
        via: str,
        strategy: bool = False,
    ) -> None:
        path_expr = io.path_expr
        verb = (
            ("readStream" if io.op == "read" else "writeStream") if io.stream else io.op
        )
        pattern = f"spark {verb}.{io.terminal}"
        # For a strategy (factory) site, self.<attr> is resolved through __init__
        # with the call site's arguments bound (ctx_fn = __init__, bind = args).
        vals = (
            self.repo.ev(path_expr, ctx_mod, ctx_fn, bind)
            if path_expr is not None
            else []
        )
        if strategy and io.mode_expr is not None:
            mode_vals = self.repo.ev(io.mode_expr, ctx_mod, ctx_fn, bind)
            io = replace(io, mode=str(mode_vals[0][0]).lower() if mode_vals else "")
        if (
            not strategy
            and path_expr is not None
            and _is_self_attr(path_expr)
            and not vals
        ):
            self.generic.append(
                {
                    "job": fn.id,
                    "evidence": ev,
                    "pattern": pattern,
                    "expression": ast.unparse(path_expr),
                    "format": io.fmt or "?",
                }
            )
            return
        extra: dict[str, str] = {"format": io.fmt or "?"}
        if io.checkpoint_expr is not None:
            ck = self.repo.ev(io.checkpoint_expr, ctx_mod, ctx_fn, bind)
            if ck:
                extra["checkpoint"] = str(ck[0][0])
                for target, _ in vals:
                    self.checkpoints.append(
                        {
                            "job": fn.id,
                            "target": str(target),
                            "checkpoint": str(ck[0][0]),
                            "evidence": ev,
                        }
                    )
        self.emit(
            fn,
            io.op,
            io.fmt,
            vals,
            self._spark_mode_label(io),
            ev,
            ast.unparse(path_expr) if path_expr is not None else "(no path argument)",
            pattern,
            via=via,
            extra=extra,
        )

    def _is_instance_of(self, name: str, fn: Func, mod: Module, origin: str) -> bool:
        for expr in fn.locals.get(name, []):
            if (
                isinstance(expr, ast.Call)
                and self.repo.external_origin(mod, expr.func) == origin
            ):
                return True
        return False

    def _connection_engine(
        self, recv: ast.expr, mod: Module, fn: Func
    ) -> tuple[str, list[Val]] | None:
        """Receiver of .execute() -> ("duckdb"|"sqlite3", db path candidates)."""
        exprs: list[tuple[ast.expr, Func]] = []
        if (
            isinstance(recv, ast.Attribute)
            and isinstance(recv.value, ast.Name)
            and recv.value.id == "self"
            and fn.class_name
        ):
            init = mod.funcs.get(f"{fn.class_name}.__init__")
            if init is not None:
                exprs = [(e, init) for e in _self_assigns(init, recv.attr)]
        elif isinstance(recv, ast.Name):
            exprs = [(e, fn) for e in fn.locals.get(recv.id, [])]
        for expr, ctx in exprs:
            if isinstance(expr, ast.Call):
                origin = self.repo.external_origin(mod, expr.func) or ""
                if origin in ("duckdb.connect", "sqlite3.connect"):
                    db = self.repo.ev(_arg(expr, 0, "database"), mod, ctx)
                    return origin.split(".")[0], db
        return None

    def _emit_sql(
        self,
        fn: Func,
        node: ast.Call,
        engine: tuple[str, list[Val]],
        mod: Module,
        ev: str,
    ) -> None:
        kind, db = engine
        sql_vals = [
            (v, o)
            for v, o in self.repo.ev(_arg(node, 0), mod, fn)
            if isinstance(v, str)
        ]
        if not sql_vals:
            self.unresolved.append(
                Unresolved(
                    fn.id,
                    ev,
                    f"{kind} {node.func.attr if isinstance(node.func, ast.Attribute) else ''}()",
                    ast.unparse(_arg(node, 0) or node),
                    "SQL text is built at runtime (not a constant)",
                )
            )
            return
        db_path = ""
        for v, _ in db:
            if isinstance(v, PathV):
                try:
                    db_path = v.path.resolve().relative_to(REPO_ROOT).as_posix()
                except ValueError:
                    db_path = v.path.as_posix()
        prefix = "duckdb" if kind == "duckdb" else "sqlite"
        for sql, origin in sql_vals:
            flat = " ".join(sql.split())
            view = re.match(
                r"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\w*\s+)?VIEW\s+(\w+)\s+AS\s+(.*)",
                flat,
            )
            if view:
                name, body = view.group(1), view.group(2)
                self.sites.append(
                    Site(
                        fn.id,
                        "write",
                        "duckdb_view",
                        f"{prefix}.{name}",
                        "create view",
                        ev,
                        f"SQL {flat[:120]} ({origin})",
                        name,
                    )
                )
                for uri in re.findall(r"delta_scan\('([^']+)'\)", body):
                    lid = lake_id(uri)
                    if lid:
                        self.sites.append(
                            Site(
                                fn.id,
                                "read",
                                "delta",
                                lid,
                                "delta_scan",
                                ev,
                                f"delta_scan('{uri}') in view {name} ({origin})",
                                uri,
                            )
                        )
                continue
            for name in re.findall(
                r"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", flat
            ):
                self.sites.append(
                    Site(
                        fn.id,
                        "write",
                        kind,
                        f"{prefix}.{name}",
                        "create",
                        ev,
                        f"SQL CREATE TABLE ({origin})",
                        db_path,
                    )
                )
            for name in re.findall(r"(?i)INSERT\s+INTO\s+(\w+)", flat):
                self.sites.append(
                    Site(
                        fn.id,
                        "write",
                        kind,
                        f"{prefix}.{name}",
                        "append (INSERT)",
                        ev,
                        f"SQL INSERT ({origin})",
                        db_path,
                    )
                )
            for name in re.findall(r"(?i)\bFROM\s+(\w+)\b(?!\s*\()", flat):
                self.sites.append(
                    Site(
                        fn.id,
                        "read",
                        kind,
                        f"{prefix}.{name}",
                        "SELECT",
                        ev,
                        f"SQL SELECT ({origin})",
                        db_path,
                    )
                )

    def _resolve_qdrant(self) -> None:
        names = sorted({c for c, _ in self.qdrant_constructions})
        for site in self.sites:
            if site.kind != "qdrant" or site.target:
                continue
            if len(names) == 1:
                site.target = f"qdrant.{names[0]}"
                site.via += (
                    "; collection = the only QdrantVectorStore(collection_name=...) in the repo ("
                    + ", ".join(e for _, e in self.qdrant_constructions)
                    + ")"
                )
            else:
                self.unresolved.append(
                    Unresolved(
                        site.job,
                        site.evidence,
                        site.via,
                        "",
                        f"vector store collection ambiguous: {names}",
                    )
                )
        self.sites = [s for s in self.sites if s.kind != "qdrant" or s.target]


def _is_self_attr(e: ast.expr) -> bool:
    return (
        isinstance(e, ast.Attribute)
        and isinstance(e.value, ast.Name)
        and e.value.id == "self"
    )


# ------------------------------------------------------------- call graph


def call_edges(repo: Repo) -> dict[str, set[str]]:
    """Static caller -> callee map over repo functions (references count too)."""
    edges: dict[str, set[str]] = {f: set() for f in repo.funcs}
    for fn in repo.funcs.values():
        mod = fn.module
        nodes: list[ast.AST] = list(_walk_no_defs(fn.body))
        if isinstance(fn.node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            nodes.extend(
                n
                for d in [*fn.node.args.defaults, *fn.node.args.kw_defaults]
                if d
                for n in ast.walk(d)
            )
        for node in nodes:
            target: Func | None = None
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                target = _resolve_callable(repo, mod, node.id)
            elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
                target = _resolve_attr_call(repo, mod, fn, node)
            if target is not None and target.id != fn.id:
                edges[fn.id].add(target.id)
    return edges


def _resolve_callable(repo: Repo, mod: Module, name: str) -> Func | None:
    fn = repo.find_func(mod, name)
    if fn is not None and fn.qualname not in ("__main__", "<module>"):
        return fn
    cls = repo.find_class(mod, name)
    if cls is not None:
        return repo.method(cls[0], cls[1], "__init__")
    return None


def _class_of(
    repo: Repo, mod: Module, ann: ast.expr | None
) -> tuple[Module, ast.ClassDef] | None:
    if isinstance(ann, ast.Name):
        return repo.find_class(mod, ann.id)
    if isinstance(ann, ast.Constant) and isinstance(ann.value, str):
        return repo.find_class(mod, ann.value)
    return None


def _resolve_attr_call(
    repo: Repo, mod: Module, fn: Func, node: ast.Attribute
) -> Func | None:
    value = node.value
    cls: tuple[Module, ast.ClassDef] | None = None
    if isinstance(value, ast.Name):
        if value.id == "self" and fn.class_name:
            cls = (
                (mod, mod.classes[fn.class_name])
                if fn.class_name in mod.classes
                else None
            )
        elif value.id in fn.annotations:
            cls = _class_of(repo, mod, fn.annotations[value.id])
        elif value.id in fn.locals:
            for expr in fn.locals[value.id]:
                if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Name):
                    cls = repo.find_class(mod, expr.func.id)
                    if cls is None:
                        callee = repo.find_func(mod, expr.func.id)
                        if callee is not None:
                            cls = _class_of(repo, callee.module, callee.returns)
                if cls:
                    break
        else:
            cls = repo.find_class(mod, value.id)
            if cls is None and value.id in mod.imports:
                target, attr = mod.imports[value.id]
                full = f"{target}.{attr}" if attr else target
                if full in repo.modules:
                    return repo.find_func(repo.modules[full], node.attr)
    elif isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
        callee = repo.find_func(mod, value.func.id)
        if callee is not None:
            cls = _class_of(repo, callee.module, callee.returns)
    if cls is None:
        return None
    return repo.method(cls[0], cls[1], node.attr)


# --------------------------------------------------------------- entry points


def dag_tasks(repo: Repo) -> list[dict[str, Any]]:
    """Airflow operators: task_id, python_callable, inlets/outlets (as lake ids)."""
    tasks: list[dict[str, Any]] = []
    for mod in repo.modules.values():
        dag_id = ""
        for node in ast.walk(mod.tree):
            if isinstance(node, ast.Call) and dotted(node.func) == "DAG":
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                        dag_id = str(kw.value.value)
        deps: list[tuple[str, str]] = []
        var_task: dict[str, str] = {}
        for node in ast.walk(mod.tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                call = node.value
                if not (dotted(call.func) or "").endswith("Operator"):
                    continue
                kws = {kw.arg: kw.value for kw in call.keywords if kw.arg}
                if "task_id" not in kws:
                    continue
                task_id = str(getattr(kws["task_id"], "value", "?"))
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        var_task[t.id] = task_id
                callable_id = ""
                pc = kws.get("python_callable")
                if isinstance(pc, ast.Name):
                    target = _resolve_callable(repo, mod, pc.id)
                    callable_id = target.id if target else pc.id

                def lake(expr: ast.expr | None) -> list[tuple[str, str]]:
                    out = []
                    for v, o in repo.ev(expr, mod, None):
                        lid = lake_id(str(v)) if isinstance(v, str) else None
                        if lid:
                            out.append((lid, o))
                    return out

                tasks.append(
                    {
                        "dag_id": dag_id,
                        "task_id": task_id,
                        "operator": dotted(call.func),
                        "callable": callable_id,
                        "inlets": lake(kws.get("inlets")),
                        "outlets": lake(kws.get("outlets")),
                        "evidence": f"{mod.rel}:{node.lineno}",
                    }
                )
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp):
                chain: list[str] = []
                cur: ast.expr = node.value
                while isinstance(cur, ast.BinOp) and isinstance(cur.op, ast.RShift):
                    if isinstance(cur.right, ast.Name):
                        chain.insert(0, cur.right.id)
                    cur = cur.left
                if isinstance(cur, ast.Name):
                    chain.insert(0, cur.id)
                deps.extend(zip(chain, chain[1:]))
        for t in tasks:
            if t["dag_id"] == dag_id and "upstream" not in t:
                t["upstream"] = [
                    var_task.get(a, a)
                    for a, b in deps
                    if var_task.get(b) == t["task_id"]
                ]
    return tasks


def fastapi_routes(repo: Repo) -> list[dict[str, str]]:
    routes = []
    for fn in repo.funcs.values():
        node = fn.node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if (
                isinstance(dec, ast.Call)
                and isinstance(dec.func, ast.Attribute)
                and dec.func.attr in ("get", "post", "put", "delete", "patch")
                and dec.args
                and isinstance(dec.args[0], ast.Constant)
            ):
                routes.append(
                    {
                        "func": fn.id,
                        "label": f"HTTP {dec.func.attr.upper()} {dec.args[0].value}",
                        "evidence": f"{fn.module.rel}:{dec.lineno}",
                    }
                )
    return routes
