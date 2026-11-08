"""Static analysis behind the catalog page.

Parses the repo's Python with ``ast`` (nothing is imported or executed) to find,
for every Delta path under ``s3a://lakehouse/``:

* the write sites (``df.write...save(path)``, ``df.writeStream...start(path)``,
  ``SinkFactory.create("delta", path).write(df)``) and the read sites
  (``spark.read...load(path)``, DuckDB ``delta_scan('s3://...')`` strings);
* what the written DataFrame is made of: the column operations
  (``withColumn``/``withColumnRenamed``/``alias``/``selectExpr``/``from_json``
  schema fields), the transform functions it flows through (and their
  docstrings), the upstream tables or raw sources it reads;
* how the writer function is started (Airflow task, ``python -m`` entry point,
  docker compose service).

Path strings are resolved by a small constant evaluator (module constants,
``Path(__file__).resolve().parents[n] / "x"`` joins, dict subscripts, ``for``
loops over literal dicts/lists, f-strings, parameter defaults).
"""

from __future__ import annotations

import ast
import io
import itertools
import re
import tokenize
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from toolkit.common import LAKE_BUCKET, LAYERS, REPO_ROOT, rel

SCAN_DIRS = ("src", "dags", "config")
REPO_MARK = "{repo}"
LAKE_RE = re.compile(
    rf"s3a?://{re.escape(LAKE_BUCKET)}/({'|'.join(LAYERS)})/([A-Za-z0-9_]+)"
)
WRITE_TERMINALS = {"save", "start", "saveAsTable", "parquet", "csv", "json"}
TABLE_OPS = {
    "dropDuplicates",
    "drop_duplicates",
    "withWatermark",
    "unionByName",
    "union",
    "filter",
    "where",
    "join",
    "drop",
    "distinct",
}
MAX_DEPTH = 8

Env = dict[str, ast.AST]


# ----------------------------------------------------------------- module index


@dataclass
class Mod:
    path: Path
    name: str
    tree: ast.Module
    lines: list[str]
    full_comments: dict[int, str]
    trailing_comments: dict[int, str]
    assigns: dict[str, list[ast.AST]]
    imports: dict[str, str]
    funcs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef]
    parents: dict[int, ast.AST]
    func_of: dict[int, str]

    def cite(self, line: int) -> str:
        return f"{rel(self.path)}:{line}"


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts)


def _comments(source: str) -> tuple[dict[int, str], dict[int, str]]:
    full: dict[int, str] = {}
    trailing: dict[int, str] = {}
    lines = source.splitlines()
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, SyntaxError):
        return full, trailing
    for tok in tokens:
        if tok.type != tokenize.COMMENT:
            continue
        line_no, col = tok.start
        text = tok.string.lstrip("#").strip()
        before = lines[line_no - 1][:col] if line_no - 1 < len(lines) else ""
        (full if not before.strip() else trailing)[line_no] = text
    return full, trailing


def _load_module(path: Path) -> Mod | None:
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return None
    full, trailing = _comments(source)
    parents: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[id(child)] = node
    assigns: dict[str, list[ast.AST]] = {}
    imports: dict[str, str] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    assigns.setdefault(target.id, []).append(stmt.value)
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            if isinstance(stmt.target, ast.Name):
                assigns.setdefault(stmt.target.id, []).append(stmt.value)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for alias in node.names:
                imports[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        elif isinstance(node, ast.ImportFrom) and node.level > 0:
            base = _module_name(path).split(".")[: -node.level]
            mod = ".".join(base + ([node.module] if node.module else []))
            for alias in node.names:
                imports[alias.asname or alias.name] = f"{mod}.{alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports[alias.asname or alias.name] = alias.name
    funcs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    func_of: dict[int, str] = {}

    def index(body: list[ast.stmt], prefix: str) -> None:
        for stmt in body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                qual = prefix + stmt.name
                funcs[qual] = stmt
                for sub in ast.walk(stmt):
                    func_of.setdefault(id(sub), qual)
                index(stmt.body, qual + ".")
            elif isinstance(stmt, ast.ClassDef):
                index(stmt.body, prefix + stmt.name + ".")

    index(tree.body, "")
    # innermost function wins: re-walk nested functions so they override parents
    for qual in sorted(funcs, key=lambda q: q.count(".")):
        for sub in ast.walk(funcs[qual]):
            func_of[id(sub)] = qual
    return Mod(
        path=path,
        name=_module_name(path),
        tree=tree,
        lines=source.splitlines(),
        full_comments=full,
        trailing_comments=trailing,
        assigns=assigns,
        imports=imports,
        funcs=funcs,
        parents=parents,
        func_of=func_of,
    )


def load_modules() -> dict[str, Mod]:
    mods: dict[str, Mod] = {}
    for directory in SCAN_DIRS:
        for path in sorted((REPO_ROOT / directory).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            mod = _load_module(path)
            if mod is not None:
                mods[mod.name] = mod
    return mods


# ---------------------------------------------------------------- evaluation


@dataclass
class Ctx:
    mods: dict[str, Mod]
    mod: Mod
    func: str | None

    def fnode(self) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
        return self.mod.funcs.get(self.func) if self.func else None


def ctx_for(mods: dict[str, Mod], mod: Mod, node: ast.AST) -> Ctx:
    return Ctx(mods, mod, mod.func_of.get(id(node)))


def _params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, ast.AST | None]:
    args = fn.args
    positional = args.posonlyargs + args.args
    defaults: list[ast.AST | None] = [None] * (
        len(positional) - len(args.defaults)
    ) + list(args.defaults)
    out: dict[str, ast.AST | None] = {
        a.arg: d for a, d in zip(positional, defaults, strict=True)
    }
    for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True):
        out[a.arg] = d
    return out


def _local_assignments(ctx: Ctx, name: str, before: int) -> list[ast.AST]:
    """Value nodes of the latest assignment(s) to ``name`` before ``before``."""
    fn = ctx.fnode()
    if fn is None:
        scope_stmts: list[ast.AST] = list(ast.walk(ctx.mod.tree))
    else:
        scope_stmts = list(ast.walk(fn))
    found: list[ast.Assign | ast.AnnAssign] = []
    for node in scope_stmts:
        if isinstance(node, ast.Assign) and node.lineno < before:
            if any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
                found.append(node)
        elif (
            isinstance(node, ast.AnnAssign)
            and node.lineno < before
            and node.value is not None
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            found.append(node)
    if fn is None:
        found = [n for n in found if ctx.mod.func_of.get(id(n)) is None]
    else:
        found = [n for n in found if ctx.mod.func_of.get(id(n)) == ctx.func]
    if not found:
        return []
    latest = max(found, key=lambda n: n.lineno)
    parent = ctx.mod.parents.get(id(latest))
    chosen = [latest]
    if isinstance(parent, ast.If):
        chosen = [n for n in found if ctx.mod.parents.get(id(n)) is parent]
    return [n.value for n in chosen if n.value is not None]


def _module_value(ctx: Ctx, name: str) -> tuple[Ctx, ast.AST] | None:
    values = ctx.mod.assigns.get(name)
    if values:
        return Ctx(ctx.mods, ctx.mod, None), values[-1]
    target = ctx.mod.imports.get(name)
    if target and "." in target:
        mod_name, attr = target.rsplit(".", 1)
        other = ctx.mods.get(mod_name)
        if other and other.assigns.get(attr):
            return Ctx(ctx.mods, other, None), other.assigns[attr][-1]
    return None


def resolve_function(ctx: Ctx, func: ast.AST) -> tuple[Mod, str] | None:
    """A call target that is a function defined in the scanned repo."""
    if isinstance(func, ast.Name):
        if func.id in ctx.mod.funcs:
            return ctx.mod, func.id
        target = ctx.mod.imports.get(func.id)
        if target and "." in target:
            mod_name, attr = target.rsplit(".", 1)
            other = ctx.mods.get(mod_name)
            if other and attr in other.funcs:
                return other, attr
            # package re-export: from src.pipelines import x
            pkg = ctx.mods.get(target.rsplit(".", 1)[0] + ".__init__")
            if pkg and attr in pkg.imports:
                sub = pkg.imports[attr]
                smod, sattr = sub.rsplit(".", 1)
                if smod in ctx.mods and sattr in ctx.mods[smod].funcs:
                    return ctx.mods[smod], sattr
    return None


def _repo_root_expr(ctx: Ctx, node: ast.AST) -> str | None:
    text = ast.unparse(node)
    match = re.fullmatch(r"Path\(__file__\)\.resolve\(\)\.parents\[(\d+)\]", text)
    if match:
        target = ctx.mod.path.resolve().parents[int(match.group(1))]
        return REPO_MARK if target == REPO_ROOT else rel(target)
    if re.fullmatch(r"Path\(__file__\)\.resolve\(\)\.parent", text):
        return rel(ctx.mod.path.resolve().parent)
    return None


def eval_str(
    ctx: Ctx, node: ast.AST, env: Env | None = None, depth: int = 0
) -> list[str]:
    """Every string value ``node`` can take (empty when not derivable)."""
    env = env or {}
    if depth > MAX_DEPTH:
        return []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    root = _repo_root_expr(ctx, node)
    if root is not None:
        return [root]
    if isinstance(node, ast.Name):
        if node.id in env:
            return eval_str(ctx, env[node.id], env, depth + 1)
        loop = _loop_candidates(ctx, node, depth)
        if loop is not None:
            return loop
        fn = ctx.fnode()
        values = _local_assignments(ctx, node.id, getattr(node, "lineno", 10**9))
        if values:
            return _flat(eval_str(ctx, v, env, depth + 1) for v in values)
        if fn is not None:
            params = _params(fn)
            if node.id in params:
                default = params[node.id]
                return eval_str(ctx, default, {}, depth + 1) if default else []
        found = _module_value(ctx, node.id)
        if found:
            return eval_str(found[0], found[1], {}, depth + 1)
        return []
    if isinstance(node, ast.Subscript):
        mapping = eval_dict(ctx, node.value, env, depth + 1)
        if mapping is None:
            return []
        keys = eval_str(ctx, node.slice, env, depth + 1)
        out: list[str] = []
        for key in keys:
            if key in mapping:
                out.extend(eval_str(mapping[key][0], mapping[key][1], {}, depth + 1))
        return out
    if isinstance(node, ast.Call):
        name = ast.unparse(node.func)
        if name in {"str", "Path", "Dataset"} and node.args:
            return eval_str(ctx, node.args[0], env, depth + 1)
        return []
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        lefts = eval_str(ctx, node.left, env, depth + 1)
        rights = eval_str(ctx, node.right, env, depth + 1)
        return [f"{a}/{b}" for a in lefts for b in rights]
    if isinstance(node, ast.JoinedStr):
        parts: list[list[str]] = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append([str(value.value)])
            elif isinstance(value, ast.FormattedValue):
                cands = eval_str(ctx, value.value, env, depth + 1)
                if not cands:
                    return []
                parts.append(cands)
        return ["".join(p) for p in itertools.product(*parts)]
    return []


def _flat(groups: Any) -> list[str]:
    out: list[str] = []
    for group in groups:
        for item in group:
            if item not in out:
                out.append(item)
    return out


def eval_dict(
    ctx: Ctx, node: ast.AST, env: Env | None = None, depth: int = 0
) -> dict[str, tuple[Ctx, ast.AST]] | None:
    """A literal dict (possibly behind a name), key -> (ctx, value node)."""
    env = env or {}
    if depth > MAX_DEPTH:
        return None
    if isinstance(node, ast.Dict):
        out: dict[str, tuple[Ctx, ast.AST]] = {}
        for key, value in zip(node.keys, node.values, strict=True):
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                out[key.value] = (ctx, value)
        return out
    if isinstance(node, ast.Name):
        if node.id in env:
            return eval_dict(ctx, env[node.id], env, depth + 1)
        values = _local_assignments(ctx, node.id, getattr(node, "lineno", 10**9))
        if values:
            return eval_dict(ctx, values[-1], env, depth + 1)
        found = _module_value(ctx, node.id)
        if found:
            return eval_dict(found[0], found[1], {}, depth + 1)
    return None


def eval_seq(
    ctx: Ctx, node: ast.AST, depth: int = 0
) -> list[tuple[Ctx, ast.AST]] | None:
    """A literal list/tuple (possibly behind a name) as (ctx, element) pairs."""
    if depth > MAX_DEPTH:
        return None
    if isinstance(node, (ast.List, ast.Tuple)):
        return [(ctx, e) for e in node.elts]
    if isinstance(node, ast.Name):
        values = _local_assignments(ctx, node.id, getattr(node, "lineno", 10**9))
        if values:
            return eval_seq(ctx, values[-1], depth + 1)
        found = _module_value(ctx, node.id)
        if found:
            return eval_seq(found[0], found[1], depth + 1)
    return None


def _enclosing_fors(ctx: Ctx, node: ast.AST) -> list[ast.For]:
    out: list[ast.For] = []
    cur = ctx.mod.parents.get(id(node))
    while cur is not None and not isinstance(
        cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)
    ):
        if isinstance(cur, ast.For):
            out.append(cur)
        cur = ctx.mod.parents.get(id(cur))
    return out


def loop_iterations(ctx: Ctx, loop: ast.For) -> list[Env] | None:
    """Bindings of the loop target for each iteration, when the iterable is literal."""
    target, it = loop.target, loop.iter
    if (
        isinstance(target, ast.Tuple)
        and len(target.elts) == 2
        and all(isinstance(e, ast.Name) for e in target.elts)
        and isinstance(it, ast.Call)
        and isinstance(it.func, ast.Attribute)
        and it.func.attr == "items"
    ):
        mapping = eval_dict(ctx, it.func.value)
        if mapping is None:
            return None
        key_name = target.elts[0].id  # type: ignore[attr-defined]
        value_name = target.elts[1].id  # type: ignore[attr-defined]
        envs = []
        for key, (vctx, vnode) in mapping.items():
            if vctx.mod is not ctx.mod or vctx.func != ctx.func:
                # module-level dict value used inside a function: resolve as constant
                vals = eval_str(vctx, vnode)
                vnode = ast.Constant(vals[0]) if len(vals) == 1 else vnode
            envs.append({key_name: ast.Constant(key), value_name: vnode})
        return envs
    if isinstance(target, ast.Name):
        seq = eval_seq(ctx, it)
        if seq is None:
            return None
        envs = []
        for ectx, elem in seq:
            vals = eval_str(ectx, elem)
            envs.append({target.id: ast.Constant(vals[0]) if len(vals) == 1 else elem})
        return envs
    return None


def _loop_candidates(ctx: Ctx, node: ast.Name, depth: int) -> list[str] | None:
    for loop in _enclosing_fors(ctx, node):
        names = {n.id for n in ast.walk(loop.target) if isinstance(n, ast.Name)}
        if node.id in names:
            envs = loop_iterations(ctx, loop) or []
            return _flat(eval_str(ctx, e[node.id], e, depth + 1) for e in envs)
    return None


def site_envs(ctx: Ctx, node: ast.AST) -> list[Env]:
    """All loop bindings under which ``node`` executes (one empty env if none)."""
    envs: list[Env] = [{}]
    for loop in _enclosing_fors(ctx, node):
        its = loop_iterations(ctx, loop)
        if not its:
            continue
        envs = [{**a, **b} for a in envs for b in its]
    return envs


def lake_ids(paths: list[str]) -> list[str]:
    out = []
    for path in paths:
        for match in LAKE_RE.finditer(path):
            tid = f"{match.group(1)}.{match.group(2)}"
            if tid not in out:
                out.append(tid)
    return out


# ------------------------------------------------------------ read/write sites


def _chain(node: ast.AST) -> list[tuple[str, ast.Call | None, ast.AST]]:
    """Method chain of ``a.b(x).c(y)`` as [(attr, call, receiver)], outermost first."""
    out: list[tuple[str, ast.Call | None, ast.AST]] = []
    cur = node
    while True:
        call = cur if isinstance(cur, ast.Call) else None
        func = cur.func if isinstance(cur, ast.Call) else cur
        if isinstance(func, ast.Attribute):
            out.append((func.attr, call, func.value))
            cur = func.value
        else:
            break
    return out


@dataclass
class Site:
    kind: str  # "write" | "read"
    table: str
    mod: Mod
    line: int
    function: str | None
    mode: str | None
    fmt: str | None
    df: ast.AST | None = None
    env: Env = field(default_factory=dict)
    via: str = ""
    node: ast.AST | None = None

    @property
    def qualname(self) -> str:
        return f"{self.mod.name}.{self.function}" if self.function else self.mod.name

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "table": self.table,
            "file": rel(self.mod.path),
            "line": self.line,
            "function": self.qualname,
            "module_level": self.function is None,
            "mode": self.mode,
            "format": self.fmt,
            "via": self.via,
        }


def _const_kw(call: ast.Call | None, name: str) -> str | None:
    if call is None:
        return None
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    return None


def find_sites(mods: dict[str, Mod]) -> list[Site]:
    sites: list[Site] = []
    for mod in mods.values():
        for node in ast.walk(mod.tree):
            if isinstance(node, ast.Call):
                sites.extend(_call_sites(mods, mod, node))
            elif isinstance(node, ast.JoinedStr) or (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and "delta_scan(" in node.value
            ):
                sites.extend(_duckdb_sites(mods, mod, node))
    return sites


def _call_sites(mods: dict[str, Mod], mod: Mod, node: ast.Call) -> list[Site]:
    ctx = ctx_for(mods, mod, node)
    out: list[Site] = []
    func = node.func
    if not isinstance(func, ast.Attribute):
        return out
    chain = _chain(node)
    attrs = [c[0] for c in chain]
    fmt = None
    mode = None
    for attr, call, _ in chain:
        if attr == "format" and call and call.args:
            vals = eval_str(ctx, call.args[0])
            fmt = vals[0] if vals else None
        if attr == "mode" and call and call.args:
            vals = eval_str(ctx, call.args[0])
            mode = vals[0] if vals else None
        if attr == "outputMode" and call and call.args:
            vals = eval_str(ctx, call.args[0])
            mode = vals[0] if vals else None
    # df.write....save(path) / df.writeStream....start(path)
    if func.attr in WRITE_TERMINALS and ("write" in attrs or "writeStream" in attrs):
        path_node = node.args[0] if node.args else None
        for kw in node.keywords:
            if kw.arg == "path":
                path_node = kw.value
        df_node = next(
            (recv for attr, _, recv in chain if attr in ("write", "writeStream")), None
        )
        stream = "writeStream" in attrs
        if path_node is not None:
            for env in site_envs(ctx, node):
                for tid in lake_ids(eval_str(ctx, path_node, env)):
                    out.append(
                        Site(
                            "write",
                            tid,
                            mod,
                            node.lineno,
                            ctx.func,
                            mode,
                            fmt or func.attr,
                            df_node,
                            env,
                            ("writeStream." if stream else "write.") + func.attr,
                            node,
                        )
                    )
    # SinkFactory.create("delta", path, mode=...).write(df)
    if (
        func.attr == "write"
        and isinstance(func.value, ast.Call)
        and ast.unparse(func.value.func).endswith("SinkFactory.create")
        and len(func.value.args) >= 2
    ):
        create = func.value
        kinds = eval_str(ctx, create.args[0])
        for env in site_envs(ctx, node):
            for tid in lake_ids(eval_str(ctx, create.args[1], env)):
                out.append(
                    Site(
                        "write",
                        tid,
                        mod,
                        node.lineno,
                        ctx.func,
                        _const_kw(create, "mode") or "append",
                        kinds[0] if kinds else None,
                        node.args[0] if node.args else None,
                        env,
                        "SinkFactory.create().write",
                        node,
                    )
                )
    # spark.read....load(path)
    if func.attr == "load" and ("read" in attrs or "readStream" in attrs):
        if node.args:
            for env in site_envs(ctx, node):
                for tid in lake_ids(eval_str(ctx, node.args[0], env)):
                    out.append(
                        Site(
                            "read",
                            tid,
                            mod,
                            node.lineno,
                            ctx.func,
                            None,
                            fmt,
                            None,
                            env,
                            ("readStream" if "readStream" in attrs else "read")
                            + ".load",
                            node,
                        )
                    )
    return out


def _duckdb_sites(mods: dict[str, Mod], mod: Mod, node: ast.AST) -> list[Site]:
    ctx = ctx_for(mods, mod, node)
    out: list[Site] = []
    text = ast.unparse(node)
    if "delta_scan(" not in text:
        return out
    seen = set()
    for env in site_envs(ctx, node):
        for value in eval_str(ctx, node, env):
            for tid in lake_ids(value):
                if tid in seen:
                    continue
                seen.add(tid)
                out.append(
                    Site(
                        "read",
                        tid,
                        mod,
                        getattr(node, "lineno", 0),
                        ctx.func,
                        None,
                        "delta (DuckDB delta_scan)",
                        None,
                        env,
                        "duckdb delta_scan",
                        node,
                    )
                )
    return out


# ----------------------------------------------------------------- tracing


@dataclass
class Trace:
    ops: list[dict[str, Any]] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    funcs: list[dict[str, Any]] = field(default_factory=list)
    table_ops: list[dict[str, Any]] = field(default_factory=list)
    selected: list[str] = field(default_factory=list)


def _docstring(node: ast.AST) -> tuple[str, int] | None:
    if not isinstance(
        node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)
    ):
        return None
    doc = ast.get_docstring(node)
    if not doc or not node.body:
        return None
    first = node.body[0]
    return doc, getattr(first, "lineno", 1)


def _func_record(mod: Mod, qual: str) -> dict[str, Any]:
    fn = mod.funcs[qual]
    doc = _docstring(fn)
    return {
        "function": f"{mod.name}.{qual}",
        "file": rel(mod.path),
        "line": fn.lineno,
        "doc": doc[0] if doc else None,
        "doc_line": doc[1] if doc else None,
    }


def _comment_for(mod: Mod, line: int) -> tuple[str, int] | None:
    """Trailing comment on ``line`` or the comment block right above it."""
    if line in mod.trailing_comments:
        return mod.trailing_comments[line], line
    block: list[str] = []
    cur = line - 1
    while cur in mod.full_comments:
        block.insert(0, mod.full_comments[cur])
        cur -= 1
    if block:
        return " ".join(block), cur + 1
    return None


def _branch_condition(mod: Mod, node: ast.AST) -> str | None:
    """``if <test>`` / ``else of <test>`` when ``node`` sits inside an if-branch."""
    child: ast.AST = node
    cur = mod.parents.get(id(node))
    while cur is not None and not isinstance(
        cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)
    ):
        if isinstance(cur, ast.If):
            test = ast.unparse(cur.test)
            in_body = any(child is s for s in cur.body)
            return f"if {test}" if in_body else f"else (not {test})"
        child = cur
        cur = mod.parents.get(id(cur))
    return None


def _helpers_in(ctx: Ctx, expr: ast.AST) -> list[dict[str, Any]]:
    """Repo functions called inside a column expression, with their bodies."""
    out = []
    for sub in ast.walk(expr):
        if isinstance(sub, ast.Call):
            target = resolve_function(ctx, sub.func)
            if target is None:
                continue
            mod, qual = target
            fn = mod.funcs[qual]
            record = _func_record(mod, qual)
            returns = [
                ast.unparse(r.value)
                for r in ast.walk(fn)
                if isinstance(r, ast.Return) and r.value is not None
            ]
            record["returns"] = returns
            out.append(record)
    return out


def _schema_fields(ctx: Ctx, expr: ast.AST, depth: int = 0) -> list[dict[str, Any]]:
    """``StructType().add("c", T())`` chains reachable from ``from_json(x, SCHEMA)``."""
    out: list[dict[str, Any]] = []
    if depth > MAX_DEPTH:
        return out
    for sub in ast.walk(expr):
        if not (
            isinstance(sub, ast.Call) and ast.unparse(sub.func).endswith("from_json")
        ):
            continue
        if len(sub.args) < 2:
            continue
        schema = sub.args[1]
        sctx, snode = ctx, schema
        schema_name = ast.unparse(schema)
        if isinstance(schema, ast.Name):
            found = _module_value(ctx, schema.id)
            local = _local_assignments(ctx, schema.id, sub.lineno)
            if local:
                snode = local[-1]
            elif found:
                sctx, snode = found
        for attr, call, _ in reversed(_chain(snode)):
            if attr == "add" and call and len(call.args) >= 2:
                names = eval_str(sctx, call.args[0])
                if names:
                    out.append(
                        {
                            "column": names[0],
                            "kind": "from_json field",
                            "expr": f"{schema_name}: {ast.unparse(call.args[1])}",
                            "file": rel(sctx.mod.path),
                            "line": call.args[0].lineno,
                        }
                    )
    return out


class Tracer:
    def __init__(self, mods: dict[str, Mod]) -> None:
        self.mods = mods

    def trace(self, ctx: Ctx, node: ast.AST, env: Env) -> Trace:
        result = Trace()
        self._visit(ctx, node, env, result, set(), 0)
        return result

    def _op(
        self,
        result: Trace,
        ctx: Ctx,
        column: str,
        kind: str,
        expr: str,
        line: int,
        helper_expr: ast.AST | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "column": column,
            "kind": kind,
            "expr": expr,
            "file": rel(ctx.mod.path),
            "line": line,
            "function": f"{ctx.mod.name}.{ctx.func}" if ctx.func else ctx.mod.name,
        }
        comment = _comment_for(ctx.mod, line)
        if comment:
            record["comment"] = comment[0]
            record["comment_line"] = comment[1]
        if helper_expr is not None:
            record["helpers"] = _helpers_in(ctx, helper_expr)
        result.ops.append(record)
        if helper_expr is not None:
            for fld in _schema_fields(ctx, helper_expr):
                fld["function"] = record["function"]
                result.ops.append(fld)

    def _visit(
        self,
        ctx: Ctx,
        node: ast.AST,
        env: Env,
        result: Trace,
        seen: set[tuple[int, str | None]],
        depth: int,
    ) -> None:
        key = (id(node), ctx.func)
        if key in seen or depth > 40:
            return
        seen.add(key)
        if isinstance(node, ast.Name):
            self._visit_name(ctx, node, env, result, seen, depth)
            return
        if not isinstance(node, ast.Call):
            return
        func = node.func
        if isinstance(func, ast.Attribute):
            self._visit_method(ctx, node, func, env, result, seen, depth)
            return
        target = resolve_function(ctx, func)
        if target is None:
            return
        for arg in node.args:
            self._visit(ctx, arg, env, result, seen, depth + 1)
        for kw in node.keywords:
            self._visit(ctx, kw.value, env, result, seen, depth + 1)
        mod, qual = target
        record = _func_record(mod, qual)
        if record["function"] not in {f["function"] for f in result.funcs}:
            result.funcs.append(record)
        callee = Ctx(self.mods, mod, qual)
        for ret in ast.walk(mod.funcs[qual]):
            if isinstance(ret, ast.Return) and ret.value is not None:
                if mod.func_of.get(id(ret)) == qual:
                    self._visit(callee, ret.value, {}, result, seen, depth + 1)

    def _visit_name(
        self,
        ctx: Ctx,
        node: ast.Name,
        env: Env,
        result: Trace,
        seen: set[tuple[int, str | None]],
        depth: int,
    ) -> None:
        if node.id in env:
            self._visit(ctx, env[node.id], env, result, seen, depth + 1)
            return
        for value in _local_assignments(ctx, node.id, node.lineno):
            self._visit(ctx, value, env, result, seen, depth + 1)

    def _visit_method(
        self,
        ctx: Ctx,
        node: ast.Call,
        func: ast.Attribute,
        env: Env,
        result: Trace,
        seen: set[tuple[int, str | None]],
        depth: int,
    ) -> None:
        attr, recv = func.attr, func.value
        chain_attrs = [c[0] for c in _chain(node)]
        if attr == "load" and ("read" in chain_attrs or "readStream" in chain_attrs):
            if node.args:
                for tid in lake_ids(eval_str(ctx, node.args[0], env)):
                    if tid not in result.reads:
                        result.reads.append(tid)
            return
        if attr == "extract":
            creates = [recv] if isinstance(recv, ast.Call) else []
            if isinstance(recv, ast.Name):
                creates = [
                    v
                    for v in _local_assignments(ctx, recv.id, node.lineno)
                    if isinstance(v, ast.Call)
                ]
            for create in creates:
                self._extractor(ctx, create, env, result)
            return
        # post-order: visit the receiver (earlier steps) first
        self._visit(ctx, recv, env, result, seen, depth + 1)
        line = node.args[0].lineno if node.args else node.lineno
        if attr == "withColumn" and len(node.args) >= 2:
            for name in eval_str(ctx, node.args[0], env):
                self._op(
                    result,
                    ctx,
                    name,
                    "withColumn",
                    ast.unparse(node.args[1]),
                    line,
                    node.args[1],
                )
        elif attr == "withColumnRenamed" and len(node.args) >= 2:
            old = eval_str(ctx, node.args[0], env)
            for name in eval_str(ctx, node.args[1], env):
                self._op(
                    result,
                    ctx,
                    name,
                    "withColumnRenamed",
                    f"renamed from {old[0] if old else ast.unparse(node.args[0])}",
                    line,
                )
                if old:
                    result.ops[-1]["renamed_from"] = old[0]
        elif attr in ("select", "selectExpr"):
            self._select(ctx, node, attr, env, result)
        elif attr in TABLE_OPS:
            args = []
            for arg in node.args:
                if isinstance(arg, ast.Starred):
                    seq = eval_seq(ctx, arg.value)
                    vals = [v for c, e in (seq or []) for v in eval_str(c, e)]
                    args.append(repr(vals) if seq is not None else ast.unparse(arg))
                else:
                    args.append(ast.unparse(arg))
            result.table_ops.append(
                {
                    "op": attr,
                    "args": ", ".join(args),
                    "file": rel(ctx.mod.path),
                    "line": node.lineno,
                    "function": (
                        f"{ctx.mod.name}.{ctx.func}" if ctx.func else ctx.mod.name
                    ),
                    "condition": _branch_condition(ctx.mod, node),
                }
            )
            if attr in ("unionByName", "union", "join") and node.args:
                self._visit(ctx, node.args[0], env, result, seen, depth + 1)

    def _select(
        self, ctx: Ctx, node: ast.Call, attr: str, env: Env, result: Trace
    ) -> None:
        selected: list[str] = []
        for arg in node.args:
            if isinstance(arg, ast.Starred):
                for ectx, elem in eval_seq(ctx, arg.value) or []:
                    selected.extend(eval_str(ectx, elem))
                continue
            if attr == "selectExpr":
                for text in eval_str(ctx, arg, env):
                    match = re.match(r"(?is)\s*(.+?)\s+as\s+(\w+)\s*$", text)
                    if match:
                        self._op(
                            result,
                            ctx,
                            match.group(2),
                            "selectExpr",
                            match.group(1),
                            arg.lineno,
                        )
                        selected.append(match.group(2))
                continue
            consts = (
                eval_str(ctx, arg, env)
                if isinstance(arg, (ast.Constant, ast.Name))
                else []
            )
            if consts:
                selected.extend(consts)
                continue
            if (
                isinstance(arg, ast.Call)
                and isinstance(arg.func, ast.Attribute)
                and arg.func.attr == "alias"
                and arg.args
            ):
                for name in eval_str(ctx, arg.args[0], env):
                    self._op(
                        result,
                        ctx,
                        name,
                        "alias",
                        ast.unparse(arg.func.value),
                        arg.args[0].lineno,
                        arg.func.value,
                    )
                    selected.append(name)
        result.selected = selected

    def _extractor(self, ctx: Ctx, create: ast.Call, env: Env, result: Trace) -> None:
        if not ast.unparse(create.func).endswith("ExtractorFactory.create"):
            return
        if len(create.args) < 2:
            return
        kinds = eval_str(ctx, create.args[0], env)
        location = eval_str(ctx, create.args[1], env)
        topic = None
        for kw in create.keywords:
            if kw.arg == "topic":
                vals = eval_str(ctx, kw.value, env)
                topic = vals[0] if vals else None
        record: dict[str, Any] = {
            "kind": kinds[0] if kinds else None,
            "location": [loc.replace(REPO_MARK + "/", "") for loc in location],
            "topic": topic,
            "file": rel(ctx.mod.path),
            "line": create.lineno,
        }
        if kinds:
            impl = self._extractor_impl(kinds[0])
            if impl:
                record["reader"] = impl
        result.sources.append(record)

    def _extractor_impl(self, kind: str) -> dict[str, Any] | None:
        """The ``extract`` body of the class ``ExtractorFactory.create`` picks for kind."""
        for mod in self.mods.values():
            fn = mod.funcs.get("ExtractorFactory.create")
            if fn is None:
                continue
            for node in ast.walk(fn):
                if not (
                    isinstance(node, ast.If)
                    and isinstance(node.test, ast.Compare)
                    and len(node.test.comparators) == 1
                    and isinstance(node.test.comparators[0], ast.Constant)
                    and node.test.comparators[0].value == kind
                ):
                    continue
                for ret in node.body:
                    if not (
                        isinstance(ret, ast.Return)
                        and isinstance(ret.value, ast.Call)
                        and isinstance(ret.value.func, ast.Name)
                    ):
                        continue
                    cls = ret.value.func.id
                    target = mod.imports.get(cls, "")
                    cmod = self.mods.get(target.rsplit(".", 1)[0]) if target else mod
                    if cmod is None or f"{cls}.extract" not in cmod.funcs:
                        continue
                    method = cmod.funcs[f"{cls}.extract"]
                    returns = [
                        ast.unparse(r.value)
                        for r in ast.walk(method)
                        if isinstance(r, ast.Return) and r.value is not None
                    ]
                    return {
                        "class": f"{cmod.name}.{cls}",
                        "file": rel(cmod.path),
                        "line": method.lineno,
                        "returns": returns,
                    }
        return None


# ---------------------------------------------------- raw source generators


def raw_generators(mods: dict[str, Mod]) -> dict[str, dict[str, Any]]:
    """Repo scripts that write the raw files / Kafka events Bronze reads.

    Key: repo-relative file path, or ``kafka:<topic>``.
    Value: {"function", "file", "line", "columns": {col: {expr, line}}, "urls"}.
    """
    out: dict[str, dict[str, Any]] = {}
    for mod in mods.values():
        for qual, fn in mod.funcs.items():
            ctx = Ctx(mods, mod, qual)
            for node in ast.walk(fn):
                if mod.func_of.get(id(node)) != qual or not isinstance(node, ast.Call):
                    continue
                name = ast.unparse(node.func)
                if name == "open" and len(node.args) >= 2:
                    modes = eval_str(ctx, node.args[1])
                    if not modes or "w" not in modes[0]:
                        continue
                    for path in eval_str(ctx, node.args[0]):
                        key = path.replace(REPO_MARK + "/", "")
                        out[key] = _file_generator(ctx, fn, node, key)
                elif name.endswith(".send") and node.args:
                    for topic in eval_str(ctx, node.args[0]):
                        out[f"kafka:{topic}"] = _kafka_generator(ctx, node)
    return out


def _file_generator(
    ctx: Ctx, fn: ast.AST, open_call: ast.Call, path: str
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "function": f"{ctx.mod.name}.{ctx.func}",
        "file": rel(ctx.mod.path),
        "line": open_call.lineno,
        "columns": {},
        "urls": [],
        "doc": None,
    }
    doc = _docstring(fn)
    if doc:
        record["doc"] = doc[0]
        record["doc_line"] = doc[1]
    rows: list[ast.List] = []
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and ast.unparse(node.func).endswith(".writerow")
            and node.args
            and isinstance(node.args[0], ast.List)
        ):
            rows.append(node.args[0])
        if isinstance(node, ast.Call) and ast.unparse(node.func).split(".")[-1] in (
            "Request",
            "urlopen",
        ):
            if node.args:
                record["urls"].extend(
                    u for u in eval_str(ctx, node.args[0]) if u.startswith("http")
                )
    rows.sort(key=lambda r: r.lineno)
    header = next(
        (
            r
            for r in rows
            if all(
                isinstance(e, ast.Constant) and isinstance(e.value, str) for e in r.elts
            )
        ),
        None,
    )
    if header is not None:
        values = next(
            (r for r in rows if r is not header and len(r.elts) == len(header.elts)),
            None,
        )
        for i, elem in enumerate(header.elts):
            col = str(elem.value)  # type: ignore[attr-defined]
            entry: dict[str, Any] = {"header_line": elem.lineno}
            if values is not None:
                entry["expr"] = ast.unparse(values.elts[i])
                entry["line"] = values.elts[i].lineno
            record["columns"][col] = entry
    return record


def _kafka_generator(ctx: Ctx, send: ast.Call) -> dict[str, Any]:
    record: dict[str, Any] = {
        "function": f"{ctx.mod.name}.{ctx.func}",
        "file": rel(ctx.mod.path),
        "line": send.lineno,
        "columns": {},
        "urls": [],
        "doc": None,
    }
    value = next((kw.value for kw in send.keywords if kw.arg == "value"), None)
    if value is None and len(send.args) > 1:
        value = send.args[1]
    if not isinstance(value, ast.Name):
        return record
    for assigned in _local_assignments(ctx, value.id, send.lineno):
        if not isinstance(assigned, ast.Call):
            continue
        target = resolve_function(ctx, assigned.func)
        if target is None:
            continue
        mod, qual = target
        fn = mod.funcs[qual]
        record["event_function"] = f"{mod.name}.{qual}"
        doc = _docstring(fn)
        if doc:
            record["doc"] = doc[0]
            record["doc_line"] = doc[1]
            record["doc_file"] = rel(mod.path)
        for ret in ast.walk(fn):
            if isinstance(ret, ast.Return) and isinstance(ret.value, ast.Dict):
                for key, val in zip(ret.value.keys, ret.value.values, strict=True):
                    if isinstance(key, ast.Constant):
                        record["columns"][str(key.value)] = {
                            "expr": ast.unparse(val),
                            "line": val.lineno,
                            "file": rel(mod.path),
                        }
    return record


# ------------------------------------------------------------- entry points


def call_edges(mods: dict[str, Mod]) -> dict[str, set[str]]:
    """caller dotted name -> callee dotted names (repo functions only)."""
    edges: dict[str, set[str]] = {}
    for mod in mods.values():
        for node in ast.walk(mod.tree):
            if not isinstance(node, ast.Call):
                continue
            qual = mod.func_of.get(id(node))
            caller = f"{mod.name}.{qual}" if qual else _main_or_module(mod, node)
            target = resolve_function(Ctx(mods, mod, qual), node.func)
            if target is None:
                continue
            edges.setdefault(caller, set()).add(f"{target[0].name}.{target[1]}")
    return edges


def _main_or_module(mod: Mod, node: ast.AST) -> str:
    cur = mod.parents.get(id(node))
    while cur is not None:
        if isinstance(cur, ast.If) and "__main__" in ast.unparse(cur.test):
            return f"{mod.name}:__main__"
        cur = mod.parents.get(id(cur))
    return f"{mod.name}:<module>"


def main_lines(mods: dict[str, Mod]) -> dict[str, int]:
    out = {}
    for mod in mods.values():
        for stmt in mod.tree.body:
            if isinstance(stmt, ast.If) and "__main__" in ast.unparse(stmt.test):
                out[f"{mod.name}:__main__"] = stmt.lineno
    return out


def dag_tasks(mods: dict[str, Mod]) -> list[dict[str, Any]]:
    tasks = []
    for mod in mods.values():
        if not mod.name.startswith("dags."):
            continue
        dag_id = None
        for node in ast.walk(mod.tree):
            if isinstance(node, ast.Call) and ast.unparse(node.func) == "DAG":
                dag_id = _const_kw(node, "dag_id")
        for node in ast.walk(mod.tree):
            if not (
                isinstance(node, ast.Call)
                and ast.unparse(node.func).endswith("Operator")
            ):
                continue
            ctx = ctx_for(mods, mod, node)
            callable_node = next(
                (kw.value for kw in node.keywords if kw.arg == "python_callable"), None
            )
            target = resolve_function(ctx, callable_node) if callable_node else None
            outlets: list[str] = []
            inlets: list[str] = []
            for kw in node.keywords:
                if kw.arg in ("outlets", "inlets"):
                    seq = eval_seq(ctx, kw.value) or []
                    ids = lake_ids([v for c, e in seq for v in eval_str(c, e)])
                    (outlets if kw.arg == "outlets" else inlets).extend(ids)
            tasks.append(
                {
                    "dag_id": dag_id,
                    "task_id": _const_kw(node, "task_id"),
                    "callable": (
                        f"{target[0].name}.{target[1]}" if target is not None else None
                    ),
                    "file": rel(mod.path),
                    "line": node.lineno,
                    "outlets": outlets,
                    "inlets": inlets,
                }
            )
    return tasks


def compose_services() -> tuple[list[dict[str, Any]], str | None]:
    """Services whose command runs ``python -m <module>``."""
    import yaml

    path = REPO_ROOT / "docker-compose.yml"
    if not path.exists():
        return [], None
    text = path.read_text(encoding="utf-8")
    root = yaml.compose(text)
    out: list[dict[str, Any]] = []
    if not isinstance(root, yaml.MappingNode):
        return out, rel(path)
    for key, value in root.value:
        if key.value != "services" or not isinstance(value, yaml.MappingNode):
            continue
        for skey, sval in value.value:
            if not isinstance(sval, yaml.MappingNode):
                continue
            for ckey, cval in sval.value:
                if ckey.value != "command":
                    continue
                command = (
                    " ".join(str(v.value) for v in cval.value)
                    if isinstance(cval, yaml.SequenceNode)
                    else str(cval.value)
                )
                match = re.search(r"python3?\s+-m\s+([\w.]+)", command)
                if match:
                    out.append(
                        {
                            "service": skey.value,
                            "module": match.group(1),
                            "file": rel(path),
                            "line": cval.start_mark.line + 1,
                        }
                    )
    return out, rel(path)


def entry_chains(
    writer: str,
    edges: dict[str, set[str]],
    tasks: list[dict[str, Any]],
    services: list[dict[str, Any]],
    mains: dict[str, int],
    mods: dict[str, Mod],
) -> list[dict[str, Any]]:
    """Ways ``writer`` gets started, each as a call path from an entry point."""

    def path_to(start: str) -> list[str] | None:
        frontier = [[start]]
        seen = {start}
        while frontier:
            cur = frontier.pop(0)
            if cur[-1] == writer:
                return cur
            for nxt in sorted(edges.get(cur[-1], ())):
                if nxt not in seen:
                    seen.add(nxt)
                    frontier.append(cur + [nxt])
        return None

    chains: list[dict[str, Any]] = []
    for task in tasks:
        if task["callable"]:
            found = path_to(task["callable"])
            if found:
                chains.append(
                    {
                        "kind": "airflow",
                        "label": f"Airflow task {task['dag_id']}.{task['task_id']}",
                        "file": task["file"],
                        "line": task["line"],
                        "path": found,
                    }
                )
    for main, line in mains.items():
        found = path_to(main)
        if not found:
            continue
        module = main.split(":")[0]
        file = rel(mods[module].path) if module in mods else module
        chains.append(
            {
                "kind": "main",
                "label": f"python -m {module}",
                "file": file,
                "line": line,
                "path": found,
            }
        )
        for svc in services:
            if svc["module"] == module:
                chains.append(
                    {
                        "kind": "compose",
                        "label": f"docker compose service {svc['service']}",
                        "service_id": f"service.{svc['service']}",
                        "file": svc["file"],
                        "line": svc["line"],
                        "path": [f"python -m {module}"] + found,
                    }
                )
    return chains
