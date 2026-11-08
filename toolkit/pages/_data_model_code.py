"""Static half of the data-model page: what the code says about tables and keys.

Everything here is parsed from the repo, never typed in:

* ``interpret_repo`` runs a tiny abstract interpreter over every module in
  ``src/`` and ``dags/`` (``ast``). It follows PySpark DataFrame chains from
  ``.load(<lake path>)`` through ``withColumn`` / ``withColumnRenamed`` /
  ``select`` / ``drop`` / ``dropDuplicates`` / ``unionByName`` and calls to the
  project's own transform functions, down to ``.save(<lake path>)`` /
  ``.start(<lake path>)`` / ``SinkFactory.create(...).write(df)``. The result is
  the table flows, column-level lineage (with the exact expression and
  file:line of every step), dedup keys, surrogate keys, temp views and
  ``partitionBy`` calls.
* ``ge_expectations`` reads the Great Expectations suites and maps them to lake
  tables through the same dict keys the quality gate uses at runtime.
* ``docstring_claims``, ``semantic_layer``, ``sql_strings`` and
  ``notebook_probes`` read stated intent (docstrings, semantic/*.yml, SQL text
  inside the code) with line numbers.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

from toolkit.common import REPO_ROOT, rel

CODE_DIRS = ("src", "dags")
SQL_SCAN_DIRS = ("src", "dags", "tests")
LAKE_PATH_RE = re.compile(r"^s3a?://lakehouse/(bronze|silver|gold)/([A-Za-z0-9_]+)/?$")
HASH_BUILTINS = frozenset({"sha2", "sha1", "md5", "hash", "xxhash64", "crc32"})
CONCAT_FUNCS = frozenset({"concat", "concat_ws"})
GENERATOR_FUNCS = frozenset(
    {"lit", "current_timestamp", "current_date", "uuid", "monotonically_increasing_id"}
)
ONE_TO_ONE = frozenset({"copy", "cast", "hash", "rename", "select", "field", "pass"})
IDENTITY_METHODS = frozenset(
    {
        "filter",
        "where",
        "orderBy",
        "sort",
        "limit",
        "cache",
        "persist",
        "repartition",
        "coalesce",
        "alias",
        "hint",
        "na",
        "fillna",
        "dropna",
        "sample",
    }
)
OPAQUE_METHODS = frozenset(
    {
        "selectExpr",
        "groupBy",
        "agg",
        "pivot",
        "toDF",
        "join",
        "crossJoin",
        "count",
        "collect",
        "show",
    }
)
WRITER_ATTRS = frozenset({"write", "writeStream"})
SINK_METHODS = frozenset({"save", "start", "saveAsTable", "toTable"})

_TABLE_ID = re.compile(r"^(bronze|silver|gold)\.[A-Za-z0-9_]+$")
Step = tuple[str, str, str, int]
"""(kind, expression source, file, line) -- one transformation applied to a column."""


def lake_table_id(path: Any) -> str | None:
    """``s3a://lakehouse/silver/orders_batch`` -> ``silver.orders_batch``."""
    if not isinstance(path, str):
        return None
    match = LAKE_PATH_RE.match(path.strip())
    if not match or match.group(2).startswith("_"):
        return None
    return f"{match.group(1)}.{match.group(2)}"


def _short(node: ast.AST, limit: int = 140) -> str:
    text = " ".join(ast.unparse(node).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ------------------------------------------------------------------ values


@dataclass(frozen=True)
class Input:
    """One upstream column feeding a column: (source, column, steps applied)."""

    source: str  # "<layer>.<table>", "param:<name>", "external:<what>", "generated", "unresolved"
    col: str
    steps: tuple[Step, ...] = ()

    def then(self, step: Step) -> Input:
        return replace(self, steps=self.steps + (step,))


@dataclass
class Dedup:
    names: tuple[str, ...]
    keys: dict[str, list[Input]]
    file: str
    line: int
    streaming: str | None = None


@dataclass
class Surrogate:
    column: str
    components: tuple[str, ...]
    expr: str
    file: str
    line: int


@dataclass
class DF:
    explicit: dict[str, list[Input]] = field(default_factory=dict)
    passthrough: list[tuple[str, tuple[Step, ...]]] = field(default_factory=list)
    dropped: set[str] = field(default_factory=set)
    dedups: list[Dedup] = field(default_factory=list)
    surrogates: list[Surrogate] = field(default_factory=list)
    via: list[tuple[str, str, int]] = field(default_factory=list)
    watermark: str | None = None
    opaque: bool = False

    @staticmethod
    def source(name: str) -> DF:
        return DF(passthrough=[(name, ())])

    def copy(self) -> DF:
        return DF(
            explicit={k: list(v) for k, v in self.explicit.items()},
            passthrough=list(self.passthrough),
            dropped=set(self.dropped),
            dedups=list(self.dedups),
            surrogates=list(self.surrogates),
            via=list(self.via),
            watermark=self.watermark,
            opaque=self.opaque,
        )

    def resolve(self, name: str) -> list[Input] | None:
        if name in self.explicit:
            return self.explicit[name]
        if "." in name and name.split(".")[0] in self.explicit:
            base = name.split(".")[0]
            return [Input(i.source, i.col, i.steps) for i in self.explicit[base]]
        if name in self.dropped or not self.passthrough:
            return None
        return [Input(src, name, steps) for src, steps in self.passthrough]

    def sources(self) -> set[str]:
        found = {src for src, _ in self.passthrough}
        for inputs in self.explicit.values():
            found.update(i.source for i in inputs)
        return found


@dataclass
class Writer:
    df: DF
    mode: str | None = None
    partition_by: tuple[str, ...] = ()
    fmt: str | None = None


@dataclass
class SinkCall:
    path: Any
    mode: str | None
    partition_by: tuple[str, ...]


@dataclass
class Items:
    mapping: dict[str, Any]


@dataclass
class Sink:
    table: str
    df: DF
    file: str
    line: int
    function: str
    mode: str | None
    partition_by: tuple[str, ...]
    fmt: str | None


# ------------------------------------------------------------------ interpreter


class Project:
    def __init__(self, files: list[Path]) -> None:
        self.modules: dict[str, ModuleInterp] = {}
        self.sinks: list[Sink] = []
        self.views: dict[str, tuple[str, str, int]] = {}
        self.partition_calls: list[tuple[str, tuple[str, ...], str, int]] = []
        self.errors: list[str] = []
        self.all_dedups: dict[tuple[str, int], tuple[str, ...]] = {}
        self.hash_funcs: set[str] = set(HASH_BUILTINS)
        self._summaries: dict[
            tuple[str, str, tuple[tuple[str, Any], ...]], DF | None
        ] = {}
        self._in_progress: set[tuple[str, str, tuple[tuple[str, Any], ...]]] = set()
        for path in files:
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (SyntaxError, UnicodeDecodeError) as error:
                self.errors.append(f"{rel(path)}: not parsed ({error})")
                continue
            dotted = ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts)
            self.modules[dotted] = ModuleInterp(self, path, dotted, tree)
        for module in self.modules.values():
            for name, func in module.funcs.items():
                if any(
                    isinstance(n, ast.Call) and _call_name(n) in HASH_BUILTINS
                    for n in ast.walk(func)
                ):
                    self.hash_funcs.add(name)

    def run(self) -> None:
        for module in self.modules.values():
            module.run_standalone()

    def summary(
        self, module: ModuleInterp, name: str, consts: dict[str, Any] | None = None
    ) -> DF | None:
        """Effect of a transform function, its DataFrame params left symbolic.

        Constant str/bool arguments are bound, so ``if is_streaming:`` takes the
        branch the caller actually selects.
        """
        bound = tuple(sorted((consts or {}).items()))
        key = (module.dotted, name, bound)
        if key in self._summaries:
            return self._summaries[key]
        if key in self._in_progress:
            return None
        self._in_progress.add(key)
        func = module.funcs[name]
        env: dict[str, Any] = {
            a.arg: DF.source(f"param:{a.arg}") for a in _params(func)
        }
        env.update(dict(bound))
        result = module.exec_block(func.body, env, record=False)
        self._in_progress.discard(key)
        summary = result if isinstance(result, DF) else None
        self._summaries[key] = summary
        return summary

    def find_function(
        self, module: ModuleInterp, name: str
    ) -> tuple[ModuleInterp, ast.FunctionDef] | None:
        if name in module.funcs:
            return module, module.funcs[name]
        target = module.imports.get(name)
        if target:
            mod_name, _, func_name = target.rpartition(".")
            other = self.modules.get(mod_name)
            if other and func_name in other.funcs:
                return other, other.funcs[func_name]
        return None


def _params(func: ast.FunctionDef) -> list[ast.arg]:
    return [*func.args.posonlyargs, *func.args.args, *func.args.kwonlyargs]


def _call_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _call_line(node: ast.Call) -> int:
    func = node.func
    return int(getattr(func, "end_lineno", None) or node.lineno)


class ModuleInterp:
    def __init__(
        self, project: Project, path: Path, dotted: str, tree: ast.Module
    ) -> None:
        self.project = project
        self.path = path
        self.file = rel(path)
        self.dotted = dotted
        self.tree = tree
        self.funcs: dict[str, ast.FunctionDef] = {}
        self.imports: dict[str, str] = {}
        self.consts: dict[str, Any] = {}
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                self.funcs[node.name] = node
            elif isinstance(node, ast.ImportFrom) and node.module:
                for alias in node.names:
                    self.imports[alias.asname or alias.name] = (
                        f"{node.module}.{alias.name}"
                    )
        for node in tree.body:
            if isinstance(node, ast.Assign | ast.AnnAssign):
                targets = (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
                value = node.value
                if value is None:
                    continue
                for target in targets:
                    if isinstance(target, ast.Name):
                        evaluated = self.eval(value, {}, record=False, const_only=True)
                        if evaluated is not None:
                            self.consts[target.id] = evaluated
        self.current_func = "<module>"

    # -- statements

    def run_standalone(self) -> None:
        top = [
            n
            for n in self.tree.body
            if not isinstance(n, ast.FunctionDef | ast.ClassDef)
        ]
        self.current_func = "<module>"
        self._safe_exec(top, {}, "<module>")
        for name, func in self.funcs.items():
            self.current_func = name
            self._safe_exec(func.body, {}, name)

    def _safe_exec(self, body: list[ast.stmt], env: dict[str, Any], where: str) -> None:
        try:
            self.exec_block(body, env, record=True)
        except Exception as error:  # the interpreter must never break the build
            self.project.errors.append(
                f"{self.file}:{where}: interpreter error {type(error).__name__}: {error}"
            )

    def exec_block(
        self, body: list[ast.stmt], env: dict[str, Any], *, record: bool
    ) -> Any:
        for stmt in body:
            if isinstance(stmt, ast.Return):
                return self.eval(stmt.value, env, record=record) if stmt.value else None
            if isinstance(stmt, ast.Assign | ast.AnnAssign):
                targets = (
                    stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
                )
                if stmt.value is None:
                    continue
                value = self.eval(stmt.value, env, record=record)
                for target in targets:
                    if isinstance(target, ast.Name):
                        env[target.id] = value
            elif isinstance(stmt, ast.Expr):
                self.eval(stmt.value, env, record=record)
            elif isinstance(stmt, ast.If):
                test = (
                    self.eval(stmt.test, env, record=record)
                    if isinstance(stmt.test, ast.Name)
                    else None
                )
                if isinstance(test, bool):
                    ret = self.exec_block(
                        stmt.body if test else stmt.orelse, env, record=record
                    )
                    if ret is not None:
                        return ret
                    continue
                left, right = dict(env), dict(env)
                ret_l = self.exec_block(stmt.body, left, record=record)
                ret_r = self.exec_block(stmt.orelse, right, record=record)
                for name in set(left) | set(right):
                    a, b = left.get(name), right.get(name)
                    if isinstance(a, DF) and isinstance(b, DF) and a is not b:
                        env[name] = _merge_branches(a, b)
                    else:
                        env[name] = a if a is not None else b
                if ret_l is not None or ret_r is not None:
                    if isinstance(ret_l, DF) and isinstance(ret_r, DF):
                        return _merge_branches(ret_l, ret_r)
                    return ret_l if ret_l is not None else ret_r
            elif isinstance(stmt, ast.For):
                self._exec_for(stmt, env, record=record)
            elif isinstance(stmt, ast.Try):
                ret = self.exec_block(stmt.body, env, record=record)
                self.exec_block(stmt.finalbody, env, record=record)
                if ret is not None:
                    return ret
            elif isinstance(stmt, ast.With):
                ret = self.exec_block(stmt.body, env, record=record)
                if ret is not None:
                    return ret
        return None

    def _exec_for(self, stmt: ast.For, env: dict[str, Any], *, record: bool) -> None:
        iterable = self.eval(stmt.iter, env, record=record)
        target = stmt.target
        if (
            isinstance(iterable, Items)
            and isinstance(target, ast.Tuple)
            and len(target.elts) == 2
        ):
            key_t, val_t = target.elts
            for key, value in iterable.mapping.items():
                loop_env = dict(env)
                if isinstance(key_t, ast.Name):
                    loop_env[key_t.id] = key
                if isinstance(val_t, ast.Name):
                    loop_env[val_t.id] = value
                self.exec_block(stmt.body, loop_env, record=record)
            return
        if isinstance(iterable, tuple) and isinstance(target, ast.Name):
            for value in iterable:
                loop_env = dict(env)
                loop_env[target.id] = value
                self.exec_block(stmt.body, loop_env, record=record)
            return
        self.exec_block(stmt.body, dict(env), record=record)

    # -- expressions

    def eval(
        self,
        node: ast.AST,
        env: dict[str, Any],
        *,
        record: bool,
        const_only: bool = False,
    ) -> Any:
        if isinstance(node, ast.Constant):
            return node.value if isinstance(node.value, str | bool) else None
        if isinstance(node, ast.Name):
            if node.id in env:
                return env[node.id]
            return self.consts.get(node.id)
        if isinstance(node, ast.List | ast.Tuple):
            items = [
                self.eval(e, env, record=record, const_only=const_only)
                for e in node.elts
            ]
            return (
                tuple(items)
                if items and all(isinstance(i, str) for i in items)
                else None
            )
        if isinstance(node, ast.Dict):
            out: dict[str, Any] = {}
            for k, v in zip(node.keys, node.values, strict=True):
                key = (
                    self.eval(k, env, record=record, const_only=const_only)
                    if k is not None
                    else None
                )
                if isinstance(key, str):
                    out[key] = self.eval(v, env, record=record, const_only=const_only)
            return out
        if isinstance(node, ast.Subscript):
            base = self.eval(node.value, env, record=record, const_only=const_only)
            key = self.eval(node.slice, env, record=record, const_only=const_only)
            if isinstance(base, dict) and isinstance(key, str):
                return base.get(key)
            return None
        if const_only:
            return None
        if isinstance(node, ast.Attribute):
            recv = self.eval(node.value, env, record=record)
            if isinstance(recv, DF) and node.attr in WRITER_ATTRS:
                return Writer(recv)
            return None
        if isinstance(node, ast.Call):
            return self.call(node, env, record=record)
        return None

    def call(self, node: ast.Call, env: dict[str, Any], *, record: bool) -> Any:
        func = node.func
        args = node.args
        kwargs = {k.arg: k.value for k in node.keywords if k.arg}
        if isinstance(func, ast.Attribute):
            method = func.attr
            recv = self.eval(func.value, env, record=record)
            if isinstance(recv, DF):
                return self.df_method(recv, method, node, env, record=record)
            if isinstance(recv, Writer):
                return self.writer_method(recv, method, node, env, record=record)
            if isinstance(recv, SinkCall) and method == "write" and args:
                df = self.eval(args[0], env, record=record)
                if record and isinstance(df, DF):
                    self.record_sink(
                        df, recv.path, node, recv.mode, recv.partition_by, "delta"
                    )
                return None
            if isinstance(recv, dict) and method == "items":
                return Items(recv)
            if method == "load" and args:
                table = lake_table_id(self.eval(args[0], env, record=record))
                if table:
                    return DF.source(table)
                return None
            if method == "extract":
                return DF(
                    passthrough=[(f"external:{_short(func.value, 80)}", ())],
                    opaque=True,
                )
            if (
                method == "create"
                and isinstance(func.value, ast.Name)
                and func.value.id == "SinkFactory"
            ):
                path = self.eval(args[1], env, record=record) if len(args) > 1 else None
                mode = (
                    self.eval(kwargs["mode"], env, record=record)
                    if "mode" in kwargs
                    else None
                )
                part = (
                    self.eval(kwargs["partition_by"], env, record=record)
                    if "partition_by" in kwargs
                    else None
                )
                parts = (
                    (part,)
                    if isinstance(part, str)
                    else part if isinstance(part, tuple) else ()
                )
                return SinkCall(path, mode if isinstance(mode, str) else None, parts)
            for arg in args:
                self.eval(arg, env, record=record)
            return None
        if isinstance(func, ast.Name):
            found = self.project.find_function(self, func.id)
            if found:
                return self.apply_function(found[0], found[1], node, env, record=record)
            for arg in args:
                self.eval(arg, env, record=record)
        return None

    def apply_function(
        self,
        module: ModuleInterp,
        func: ast.FunctionDef,
        node: ast.Call,
        env: dict[str, Any],
        *,
        record: bool,
    ) -> Any:
        bound: dict[str, Any] = {}
        params = _params(func)
        for param, arg in zip(params, node.args, strict=False):
            bound[param.arg] = self.eval(arg, env, record=record)
        for keyword in node.keywords:
            if keyword.arg:
                bound[keyword.arg] = self.eval(keyword.value, env, record=record)
        consts = {k: v for k, v in bound.items() if isinstance(v, str | bool)}
        summary = self.project.summary(module, func.name, consts)
        if summary is None:
            return None
        result = summary.copy()
        for param, value in bound.items():
            if isinstance(value, DF):
                result = _substitute(result, f"param:{param}", value)
        result.via.append((f"{module.dotted}.{func.name}", module.file, func.lineno))
        return result

    def writer_method(
        self,
        writer: Writer,
        method: str,
        node: ast.Call,
        env: dict[str, Any],
        *,
        record: bool,
    ) -> Any:
        args = [self.eval(a, env, record=record) for a in node.args]
        if method in SINK_METHODS:
            path = args[0] if args else None
            for keyword in node.keywords:
                if keyword.arg == "path":
                    path = self.eval(keyword.value, env, record=record)
            if record:
                self.record_sink(
                    writer.df, path, node, writer.mode, writer.partition_by, writer.fmt
                )
            return None
        if method in ("mode", "outputMode") and args and isinstance(args[0], str):
            writer.mode = args[0]
        elif method == "format" and args and isinstance(args[0], str):
            writer.fmt = args[0]
        elif method == "partitionBy":
            writer.partition_by = tuple(a for a in args if isinstance(a, str))
            self.project.partition_calls.append(
                ("<writer>", writer.partition_by, self.file, _call_line(node))
            )
        return writer

    def record_sink(
        self,
        df: DF,
        path: Any,
        node: ast.Call,
        mode: str | None,
        partition_by: tuple[str, ...],
        fmt: str | None,
    ) -> None:
        table = lake_table_id(path)
        if not table:
            return
        self.project.sinks.append(
            Sink(
                table=table,
                df=df,
                file=self.file,
                line=_call_line(node),
                function=f"{self.dotted}.{self.current_func}",
                mode=mode,
                partition_by=partition_by,
                fmt=fmt,
            )
        )

    # -- DataFrame methods

    def df_method(
        self, df: DF, method: str, node: ast.Call, env: dict[str, Any], *, record: bool
    ) -> Any:
        line = _call_line(node)
        args = node.args
        if method == "withColumn" and len(args) >= 2:
            name = self.eval(args[0], env, record=record)
            if not isinstance(name, str):
                return _opaque(df)
            out = df.copy()
            out.explicit[name] = self.column_inputs(df, args[1], line)
            out.dropped.discard(name)
            kind = self.expr_kind(args[1])
            if kind == "concat":
                refs = tuple(_column_refs(args[1]))
                if len(refs) >= 2:
                    out.surrogates.append(
                        Surrogate(name, refs, _short(args[1]), self.file, line)
                    )
            return out
        if method == "withColumnRenamed" and len(args) >= 2:
            old, new = (self.eval(a, env, record=record) for a in args[:2])
            if not (isinstance(old, str) and isinstance(new, str)):
                return _opaque(df)
            out = df.copy()
            step: Step = ("rename", f"{old} -> {new}", self.file, line)
            out.explicit[new] = [
                i.then(step) for i in (df.resolve(old) or [Input("unresolved", old)])
            ]
            out.explicit.pop(old, None)
            out.dropped.add(old)
            out.dropped.discard(new)
            return out
        if method == "drop":
            out = df.copy()
            for name in self._names(args, env, record=record):
                out.explicit.pop(name, None)
                out.dropped.add(name)
            return out
        if method == "select":
            return self.select(df, args, env, line, record=record)
        if method in ("dropDuplicates", "drop_duplicates", "distinct"):
            names = tuple(self._names(args, env, record=record))
            out = df.copy()
            if names:
                self.project.all_dedups[(self.file, line)] = names
                out.dedups.append(
                    Dedup(
                        names,
                        {n: df.resolve(n) or [] for n in names},
                        self.file,
                        line,
                        df.watermark,
                    )
                )
            return out
        if method == "withWatermark":
            values = [self.eval(a, env, record=record) for a in args]
            out = df.copy()
            out.watermark = (
                " ".join(v for v in values if isinstance(v, str)) or "watermark"
            )
            return out
        if method in ("unionByName", "union", "unionAll") and args:
            other = self.eval(args[0], env, record=record)
            if isinstance(other, DF):
                return _union(df, other)
            return _opaque(df)
        if (
            method
            in (
                "createOrReplaceTempView",
                "createTempView",
                "createOrReplaceGlobalTempView",
            )
            and args
        ):
            view = self.eval(args[0], env, record=record)
            tables = [s for s in df.sources() if _TABLE_ID.match(s)]
            if isinstance(view, str) and len(tables) == 1:
                self.project.views[view] = (tables[0], self.file, line)
            return None
        if method in IDENTITY_METHODS:
            for arg in args:
                self.eval(arg, env, record=record)
            return df
        if method in OPAQUE_METHODS:
            return _opaque(df)
        return _opaque(df)

    def select(
        self,
        df: DF,
        args: list[ast.expr],
        env: dict[str, Any],
        line: int,
        *,
        record: bool,
    ) -> DF:
        out = DF(
            dedups=list(df.dedups),
            surrogates=list(df.surrogates),
            via=list(df.via),
            watermark=df.watermark,
        )
        for arg in args:
            if isinstance(arg, ast.Starred):
                names = self.eval(arg.value, env, record=record)
                if isinstance(names, tuple):
                    expr = _short(arg)
                    for name in names:
                        step: Step = ("select", expr, self.file, line)
                        out.explicit[name] = [
                            i.then(step)
                            for i in (df.resolve(name) or [Input("unresolved", name)])
                        ]
                else:
                    out.opaque = True
                continue
            value = (
                self.eval(arg, env, record=record)
                if isinstance(arg, ast.Constant | ast.Name)
                else None
            )
            if isinstance(value, str):
                struct = value[:-2] if value.endswith(".*") else None
                if struct and struct in df.explicit:
                    # select("data.*"): the struct's fields, from wherever the struct came from
                    step = ("select", _short(arg), self.file, line)
                    out.passthrough.extend(
                        (i.source, i.steps + (step,)) for i in df.explicit[struct]
                    )
                    continue
                if value.endswith("*"):
                    out.passthrough.extend(df.passthrough)
                    out.explicit.update({k: list(v) for k, v in df.explicit.items()})
                    out.dropped |= df.dropped
                    continue
                step = ("select", _short(arg), self.file, line)
                out.explicit[value] = [
                    i.then(step)
                    for i in (df.resolve(value) or [Input("unresolved", value)])
                ]
                continue
            alias, inner = _strip_alias(arg)
            refs = list(_column_refs(inner))
            name = alias or (
                refs[0] if len(refs) == 1 and self.expr_kind(inner) == "copy" else None
            )
            if not name:
                out.opaque = True
                continue
            out.explicit[name] = self.column_inputs(df, inner, line, shown=arg)
        return out

    def _names(
        self, args: list[ast.expr], env: dict[str, Any], *, record: bool
    ) -> list[str]:
        names: list[str] = []
        for arg in args:
            node = arg.value if isinstance(arg, ast.Starred) else arg
            value = self.eval(node, env, record=record)
            if isinstance(value, str):
                names.append(value)
            elif isinstance(value, tuple):
                names.extend(value)
        return names

    def expr_kind(self, expr: ast.expr) -> str:
        refs = list(_column_refs(expr))
        calls = {_call_name(n) for n in ast.walk(expr) if isinstance(n, ast.Call)}
        if not refs:
            return "generated"
        if calls & self.project.hash_funcs:
            return "hash"
        if calls & CONCAT_FUNCS:
            return "concat"
        if calls <= {"col", "column"}:
            return "field" if "." in refs[0] else "copy"
        if calls <= {"col", "column", "cast", "astype"}:
            return "cast"
        return "computed"

    def column_inputs(
        self, df: DF, expr: ast.expr, line: int, shown: ast.expr | None = None
    ) -> list[Input]:
        kind = self.expr_kind(expr)
        step: Step = (kind, _short(shown or expr), self.file, line)
        refs = list(dict.fromkeys(_column_refs(expr)))
        if not refs:
            return [Input("generated", "", (step,))]
        inputs: list[Input] = []
        for ref in refs:
            resolved = df.resolve(ref.split(".")[0]) if "." in ref else df.resolve(ref)
            if resolved is None:
                inputs.append(Input("unresolved", ref, (step,)))
                continue
            for item in resolved:
                col = ref if "." in ref and item.col == ref.split(".")[0] else item.col
                inputs.append(Input(item.source, col, item.steps + (step,)))
        return inputs


def _strip_alias(node: ast.expr) -> tuple[str | None, ast.expr]:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in ("alias", "name")
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.args[0].value, node.func.value
    return None, node


def _column_refs(expr: ast.AST) -> Iterator[str]:
    for node in ast.walk(expr):
        if (
            isinstance(node, ast.Call)
            and _call_name(node) in ("col", "column")
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            yield node.args[0].value


def _opaque(df: DF) -> DF:
    out = df.copy()
    out.opaque = True
    return out


def _union(a: DF, b: DF) -> DF:
    out = a.copy()
    names = set(a.explicit) | set(b.explicit)
    for name in names:
        left = a.resolve(name) or []
        right = b.resolve(name) or []
        out.explicit[name] = left + right
    out.passthrough = a.passthrough + b.passthrough
    out.dropped = a.dropped | b.dropped
    out.dedups = a.dedups + b.dedups
    out.surrogates = a.surrogates + b.surrogates
    out.via = a.via + b.via
    out.opaque = a.opaque or b.opaque
    return out


def _merge_branches(a: DF, b: DF) -> DF:
    """Both arms of an ``if``: keep the first arm's columns, record every dedup."""
    out = a.copy()
    seen = {(d.file, d.line) for d in out.dedups}
    out.dedups.extend(d for d in b.dedups if (d.file, d.line) not in seen)
    out.watermark = a.watermark or b.watermark
    return out


def _substitute(result: DF, param: str, arg: DF) -> DF:
    def sub(inputs: list[Input]) -> list[Input]:
        out: list[Input] = []
        for item in inputs:
            if item.source != param:
                out.append(item)
                continue
            resolved = (
                arg.resolve(item.col.split(".")[0])
                if "." in item.col
                else arg.resolve(item.col)
            )
            if resolved is None:
                out.append(Input("unresolved", item.col, item.steps))
                continue
            for up in resolved:
                col = item.col if "." in item.col else up.col
                out.append(Input(up.source, col, up.steps + item.steps))
        return out

    new = DF(
        dropped=set(result.dropped),
        via=list(arg.via) + list(result.via),
        watermark=result.watermark or arg.watermark,
        opaque=result.opaque or arg.opaque,
    )
    for name, inputs in result.explicit.items():
        new.explicit[name] = sub(inputs)
    for src, steps in result.passthrough:
        if src != param:
            new.passthrough.append((src, steps))
            continue
        for name, inputs in arg.explicit.items():
            if name not in result.explicit and name not in result.dropped:
                new.explicit[name] = [
                    Input(i.source, i.col, i.steps + steps) for i in inputs
                ]
        new.passthrough.extend(
            (asrc, asteps + steps) for asrc, asteps in arg.passthrough
        )
        new.dropped |= arg.dropped - set(arg.explicit)
    new.dedups = list(arg.dedups) + [
        Dedup(
            d.names, {k: sub(v) for k, v in d.keys.items()}, d.file, d.line, d.streaming
        )
        for d in result.dedups
    ]
    new.surrogates = list(arg.surrogates) + list(result.surrogates)
    return new


def code_files(dirs: tuple[str, ...] = CODE_DIRS) -> list[Path]:
    files: list[Path] = []
    for name in dirs:
        base = REPO_ROOT / name
        if base.is_dir():
            files.extend(
                p for p in sorted(base.rglob("*.py")) if "__pycache__" not in p.parts
            )
    return files


def interpret_repo() -> Project:
    project = Project(code_files())
    project.run()
    return project


# ------------------------------------------------------------------ export helpers


def input_dict(item: Input) -> dict[str, Any]:
    return {
        "source": item.source,
        "col": item.col,
        "steps": [
            {"kind": k, "expr": e, "loc": f"{f}:{ln}"} for k, e, f, ln in item.steps
        ],
    }


def sink_columns(sink: Sink, actual_columns: list[str]) -> dict[str, Any]:
    """Explain each actual Delta column of a sink's table from the DataFrame state."""
    explained: dict[str, Any] = {}
    for col in actual_columns:
        inputs = sink.df.resolve(col)
        explained[col] = None if inputs is None else [input_dict(i) for i in inputs]
    missing = sorted(set(sink.df.explicit) - set(actual_columns))
    return {"columns": explained, "code_columns_missing_in_delta": missing}


# ------------------------------------------------------------------ Great Expectations

_GE_EXPECTATION = re.compile(r"^Expect[A-Z]\w*To[A-Z]\w*$")


def ge_expectations(project: Project) -> list[dict[str, Any]]:
    """Every ``Expect*`` call, mapped to lake tables via the dicts that select the suite."""
    found: list[dict[str, Any]] = []
    for module in project.modules.values():
        suite_calls: dict[str, list[ast.Call]] = {}
        for name, func in module.funcs.items():
            calls = [
                n
                for n in ast.walk(func)
                if isinstance(n, ast.Call) and _GE_EXPECTATION.match(_call_name(n))
            ]
            if calls:
                suite_calls[name] = calls
        if not suite_calls:
            continue
        key_to_suite: dict[str, tuple[str, int]] = {}
        key_to_table: dict[str, tuple[str, int]] = {}
        for node in module.tree.body:
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Dict):
                continue
            for k, v in zip(node.value.keys, node.value.values, strict=True):
                if not (isinstance(k, ast.Constant) and isinstance(k.value, str)):
                    continue
                if isinstance(v, ast.Name) and v.id in suite_calls:
                    key_to_suite[k.value] = (v.id, k.lineno)
                elif isinstance(v, ast.Constant):
                    table = lake_table_id(v.value)
                    if table:
                        key_to_table[k.value] = (table, k.lineno)
        for key, (suite, suite_line) in key_to_suite.items():
            table_info = key_to_table.get(key)
            for call in suite_calls[suite]:
                columns: list[str] = []
                params: dict[str, str] = {}
                for keyword in call.keywords:
                    if keyword.arg == "column" and isinstance(
                        keyword.value, ast.Constant
                    ):
                        columns = [str(keyword.value.value)]
                    elif keyword.arg == "column_list" and isinstance(
                        keyword.value, ast.List | ast.Tuple
                    ):
                        columns = [
                            str(e.value)
                            for e in keyword.value.elts
                            if isinstance(e, ast.Constant)
                        ]
                    elif keyword.arg:
                        params[keyword.arg] = _short(keyword.value, 40)
                found.append(
                    {
                        "table": table_info[0] if table_info else None,
                        "suite_key": key,
                        "suite": suite,
                        "expectation": _call_name(call),
                        "columns": columns,
                        "params": params,
                        "loc": f"{module.file}:{call.lineno}",
                        "mapping_loc": f"{module.file}:{suite_line}"
                        + (f", {module.file}:{table_info[1]}" if table_info else ""),
                    }
                )
    return found


# ------------------------------------------------------------------ docstrings

_DOC_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("grain", re.compile(r"[Gg]rain:\s*one row per\s+``(\w+)``")),
    ("keyed_by", re.compile(r"keyed by\s+``(\w+)``")),
    (
        "foreign_keys",
        re.compile(r"[Ff]oreign keys?:\s*((?:``\w+``[\s,]*(?:and\s+)?)+)"),
    ),
    ("joins_to", re.compile(r"``(\w+)\.(\w+)``\s+joins\s+to\s+``(\w+)\.(\w+)``")),
)
_PK_PHRASE = re.compile(r"primary\s+key")
_IDENT = re.compile(r"``(\w+)``")


def docstring_claims(project: Project) -> list[dict[str, Any]]:
    """Key statements in function docstrings, with the exact line of each."""
    claims: list[dict[str, Any]] = []
    for module in project.modules.values():
        for func in module.funcs.values():
            if not (
                func.body
                and isinstance(func.body[0], ast.Expr)
                and isinstance(func.body[0].value, ast.Constant)
                and isinstance(func.body[0].value.value, str)
            ):
                continue
            text = func.body[0].value.value
            start = func.body[0].lineno
            qual = f"{module.dotted}.{func.name}"

            def at(
                pos: int,
                _text: str = text,
                _start: int = start,
                _file: str = module.file,
            ) -> str:
                return f"{_file}:{_start + _text[:pos].count(chr(10))}"

            for kind, pattern in _DOC_PATTERNS:
                for match in pattern.finditer(text):
                    if kind == "foreign_keys":
                        cols = _IDENT.findall(match.group(1))
                        claims.append(
                            {
                                "kind": kind,
                                "function": qual,
                                "columns": cols,
                                "loc": at(match.start()),
                            }
                        )
                    elif kind == "joins_to":
                        claims.append(
                            {
                                "kind": kind,
                                "function": qual,
                                "child": [match.group(1), match.group(2)],
                                "parent": [match.group(3), match.group(4)],
                                "loc": at(match.start()),
                            }
                        )
                    else:
                        claims.append(
                            {
                                "kind": kind,
                                "function": qual,
                                "columns": [match.group(1)],
                                "loc": at(match.start()),
                            }
                        )
            for match in _PK_PHRASE.finditer(text):
                window = text[max(0, match.start() - 80) : match.start()]
                idents = _IDENT.findall(window)
                if idents:
                    claims.append(
                        {
                            "kind": "primary_key",
                            "function": qual,
                            "columns": [idents[-1]],
                            "loc": at(match.start()),
                        }
                    )
    return claims


# ------------------------------------------------------------------ semantic layer (yaml with line numbers)


def _yaml_records(path: Path, list_key: str) -> list[dict[str, Any]]:
    """Records under ``list_key`` with the line of every field (1-based)."""
    root = yaml.compose(path.read_text(encoding="utf-8"))
    out: list[dict[str, Any]] = []
    if not isinstance(root, yaml.MappingNode):
        return out
    for key_node, value_node in root.value:
        if key_node.value != list_key or not isinstance(value_node, yaml.SequenceNode):
            continue
        for item in value_node.value:
            if not isinstance(item, yaml.MappingNode):
                continue
            record: dict[str, Any] = {
                "_loc": f"{rel(path)}:{item.start_mark.line + 1}",
                "_lines": {},
            }
            for k, v in item.value:
                if isinstance(v, yaml.ScalarNode):
                    record[k.value] = v.value
                    record["_lines"][k.value] = v.start_mark.line + 1
            out.append(record)
    return out


def semantic_layer() -> dict[str, Any]:
    base = REPO_ROOT / "semantic"
    joins: list[dict[str, Any]] = []
    dims: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    sources: list[str] = []
    if (base / "joins.yml").is_file():
        joins = _yaml_records(base / "joins.yml", "joins")
        sources.append(rel(base / "joins.yml"))
    for path in sorted((base / "dimensions").glob("*.yml")):
        dims.extend(_yaml_records(path, "dimensions"))
        sources.append(rel(path))
    for path in sorted((base / "metrics").glob("*.yml")):
        metrics.extend(_yaml_records(path, "metrics"))
        sources.append(rel(path))
    for record in dims + metrics:
        expr = record.get("column") or record.get("expression") or ""
        record["_refs"] = sql_column_refs(expr)
    return {"joins": joins, "dimensions": dims, "metrics": metrics, "sources": sources}


def sql_column_refs(expr: str) -> list[str]:
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(expr, dialect="duckdb")
    except Exception:
        return []
    if tree is None:
        return []
    return sorted({c.name for c in tree.find_all(exp.Column)})


def join_consumers() -> list[str]:
    """Code locations that read ``left_key``/``right_key`` of a semantic join."""
    hits: list[str] = []
    for path in code_files(("src",)):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        lines = sorted(
            {
                n.lineno
                for n in ast.walk(tree)
                if isinstance(n, ast.Attribute) and n.attr in ("left_key", "right_key")
            }
        )
        hits.extend(f"{rel(path)}:{ln}" for ln in lines)
    return hits


# ------------------------------------------------------------------ SQL text inside code


def _string_constants(path: Path) -> Iterator[tuple[str, int, ast.Call | None]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return
    parents: dict[int, ast.Call] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for arg in node.args:
                parents[id(arg)] = node
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value, node.lineno, parents.get(id(node))


def sql_strings() -> list[dict[str, Any]]:
    """Equi-join conditions (``a.x = b.y``) in SQL text anywhere in src/, dags/, tests/."""
    import sqlglot
    from sqlglot import exp

    found: list[dict[str, Any]] = []
    for path in code_files(SQL_SCAN_DIRS):
        for text, line, _ in _string_constants(path):
            if not re.search(r"\bjoin\b", text, re.I) or not re.search(
                r"\bon\b", text, re.I
            ):
                continue
            try:
                statements = sqlglot.parse(text, dialect="duckdb")
            except Exception:
                continue
            for statement in statements:
                if statement is None:
                    continue
                for join in statement.find_all(exp.Join):
                    on = join.args.get("on")
                    if on is None:
                        continue
                    for eq in on.find_all(exp.EQ):
                        left, right = eq.this, eq.expression
                        if (
                            isinstance(left, exp.Column)
                            and isinstance(right, exp.Column)
                            and left.table
                            and right.table
                        ):
                            found.append(
                                {
                                    "left": [left.table, left.name],
                                    "right": [right.table, right.name],
                                    "loc": f"{rel(path)}:{line}",
                                }
                            )
    return found


def notebook_probes(project: Project) -> list[dict[str, Any]]:
    """Key checks written as ``spark.sql`` in the code: duplicate and null probes."""
    import sqlglot
    from sqlglot import exp

    probes: list[dict[str, Any]] = []
    for module in project.modules.values():
        for text, line, parent in _string_constants(module.path):
            if parent is None or _call_name(parent) != "sql":
                continue
            try:
                tree = sqlglot.parse_one(text, dialect="spark")
            except Exception:
                continue
            if (
                not isinstance(tree, exp.Select | exp.With)
                and tree is not None
                and not tree.find(exp.Select)
            ):
                continue
            if tree is None:
                continue
            for select in tree.find_all(exp.Select):
                source = select.args.get("from")
                if source is None:
                    source = select.args.get("from_")
                table_expr = source.find(exp.Table) if source is not None else None
                if table_expr is None:
                    continue
                view = table_expr.name
                loc = f"{module.file}:{line}"
                group = select.args.get("group")
                having = select.args.get("having")
                if group is not None and having is not None and having.find(exp.Count):
                    cols = [c.name for c in group.find_all(exp.Column)]
                    probes.append(
                        {
                            "kind": "duplicate_probe",
                            "view": view,
                            "columns": cols,
                            "loc": loc,
                            "sql": " ".join(text.split()),
                        }
                    )
                for count in select.find_all(exp.Count):
                    inner = count.this
                    if isinstance(inner, exp.Distinct):
                        cols = [c.name for c in inner.find_all(exp.Column)]
                        if cols and group is None:
                            probes.append(
                                {
                                    "kind": "distinct_probe",
                                    "view": view,
                                    "columns": cols,
                                    "loc": loc,
                                    "sql": " ".join(text.split()),
                                }
                            )
                for window in select.find_all(exp.Window):
                    if window.this.find(exp.Count) is not None or isinstance(
                        window.this, exp.Count
                    ):
                        parts = window.args.get("partition_by") or []
                        cols = [
                            c.name
                            for p in parts
                            for c in (
                                [p]
                                if isinstance(p, exp.Column)
                                else p.find_all(exp.Column)
                            )
                        ]
                        if cols:
                            probes.append(
                                {
                                    "kind": "window_probe",
                                    "view": view,
                                    "columns": cols,
                                    "loc": loc,
                                    "sql": " ".join(text.split()),
                                }
                            )
                where = select.args.get("where")
                if where is not None and select.find(exp.Count) and group is None:
                    null_cols = [
                        n.this.name
                        for n in where.find_all(exp.Is)
                        if isinstance(n.this, exp.Column)
                    ]
                    if null_cols:
                        probes.append(
                            {
                                "kind": "null_probe",
                                "view": view,
                                "columns": null_cols,
                                "loc": loc,
                                "sql": " ".join(text.split()),
                            }
                        )
    unique: dict[tuple[str, str, tuple[str, ...], str], dict[str, Any]] = {}
    for probe in probes:
        key = (probe["kind"], probe["view"], tuple(probe["columns"]), probe["loc"])
        unique.setdefault(key, probe)
    for probe in unique.values():
        view = project.views.get(probe["view"])
        probe["table"] = view[0] if view else None
        probe["view_loc"] = f"{view[1]}:{view[2]}" if view else None
    return list(unique.values())
