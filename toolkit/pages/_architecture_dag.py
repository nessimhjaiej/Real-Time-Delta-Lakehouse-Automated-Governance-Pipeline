"""Airflow DAG facts parsed with ``ast`` (the DAG file is never imported).

Finds ``DAG(...)`` (as ``with DAG(...) as x`` or ``x = DAG(...)``), every
``*Operator``/``*Sensor`` call assigned to a name, its keyword arguments
(task_id, python_callable, retries, retry_delay, inlets, outlets, callbacks),
``Dataset("uri")`` constants, and the dependency order from ``>>`` / ``<<``
chains, ``set_downstream`` / ``set_upstream`` and ``chain(...)``.
"""

from __future__ import annotations

import ast
import re
from typing import Any

from toolkit.common import LAYERS, rel
from toolkit.pages._architecture_code import CodeIndex, Module, _dotted

_LAKE_RE = re.compile(r"^s3a?://([\w-]+)/(\w+)/(\w+)/?$")


def lake_id(uri: str) -> str | None:
    """'s3a://lakehouse/bronze/orders_batch' -> 'bronze.orders_batch'."""
    match = _LAKE_RE.match(uri)
    if match and match.group(2) in LAYERS:
        return f"{match.group(2)}.{match.group(3)}"
    return None


def _src(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # noqa: BLE001 - ast.unparse on odd nodes
        return "?"


def _const(node: ast.AST | None) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    return None


class _DagFile:
    def __init__(self, index: CodeIndex, mod: Module) -> None:
        self.index = index
        self.mod = mod
        self.datasets: dict[str, dict[str, Any]] = {}  # const name -> dataset
        self._collect_datasets()

    def full(self, dotted: str) -> str:
        return CodeIndex._full_name(self.mod.imports, dotted)

    def _collect_datasets(self) -> None:
        for name, value in self.mod.consts.items():
            if isinstance(value, ast.Call):
                fname = _dotted(value.func)
                if fname and self.full(fname).endswith(".Dataset") and value.args:
                    uri = _const(value.args[0])
                    if isinstance(uri, str):
                        self.datasets[name] = {
                            "uri": uri,
                            "id": lake_id(uri) or uri,
                            "const": name,
                            "line": value.lineno,
                        }

    def dataset_list(
        self, node: ast.AST | None, depth: int = 0
    ) -> list[dict[str, Any]]:
        if node is None or depth > 4:
            return []
        if isinstance(node, (ast.List, ast.Tuple)):
            out: list[dict[str, Any]] = []
            for elt in node.elts:
                out.extend(self.dataset_list(elt, depth + 1))
            return out
        if isinstance(node, ast.Name):
            if node.id in self.datasets:
                return [self.datasets[node.id]]
            if node.id in self.mod.consts:
                return self.dataset_list(self.mod.consts[node.id], depth + 1)
        if isinstance(node, ast.Call):
            fname = _dotted(node.func)
            if fname and self.full(fname).endswith(".Dataset") and node.args:
                uri = _const(node.args[0])
                if isinstance(uri, str):
                    return [
                        {
                            "uri": uri,
                            "id": lake_id(uri) or uri,
                            "const": None,
                            "line": node.lineno,
                        }
                    ]
        return []

    def callable_id(self, node: ast.AST | None) -> str | None:
        dotted = _dotted(node) if node is not None else None
        if not dotted:
            return None
        resolved = self.index.resolve(self.full(dotted))
        if resolved and resolved[0] in ("unit", "class"):
            return resolved[1]
        return self.full(dotted)

    def default_args(self, node: ast.AST | None) -> dict[str, ast.AST]:
        if isinstance(node, ast.Name):
            node = self.mod.consts.get(node.id)
        if isinstance(node, ast.Dict):
            return {
                str(k.value): v
                for k, v in zip(node.keys, node.values)
                if isinstance(k, ast.Constant)
            }
        return {}


def _is_dag_call(df: _DagFile, node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    fname = _dotted(node.func)
    return bool(fname) and df.full(fname or "").rsplit(".", 1)[-1] == "DAG"


def parse_dags(index: CodeIndex, dag_dir: str = "dags") -> list[dict[str, Any]]:
    dags: list[dict[str, Any]] = []
    for mod in index.modules.values():
        if not rel(mod.path).startswith(f"{dag_dir}/"):
            continue
        df = _DagFile(index, mod)
        for node in ast.walk(mod.tree):
            dag_call: ast.Call | None = None
            body: list[ast.stmt] = []
            if isinstance(node, ast.With):
                for item in node.items:
                    if _is_dag_call(df, item.context_expr):
                        dag_call = item.context_expr  # type: ignore[assignment]
                        body = node.body
            if dag_call is None:
                continue
            dags.append(_parse_one(df, dag_call, body))
    return dags


def _parse_one(df: _DagFile, call: ast.Call, body: list[ast.stmt]) -> dict[str, Any]:
    kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
    dag_id = _const(kwargs.get("dag_id")) or _const(call.args[0] if call.args else None)
    defaults = df.default_args(kwargs.get("default_args"))
    file = rel(df.mod.path)
    dag: dict[str, Any] = {
        "id": f"dag.{dag_id}",
        "dag_id": dag_id,
        "file": file,
        "line": call.lineno,
        "attrs": {
            k: _src(v)
            for k, v in kwargs.items()
            if k
            in (
                "schedule",
                "schedule_interval",
                "start_date",
                "catchup",
                "tags",
                "description",
                "max_active_runs",
                "default_args",
            )
        },
        "default_args": {k: _src(v) for k, v in defaults.items()},
        "tasks": [],
        "edges": [],
        "datasets": {},
    }
    tasks_by_var: dict[str, dict[str, Any]] = {}
    for stmt in ast.walk(ast.Module(body=body, type_ignores=[])):
        if not (isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call)):
            continue
        fname = _dotted(stmt.value.func) or ""
        op_class = df.full(fname)
        short = op_class.rsplit(".", 1)[-1]
        if not (short.endswith("Operator") or short.endswith("Sensor")):
            continue
        tk = {kw.arg: kw.value for kw in stmt.value.keywords if kw.arg}
        task_id = _const(tk.get("task_id"))
        retries_src = (
            "task"
            if "retries" in tk
            else ("default_args" if "retries" in defaults else None)
        )
        retries_node = tk.get("retries", defaults.get("retries"))
        delay_node = tk.get("retry_delay", defaults.get("retry_delay"))
        inlets = df.dataset_list(tk.get("inlets"))
        outlets = df.dataset_list(tk.get("outlets"))
        task = {
            "id": f"{dag['id']}.{task_id}",
            "task_id": task_id,
            "var": (
                stmt.targets[0].id if isinstance(stmt.targets[0], ast.Name) else None
            ),
            "operator": op_class,
            "line": stmt.lineno,
            "callable": df.callable_id(tk.get("python_callable")),
            "retries": _src(retries_node) if retries_node is not None else None,
            "retries_from": retries_src,
            "retry_delay": _src(delay_node) if delay_node is not None else None,
            "inlets": [d["id"] for d in inlets],
            "outlets": [d["id"] for d in outlets],
            "callbacks": {
                k: df.callable_id(v)
                for k, v in tk.items()
                if k.startswith("on_") and k.endswith("_callback")
            },
            "other_kwargs": {
                k: _src(v)
                for k, v in tk.items()
                if k
                not in (
                    "task_id",
                    "python_callable",
                    "retries",
                    "retry_delay",
                    "inlets",
                    "outlets",
                )
                and not k.endswith("_callback")
            },
        }
        for d in inlets + outlets:
            dag["datasets"][d["id"]] = {
                "uri": d["uri"],
                "line": d["line"],
                "const": d["const"],
            }
        if task["var"]:
            tasks_by_var[task["var"]] = task
        dag["tasks"].append(task)

    def ids(node: ast.AST) -> list[str]:
        if isinstance(node, ast.Name) and node.id in tasks_by_var:
            return [tasks_by_var[node.id]["id"]]
        if isinstance(node, (ast.List, ast.Tuple)):
            return [i for e in node.elts for i in ids(e)]
        if isinstance(node, ast.BinOp) and isinstance(
            node.op, (ast.RShift, ast.LShift)
        ):
            return chain_binop(node)
        return []

    def add(src: str, dst: str, line: int, how: str) -> None:
        edge = {"source": src, "target": dst, "line": line, "how": how}
        if not any(e["source"] == src and e["target"] == dst for e in dag["edges"]):
            dag["edges"].append(edge)

    def chain_binop(node: ast.BinOp) -> list[str]:
        left = ids(node.left)
        right = ids(node.right)
        for a in left:
            for b in right:
                if isinstance(node.op, ast.RShift):
                    add(a, b, node.lineno, ">>")
                else:
                    add(b, a, node.lineno, "<<")
        return right

    for stmt in ast.walk(ast.Module(body=body, type_ignores=[])):
        if isinstance(stmt, ast.Expr):
            value = stmt.value
            if isinstance(value, ast.BinOp):
                chain_binop(value)
            elif isinstance(value, ast.Call):
                fname = _dotted(value.func) or ""
                if fname.endswith(".set_downstream") or fname.endswith(".set_upstream"):
                    owner = (
                        ids(value.func.value)
                        if isinstance(value.func, ast.Attribute)
                        else []
                    )
                    others = [i for a in value.args for i in ids(a)]
                    for a in owner:
                        for b in others:
                            if fname.endswith(".set_downstream"):
                                add(a, b, value.lineno, "set_downstream")
                            else:
                                add(b, a, value.lineno, "set_upstream")
                elif df.full(fname).rsplit(".", 1)[-1] == "chain":
                    groups = [ids(a) for a in value.args]
                    for left, right in zip(groups, groups[1:]):
                        for a in left:
                            for b in right:
                                add(a, b, value.lineno, "chain()")
    return dag
