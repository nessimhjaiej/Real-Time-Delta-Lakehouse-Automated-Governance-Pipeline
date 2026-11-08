"""Static call-graph extractor used by the call-graph page (private helper).

Parses every first-party Python file with ``ast`` and resolves each call site
to a definition: through imports (absolute, relative, ``as`` aliases, package
re-exports), module-level aliases, ``self``/``cls``/``super()``, annotated
parameters and return types, simple local type inference (``x = Cls()``,
``x = f()`` with an annotated or inferable return), pytest fixtures, dicts of
functions, and functions passed as values (callbacks, ``add_node``, default
arguments, closures returned by factories).

Values are tracked as small tuples:
    ("def", id)        a first-party function / method / class object
    ("inst", class_id) an instance of a first-party class
    ("super", class_id) the object returned by ``super()`` inside class_id
    ("mod", module)    a first-party module
    ("ext", dotted)    anything that comes from a third-party / stdlib package
    ("builtin", name)  a builtin function or a value of a builtin type
    ("funcs", id, ...) a dict whose values are first-party functions
    ("dyn", reason)    a value whose target cannot be known statically
"""

from __future__ import annotations

import ast
import builtins
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field

Value = tuple[str, ...]

HTTP_VERBS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})
TASK_CALLBACK_KWARGS = (
    "python_callable",
    "on_failure_callback",
    "on_success_callback",
    "on_retry_callback",
    "on_execute_callback",
    "sla_miss_callback",
)
BUILTIN_TYPES = frozenset(
    {"str", "int", "float", "bool", "list", "dict", "set", "tuple", "bytes"}
    | {"frozenset", "bytearray", "complex", "object", "type"}
)
FIRST_PARTY_ROOTS = ("src", "dags", "config", "tests")


def is_stdlib(package: str) -> bool:
    return package in sys.stdlib_module_names or package in sys.builtin_module_names


def first_doc_line(node: ast.AST) -> str:
    if not isinstance(
        node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)
    ):
        return ""
    doc = ast.get_docstring(node)
    if not doc:
        return ""
    for line in doc.strip().splitlines():
        if line.strip():
            return line.strip()
    return ""


def iter_block(stmts: list[ast.stmt]) -> Iterator[ast.stmt]:
    """Statements of one scope, flattening if/with/try/for/while blocks."""
    for stmt in stmts:
        yield stmt
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for name in ("body", "orelse", "finalbody"):
            inner = getattr(stmt, name, None)
            if isinstance(inner, list) and inner and isinstance(inner[0], ast.stmt):
                yield from iter_block(inner)
        if isinstance(stmt, ast.Try):
            for handler in stmt.handlers:
                yield from iter_block(handler.body)
        if isinstance(stmt, ast.Match):
            for case in stmt.cases:
                yield from iter_block(case.body)


def own_nodes(roots: list[ast.AST]) -> Iterator[ast.AST]:
    """Every AST node of a scope's own code, not entering nested defs/classes."""
    nested = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
    stack = [r for r in reversed(roots) if not isinstance(r, nested)]
    while stack:
        node = stack.pop()
        yield node
        children = list(ast.iter_child_nodes(node))
        for child in reversed(children):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            stack.append(child)


def is_main_guard(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.If) or not isinstance(stmt.test, ast.Compare):
        return False
    test = stmt.test
    parts = [test.left, *test.comparators]
    names = {p.id for p in parts if isinstance(p, ast.Name)}
    consts = {p.value for p in parts if isinstance(p, ast.Constant)}
    return names == {"__name__"} and consts == {"__main__"}


@dataclass
class ModuleInfo:
    name: str
    path: str
    tree: ast.Module
    is_package: bool
    text: str
    parents: dict[ast.AST, ast.AST] = field(default_factory=dict)
    is_notebook: bool = False
    main_guard: ast.If | None = None


@dataclass
class Def:
    id: str
    module: str
    qualname: str
    kind: str  # function | method | class | closure | module | main
    node: ast.AST
    line: int
    end_line: int
    cls: str | None = None
    scope_key: str = ""
    code: list[ast.AST] = field(default_factory=list)
    decorators: list[str] = field(default_factory=list)
    is_property: bool = False
    is_static: bool = False
    is_classmethod: bool = False
    is_abstract: bool = False
    is_fixture: bool = False
    is_test: bool = False
    externals: set[str] = field(default_factory=set)
    entry: list[str] = field(default_factory=list)


@dataclass
class Scope:
    key: str
    module: str
    parent: Scope | None
    cls: str | None = None
    def_id: str | None = None
    bindings: dict[str, list[tuple[object, ...]]] = field(default_factory=dict)

    def bind(self, name: str, binding: tuple[object, ...]) -> None:
        self.bindings.setdefault(name, []).append(binding)


@dataclass
class EdgeInfo:
    labels: set[str] = field(default_factory=set)
    lines: list[str] = field(default_factory=list)


@dataclass
class CallSite:
    def_id: str
    location: str
    expr: str
    category: str  # first-party | external | builtin | dynamic | unresolved
    detail: str = ""


class Analyzer:
    def __init__(self, files: dict[str, tuple[str, str]]) -> None:
        """files: module name -> (repo-relative path, source text)."""
        self.modules: dict[str, ModuleInfo] = {}
        self.defs: dict[str, Def] = {}
        self.scopes: dict[str, Scope] = {}
        self.class_members: dict[str, dict[str, str]] = {}
        self.class_fields: dict[str, dict[str, tuple[str, ast.expr, bool]]] = {}
        self.class_bases: dict[str, list[ast.expr]] = {}
        self.edges: dict[tuple[str, str, str], EdgeInfo] = {}
        self.call_sites: list[CallSite] = []
        self.entries: list[dict[str, object]] = []
        self.parse_errors: list[str] = []
        self._guard: set[tuple[str, str]] = set()
        self._memo: dict[tuple[str, str], Value | None] = {}
        self._ret_guard: set[str] = set()
        self._handled: set[int] = set()
        for name, (path, text) in sorted(files.items()):
            try:
                tree = ast.parse(text, filename=path)
            except SyntaxError as error:
                self.parse_errors.append(f"{path}: {error}")
                continue
            info = ModuleInfo(
                name=name,
                path=path,
                tree=tree,
                is_package=path.endswith("__init__.py"),
                text=text,
                is_notebook=any(line.startswith("# %%") for line in text.splitlines()),
            )
            for parent in ast.walk(tree):
                for child in ast.iter_child_nodes(parent):
                    info.parents[child] = parent
            for stmt in tree.body:
                if is_main_guard(stmt):
                    info.main_guard = stmt  # type: ignore[assignment]
            self.modules[name] = info

    # ------------------------------------------------------------ collection

    def run(self) -> None:
        for module in self.modules.values():
            self._collect_module(module)
        for definition in list(self.defs.values()):
            self._analyse_def(definition)
        self._dispatch_edges()
        self._constructor_edges()
        self.pytest_pass()

    def _collect_module(self, module: ModuleInfo) -> None:
        scope = Scope(key=module.name, module=module.name, parent=None)
        self.scopes[module.name] = scope
        self._collect_bindings(scope, module.tree.body, module.name, None)
        module_code: list[ast.AST] = []
        for stmt in module.tree.body:
            if stmt is module.main_guard:
                continue
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            module_code.append(stmt)
        self.defs[f"{module.name}.<module>"] = Def(
            id=f"{module.name}.<module>",
            module=module.name,
            qualname="<module>",
            kind="module",
            node=module.tree,
            line=1,
            end_line=len(module.text.splitlines()),
            scope_key=module.name,
            code=module_code,
        )
        if module.main_guard is not None:
            guard = module.main_guard
            self.defs[f"{module.name}.__main__"] = Def(
                id=f"{module.name}.__main__",
                module=module.name,
                qualname="__main__",
                kind="main",
                node=guard,
                line=guard.lineno,
                end_line=guard.end_lineno or guard.lineno,
                scope_key=module.name,
                code=list(guard.body),
            )

    def _collect_bindings(
        self,
        scope: Scope,
        stmts: list[ast.stmt],
        qual_prefix: str,
        cls: str | None,
    ) -> None:
        """Bind names of one scope and register the defs/classes it contains."""
        module = scope.module
        for stmt in iter_block(stmts):
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                def_id = f"{qual_prefix}.{stmt.name}"
                scope.bind(stmt.name, ("def", def_id))
                self._register_function(stmt, def_id, scope, cls)
            elif isinstance(stmt, ast.ClassDef):
                def_id = f"{qual_prefix}.{stmt.name}"
                scope.bind(stmt.name, ("def", def_id))
                self._register_class(stmt, def_id, scope)
            elif isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    self._bind_target(scope, target, ("expr", stmt.value))
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                scope.bind(stmt.target.id, ("ann", stmt.annotation))
                if stmt.value is not None:
                    scope.bind(stmt.target.id, ("expr", stmt.value))
            elif isinstance(stmt, (ast.For, ast.AsyncFor)):
                self._bind_iter(scope, stmt.target, stmt.iter)
            elif isinstance(stmt, (ast.With, ast.AsyncWith)):
                for item in stmt.items:
                    if isinstance(item.optional_vars, ast.Name):
                        scope.bind(item.optional_vars.id, ("with", item.context_expr))
            elif isinstance(stmt, ast.Try):
                for handler in stmt.handlers:
                    if handler.name and handler.type is not None:
                        scope.bind(handler.name, ("except", handler.type))
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if alias.asname:
                        scope.bind(alias.asname, ("import", "mod", alias.name))
                    else:
                        top = alias.name.split(".")[0]
                        scope.bind(top, ("import", "mod", top))
            elif isinstance(stmt, ast.ImportFrom):
                base = self._absolute_module(module, stmt.module, stmt.level)
                for alias in stmt.names:
                    if alias.name == "*":
                        continue
                    scope.bind(
                        alias.asname or alias.name, ("import", "from", base, alias.name)
                    )
        # comprehension variables (approximation: bound in the enclosing scope)
        roots: list[ast.AST] = [s for s in stmts]
        for node in own_nodes(roots):
            if isinstance(node, ast.comprehension):
                self._bind_iter(scope, node.target, node.iter)
            elif isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
                scope.bind(node.target.id, ("expr", node.value))

    def _bind_target(
        self, scope: Scope, target: ast.expr, binding: tuple[object, ...]
    ) -> None:
        if isinstance(target, ast.Name):
            scope.bind(target.id, binding)
        elif isinstance(target, (ast.Tuple, ast.List)):
            rhs = binding[1]
            for index, elt in enumerate(target.elts):
                if isinstance(elt, ast.Name):
                    scope.bind(elt.id, ("unpack", rhs, index, len(target.elts)))

    def _bind_iter(self, scope: Scope, target: ast.expr, iterable: ast.expr) -> None:
        if isinstance(target, ast.Name):
            scope.bind(target.id, ("iter", iterable, -1))
        elif isinstance(target, (ast.Tuple, ast.List)):
            for index, elt in enumerate(target.elts):
                if isinstance(elt, ast.Name):
                    scope.bind(elt.id, ("iter", iterable, index))

    def _absolute_module(self, module: str, name: str | None, level: int) -> str:
        if level == 0:
            return name or ""
        info = self.modules[module]
        package = module if info.is_package else module.rpartition(".")[0]
        for _ in range(level - 1):
            package = package.rpartition(".")[0]
        return f"{package}.{name}" if name else package

    def _decorator_names(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef
    ) -> list[str]:
        return [ast.unparse(d) for d in node.decorator_list]

    def _register_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        def_id: str,
        parent: Scope,
        cls: str | None,
    ) -> None:
        module = parent.module
        qualname = def_id[len(module) + 1 :]
        decorators = self._decorator_names(node)
        tails = {d.split("(")[0].split(".")[-1] for d in decorators}
        if cls is not None:
            kind = "method"
        elif parent.def_id is not None:
            kind = "closure"
        else:
            kind = "function"
        definition = Def(
            id=def_id,
            module=module,
            qualname=qualname,
            kind=kind,
            node=node,
            line=node.lineno,
            end_line=node.end_lineno or node.lineno,
            cls=cls,
            code=list(node.body),
            decorators=decorators,
            is_property="property" in tails or "cached_property" in tails,
            is_static="staticmethod" in tails,
            is_classmethod="classmethod" in tails,
            is_abstract="abstractmethod" in tails,
            is_fixture="fixture" in tails,
        )
        self.defs[def_id] = definition
        if cls is not None:
            self.class_members.setdefault(cls, {})[node.name] = def_id
        # Methods see the module (not the class body) as their enclosing scope.
        enclosing = parent
        while enclosing.cls is not None and enclosing.def_id == enclosing.cls:
            assert enclosing.parent is not None
            enclosing = enclosing.parent
        scope = Scope(
            key=def_id, module=module, parent=enclosing, cls=cls, def_id=def_id
        )
        definition.scope_key = def_id
        self.scopes[def_id] = scope
        args = node.args
        positional = [*args.posonlyargs, *args.args]
        defaults: list[ast.expr | None] = [None] * (
            len(positional) - len(args.defaults)
        ) + list(args.defaults)
        for index, arg in enumerate(positional):
            role = ""
            if index == 0 and cls is not None and not definition.is_static:
                role = "cls" if definition.is_classmethod else "self"
            scope.bind(
                arg.arg, ("param", arg.annotation, defaults[index], role, arg.arg)
            )
        for arg, default in zip(args.kwonlyargs, args.kw_defaults):
            scope.bind(arg.arg, ("param", arg.annotation, default, "", arg.arg))
        if args.vararg:
            scope.bind(args.vararg.arg, ("expr", ast.Tuple(elts=[], ctx=ast.Load())))
        if args.kwarg:
            scope.bind(args.kwarg.arg, ("expr", ast.Dict(keys=[], values=[])))
        self._collect_bindings(scope, node.body, f"{def_id}.<locals>", None)

    def _register_class(self, node: ast.ClassDef, def_id: str, parent: Scope) -> None:
        module = parent.module
        definition = Def(
            id=def_id,
            module=module,
            qualname=def_id[len(module) + 1 :],
            kind="class",
            node=node,
            line=node.lineno,
            end_line=node.end_lineno or node.lineno,
            code=[
                s
                for s in node.body
                if not isinstance(
                    s, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
                )
            ],
            decorators=[ast.unparse(d) for d in node.decorator_list],
            scope_key=def_id,
        )
        self.defs[def_id] = definition
        self.class_bases[def_id] = list(node.bases)
        self.class_members.setdefault(def_id, {})
        fields = self.class_fields.setdefault(def_id, {})
        class_scope = Scope(
            key=def_id, module=module, parent=parent, cls=def_id, def_id=def_id
        )
        self.scopes[def_id] = class_scope
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                fields[stmt.target.id] = (parent.key, stmt.annotation, True)
            elif isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        fields.setdefault(target.id, (parent.key, stmt.value, False))
        self._collect_bindings(class_scope, node.body, def_id, def_id)
        # instance attributes assigned through self.<attr> in any method
        for member_id in list(self.class_members[def_id].values()):
            member = self.defs[member_id]
            fn = member.node
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not fn.args.args or member.is_static:
                continue
            self_name = fn.args.args[0].arg
            for sub in own_nodes(list(fn.body)):
                targets: list[ast.expr] = []
                value: ast.expr | None = None
                annotation = False
                if isinstance(sub, ast.Assign):
                    targets, value = list(sub.targets), sub.value
                elif isinstance(sub, ast.AnnAssign):
                    targets, value, annotation = [sub.target], sub.annotation, True
                for target in targets:
                    if (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == self_name
                        and value is not None
                    ):
                        fields.setdefault(target.attr, (member_id, value, annotation))

    # ------------------------------------------------------------- inference

    def resolve_name(self, scope: Scope, name: str) -> Value | None:
        current: Scope | None = scope
        while current is not None:
            if name in current.bindings:
                return self._infer_bindings(current, name)
            current = current.parent
        if name.startswith("__") and name.endswith("__"):
            return ("builtin", name)
        if hasattr(builtins, name):
            return ("builtin", name)
        return None

    def _infer_bindings(self, scope: Scope, name: str) -> Value | None:
        key = (scope.key, name)
        if key in self._memo:
            return self._memo[key]
        if key in self._guard:
            return None
        self._guard.add(key)
        try:
            result: Value | None = None
            for binding in scope.bindings.get(name, []):
                value = self._infer_binding(scope, binding)
                if value is not None and value != ("builtin", "NoneType"):
                    result = value
                    break
        finally:
            self._guard.discard(key)
        if not self._guard:
            self._memo[key] = result
        return result

    def _infer_binding(self, scope: Scope, binding: tuple[object, ...]) -> Value | None:
        kind = binding[0]
        if kind == "def":
            return ("def", str(binding[1]))
        if kind == "expr":
            return self.infer(binding[1], scope)  # type: ignore[arg-type]
        if kind == "ann":
            return self.type_from_annotation(binding[1], scope)  # type: ignore[arg-type]
        if kind == "param":
            return self._infer_param(scope, binding)
        if kind == "unpack":
            return self._infer_unpack(scope, binding[1], int(binding[2]))  # type: ignore[arg-type,call-overload]
        if kind == "iter":
            return self._infer_iter(scope, binding[1], int(binding[2]))  # type: ignore[arg-type,call-overload]
        if kind == "with":
            value = self.infer(binding[1], scope)  # type: ignore[arg-type]
            if value and value[0] == "inst":
                enter = self.member(value[1], "__enter__")
                if enter:
                    return self.return_type(enter)
            return value
        if kind == "except":
            target = binding[1]
            if isinstance(target, ast.Tuple) and target.elts:
                target = target.elts[0]
            value = self.infer(target, scope)  # type: ignore[arg-type]
            return self._instance_of(value)
        if kind == "import":
            return self._resolve_import(binding)
        return None

    def _infer_param(self, scope: Scope, binding: tuple[object, ...]) -> Value | None:
        _, annotation, default, role, name = binding
        if role == "self" and scope.cls:
            return ("inst", scope.cls)
        if role == "cls" and scope.cls:
            return ("def", scope.cls)
        outer = scope.parent or scope
        if isinstance(annotation, ast.expr):
            value = self.type_from_annotation(annotation, outer)
            if value is not None:
                if value[0] == "dyn" and isinstance(default, ast.expr):
                    default_value = self.infer(default, outer)
                    if default_value and default_value[0] == "def":
                        return (
                            "dyn",
                            "callable parameter (default: " + default_value[1] + ")",
                        )
                return value
        if isinstance(default, ast.expr):
            default_value = self.infer(default, outer)
            if default_value and default_value[0] == "def":
                return ("dyn", "callable parameter (default: " + default_value[1] + ")")
            if default_value and default_value != ("builtin", "NoneType"):
                return default_value
        fixture = self._fixture_type(scope, str(name))
        if fixture is not None:
            return fixture
        return ("dyn", "untyped parameter")

    def fixture_index(self) -> dict[str, dict[str, str]]:
        index: dict[str, dict[str, str]] = {}
        for definition in self.defs.values():
            if definition.is_fixture:
                name = definition.qualname.split(".")[-1]
                index.setdefault(definition.module, {})[name] = definition.id
        return index

    def _fixture_type(self, scope: Scope, name: str) -> Value | None:
        """Type of a pytest fixture parameter, resolved the way pytest finds it."""
        if not name or not scope.def_id or not scope.module.startswith("tests"):
            return None
        fixture_id = self._lookup_fixture(scope.module, name, self.fixture_index())
        if not fixture_id or fixture_id == scope.def_id:
            return None
        node = self.defs[fixture_id].node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return None
        fixture_scope = self.scopes[fixture_id]
        outer = fixture_scope.parent or fixture_scope
        annotation = node.returns
        if isinstance(annotation, ast.Subscript):
            base = self.infer(annotation.value, outer)
            tail = base[-1].split(".")[-1] if base else ""
            if tail in ("Iterator", "Generator", "Iterable", "AsyncIterator"):
                inner = annotation.slice
                if isinstance(inner, ast.Tuple) and inner.elts:
                    inner = inner.elts[0]
                return self.type_from_annotation(inner, outer)
        if annotation is not None:
            return self.type_from_annotation(annotation, outer)
        for sub in own_nodes(list(node.body)):
            if isinstance(sub, (ast.Return, ast.Yield)) and sub.value is not None:
                value = self.infer(sub.value, fixture_scope)
                if value is not None:
                    return value
        return ("dyn", "pytest fixture")

    def _infer_unpack(self, scope: Scope, rhs: object, index: int) -> Value | None:
        if not isinstance(rhs, ast.expr):
            return None
        if isinstance(rhs, ast.IfExp):
            return self._infer_unpack(scope, rhs.body, index) or self._infer_unpack(
                scope, rhs.orelse, index
            )
        if isinstance(rhs, (ast.Tuple, ast.List)) and index < len(rhs.elts):
            return self.infer(rhs.elts[index], scope)
        if isinstance(rhs, ast.Call):
            callee = self.infer(rhs.func, scope)
            if callee and callee[0] == "def":
                fn = self.defs[callee[1]].node
                if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    ann = fn.returns
                    if isinstance(ann, ast.Subscript) and isinstance(
                        ann.slice, ast.Tuple
                    ):
                        elts = ann.slice.elts
                        if index < len(elts):
                            fscope = self.scopes[callee[1]].parent or scope
                            return self.type_from_annotation(elts[index], fscope)
                    for node in own_nodes(list(fn.body)):
                        if isinstance(node, ast.Return) and isinstance(
                            node.value, ast.Tuple
                        ):
                            if index < len(node.value.elts):
                                return self.infer(
                                    node.value.elts[index], self.scopes[callee[1]]
                                )
            if callee and callee[0] == "ext":
                return callee
        value = self.infer(rhs, scope)
        if value and value[0] == "ext":
            return value
        return None

    def _infer_iter(
        self, scope: Scope, iterable: object, index: int, depth: int = 0
    ) -> Value | None:
        if not isinstance(iterable, ast.expr) or depth > 6:
            return None
        # Unwrap builtins that pass elements through: enumerate/zip/sorted/list...
        if (
            isinstance(iterable, ast.Call)
            and isinstance(iterable.func, ast.Name)
            and self.resolve_name(scope, iterable.func.id)
            == ("builtin", iterable.func.id)
            and iterable.args
        ):
            name = iterable.func.id
            if name == "enumerate":
                if index == 0:
                    return ("builtin", "int")
                return self._infer_iter(scope, iterable.args[0], -1, depth + 1)
            if name == "zip":
                if 0 <= index < len(iterable.args):
                    return self._infer_iter(scope, iterable.args[index], -1, depth + 1)
                return None
            if name in ("sorted", "list", "reversed", "set", "tuple", "iter"):
                return self._infer_iter(scope, iterable.args[0], index, depth + 1)
        if isinstance(iterable, ast.Name):
            for binding in self._find_bindings(scope, iterable.id):
                if (
                    binding[0] == "expr"
                    and isinstance(binding[1], ast.Call)
                    and isinstance(binding[1].func, ast.Name)
                    and binding[1].func.id in ("list", "sorted", "tuple", "set")
                ):
                    return self._infer_iter(scope, binding[1], index, depth + 1)
        source: ast.expr | None = iterable
        method = ""
        if (
            isinstance(iterable, ast.Call)
            and isinstance(iterable.func, ast.Attribute)
            and iterable.func.attr in ("items", "values", "keys")
        ):
            method = iterable.func.attr
            source = iterable.func.value
        literal: ast.expr | None = source
        if isinstance(source, ast.Name):
            for binding in self._find_bindings(scope, source.id):
                if binding[0] == "expr" and isinstance(binding[1], ast.expr):
                    literal = binding[1]
                    break
        if isinstance(literal, ast.Dict) and literal.values:
            if (method == "items" and index == 1) or method == "values":
                for value_node in literal.values:
                    value = self.infer(value_node, scope)
                    if value is not None:
                        return value
            return (
                ("builtin", "str")
                if all(
                    isinstance(k, ast.Constant) and isinstance(k.value, str)
                    for k in literal.keys
                )
                else None
            )
        if isinstance(literal, (ast.List, ast.Tuple, ast.Set)) and literal.elts:
            first = literal.elts[0]
            if index >= 0 and isinstance(first, (ast.Tuple, ast.List)):
                if index < len(first.elts):
                    return self.infer(first.elts[index], scope)
                return None
            return self.infer(first, scope)
        value = self.infer(iterable, scope)
        if value and value[0] in ("ext", "dyn"):
            return value
        if value and value[0] == "builtin":
            element = self._element(value)
            if element is not None and index < 0:
                return element
            return ("dyn", "item of a builtin container")
        return None

    @staticmethod
    def _element(value: Value) -> Value | None:
        """Element type carried by an annotated builtin container, e.g. list[Chunk]."""
        if value[0] == "builtin" and len(value) > 2:
            return value[2:]
        return None

    def _find_bindings(self, scope: Scope, name: str) -> list[tuple[object, ...]]:
        current: Scope | None = scope
        while current is not None:
            if name in current.bindings:
                return current.bindings[name]
            current = current.parent
        return []

    def _resolve_import(self, binding: tuple[object, ...]) -> Value | None:
        if binding[1] == "mod":
            dotted = str(binding[2])
            if dotted in self.modules or self._first_party_prefix(dotted):
                return ("mod", dotted)
            return ("ext", dotted)
        base, name = str(binding[2]), str(binding[3])
        if base in self.modules:
            value = self.lookup_symbol(base, name)
            if value is not None:
                return value
        if f"{base}.{name}" in self.modules:
            return ("mod", f"{base}.{name}")
        if base in self.modules or self._first_party_prefix(base):
            return None
        return ("ext", f"{base}.{name}")

    def _first_party_prefix(self, dotted: str) -> bool:
        return any(m == dotted or m.startswith(dotted + ".") for m in self.modules)

    def lookup_symbol(self, module: str, name: str) -> Value | None:
        scope = self.scopes.get(module)
        if scope is None:
            return None
        if name in scope.bindings:
            return self._infer_bindings(scope, name)
        if f"{module}.{name}" in self.modules:
            return ("mod", f"{module}.{name}")
        return None

    def _instance_of(self, value: Value | None) -> Value | None:
        if value is None:
            return None
        if value[0] == "def" and self.defs[value[1]].kind == "class":
            return ("inst", value[1])
        if value[0] in ("ext", "builtin", "dyn"):
            return value
        return None

    def type_from_annotation(self, node: ast.expr, scope: Scope) -> Value | None:
        if isinstance(node, ast.Constant):
            if node.value is None:
                return None
            if isinstance(node.value, str):
                try:
                    parsed = ast.parse(node.value, mode="eval").body
                except SyntaxError:
                    return None
                return self.type_from_annotation(parsed, scope)
            return None
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            for side in (node.left, node.right):
                value = self.type_from_annotation(side, scope)
                if value is not None:
                    return value
            return None
        if isinstance(node, ast.Subscript):
            base = self.infer(node.value, scope)
            base_name = base[-1] if base else ""
            tail = base_name.split(".")[-1]
            elts = (
                list(node.slice.elts)
                if isinstance(node.slice, ast.Tuple)
                else [node.slice]
            )
            if tail in ("Optional", "Union"):
                for elt in elts:
                    value = self.type_from_annotation(elt, scope)
                    if value is not None:
                        return value
                return None
            if tail == "Annotated":
                return self.type_from_annotation(elts[0], scope)
            if tail == "Callable":
                return ("dyn", "callable-typed value")
            if tail == "type" and elts:
                return self.infer(elts[0], scope)  # the class object itself
            sequences = ("list", "set", "frozenset", "tuple", "Sequence", "Iterable")
            sequences += ("Iterator", "Generator", "MutableSequence", "AbstractSet")
            mappings = ("dict", "Mapping", "MutableMapping", "defaultdict")
            if base and base[0] in ("builtin", "ext") and tail in sequences + mappings:
                element_node = elts[-1] if tail in mappings else elts[0]
                element = self.type_from_annotation(element_node, scope)
                kind = "dict" if tail in mappings else tail
                if element is not None and element[0] in ("inst", "ext", "def"):
                    return ("builtin", kind, *element)
                return ("builtin", kind)
            if base and base[0] == "builtin":
                return ("builtin", tail)
            return self._instance_of(base) if base else None
        value = self.infer(node, scope)
        if value is None:
            return None
        if value[0] == "ext" and value[1].split(".")[-1] == "Callable":
            return ("dyn", "callable-typed value")
        if value[0] == "ext" and value[1] in ("typing.Any", "typing_extensions.Any"):
            return ("dyn", "Any-typed value")
        if value[0] == "dyn":
            return value
        return self._instance_of(value)

    def infer(self, node: ast.expr, scope: Scope) -> Value | None:
        if isinstance(node, ast.Name):
            return self.resolve_name(scope, node.id)
        if isinstance(node, ast.Attribute):
            return self.attribute(self.infer(node.value, scope), node.attr, scope)
        if isinstance(node, ast.Call):
            return self.call_result(self.infer(node.func, scope), scope)
        if isinstance(node, ast.Constant):
            return ("builtin", type(node.value).__name__)
        if isinstance(node, ast.JoinedStr):
            return ("builtin", "str")
        if isinstance(node, (ast.List, ast.ListComp)):
            return ("builtin", "list")
        if isinstance(node, ast.Tuple):
            return ("builtin", "tuple")
        if isinstance(node, (ast.Set, ast.SetComp)):
            return ("builtin", "set")
        if isinstance(node, ast.Dict):
            targets: list[str] = []
            for value_node in node.values:
                value = self.infer(value_node, scope) if value_node else None
                if value and value[0] == "def" and self.defs[value[1]].kind != "class":
                    targets.append(value[1])
                else:
                    targets = []
                    break
            if targets:
                return ("funcs", *dict.fromkeys(targets))
            return ("builtin", "dict")
        if isinstance(node, ast.DictComp):
            return ("builtin", "dict")
        if isinstance(node, ast.GeneratorExp):
            return ("builtin", "generator")
        if isinstance(node, ast.Lambda):
            return ("builtin", "function")
        if isinstance(node, (ast.Compare,)):
            return ("builtin", "bool")
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Not):
                return ("builtin", "bool")
            return self.infer(node.operand, scope)
        if isinstance(node, ast.BinOp):
            left = self.infer(node.left, scope)
            if left and left[0] in ("ext", "inst"):
                return left
            right = self.infer(node.right, scope)
            if right and right[0] in ("ext", "inst"):
                return right
            return left or right
        if isinstance(node, ast.BoolOp):
            for value_node in node.values:
                value = self.infer(value_node, scope)
                if value is not None and value != ("builtin", "NoneType"):
                    return value
            return None
        if isinstance(node, ast.IfExp):
            return self.infer(node.body, scope) or self.infer(node.orelse, scope)
        if isinstance(node, (ast.Await, ast.Starred, ast.NamedExpr)):
            return self.infer(node.value, scope)
        if isinstance(node, ast.Subscript):
            base = self.infer(node.value, scope)
            if base is None:
                return None
            if base[0] in ("funcs", "ext", "dyn"):
                return base
            if (
                base[0] == "inst"
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
            ):
                field_value = self.field_type(base[1], node.slice.value)
                if field_value is not None:
                    return field_value
            if base[0] == "builtin":
                element = self._element(base)
                if element is not None and not isinstance(node.slice, ast.Slice):
                    return element
                if isinstance(node.slice, ast.Slice):
                    return base
                return ("dyn", "item of a builtin container")
            return ("dyn", "subscript")
        return None

    def mro(self, class_id: str) -> tuple[list[str], list[Value]]:
        """First-party classes in method-resolution order, plus non-first-party bases."""
        order: list[str] = []
        outside: list[Value] = []
        stack = [class_id]
        while stack:
            current = stack.pop(0)
            if current in order:
                continue
            order.append(current)
            scope = self.scopes[current].parent or self.scopes[current]
            for base in self.class_bases.get(current, []):
                value = self.infer(base, scope)
                if value and value[0] == "def" and self.defs[value[1]].kind == "class":
                    stack.append(value[1])
                elif value is not None:
                    outside.append(value)
        return order, outside

    def member(self, class_id: str, name: str, skip_self: bool = False) -> str | None:
        order, _ = self.mro(class_id)
        for current in order[1:] if skip_self else order:
            member_id = self.class_members.get(current, {}).get(name)
            if member_id:
                return member_id
        return None

    def field_type(self, class_id: str, name: str) -> Value | None:
        order, _ = self.mro(class_id)
        for current in order:
            entry = self.class_fields.get(current, {}).get(name)
            if entry:
                scope_key, expr, is_annotation = entry
                scope = self.scopes[scope_key]
                if is_annotation:
                    return self.type_from_annotation(expr, scope)
                return self.infer(expr, scope)
        return None

    def attribute(self, base: Value | None, attr: str, scope: Scope) -> Value | None:
        if base is None:
            return None
        kind = base[0]
        if kind == "dyn":
            return base
        if kind == "mod":
            value = self.lookup_symbol(base[1], attr)
            if value is not None:
                return value
            if f"{base[1]}.{attr}" in self.modules:
                return ("mod", f"{base[1]}.{attr}")
            return None
        if kind == "ext":
            return ("ext", f"{base[1]}.{attr}")
        if kind == "builtin":
            return ("builtin", f"{base[1]}.{attr}")
        if kind == "funcs":
            return ("builtin", f"dict.{attr}")
        if kind in ("def", "inst", "super"):
            target = base[1]
            definition = self.defs[target]
            if kind == "def" and definition.kind != "class":
                return ("builtin", f"function.{attr}")
            member_id = self.member(target, attr, skip_self=kind == "super")
            if member_id:
                member_def = self.defs[member_id]
                if kind == "inst" and member_def.is_property:
                    return self.return_type(member_id)
                return ("def", member_id)
            if kind == "inst":
                field_value = self.field_type(target, attr)
                if field_value is not None:
                    return field_value
            _, outside = self.mro(target)
            for value in outside:
                if value[0] == "ext":
                    return ("ext", f"{value[1]}.{attr}")
            for value in outside:
                if value[0] == "builtin":
                    return ("builtin", f"{value[1]}.{attr}")
            return None
        return None

    def call_result(self, callee: Value | None, scope: Scope) -> Value | None:
        if callee is None:
            return None
        kind = callee[0]
        if kind == "def":
            definition = self.defs[callee[1]]
            if definition.kind == "class":
                return ("inst", callee[1])
            return self.return_type(callee[1])
        if kind == "funcs":
            return self.return_type(callee[1])
        if kind in ("ext", "dyn"):
            return callee
        if kind == "builtin":
            name = callee[1]
            if name == "super":
                current: Scope | None = scope
                while current is not None and current.cls is None:
                    current = current.parent
                if current is not None and current.cls:
                    return ("super", current.cls)
                return None
            if name in BUILTIN_TYPES:
                return ("builtin", name)
            return ("builtin", f"{name}()")
        if kind == "inst":
            call = self.member(callee[1], "__call__")
            return self.return_type(call) if call else None
        return None

    def return_type(self, def_id: str) -> Value | None:
        definition = self.defs[def_id]
        node = definition.node
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return None
        if def_id in self._ret_guard:
            return None
        self._ret_guard.add(def_id)
        try:
            scope = self.scopes[def_id]
            if node.returns is not None:
                return self.type_from_annotation(node.returns, scope.parent or scope)
            for sub in own_nodes(list(node.body)):
                if isinstance(sub, ast.Return) and sub.value is not None:
                    value = self.infer(sub.value, scope)
                    if value is not None:
                        return value
            return None
        finally:
            self._ret_guard.discard(def_id)

    # -------------------------------------------------------------- analysis

    def add_edge(
        self, source: str, target: str, kind: str, label: str, location: str
    ) -> None:
        info = self.edges.setdefault((source, target, kind), EdgeInfo())
        if label:
            info.labels.add(label)
        info.lines.append(location)

    def _loc(self, module: str, node: ast.AST) -> str:
        line = getattr(node, "lineno", 0)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            # cite the line of ".method(" in a chained, multi-line call
            line = node.func.end_lineno or line
        return f"{self.modules[module].path}:{line}"

    @staticmethod
    def _bound_locally(scope: Scope, name: str) -> bool:
        current: Scope | None = scope
        while current is not None:
            if name in current.bindings:
                return True
            current = current.parent
        return False

    def _short(self, node: ast.AST) -> str:
        text = ast.unparse(node).replace("\n", " ")
        return text if len(text) <= 90 else text[:87] + "..."

    def _analyse_def(self, definition: Def) -> None:
        module = definition.module
        info = self.modules[module]
        scope = self.scopes[definition.scope_key]
        outer = scope.parent or scope
        node = definition.node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            self._analyse_decorators(definition, node, outer)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            positional = [*args.posonlyargs, *args.args]
            defaults = list(
                zip(positional[len(positional) - len(args.defaults) :], args.defaults)
            )
            defaults += [(a, d) for a, d in zip(args.kwonlyargs, args.kw_defaults) if d]
            for arg, default in defaults:
                value = self.infer(default, outer)
                if value and value[0] == "def" and self.defs[value[1]].kind != "class":
                    self.add_edge(
                        definition.id,
                        value[1],
                        "registers",
                        f"default of {arg.arg}",
                        self._loc(module, default),
                    )
        annotation_nodes: set[int] = set()
        for sub in own_nodes(definition.code):
            if isinstance(sub, ast.AnnAssign):
                annotation_nodes.update(id(n) for n in ast.walk(sub.annotation))
        for sub in own_nodes(definition.code):
            if id(sub) in annotation_nodes:
                continue
            if isinstance(sub, ast.Call):
                self._analyse_call(definition, sub, scope, info)
            elif isinstance(sub, (ast.Name, ast.Attribute)) and isinstance(
                sub.ctx, ast.Load
            ):
                self._analyse_reference(definition, sub, scope, info)

    def _analyse_decorators(
        self,
        definition: Def,
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
        outer: Scope,
    ) -> None:
        module = definition.module
        for decorator in node.decorator_list:
            func = decorator.func if isinstance(decorator, ast.Call) else decorator
            value = self.infer(func, outer)
            if value is None:
                self.call_sites.append(
                    CallSite(
                        definition.id,
                        self._loc(module, decorator),
                        "@" + self._short(decorator),
                        "unresolved",
                        "decorator",
                    )
                )
                continue
            if value[0] == "ext":
                package = value[1].split(".")[0]
                definition.externals.add(package)
                tail = value[1].split(".")[-1]
                if (
                    isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Attribute)
                    and tail in HTTP_VERBS
                    and package in ("fastapi", "starlette")
                ):
                    path = ""
                    if decorator.args and isinstance(decorator.args[0], ast.Constant):
                        path = str(decorator.args[0].value)
                    app_name = ast.unparse(decorator.func.value)
                    route_id = f"route:{tail.upper()} {path}"
                    self.entries.append(
                        {
                            "id": route_id,
                            "kind": "api-route",
                            "label": f"{tail.upper()} {path}",
                            "module": module,
                            "app": app_name,
                            "location": self._loc(module, decorator),
                            "targets": [definition.id],
                        }
                    )
                    self.add_edge(
                        route_id,
                        definition.id,
                        "registers",
                        "route handler",
                        self._loc(module, decorator),
                    )
                    definition.entry.append(f"API route {tail.upper()} {path}")
                if package == "pytest" and tail == "fixture":
                    definition.is_fixture = True
            elif value[0] == "def" and self.defs[value[1]].kind != "class":
                self.add_edge(
                    value[1],
                    definition.id,
                    "registers",
                    "decorates",
                    self._loc(module, decorator),
                )
            if isinstance(decorator, ast.Call):
                # functions handed to a decorator (e.g. @pytest.mark.parametrize)
                label = f"argument to @{self._short(decorator.func)}"
                for sub in ast.walk(decorator):
                    if sub is decorator.func or not isinstance(
                        sub, (ast.Name, ast.Attribute)
                    ):
                        continue
                    parent = self.modules[module].parents.get(sub)
                    if isinstance(parent, ast.Attribute) and parent.value is sub:
                        continue
                    if isinstance(parent, ast.Call) and parent.func is sub:
                        continue
                    ref = self.infer(sub, outer)
                    if ref and ref[0] == "def" and self.defs[ref[1]].kind != "class":
                        self.add_edge(
                            definition.id,
                            ref[1],
                            "registers",
                            label,
                            self._loc(module, sub),
                        )

    def _library_dispatch(
        self, definition: Def, call: ast.Call, dotted: str, location: str
    ) -> None:
        """Library calls that run first-party code we can name statically."""
        tail = dotted.split(".")[-1]
        first = call.args[0] if call.args else None
        literal = (
            str(first.value)
            if isinstance(first, ast.Constant) and isinstance(first.value, str)
            else None
        )
        if literal is None:
            return
        if dotted == "importlib.import_module" and literal in self.modules:
            self.add_edge(
                definition.id,
                f"{literal}.<module>",
                "calls",
                "importlib.import_module (runs module-level code)",
                location,
            )
        if ".TestClient." in f"{dotted}." and tail in HTTP_VERBS:
            path = literal.split("?")[0]
            self.add_edge(
                definition.id,
                f"route:{tail.upper()} {path}",
                "calls",
                "HTTP request through TestClient",
                location,
            )

    def _analyse_call(
        self, definition: Def, call: ast.Call, scope: Scope, info: ModuleInfo
    ) -> None:
        module = definition.module
        location = self._loc(module, call)
        callee = self.infer(call.func, scope)
        expr = self._short(call.func)
        source = definition.id
        if callee is None:
            self.call_sites.append(CallSite(source, location, expr, "unresolved"))
        elif callee[0] == "def":
            target = self.defs[callee[1]]
            label = "constructs" if target.kind == "class" else ""
            self.add_edge(source, callee[1], "calls", label, location)
            self.call_sites.append(
                CallSite(source, location, expr, "first-party", callee[1])
            )
        elif callee[0] == "funcs":
            for target_id in callee[1:]:
                self.add_edge(
                    source, target_id, "calls", "via dict of functions", location
                )
            self.call_sites.append(
                CallSite(source, location, expr, "first-party", ", ".join(callee[1:]))
            )
        elif callee[0] == "ext":
            package = callee[1].split(".")[0]
            definition.externals.add(package)
            self.call_sites.append(
                CallSite(source, location, expr, "external", callee[1])
            )
            self._library_dispatch(definition, call, callee[1], location)
        elif (
            callee[0] == "builtin"
            and isinstance(call.func, ast.Name)
            and self._bound_locally(scope, call.func.id)
        ):
            # a variable/parameter holding a value of a builtin type is called
            self.call_sites.append(
                CallSite(
                    source,
                    location,
                    expr,
                    "dynamic",
                    f"call of a variable typed {callee[1]}",
                )
            )
        elif callee[0] == "builtin" or callee[0] == "super":
            self.call_sites.append(CallSite(source, location, expr, "builtin"))
        elif callee[0] == "dyn":
            self.call_sites.append(
                CallSite(source, location, expr, "dynamic", callee[1])
            )
        else:
            self.call_sites.append(
                CallSite(source, location, expr, "unresolved", callee[0])
            )
        self._analyse_task(definition, call, scope, info)
        # A call passed as an argument whose result is a function (closure factory).
        arguments: list[tuple[ast.expr, str]] = [
            (arg, f"argument to {expr}") for arg in call.args
        ]
        arguments += [
            (kw.value, f"{kw.arg}= of {expr}") for kw in call.keywords if kw.arg
        ]
        for arg, label in arguments:
            if isinstance(arg, ast.Call):
                value = self.infer(arg, scope)
                if value and value[0] == "def" and self.defs[value[1]].kind != "class":
                    self.add_edge(source, value[1], "registers", label, location)

    def _analyse_task(
        self, definition: Def, call: ast.Call, scope: Scope, info: ModuleInfo
    ) -> None:
        keywords = {kw.arg: kw.value for kw in call.keywords if kw.arg}
        if "python_callable" not in keywords:
            return
        task = keywords.get("task_id")
        task_id = str(task.value) if isinstance(task, ast.Constant) else "?"
        dag_id = ""
        parent = info.parents.get(call)
        while parent is not None and not dag_id:
            if isinstance(parent, ast.With):
                for item in parent.items:
                    ctx = item.context_expr
                    if isinstance(ctx, ast.Call):
                        for kw in ctx.keywords:
                            if kw.arg == "dag_id" and isinstance(
                                kw.value, ast.Constant
                            ):
                                dag_id = str(kw.value.value)
            parent = info.parents.get(parent)
        entry_id = f"airflow:{dag_id or definition.module}.{task_id}"
        location = self._loc(definition.module, call)
        targets: list[str] = []
        for name in TASK_CALLBACK_KWARGS:
            value_node = keywords.get(name)
            if value_node is None:
                continue
            self._handled.update(id(n) for n in ast.walk(value_node))
            value = self.infer(value_node, scope)
            if value and value[0] == "def":
                self.add_edge(entry_id, value[1], "registers", name, location)
                targets.append(value[1])
                self.defs[value[1]].entry.append(
                    f"Airflow task {dag_id}.{task_id} ({name})"
                )
        self.add_edge(definition.id, entry_id, "registers", "defines task", location)
        self.entries.append(
            {
                "id": entry_id,
                "kind": "airflow-task",
                "label": f"task {task_id}",
                "module": definition.module,
                "dag_id": dag_id,
                "task_id": task_id,
                "location": location,
                "targets": targets,
            }
        )

    def _analyse_reference(
        self,
        definition: Def,
        node: ast.Name | ast.Attribute,
        scope: Scope,
        info: ModuleInfo,
    ) -> None:
        if id(node) in self._handled:
            return
        parent = info.parents.get(node)
        if isinstance(parent, ast.Call) and parent.func is node:
            return
        if isinstance(parent, ast.Attribute) and parent.value is node:
            return
        value = self.infer(node, scope)
        module = definition.module
        location = self._loc(module, node)
        if isinstance(node, ast.Attribute):
            base = self.infer(node.value, scope)
            if base and base[0] == "inst":
                member_id = self.member(base[1], node.attr)
                if member_id and self.defs[member_id].is_property:
                    self.add_edge(
                        definition.id, member_id, "calls", "property", location
                    )
                    self.call_sites.append(
                        CallSite(
                            definition.id,
                            location,
                            self._short(node),
                            "first-party",
                            member_id + " (property)",
                        )
                    )
                    return
        if not value or value[0] != "def":
            return
        target = self.defs[value[1]]
        if target.kind == "class":
            return
        if isinstance(parent, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            return  # an alias; calls through it are resolved where they happen
        if isinstance(parent, ast.Return):
            self.add_edge(
                definition.id, target.id, "returns", "returns closure", location
            )
            return
        if isinstance(parent, ast.keyword):
            call = info.parents.get(parent)
            label = f"{parent.arg}="
            if isinstance(call, ast.Call):
                label += f" of {self._short(call.func)}"
        elif isinstance(parent, ast.Call):
            label = f"argument to {self._short(parent.func)}"
        elif isinstance(parent, ast.Dict):
            label = "dict value"
        elif isinstance(parent, (ast.List, ast.Tuple, ast.Set)):
            label = "in a collection"
        else:
            label = "referenced as a value"
        self.add_edge(definition.id, target.id, "registers", label, location)

    def _dispatch_edges(self) -> None:
        """Base method -> first-party overrides (a call typed to the base may land there)."""
        classes = [d for d in self.defs.values() if d.kind == "class"]
        for cls in classes:
            order, _ = self.mro(cls.id)
            for base_id in order[1:]:
                for name, base_member in self.class_members.get(base_id, {}).items():
                    override = self.class_members.get(cls.id, {}).get(name)
                    if override and not name.startswith("__"):
                        base_def = self.defs[base_member]
                        label = (
                            "abstract, implemented in"
                            if base_def.is_abstract
                            else "overridden in"
                        )
                        self.add_edge(
                            base_member,
                            override,
                            "dispatch",
                            f"{label} {cls.qualname}",
                            f"{self.modules[cls.module].path}:{self.defs[override].line}",
                        )

    def _constructor_edges(self) -> None:
        for definition in list(self.defs.values()):
            if definition.kind != "class":
                continue
            init = self.member(definition.id, "__init__")
            if init:
                self.add_edge(
                    definition.id,
                    init,
                    "calls",
                    "constructor",
                    f"{self.modules[definition.module].path}:{self.defs[init].line}",
                )

    # ------------------------------------------------------------- pytest

    def pytest_pass(self) -> None:
        """Mark pytest-collected tests and fixtures; resolve fixture parameters."""
        fixtures = self.fixture_index()
        for definition in self.defs.values():
            module = self.modules[definition.module]
            filename = module.path.rsplit("/", 1)[-1]
            if not definition.module.startswith("tests."):
                continue
            if not (filename.startswith("test_") or filename == "conftest.py"):
                continue
            name = definition.qualname.split(".")[-1]
            is_test_fn = definition.kind == "function" and name.startswith("test")
            is_test_method = (
                definition.kind == "method"
                and name.startswith("test")
                and definition.qualname.split(".")[0].startswith("Test")
            )
            if (is_test_fn or is_test_method) and filename.startswith("test_"):
                definition.is_test = True
            if not (definition.is_test or definition.is_fixture):
                continue
            node = definition.node
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for arg in node.args.args:
                fixture_id = self._lookup_fixture(definition.module, arg.arg, fixtures)
                if fixture_id and fixture_id != definition.id:
                    self.add_edge(
                        definition.id,
                        fixture_id,
                        "calls",
                        "fixture (via pytest)",
                        f"{module.path}:{node.lineno}",
                    )

    def _lookup_fixture(
        self, module: str, name: str, fixtures: dict[str, dict[str, str]]
    ) -> str | None:
        if name in fixtures.get(module, {}):
            return fixtures[module][name]
        package = module.rpartition(".")[0]
        while package:
            conftest = f"{package}.conftest"
            if name in fixtures.get(conftest, {}):
                return fixtures[conftest][name]
            package = package.rpartition(".")[0]
        return None
