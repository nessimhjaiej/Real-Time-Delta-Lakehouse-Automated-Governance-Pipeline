"""Join code facts, repo descriptions and lake stats into catalog records."""

from __future__ import annotations

import ast
import csv
import json
import re
from typing import Any

from toolkit.common import REPO_ROOT, delta_uri, is_pii, mask_if_pii, mask_value, rel
from toolkit.pages import _catalog_code as cc
from toolkit.pages import _catalog_docs as cd

MAX_SAMPLE_CHARS = 300
COL_REF_RE = re.compile(r"\bf?\.?col\(\s*'([^']+)'\s*\)")
DESCRIPTION_KINDS = ("semantic", "docstring", "comment", "policy", "doc")


def cite(file: str, line: int | None) -> str:
    return f"{file}:{line}" if line else file


def _raw_doc(mod: cc.Mod, qual: str | None) -> tuple[str, int] | None:
    node: ast.AST = mod.funcs[qual] if qual else mod.tree
    body = getattr(node, "body", [])
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[0].value.value, body[0].lineno
    return None


def _line_of(raw: str, start: int, needle: str) -> int:
    """Line inside a raw docstring where ``needle``'s first words appear."""
    words = needle.split()[:4]
    probe = " ".join(words)
    flat_lines = raw.splitlines()
    for i, line in enumerate(flat_lines):
        if words and words[0] in line:
            joined = " ".join(" ".join(flat_lines[i : i + 3]).split())
            if probe in joined:
                return start + i
    return start


def _first_paragraph(text: str) -> str:
    return cd._norm(re.split(r"\n\s*\n", text.strip())[0])


def _short(value: Any) -> Any:
    if isinstance(value, str) and len(value) > MAX_SAMPLE_CHARS:
        return value[:MAX_SAMPLE_CHARS] + "…"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, default=str)
        return _short(text)
    return value


class Catalog:
    """Builds the JSON records the page renders."""

    def __init__(
        self,
        lake_ids: list[str],
        logs: dict[str, dict[str, Any]],
        profiles: dict[str, dict[str, Any]],
        profile_errors: dict[str, str],
    ) -> None:
        self.lake_ids = lake_ids
        self.logs = logs
        self.profiles = profiles
        self.profile_errors = profile_errors
        self.mods = cc.load_modules()
        self.sites = cc.find_sites(self.mods)
        self.tracer = cc.Tracer(self.mods)
        self.generators = cc.raw_generators(self.mods)
        self.tasks = cc.dag_tasks(self.mods)
        self.services, self.compose_file = cc.compose_services()
        self.edges = cc.call_edges(self.mods)
        self.mains = cc.main_lines(self.mods)
        self.semantic = cd.semantic_entries()
        self.joins = cd.semantic_joins()
        self.policy = cd.access_policy()
        self.yaml_comments = cd.yaml_comment_blocks(
            [REPO_ROOT / "policies" / "access.yml"]
            + sorted((REPO_ROOT / "semantic").rglob("*.yml"))
        )
        self.md_lines = cd.markdown_lines()
        self.layers = cd.readme_layers()
        self.raw_files_read: list[str] = []
        self.traces: dict[str, list[tuple[cc.Site, cc.Trace]]] = {}
        for site in self.sites:
            if site.kind == "write" and site.df is not None:
                ctx = cc.Ctx(self.mods, site.mod, site.function)
                trace = self.tracer.trace(ctx, site.df, site.env)
                self.traces.setdefault(site.table, []).append((site, trace))
        self.view_names = self._semantic_table_map()

    # ------------------------------------------------------------ lookups

    def _semantic_table_map(self) -> dict[str, str]:
        """Semantic-layer table name -> lake table id.

        The DuckDB executor registers each Gold Delta table as a view named after
        it (the ``delta_scan`` read sites); a name maps to the table it scans.
        """
        out: dict[str, str] = {}
        for site in self.sites:
            if site.via == "duckdb delta_scan":
                out.setdefault(site.table.split(".", 1)[1], site.table)
        for tid in self.lake_ids:
            name = tid.split(".", 1)[1]
            same = [t for t in self.lake_ids if t.split(".", 1)[1] == name]
            if name not in out and len(same) == 1:
                out[name] = tid
        return out

    def _schema(self, tid: str) -> list[str]:
        prof = self.profiles.get(tid)
        return [c["name"] for c in prof["columns"]] if prof else []

    # ------------------------------------------------------------ tables

    def build(self) -> list[dict[str, Any]]:
        tables = [self._table(tid) for tid in self.lake_ids]
        by_id = {t["id"]: t for t in tables}
        self._inherit(by_id)
        self._propagate_masks(by_id)
        for table in tables:
            self._apply_masks(table)
            cols = table["columns"]
            table["described"] = sum(1 for c in cols if c["descriptions"])
            table["described_incl"] = sum(
                1 for c in cols if c["descriptions"] or c["inherited"]
            )
        return tables

    def code_only(self) -> list[dict[str, Any]]:
        """Lake paths named in code (sites, DAG datasets) but absent on MinIO."""
        named: dict[str, list[str]] = {}
        for site in self.sites:
            named.setdefault(site.table, []).append(
                f"{site.kind} {cite(rel(site.mod.path), site.line)}"
            )
        for task in self.tasks:
            for tid in task["outlets"] + task["inlets"]:
                named.setdefault(tid, []).append(
                    f"Airflow Dataset on task {task['task_id']} "
                    f"{cite(task['file'], task['line'])}"
                )
        return [
            {"id": tid, "where": where}
            for tid, where in sorted(named.items())
            if tid not in self.lake_ids
        ]

    def _table(self, tid: str) -> dict[str, Any]:
        layer, name = tid.split(".", 1)
        log = self.logs.get(tid, {})
        prof = self.profiles.get(tid)
        traces = self.traces.get(tid, [])
        writers = []
        for site, _ in traces:
            record = site.to_json()
            record["runs"] = cc.entry_chains(
                site.qualname,
                self.edges,
                self.tasks,
                self.services,
                self.mains,
                self.mods,
            )
            writers.append(record)
        readers = [
            s.to_json() for s in self.sites if s.kind == "read" and s.table == tid
        ]
        upstream: list[str] = []
        raw_sources: list[dict[str, Any]] = []
        table_ops: list[dict[str, Any]] = []
        transforms: list[dict[str, Any]] = []
        for _, trace in traces:
            upstream.extend(u for u in trace.reads if u not in upstream)
            for src in trace.sources:
                raw_sources.append(self._raw_source(src))
            table_ops.extend(trace.table_ops)
            for func in trace.funcs:
                if func["function"] not in {t["function"] for t in transforms}:
                    transforms.append(func)
        record: dict[str, Any] = {
            "id": tid,
            "layer": layer,
            "name": name,
            "uri": delta_uri(layer, name),
            "rows": prof["rows"] if prof else None,
            "profile_error": self.profile_errors.get(tid),
            "version": log.get("version"),
            "commit_files": log.get("commit_files"),
            "log_complete": log.get("log_complete"),
            "last_commit": log.get("last_commit") or {},
            "created": log.get("created"),
            "operations": log.get("operations") or {},
            "partition_columns": log.get("partition_columns") or [],
            "active_files": log.get("active_files"),
            "size_bytes": log.get("size_bytes"),
            "writers": writers,
            "readers": readers,
            "dag_outlet_of": [
                {
                    "dag_id": t["dag_id"],
                    "task_id": t["task_id"],
                    "cite": cite(t["file"], t["line"]),
                }
                for t in self.tasks
                if tid in t["outlets"]
            ],
            "upstream": upstream,
            "raw_sources": raw_sources,
            "table_ops": table_ops,
            "transforms": transforms,
            "descriptions": self._table_descriptions(tid, traces, raw_sources),
            "semantic": [
                {
                    "kind": e["kind"],
                    "name": e["name"],
                    "expression": e["expression"],
                    "cite": cite(e["file"], e["item_line"]),
                }
                for e in self.semantic
                if self.view_names.get(e["table"]) == tid
            ],
            "joins": [
                {**j, "cite": cite(j["file"], j["line"])}
                for j in self.joins
                if tid
                in (
                    self.view_names.get(j.get("left_table") or ""),
                    self.view_names.get(j.get("right_table") or ""),
                )
            ],
            "policy_roles": [
                {"role": role, "cite": cite(self.policy["file"], entry["tables"][name])}
                for role, entry in self.policy["roles"].items()
                if name in entry["tables"] and self.view_names.get(name) == tid
            ],
        }
        record["columns"] = self._columns(tid, prof, log, traces, raw_sources)
        record["sample_columns"] = [c["name"] for c in prof["columns"]] if prof else []
        record["_raw_rows"] = prof["sample_rows"] if prof else []
        return record

    def _raw_source(self, src: dict[str, Any]) -> dict[str, Any]:
        out = dict(src)
        key = (
            f"kafka:{src['topic']}"
            if src.get("kind") == "kafka" and src.get("topic")
            else (src["location"][0] if src.get("location") else None)
        )
        out["generator"] = self.generators.get(key) if key else None
        out["fields"] = None
        if src.get("kind") in ("csv", "api") and src.get("location"):
            path = REPO_ROOT / src["location"][0]
            out["fields"] = self._raw_fields(path, src["kind"])
            if out["fields"] is not None:
                self.raw_files_read.append(rel(path))
        if src.get("kind") == "kafka":
            out["topic_id"] = f"kafka.{src.get('topic')}"
        return out

    @staticmethod
    def _raw_fields(path: Any, kind: str) -> list[str] | None:
        """Header row of a CSV / top-level keys of a JSON array (no values read)."""
        try:
            if kind == "csv":
                with open(path, encoding="utf-8", newline="") as handle:
                    return next(csv.reader(handle))
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, StopIteration, json.JSONDecodeError, UnicodeDecodeError):
            return None
        if isinstance(data, list):
            keys: list[str] = []
            for item in data:
                if isinstance(item, dict):
                    keys.extend(k for k in item if k not in keys)
            return keys
        return None

    def _table_descriptions(
        self,
        tid: str,
        traces: list[tuple[cc.Site, cc.Trace]],
        raw_sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        layer, name = tid.split(".", 1)
        out: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(text: str, file: str, line: int | None, label: str) -> None:
            text = cd._norm(text)
            if text and text not in seen:
                seen.add(text)
                out.append({"text": text, "cite": cite(file, line), "label": label})

        for site, trace in traces:
            mod_doc = _raw_doc(site.mod, None)
            if mod_doc:
                raw, start = mod_doc
                summary = _first_paragraph(raw)
                add(
                    summary,
                    rel(site.mod.path),
                    _line_of(raw, start, summary),
                    "module docstring",
                )
                pattern = (
                    rf"\b{re.escape(name)}\b"
                    if "_" in name
                    else rf"\b{layer}[._]{re.escape(name)}\b"
                )
                for i, line in enumerate(raw.splitlines()):
                    if re.search(pattern, line) and cd._norm(line) != summary:
                        add(
                            line.strip(" -*"),
                            rel(site.mod.path),
                            start + i,
                            "module docstring",
                        )
            if site.function:
                doc = _raw_doc(site.mod, site.function)
                if doc:
                    para = _first_paragraph(doc[0])
                    add(
                        para,
                        rel(site.mod.path),
                        doc[1],
                        f"docstring of writer {site.qualname}",
                    )
            comment = cc._comment_for(site.mod, _stmt_line(site))
            if comment:
                add(
                    comment[0],
                    rel(site.mod.path),
                    comment[1],
                    "comment above the write",
                )
            for func in trace.funcs:
                if func["doc"]:
                    add(
                        _first_paragraph(func["doc"]),
                        func["file"],
                        func["doc_line"],
                        f"docstring of transform {func['function']}",
                    )
        for src in raw_sources:
            gen = src.get("generator")
            if gen and gen.get("doc"):
                add(
                    _first_paragraph(gen["doc"]),
                    gen.get("doc_file", gen["file"]),
                    gen.get("doc_line"),
                    f"docstring of raw-source generator "
                    f"{gen.get('event_function', gen['function'])}",
                )
        if layer in self.layers:
            entry = self.layers[layer]
            add(entry["text"], entry["file"], entry["line"], "README layer description")
        return out

    # ------------------------------------------------------------ columns

    def _columns(
        self,
        tid: str,
        prof: dict[str, Any] | None,
        log: dict[str, Any],
        traces: list[tuple[cc.Site, cc.Trace]],
        raw_sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        layer, name = tid.split(".", 1)
        delta = {f["name"]: f for f in (log.get("delta_schema") or [])}
        base = (
            prof["columns"]
            if prof
            else [{"name": f["name"], "duck_type": None} for f in delta.values()]
        )
        upstream_cols = {u: set(self._schema(u)) for _, t in traces for u in t.reads}
        out = []
        for col in base:
            cname = col["name"]
            ops = [
                op for _, trace in traces for op in trace.ops if op["column"] == cname
            ]
            produced = self._produced(cname, ops, traces, upstream_cols, raw_sources)
            hashed_from = self._hashed_from(ops)
            pii = is_pii(cname)
            dcol = delta.get(cname, {})
            # raw values stay in "_raw" until _apply_masks() has decided masking
            record = {
                "name": cname,
                "duck_type": col.get("duck_type"),
                "delta_type": dcol.get("type"),
                "nullable": dcol.get("nullable"),
                "nulls": col.get("nulls"),
                "null_pct": col.get("null_pct"),
                "distinct": col.get("distinct"),
                "_raw": {
                    "min": col.get("min"),
                    "max": col.get("max"),
                    "samples": col.get("samples", []),
                },
                "pii": pii,
                "hashed_from": hashed_from,
                "derived_from_masked": [],
                "masked": pii or bool(hashed_from),
                "descriptions": self._column_descriptions(tid, cname, ops, traces),
                "inherited": [],
                "produced": produced,
                "governance": self._governance(tid, cname),
            }
            out.append(record)
        return out

    @staticmethod
    def _hashed_from(ops: list[dict[str, Any]]) -> list[str]:
        """PII columns this column is a SHA-2 hash of (from the producing code)."""
        out: list[str] = []
        for op in ops:
            uses_sha = "sha2(" in op["expr"] or any(
                "sha2(" in r
                for h in op.get("helpers", [])
                for r in h.get("returns", [])
            )
            if not uses_sha:
                continue
            for ref in COL_REF_RE.findall(op["expr"]):
                if is_pii(ref) and ref not in out:
                    out.append(ref)
        return out

    def _produced(
        self,
        cname: str,
        ops: list[dict[str, Any]],
        traces: list[tuple[cc.Site, cc.Trace]],
        upstream_cols: dict[str, set[str]],
        raw_sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        creating = {
            "withColumn",
            "alias",
            "withColumnRenamed",
            "selectExpr",
            "from_json field",
        }
        made_before: set[str] = set()
        for _, trace in traces:
            for op in trace.ops:
                if op["column"] == cname:
                    break
                made_before.add(op["column"])
        for src in raw_sources:
            fields = src.get("fields")
            gen = src.get("generator") or {}
            gcol = (gen.get("columns") or {}).get(cname)
            if src.get("kind") == "kafka":
                if gcol:
                    out.append(
                        {
                            "kind": "raw source",
                            "text": (
                                f"Kafka topic {src.get('topic')}: event field set to "
                                f"{gcol['expr']} by {gen.get('event_function', gen.get('function'))}"
                            ),
                            "cite": cite(
                                gcol.get("file", gen.get("file", "")), gcol.get("line")
                            ),
                            "link": src.get("topic_id"),
                        }
                    )
                continue
            if fields is not None and cname in fields:
                location = src["location"][0]
                reader = src.get("reader") or {}
                text = f"field of raw file {location}"
                if reader.get("returns"):
                    text += f", read by {reader['returns'][0]}"
                out.append(
                    {
                        "kind": "raw source",
                        "text": text,
                        "cite": cite(src["file"], src["line"]),
                    }
                )
                if gcol and gcol.get("expr"):
                    out.append(
                        {
                            "kind": "raw source",
                            "text": f"written to {location} as {gcol['expr']} by {gen['function']}",
                            "cite": cite(gen["file"], gcol.get("line")),
                        }
                    )
                elif gen and gen.get("urls"):
                    out.append(
                        {
                            "kind": "raw source",
                            "text": f"{location} staged from {', '.join(gen['urls'])} by {gen['function']}",
                            "cite": cite(gen["file"], gen.get("line")),
                        }
                    )
        for op in ops:
            text = f"{op['kind']}: {op['expr']}"
            if op.get("condition"):
                text += f" [{op['condition']}]"
            entry: dict[str, Any] = {
                "kind": op["kind"],
                "text": text,
                "cite": cite(op["file"], op["line"]),
                "function": op.get("function"),
            }
            inputs = []
            # struct field access col('rating.rate') reads column 'rating'
            refs = sorted({r.split(".")[0] for r in COL_REF_RE.findall(op["expr"])})
            entry["refs"] = refs
            for ref in refs:
                if ref in made_before and ref != cname:
                    continue
                for up, cols in upstream_cols.items():
                    if ref in cols:
                        inputs.append(f"{up}.{ref}")
            if op.get("renamed_from"):
                for up, cols in upstream_cols.items():
                    if op["renamed_from"] in cols:
                        inputs.append(f"{up}.{op['renamed_from']}")
            entry["inputs"] = sorted(set(inputs))
            out.append(entry)
            for helper in op.get("helpers", []):
                out.append(
                    {
                        "kind": "helper",
                        "text": (
                            f"{helper['function']} returns "
                            f"{' | '.join(helper.get('returns', [])) or '?'}"
                        ),
                        "cite": cite(helper["file"], helper["line"]),
                        "function": helper["function"],
                    }
                )
        if not any(op["kind"] in creating for op in ops):
            ups = [f"{u}.{cname}" for u, cols in upstream_cols.items() if cname in cols]
            if ups:
                out.append(
                    {
                        "kind": "passthrough",
                        "text": "passed through unchanged from " + ", ".join(ups),
                        "cite": cite(rel(traces[0][0].mod.path), traces[0][0].line),
                        "inputs": ups,
                    }
                )
        return out

    def _column_descriptions(
        self,
        tid: str,
        cname: str,
        ops: list[dict[str, Any]],
        traces: list[tuple[cc.Site, cc.Trace]],
    ) -> list[dict[str, Any]]:
        layer, name = tid.split(".", 1)
        out: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def add(text: str, where: str, label: str, kind: str) -> None:
            text = cd._norm(text)
            if text and (text, where) not in seen:
                seen.add((text, where))
                out.append({"text": text, "cite": where, "label": label, "kind": kind})

        # semantic layer
        for entry in self.semantic:
            if self.view_names.get(entry["table"]) != tid:
                continue
            if cname in entry["columns"]:
                add(
                    entry["description"],
                    cite(entry["file"], entry["line"]),
                    f"semantic {entry['kind']} {entry['name']} ({entry['expression']})",
                    "semantic",
                )
        # docstrings of writer + transforms + helpers
        doc_sources: list[tuple[cc.Mod, str | None, str]] = []
        for site, trace in traces:
            doc_sources.append(
                (site.mod, site.function, f"docstring of {site.qualname}")
            )
            for func in trace.funcs:
                mod_name, qual = self._split_func(func["function"])
                if mod_name:
                    doc_sources.append(
                        (self.mods[mod_name], qual, f"docstring of {func['function']}")
                    )
        for op in ops:
            for helper in op.get("helpers", []):
                mod_name, qual = self._split_func(helper["function"])
                if mod_name and qual:
                    raw = _raw_doc(self.mods[mod_name], qual)
                    if raw:
                        para = _first_paragraph(raw[0])
                        add(
                            para,
                            cite(rel(self.mods[mod_name].path), raw[1]),
                            f"docstring of {helper['function']} (computes this column)",
                            "docstring",
                        )
        for mod, qual, label in doc_sources:
            raw = _raw_doc(mod, qual)
            if not raw:
                continue
            for sentence in cd.sentences(raw[0]):
                if cd.mentions(sentence, name, cname):
                    add(
                        sentence,
                        cite(rel(mod.path), _line_of(raw[0], raw[1], sentence)),
                        label,
                        "docstring",
                    )
        # comments on the producing line and inside the visited functions
        for op in ops:
            if op.get("comment"):
                add(
                    op["comment"],
                    cite(op["file"], op.get("comment_line")),
                    "comment on the producing line",
                    "comment",
                )
        for mod, qual, _ in doc_sources:
            if not qual:
                continue
            fn = mod.funcs[qual]
            end = getattr(fn, "end_lineno", fn.lineno)
            for line, text in mod.full_comments.items():
                if fn.lineno <= line <= end and cd.mentions(text, name, cname):
                    add(
                        text,
                        cite(rel(mod.path), line),
                        f"comment in {mod.name}.{qual}",
                        "comment",
                    )
        # YAML comments that name table.column explicitly
        for block in self.yaml_comments:
            if re.search(rf"\b{re.escape(name)}\.{re.escape(cname)}\b", block["text"]):
                if self.view_names.get(name) == tid:
                    add(
                        block["text"],
                        cite(block["file"], block["line"]),
                        "YAML comment",
                        "policy",
                    )
        # markdown lines that name the table and the column
        for line in self.md_lines:
            text = line["text"]
            if re.search(rf"\b{re.escape(name)}\.{re.escape(cname)}\b", text) or (
                re.search(rf"\b{re.escape(name)}\b", text)
                and cd.mentions(text, name, cname)
            ):
                add(text, cite(line["file"], line["line"]), "documentation", "doc")
        return out

    def _split_func(self, dotted: str) -> tuple[str | None, str | None]:
        for mod_name in sorted(self.mods, key=len, reverse=True):
            if dotted.startswith(mod_name + "."):
                qual = dotted[len(mod_name) + 1 :]
                if qual in self.mods[mod_name].funcs:
                    return mod_name, qual
        return None, None

    def _governance(self, tid: str, cname: str) -> list[dict[str, Any]]:
        name = tid.split(".", 1)[1]
        if self.view_names.get(name) != tid:
            return []
        out = []
        for role, entry in self.policy["roles"].items():
            allowed = entry["allowed"].get(name, {})
            pii = entry["pii"].get(name, {})
            if cname in pii:
                out.append(
                    {
                        "text": f"{role}: PII, blocked from SELECT and filters",
                        "kind": "fail",
                        "cite": cite(self.policy["file"], pii[cname]),
                    }
                )
            if cname in allowed:
                out.append(
                    {
                        "text": f"{role}: allowed column",
                        "kind": "ok",
                        "cite": cite(self.policy["file"], allowed[cname]),
                    }
                )
            elif name in entry["tables"] and cname not in pii:
                out.append(
                    {
                        "text": f"{role}: not in allowed_columns",
                        "kind": "warn",
                        "cite": cite(self.policy["file"], entry["tables"][name]),
                    }
                )
        return out

    # ------------------------------------------------------------ masking

    @staticmethod
    def _propagate_masks(by_id: dict[str, dict[str, Any]]) -> None:
        """Mask columns built from masked columns (e.g. ids that embed a PII hash).

        A column is masked when its name is PII (``is_pii``), when the code hashes
        a PII column into it, or -- to a fixpoint -- when one of its inputs (an
        upstream column it is passed/renamed/computed from, or a sibling column
        its expression references) is masked.
        """
        changed = True
        while changed:
            changed = False
            for tid, table in by_id.items():
                siblings = {c["name"]: c for c in table["columns"]}
                for col in table["columns"]:
                    if col["masked"]:
                        continue
                    sources: list[str] = []
                    for p in col["produced"]:
                        for ref in p.get("inputs", []):
                            utid, ucol = ref.rsplit(".", 1)
                            up = by_id.get(utid)
                            ucolrec = (
                                next(
                                    (c for c in up["columns"] if c["name"] == ucol),
                                    None,
                                )
                                if up
                                else None
                            )
                            if ucolrec and ucolrec["masked"]:
                                sources.append(ref)
                        for ref in p.get("refs", []):
                            sib = siblings.get(ref)
                            if sib is not None and sib is not col and sib["masked"]:
                                sources.append(f"{tid}.{ref}")
                    if sources:
                        col["masked"] = True
                        col["derived_from_masked"] = sorted(set(sources))
                        changed = True

    @staticmethod
    def _apply_masks(table: dict[str, Any]) -> None:
        """Replace raw values with display values; masked columns via mask_value."""

        def show(col: dict[str, Any], value: Any) -> Any:
            if value is None:
                return None
            value = _short(value)
            if is_pii(col["name"]):
                return mask_if_pii(col["name"], value)
            return mask_value(value) if col["masked"] else value

        by_name = {c["name"]: c for c in table["columns"]}
        for col in table["columns"]:
            raw = col.pop("_raw")
            col["min"] = show(col, raw["min"])
            col["max"] = show(col, raw["max"])
            col["samples"] = [show(col, v) for v in raw["samples"]]
        names = table["sample_columns"]
        table["sample_rows"] = [
            [
                show(by_name[n], v) if n in by_name else mask_value(v)
                for n, v in zip(names, row, strict=True)
            ]
            for row in table.pop("_raw_rows")
        ]

    # ------------------------------------------------------------ inheritance

    def _inherit(self, by_id: dict[str, dict[str, Any]]) -> None:
        """Copy upstream descriptions onto columns that pass through or are renamed/cast."""

        def upstream_of(col: dict[str, Any]) -> list[str]:
            ups: list[str] = []
            for p in col["produced"]:
                if p["kind"] == "passthrough":
                    ups.extend(p.get("inputs", []))
                elif p["kind"] == "withColumnRenamed":
                    ups.extend(p.get("inputs", []))
                elif p["kind"] in ("withColumn", "alias") and re.fullmatch(
                    rf"\w+: f?\.?col\('{re.escape(col['name'])}'\)\.cast\('\w+'\)( \[.*\])?",
                    p["text"],
                ):
                    ups.extend(p.get("inputs", []))
            return ups

        def resolve(tid: str, cname: str, depth: int) -> list[dict[str, Any]]:
            table = by_id.get(tid)
            if table is None or depth > 6:
                return []
            col = next((c for c in table["columns"] if c["name"] == cname), None)
            if col is None:
                return []
            found = [{**d, "via": f"{tid}.{cname}"} for d in col["descriptions"]]
            for up in upstream_of(col):
                utid, ucol = up.rsplit(".", 1)
                found.extend(resolve(utid, ucol, depth + 1))
            return found

        for tid, table in by_id.items():
            for col in table["columns"]:
                inherited: list[dict[str, Any]] = []
                keys = {(d["text"], d["cite"]) for d in col["descriptions"]}
                for up in upstream_of(col):
                    utid, ucol = up.rsplit(".", 1)
                    for d in resolve(utid, ucol, 1):
                        if (d["text"], d["cite"]) not in keys:
                            keys.add((d["text"], d["cite"]))
                            inherited.append(d)
                col["inherited"] = inherited


def _stmt_line(site: cc.Site) -> int:
    """First line of the statement holding the write call."""
    node = site.node
    cur = site.mod.parents.get(id(node)) if node is not None else None
    line = site.line
    while cur is not None and not isinstance(cur, ast.stmt):
        cur = site.mod.parents.get(id(cur))
    if cur is not None:
        line = cur.lineno
    return line
