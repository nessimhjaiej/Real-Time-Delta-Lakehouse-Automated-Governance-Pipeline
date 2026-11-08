"""Assemble the data model: join what the code claims with what the lake holds.

``build(conn)`` returns the JSON-serialisable snapshot for the data-model page.
Every key and relationship carries the code evidence (file:line) that produced
it, and a verification result computed with DuckDB against the Delta tables.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import yaml

from toolkit.common import REPO_ROOT, is_pii, list_lake_tables, rel
from toolkit.pages import _data_model_code as code
from toolkit.pages import _data_model_lake as lake

STRENGTH_ORDER = ("asserted", "propagated", "probe", "pre-dedup")


def _layer(table: str) -> str:
    return table.split(".", 1)[0]


def _policy_pii(resolve: Any) -> tuple[dict[str, dict[str, str]], list[str]]:
    """``policies/*.yml`` pii_columns -> {table id: {column: file:line}}."""
    out: dict[str, dict[str, str]] = defaultdict(dict)
    sources: list[str] = []
    base = REPO_ROOT / "policies"
    for path in sorted(base.glob("*.yml")) if base.is_dir() else []:
        root = yaml.compose(path.read_text(encoding="utf-8"))
        found = False
        for node in _walk_yaml(root):
            if not isinstance(node, yaml.MappingNode):
                continue
            for key, value in node.value:
                if key.value != "pii_columns" or not isinstance(
                    value, yaml.MappingNode
                ):
                    continue
                found = True
                for table_key, cols in value.value:
                    table = resolve(table_key.value)
                    if not table or not isinstance(cols, yaml.SequenceNode):
                        continue
                    for item in cols.value:
                        out[table][
                            str(item.value)
                        ] = f"{rel(path)}:{item.start_mark.line + 1}"
        if found:
            sources.append(rel(path))
    return dict(out), sources


def _walk_yaml(node: Any) -> Any:
    yield node
    if isinstance(node, yaml.MappingNode):
        for key, value in node.value:
            yield from _walk_yaml(key)
            yield from _walk_yaml(value)
    elif isinstance(node, yaml.SequenceNode):
        for item in node.value:
            yield from _walk_yaml(item)


def _steps_text(steps: list[dict[str, Any]]) -> str:
    return " → ".join(f"{s['kind']}: {s['expr']} ({s['loc']})" for s in steps)


def build(conn: Any) -> dict[str, Any]:
    sources: list[str] = []
    tables = list_lake_tables(conn)
    sources.append(
        "duckdb: glob('s3://lakehouse/*/*/_delta_log/*.json') via toolkit.common.list_lake_tables"
    )
    lake_info = lake.load_tables(conn, tables)
    sources.extend(
        f"duckdb: delta_scan('{lake_info[t]['uri']}') -> DESCRIBE, count(*), per-column null/distinct counts"
        for t in tables
    )
    logs = lake.delta_logs(conn)
    sources.append(
        f"duckdb: read_ndjson_objects('{lake.LOG_GLOB}') -> metaData.schemaString, partitionColumns, commitInfo"
    )

    by_name: dict[str, list[str]] = defaultdict(list)
    for table in tables:
        by_name[table.split(".", 1)[1]].append(table)

    def resolve_bare(name: str) -> str | None:
        hits = by_name.get(name, [])
        return hits[0] if len(hits) == 1 else None

    project = code.interpret_repo()
    ge = code.ge_expectations(project)
    docs = code.docstring_claims(project)
    sqls = code.sql_strings()
    probes = code.notebook_probes(project)
    semantic = code.semantic_layer()
    consumers = code.join_consumers()
    policy_pii, policy_sources = _policy_pii(resolve_bare)
    notes: list[dict[str, str]] = [
        {"kind": "interpreter", "text": e} for e in project.errors
    ]

    # ---------------------------------------------------------------- tables
    model: dict[str, dict[str, Any]] = {}
    for table in tables:
        info = lake_info[table]
        log = logs.get(table, {})
        fields = {f["name"]: f for f in log.get("fields", [])}
        columns = []
        for col in info.get("columns", []):
            name = col["name"]
            stats = info.get("col_stats", {}).get(name, {})
            columns.append(
                {
                    "name": name,
                    "duckdb_type": col["duckdb_type"],
                    "delta_type": fields.get(name, {}).get("type"),
                    "nullable": fields.get(name, {}).get("nullable"),
                    "nulls": stats.get("nulls"),
                    "distinct": stats.get("distinct"),
                    "pii_name": is_pii(name),
                    "pii_policy": policy_pii.get(table, {}).get(name),
                    "upstream": [],
                    "downstream": [],
                    "not_null_code": [],
                    "semantic": [],
                    "generated": None,
                    "external": None,
                }
            )
        delta_names = [f["name"] for f in log.get("fields", [])]
        model[table] = {
            "id": table,
            "layer": _layer(table),
            "name": table.split(".", 1)[1],
            "uri": info["uri"],
            "error": info.get("error"),
            "rows": info.get("rows"),
            "columns": columns,
            "schema_matches_delta_log": (
                [c["name"] for c in columns] == delta_names if delta_names else None
            ),
            "partition_columns": log.get("partition_columns"),
            "delta_versions": log.get("versions"),
            "latest_version": log.get("latest_version"),
            "latest_operation": log.get("latest_operation"),
            "schema_history": log.get("schema_history", []),
            "writers": [],
        }

    def column(table: str, name: str) -> dict[str, Any] | None:
        for col in model.get(table, {}).get("columns", []):
            if col["name"] == name:
                return col
        return None

    # ---------------------------------------------------------------- writers + column lineage
    sinks_by_table: dict[str, list[code.Sink]] = defaultdict(list)
    for sink in project.sinks:
        sinks_by_table[sink.table].append(sink)
        sources.append(f"{sink.file}:{sink.line}")
    lineage: list[dict[str, Any]] = []
    for table, sinks in sinks_by_table.items():
        if table not in model:
            notes.append(
                {
                    "kind": "missing table",
                    "text": f"{table} is written by code ({sinks[0].file}:{sinks[0].line}) but has no Delta log on MinIO",
                }
            )
            continue
        actual = [c["name"] for c in model[table]["columns"]]
        for sink in sinks:
            upstream_tables = sorted(s for s in sink.df.sources() if s in model)
            externals = sorted(
                s.removeprefix("external:")
                for s in sink.df.sources()
                if s.startswith("external:")
            )
            model[table]["writers"].append(
                {
                    "function": sink.function,
                    "loc": f"{sink.file}:{sink.line}",
                    "mode": sink.mode,
                    "format": sink.fmt,
                    "partition_by_code": list(sink.partition_by),
                    "reads": upstream_tables,
                    "external": externals,
                    "via": [f"{q} ({f}:{ln})" for q, f, ln in sink.df.via],
                }
            )
            explained = code.sink_columns(sink, actual)
            for missing in explained["code_columns_missing_in_delta"]:
                notes.append(
                    {
                        "kind": "schema drift",
                        "text": f"code at {sink.file}:{sink.line} writes {table}.{missing}, which the Delta schema does not have",
                    }
                )
            for col_name, inputs in explained["columns"].items():
                target = column(table, col_name)
                assert target is not None
                if inputs is None:
                    target.setdefault("unexplained", []).append(
                        f"{sink.file}:{sink.line}"
                    )
                    continue
                for item in inputs:
                    src = item["source"]
                    if src == "generated":
                        target["generated"] = _steps_text(item["steps"])
                        continue
                    if src.startswith("external:"):
                        target["external"] = src.removeprefix("external:")
                        continue
                    if src not in model:
                        target.setdefault("unexplained", []).append(
                            f"{src}.{item['col']} ({sink.file}:{sink.line})"
                        )
                        continue
                    kinds = [s["kind"] for s in item["steps"]]
                    base = item["col"].split(".")[0]
                    edge = {
                        "from_table": src,
                        "from_col": item["col"],
                        "to_table": table,
                        "to_col": col_name,
                        "derivation": (
                            "derived from code" if kinds else "derived by name match"
                        ),
                        "one_to_one": all(k in code.ONE_TO_ONE for k in kinds),
                        "kinds": kinds,
                        "steps": item["steps"],
                        "writer": f"{sink.file}:{sink.line}",
                        "from_exists": column(src, base) is not None,
                    }
                    edge["i"] = len(lineage)
                    lineage.append(edge)
                    target["upstream"].append(edge)
                    source_col = column(src, base)
                    if source_col is not None:
                        source_col["downstream"].append(edge)
                    else:
                        notes.append(
                            {
                                "kind": "lineage",
                                "text": f"code maps {src}.{item['col']} -> {table}.{col_name}, but {src} has no column {base}",
                            }
                        )
            for part in sink.partition_by:
                if part not in (model[table]["partition_columns"] or []):
                    notes.append(
                        {
                            "kind": "partition",
                            "text": f"{sink.file}:{sink.line} partitions {table} by {part}; "
                            f"Delta log partitionColumns = {model[table]['partition_columns']}",
                        }
                    )
    for table in model:
        if table not in sinks_by_table:
            notes.append(
                {
                    "kind": "no writer",
                    "text": f"{table} exists on MinIO but no code path that writes it was found (writer not derivable)",
                }
            )

    def hashed(table: str, col: str, seen: frozenset[str] = frozenset()) -> list[str]:
        """Hash steps applied to this column anywhere upstream along 1:1 edges."""
        key = f"{table}.{col}"
        if key in seen:
            return []
        target = column(table, col)
        found: list[str] = []
        for edge in target["upstream"] if target else []:
            if not edge["one_to_one"]:
                continue
            found.extend(
                f"{s['expr']} ({s['loc']})"
                for s in edge["steps"]
                if s["kind"] == "hash"
            )
            found.extend(
                hashed(edge["from_table"], edge["from_col"].split(".")[0], seen | {key})
            )
        return list(dict.fromkeys(found))

    # ---------------------------------------------------------------- column constraints from code
    for exp in ge:
        if not exp["table"] or exp["table"] not in model:
            if exp["table"] is None:
                notes.append(
                    {
                        "kind": "GE",
                        "text": f"{exp['loc']}: suite {exp['suite']} key {exp['suite_key']} does not map to a lake table",
                    }
                )
            continue
        if exp["expectation"] == "ExpectColumnValuesToNotBeNull":
            for col in exp["columns"]:
                target = column(exp["table"], col)
                if target is None:
                    notes.append(
                        {
                            "kind": "GE",
                            "text": f"{exp['loc']}: {exp['table']} has no column {col}",
                        }
                    )
                else:
                    target["not_null_code"].append(
                        {"kind": "GE not-null", "loc": exp["loc"]}
                    )
    for probe in probes:
        if probe["kind"] == "null_probe" and probe.get("table") in model:
            for col in probe["columns"]:
                target = column(probe["table"], col)
                if target is not None:
                    target["not_null_code"].append(
                        {"kind": "notebook null probe", "loc": probe["loc"]}
                    )
    for rec in semantic["dimensions"] + semantic["metrics"]:
        table = resolve_bare(rec.get("table", ""))
        for ref in rec.get("_refs", []):
            target = column(table, ref) if table else None
            if target is not None:
                role = "dimension" if "column" in rec else "metric"
                target["semantic"].append(
                    {"name": rec.get("name"), "role": role, "loc": rec["_loc"]}
                )
            elif table:
                notes.append(
                    {
                        "kind": "semantic",
                        "text": f"{rec['_loc']}: {rec.get('name')} uses {table}.{ref}, which is not a Delta column",
                    }
                )

    # ---------------------------------------------------------------- key candidates
    keys: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}

    def add_key(
        table: str | None,
        cols: list[str],
        kind: str,
        text: str,
        loc: str,
        strength: str,
    ) -> None:
        if not table or table not in model or not cols:
            return
        entry = keys.setdefault(
            (table, tuple(cols)),
            {"table": table, "columns": list(cols), "evidence": []},
        )
        if not any(e["loc"] == loc and e["kind"] == kind for e in entry["evidence"]):
            entry["evidence"].append(
                {"kind": kind, "text": text, "loc": loc, "strength": strength}
            )

    for exp in ge:
        if exp["expectation"] in (
            "ExpectColumnValuesToBeUnique",
            "ExpectCompoundColumnsToBeUnique",
            "ExpectSelectColumnValuesToBeUniqueWithinRecord",
        ):
            add_key(
                exp["table"],
                exp["columns"],
                "GE unique",
                f"{exp['expectation']}({', '.join(exp['columns'])}) in {exp['suite']} "
                f"(suite chosen by key {exp['suite_key']!r} at {exp['mapping_loc']})",
                exp["loc"],
                "asserted",
            )

    via_tables: dict[str, set[str]] = defaultdict(set)
    for sink in project.sinks:
        for qual, _, _ in sink.df.via:
            via_tables[qual].add(sink.table)
    fk_doc_hints: list[dict[str, Any]] = []
    for claim in docs:
        if claim["kind"] in ("grain", "keyed_by", "primary_key"):
            for table in sorted(via_tables.get(claim["function"], set())):
                add_key(
                    table,
                    claim["columns"],
                    "docstring",
                    f"docstring of {claim['function']} ({claim['kind'].replace('_', ' ')})",
                    claim["loc"],
                    "asserted",
                )
        elif claim["kind"] == "foreign_keys":
            for table in sorted(via_tables.get(claim["function"], set())):
                for col in claim["columns"]:
                    fk_doc_hints.append(
                        {
                            "table": table,
                            "col": col,
                            "loc": claim["loc"],
                            "function": claim["function"],
                        }
                    )

    surrogates: list[dict[str, Any]] = []
    dedup_keys: list[dict[str, Any]] = []
    for table, sinks in sinks_by_table.items():
        if table not in model:
            continue
        actual = {c["name"] for c in model[table]["columns"]}
        for sink in sinks:
            for sur in {
                (s.file, s.line, s.column): s for s in sink.df.surrogates
            }.values():
                if sur.column in actual:
                    loc = f"{sur.file}:{sur.line}"
                    add_key(
                        table,
                        [sur.column],
                        "surrogate",
                        f"surrogate key built as {sur.expr}",
                        loc,
                        "asserted",
                    )
                    surrogates.append(
                        {
                            "table": table,
                            "column": sur.column,
                            "components": list(sur.components),
                            "expr": sur.expr,
                            "loc": loc,
                        }
                    )
            for dedup in {(d.file, d.line): d for d in sink.df.dedups}.values():
                loc = f"{dedup.file}:{dedup.line}"
                mapped: list[str] = []
                inputs_tables: set[str] = set()
                for name in dedup.names:
                    ups = dedup.keys.get(name) or []
                    if len(ups) != 1:
                        break
                    up = ups[0]
                    inputs_tables.add(up.source)
                    hit = None
                    for out_col in sorted(actual):
                        resolved = sink.df.resolve(out_col) or []
                        if (
                            len(resolved) == 1
                            and resolved[0].source == up.source
                            and resolved[0].col == up.col
                            and all(s[0] in code.ONE_TO_ONE for s in resolved[0].steps)
                        ):
                            hit = out_col
                            break
                    if hit is None:
                        break
                    mapped.append(hit)
                stream = (
                    f"; streaming, withWatermark({dedup.streaming})"
                    if dedup.streaming
                    else ""
                )
                if len(mapped) == len(dedup.names):
                    renamed = ", ".join(
                        f"{a}→{b}" if a != b else a
                        for a, b in zip(dedup.names, mapped, strict=True)
                    )
                    add_key(
                        table,
                        mapped,
                        "dedup",
                        f"dropDuplicates([{', '.join(dedup.names)}]) before write; mapped to output columns: {renamed}{stream}",
                        loc,
                        "asserted",
                    )
                    dedup_keys.append(
                        {
                            "table": table,
                            "columns": mapped,
                            "names": list(dedup.names),
                            "loc": loc,
                            "streaming": dedup.streaming,
                        }
                    )
                else:
                    notes.append(
                        {
                            "kind": "dedup",
                            "text": f"{loc}: dedup key {list(dedup.names)} could not be mapped 1:1 onto {table}'s columns",
                        }
                    )
                for src in inputs_tables:
                    if src in model and all(
                        column(src, n) is not None for n in dedup.names
                    ):
                        add_key(
                            src,
                            list(dedup.names),
                            "dedup input",
                            f"deduplicated on the way to {table} — duplicates are expected here and removed downstream{stream}",
                            loc,
                            "pre-dedup",
                        )

    attributed = {(d.file, d.line) for sink in project.sinks for d in sink.df.dedups}
    for (file, line), names in project.all_dedups.items():
        if (file, line) not in attributed:
            notes.append(
                {
                    "kind": "dedup",
                    "text": f"{file}:{line}: dropDuplicates({list(names)}) does not reach a Delta write "
                    "on MinIO, so it is not attributed to a lake table",
                }
            )

    for probe in probes:
        if probe["kind"] in (
            "distinct_probe",
            "duplicate_probe",
            "window_probe",
        ) and probe.get("table"):
            add_key(
                probe["table"],
                probe["columns"],
                "notebook probe",
                f"{probe['kind'].replace('_', ' ')} (view {probe['view']} registered at {probe['view_loc']}): {probe['sql'][:160]}",
                probe["loc"],
                "probe",
            )

    # ---------------------------------------------------------------- relationships
    fks: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    def add_fk(
        child: str | None,
        ccol: str,
        parent: str | None,
        pcol: str,
        kind: str,
        text: str,
        loc: str,
        strength: str,
    ) -> bool:
        if not child or not parent or child not in model or parent not in model:
            notes.append(
                {
                    "kind": "relationship",
                    "text": f"{loc}: {kind} {child}.{ccol} -> {parent}.{pcol} does not resolve to two lake tables",
                }
            )
            return False
        key = (child, ccol, parent, pcol)
        new = key not in fks
        entry = fks.setdefault(
            key,
            {
                "child": child,
                "child_col": ccol,
                "parent": parent,
                "parent_col": pcol,
                "evidence": [],
            },
        )
        if not any(e["loc"] == loc and e["kind"] == kind for e in entry["evidence"]):
            entry["evidence"].append(
                {"kind": kind, "text": text, "loc": loc, "strength": strength}
            )
        return new

    consumer_text = (
        f"; compiled into JOIN … ON at {', '.join(consumers)}" if consumers else ""
    )
    for join in semantic["joins"]:
        add_fk(
            resolve_bare(join.get("left_table", "")),
            join.get("left_key", ""),
            resolve_bare(join.get("right_table", "")),
            join.get("right_key", ""),
            "semantic join",
            f"{join.get('join_type', '?')} join {join.get('left_table')}.{join.get('left_key')} = "
            f"{join.get('right_table')}.{join.get('right_key')}{consumer_text}",
            join["_loc"],
            "declared",
        )
    for claim in docs:
        if claim["kind"] == "joins_to":
            add_fk(
                resolve_bare(claim["child"][0]),
                claim["child"][1],
                resolve_bare(claim["parent"][0]),
                claim["parent"][1],
                "docstring",
                f"docstring of {claim['function']}: “{'.'.join(claim['child'])} joins to {'.'.join(claim['parent'])}”",
                claim["loc"],
                "declared",
            )
    for sql in sqls:
        left, right = resolve_bare(sql["left"][0]), resolve_bare(sql["right"][0])
        pair = [(left, sql["left"][1]), (right, sql["right"][1])]
        # orient child -> parent by an existing declaration, else as written
        if (right, sql["right"][1], left, sql["left"][1]) in fks:
            pair.reverse()
        add_fk(
            pair[0][0],
            pair[0][1],
            pair[1][0],
            pair[1][1],
            "SQL in code",
            f"JOIN … ON {'.'.join(sql['left'])} = {'.'.join(sql['right'])}",
            sql["loc"],
            "declared",
        )
    for hint in fk_doc_hints:
        matched = [
            fk for k, fk in fks.items() if k[0] == hint["table"] and k[1] == hint["col"]
        ]
        for fk in matched:
            fk["evidence"].append(
                {
                    "kind": "docstring",
                    "text": f"docstring of {hint['function']} lists {hint['col']} as a foreign key",
                    "loc": hint["loc"],
                    "strength": "declared",
                }
            )
        if not matched:
            notes.append(
                {
                    "kind": "relationship",
                    "text": f"{hint['loc']}: docstring calls {hint['table']}.{hint['col']} a foreign key "
                    "but no parent is stated anywhere (not derivable)",
                }
            )

    def one_to_one_ups(table: str, col: str) -> list[dict[str, Any]]:
        target = column(table, col)
        return [
            e
            for e in (target["upstream"] if target else [])
            if e["one_to_one"] and "." not in e["from_col"]
        ]

    worklist = list(fks)
    while worklist:
        child, ccol, parent, pcol = worklist.pop()
        for cu in one_to_one_ups(child, ccol):
            for pu in one_to_one_ups(parent, pcol):
                if (
                    _layer(cu["from_table"]) != _layer(pu["from_table"])
                    or cu["from_table"] == pu["from_table"]
                ):
                    continue
                c_hash = [s["expr"] for s in cu["steps"] if s["kind"] == "hash"]
                p_hash = [s["expr"] for s in pu["steps"] if s["kind"] == "hash"]
                if c_hash or p_hash:
                    same = (
                        "same hash expression on both sides, so hashed keys stay joinable"
                        if c_hash == p_hash
                        else f"hashing differs: child {c_hash or 'none'} vs parent {p_hash or 'none'}"
                    )
                else:
                    same = "no hashing between the layers"
                text = (
                    f"projected from {child}.{ccol} → {parent}.{pcol} back through column lineage: "
                    f"{child}.{ccol} ← {cu['from_table']}.{cu['from_col']} [{', '.join(cu['kinds']) or 'name match'}], "
                    f"{parent}.{pcol} ← {pu['from_table']}.{pu['from_col']} [{', '.join(pu['kinds']) or 'name match'}]; {same}"
                )
                locs = ", ".join(
                    dict.fromkeys(
                        [
                            *(s["loc"] for s in cu["steps"] + pu["steps"]),
                            cu["writer"],
                            pu["writer"],
                        ]
                    )
                )
                if add_fk(
                    cu["from_table"],
                    cu["from_col"],
                    pu["from_table"],
                    pu["from_col"],
                    "lineage projection",
                    text,
                    locs,
                    "projected",
                ):
                    worklist.append(
                        (
                            cu["from_table"],
                            cu["from_col"],
                            pu["from_table"],
                            pu["from_col"],
                        )
                    )

    for fk in fks.values():
        strength = (
            "declared"
            if any(e["strength"] == "declared" for e in fk["evidence"])
            else "projected"
        )
        fk["strength"] = strength
        add_key(
            fk["parent"],
            [fk["parent_col"]],
            "FK parent",
            f"referenced by {fk['child']}.{fk['child_col']} ({strength}) — a parent key must be unique or joins fan out",
            fk["evidence"][0]["loc"],
            "asserted" if strength == "declared" else "propagated",
        )

    # forward propagation of single-column keys along 1:1 lineage
    changed = True
    while changed:
        changed = False
        for (table, cols), entry in list(keys.items()):
            if len(cols) != 1 or not any(
                e["strength"] in ("asserted", "propagated") for e in entry["evidence"]
            ):
                continue
            source_col = column(table, cols[0])
            for edge in source_col["downstream"] if source_col else []:
                if not edge["one_to_one"] or edge["from_col"] != cols[0]:
                    continue
                target = column(edge["to_table"], edge["to_col"])
                fan_in = (
                    len({e["from_table"] for e in target["upstream"]}) if target else 1
                )
                union = (
                    f"; {edge['to_table']}.{edge['to_col']} is unioned from {fan_in} tables, and no code checks uniqueness across them"
                    if fan_in > 1
                    else ""
                )
                before = len(
                    keys.get((edge["to_table"], (edge["to_col"],)), {}).get(
                        "evidence", []
                    )
                )
                add_key(
                    edge["to_table"],
                    [edge["to_col"]],
                    "propagated",
                    f"key {table}.{cols[0]} flows here 1:1 [{', '.join(edge['kinds']) or 'name match'}]{union}",
                    edge["writer"],
                    "propagated",
                )
                if (
                    len(keys[(edge["to_table"], (edge["to_col"],))]["evidence"])
                    > before
                ):
                    changed = True

    # ---------------------------------------------------------------- verify against the data
    for entry in keys.values():
        table, cols = entry["table"], entry["columns"]
        entry["id"] = f"{table}:{'+'.join(cols)}"
        strengths = [e["strength"] for e in entry["evidence"]]
        entry["strength"] = min(strengths, key=STRENGTH_ORDER.index)
        missing = [c for c in cols if column(table, c) is None]
        if missing or model[table].get("error"):
            entry["check"] = None
            entry["status"] = "fail"
            entry["verdict"] = (
                f"column(s) {missing} not in the Delta schema"
                if missing
                else "table unreadable"
            )
            continue
        check = lake.check_key(conn, table, cols)
        entry["check"] = check
        bad = check["duplicate_keys"] or check["null_rows"]
        verdict = (
            f"{check['duplicate_keys']} key value(s) repeat ({check['rows_in_duplicate_keys']} rows, {check['surplus_rows']} surplus); "
            f"{check['null_rows']} row(s) with a null key"
            if bad
            else f"unique and non-null across {check['rows']} rows"
        )
        entry["verdict"] = verdict
        if entry["strength"] in ("asserted", "propagated"):
            entry["status"] = "fail" if bad else "ok"
        else:
            entry["status"] = "info"

    for fk in fks.values():
        ctype = (column(fk["child"], fk["child_col"]) or {}).get("duckdb_type")
        ptype = (column(fk["parent"], fk["parent_col"]) or {}).get("duckdb_type")
        fk["types"] = [ctype, ptype]
        fk["id"] = f"{fk['child']}.{fk['child_col']}->{fk['parent']}.{fk['parent_col']}"
        if ctype is None or ptype is None:
            fk["check"] = None
            fk["status"] = "fail"
            fk["verdict"] = "column missing from the Delta schema"
            continue
        check = lake.check_fk(
            conn,
            fk["child"],
            fk["child_col"],
            fk["parent"],
            fk["parent_col"],
            (ctype, ptype),
        )
        fk["check"] = check
        problems = []
        if check["orphan_rows"]:
            problems.append(
                f"{check['orphan_rows']} orphan row(s) ({check['orphan_keys']} distinct key(s)) with no parent"
            )
        if check["parent_duplicate_keys"]:
            problems.append(
                f"parent key repeats for {check['parent_duplicate_keys']} value(s), so joins fan out"
            )
        if check["null_fk_rows"]:
            problems.append(f"{check['null_fk_rows']} child row(s) with a null key")
        warn_only = []
        if check["compared_as_text"]:
            warn_only.append(f"types differ ({ctype} vs {ptype}); compared as text")
        if problems:
            fk["status"] = "fail" if fk["strength"] == "declared" else "warn"
        elif warn_only:
            fk["status"] = "warn"
        else:
            fk["status"] = "ok"
        fk["verdict"] = (
            "; ".join(problems + warn_only) or "every child key has a parent"
        )

    # PII: hashed in code? hashed in data?
    for table, info in model.items():
        if info.get("error"):
            continue
        for col in info["columns"]:
            col["hashed_by"] = hashed(table, col["name"])
            if col["pii_name"] or col["pii_policy"] or col["hashed_by"]:
                col["hex64"] = lake.hex64_rate(conn, table, col["name"])
            rows = info["rows"] or 0
            col["unique_in_data"] = bool(
                rows > 1 and col.get("nulls") == 0 and col.get("distinct") == rows
            )

    # ---------------------------------------------------------------- disagreements
    issues: list[dict[str, Any]] = []
    for entry in keys.values():
        if entry["status"] == "fail":
            claims = "; ".join(
                f"{e['kind']} at {e['loc']}"
                for e in entry["evidence"]
                if e["strength"] in ("asserted", "propagated")
            )
            issues.append(
                {
                    "severity": "fail",
                    "subject": entry["table"],
                    "topic": f"key ({', '.join(entry['columns'])})",
                    "code": f"expected unique + non-null: {claims}",
                    "data": entry["verdict"],
                }
            )
    for fk in fks.values():
        if fk["status"] in ("fail", "warn"):
            claims = "; ".join(f"{e['kind']} at {e['loc']}" for e in fk["evidence"])
            issues.append(
                {
                    "severity": fk["status"],
                    "subject": fk["child"],
                    "topic": f"{fk['child_col']} → {fk['parent']}.{fk['parent_col']}",
                    "code": claims[:400],
                    "data": fk["verdict"],
                }
            )
    for sur in surrogates:
        for dk in dedup_keys:
            if dk["table"] != sur["table"]:
                continue
            missing = [c for c in dk["columns"] if c not in sur["components"]]
            if not missing:
                continue
            check = keys.get((sur["table"], (sur["column"],)), {}).get("check") or {}
            issues.append(
                {
                    "severity": "fail" if check.get("duplicate_keys") else "warn",
                    "subject": sur["table"],
                    "topic": f"{sur['column']} vs dedup key",
                    "code": (
                        f"{sur['column']} = {sur['expr']} ({sur['loc']}) is built from {sur['components']}, but rows are deduplicated on "
                        f"{dk['columns']} ({dk['loc']}): {missing} is in the dedup key and not in {sur['column']}, so rows that differ only in "
                        f"{missing} survive dedup with the same {sur['column']}"
                    ),
                    "data": f"{check.get('duplicate_keys', '?')} repeated {sur['column']} value(s), {check.get('rows_in_duplicate_keys', '?')} rows",
                }
            )
    for table, info in model.items():
        for col in info["columns"]:
            for rule in col["not_null_code"]:
                if col.get("nulls"):
                    issues.append(
                        {
                            "severity": (
                                "fail" if rule["kind"].startswith("GE") else "warn"
                            ),
                            "subject": table,
                            "topic": f"{col['name']} not null",
                            "code": f"{rule['kind']} at {rule['loc']}",
                            "data": f"{col['nulls']} null value(s)",
                        }
                    )
            hx = col.get("hex64")
            if (
                info["layer"] != "bronze"
                and (col["pii_name"] or col["pii_policy"])
                and not col["hashed_by"]
                and hx
                and hx["rate"] is not None
                and hx["rate"] < 1
            ):
                policy = (
                    f"; blocked from queries by {col['pii_policy']}"
                    if col["pii_policy"]
                    else ""
                )
                issues.append(
                    {
                        "severity": "warn",
                        "subject": table,
                        "topic": f"{col['name']} is personal data",
                        "code": f"no hash step on any path into {table}.{col['name']}{policy}",
                        "data": f"{hx['non_null'] - hx['hex64']} of {hx['non_null']} values are not "
                        f"SHA-256 hex digests (raw values reach {info['layer']})",
                    }
                )
    order = {"fail": 0, "warn": 1}
    issues.sort(key=lambda i: (order.get(i["severity"], 2), i["subject"], i["topic"]))

    # ---------------------------------------------------------------- sources + summary
    code_files = {e["loc"].split(":")[0] for e in ge} | {
        d["loc"].split(":")[0] for d in docs
    }
    code_files |= {s["loc"].split(":")[0] for s in sqls} | {
        p["loc"].split(":")[0] for p in probes
    }
    code_files |= {c.split(":")[0] for c in consumers}
    sources.extend(sorted(code_files))
    sources.extend(semantic["sources"])
    sources.extend(policy_sources)
    sources.append(
        f"ast interpreter over {len(project.modules)} modules in {', '.join(code.CODE_DIRS)}/ ({len(project.sinks)} lake writes found)"
    )
    sources.append(
        f"sqlglot over string constants in {', '.join(code.SQL_SCAN_DIRS)}/ (JOIN … ON) and spark.sql(...) calls (key probes)"
    )

    for info in model.values():
        for col in info["columns"]:
            col["upstream"] = [e["i"] for e in col["upstream"]]
            col["downstream"] = [e["i"] for e in col["downstream"]]

    key_list = sorted(
        keys.values(), key=lambda k: (tables.index(k["table"]), k["columns"])
    )
    fk_list = sorted(
        fks.values(),
        key=lambda f: (tables.index(f["child"]), f["child_col"], f["parent"]),
    )
    asserted = [k for k in key_list if k["status"] in ("ok", "fail")]
    summary = {
        "tables": len(tables),
        "relationships": len(fk_list),
        "keys holding": f"{sum(k['status'] == 'ok' for k in asserted)}/{len(asserted)}",
        "code vs data issues": len(issues),
    }
    return {
        "summary": summary,
        "sources": list(dict.fromkeys(sources)),
        "tables": [model[t] for t in tables],
        "keys": key_list,
        "relationships": fk_list,
        "lineage": lineage,
        "issues": issues,
        "notes": notes,
        "probes": probes,
    }
