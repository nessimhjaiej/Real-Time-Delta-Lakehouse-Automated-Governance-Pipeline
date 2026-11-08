---
name: de-toolkit
description: Build, refresh, audit and view the data engineering toolkit website (architecture, data lineage, call graph, data model, data quality, data catalog) generated from this repo and its running stack. Use when asked to generate or update the toolkit/site, to show lineage, the call graph, the data model, quality gate results or the catalog, to check a toolkit page against the repo, or when a pipeline/schema/DAG change means the pages are stale.
---

# DE toolkit (generated site)

A static site in `reports/toolkit/` (gitignored). Every fact on it is **derived** from
the code (parsed with `ast`/YAML), the resolved compose config, the Delta tables on
MinIO or live command output. Nothing is typed in by hand; what cannot be derived is
shown as "not derivable" / "No description in the repo". Keep it that way.

## Layout

- `toolkit/build.py` - the builder. `toolkit/common.py` - the page contract (read its
  docstring), HTML helpers, DuckDB-over-MinIO lake connection, PII masking.
- `toolkit/assets/` - `style.css`, `site.js` (theme, `?focus=`), `graph.js` (shared
  Cytoscape graph: click a node = full upstream (orange) / downstream (green) chain).
- `toolkit/pages/<page>.py` + private `_<page>_*.py` helpers. Each page defines
  `SLUG, TITLE, NAV_LABEL, BLURB, LIVE, collect(), render(data)`; `collect()` returns
  `summary` and `sources`, `render()` is pure.

| Page | Slug | LIVE | Derived from |
|---|---|---|---|
| Architecture | `architecture` | yes (docker state) | `docker compose config`, Dockerfiles, `dags/`, env/URL literals in code |
| Lineage | `lineage` | yes | ast read/write extraction, DAG Datasets, MinIO table list, Delta log |
| Call graph | `call-graph` | no | ast over `src/ dags/ config/ tests/` + root scripts |
| Data model | `data-model` | yes | Delta schemas, GE/dedupe/join code, verified with DuckDB (PK/FK/orphans) |
| Data quality | `quality` | yes (**runs Spark**) | the repo's own GE suites run against Silver |
| Data catalog | `catalog` | yes | Delta schemas + stats via DuckDB; descriptions only from semantic YAML, docstrings, policy docs, README |

Cross-page links use canonical ids: tables `<layer>.<table>` (`silver.orders_stream`),
`kafka.<topic>`, `service.<name>`, callables `<dotted.module>.<qualname>`, via
`?focus=<id>`.

## Build

```
docker compose up -d minio                       # live pages need the lake
python -m toolkit.build                          # reuses live snapshots, re-parses the repo
python -m toolkit.build --refresh                # re-read MinIO/docker/Spark (quality: ~5 min)
python -m toolkit.build --only lineage catalog   # selected pages
```

Output: `reports/toolkit/index.html`. Live pages cache to `_snapshots/<slug>.json`; if a
source is down the last snapshot is used and the footer says STALE. Only the quality
page starts Spark - do not refresh it casually (RAM); build with `--only` and skip it.
Graphs load Cytoscape from a CDN (needs internet); every graph has an HTML table
fallback below it.

When to refresh: after changing pipelines, schemas, the DAG, compose, policies or the
semantic layer; code-only pages (call-graph) re-parse on every build.

## Check a page against the repo (do this before trusting a page)

Profile -> judge -> fix, same spirit as `data-profiling`:

1. Open `reports/toolkit/_snapshots/<slug>.json`.
2. Verify it **independently of the extractor**: read the source files or query the
   data yourself. Every node exists, every edge is real (cite file:line), nothing that
   exists is missing (grep for entry points, other read/write paths, other tables),
   counts are right.
3. Fix the **extractor**, never the output; rebuild; re-check.
4. Suspicious numbers deserve a look first (e.g. `unresolved calls: 0` on 318
   functions; a container count taken from a stale snapshot).

Known unchecked as of the first build: all six pages were generated but not yet audited
this way.

## View it

- Open `reports/toolkit/index.html`.
- Headless screenshots (`chrome --screenshot`) go blank on `?focus=` deep links
  because the page scrolls; that is a capture artifact, not a bug. To check a page for
  real use `scripts/probe_page.mjs` (Chrome DevTools Protocol, needs Node 22+ and
  Chrome): `node .claude/skills/de-toolkit/scripts/probe_page.mjs "<file:// url>?focus=<id>" out.png`
  prints scroll position, the selected node and its upstream/downstream counts, any JS
  exceptions, and writes a real screenshot.

## Quality bar for changes

- `black`, `ruff` pass on `toolkit/`; `mypy toolkit` currently reports 60 errors in 15
  files (annotations only) - `toolkit/` is not in CI yet (CI checks `config/ src/
  tests/`), fix before adding it.
- Escape every value (`esc`, `table_html`); never show raw PII (`mask_if_pii`); never
  print `.env` values (env var **names** only).
- Shared files (`common.py`, `build.py`, `assets/`) are changed deliberately by hand;
  page modules must not edit each other.
- Don't start extra docker services for a page; MinIO is enough.
