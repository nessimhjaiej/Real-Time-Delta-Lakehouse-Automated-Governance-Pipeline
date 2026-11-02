---
name: data-profiling
description: Profile a table or dataset before trusting it or building on it - row counts, schema and types, missing data, duplicates and key uniqueness, outliers, impossible values, date ranges and format problems. Use when exploring a new Bronze/Silver/Gold table, when asked to check data quality or "how's the data", before writing cleaning logic or Great Expectations rules, or when a number looks wrong.
---

# Data profiling

Goal: say what is true about the data *before* anyone fixes it, then recommend
which layer should fix each problem. Profile first, judge second, fix last.

## 1. Run the profiler

The script does the repeatable checks as Spark aggregations (light on RAM):

```
python .claude/skills/data-profiling/scripts/profile_table.py <path> [--key col1,col2] [--format delta|parquet|csv|json]
```

- `--html reports/data-profiling/<layer>_<table>.html` also writes a
  self-contained HTML report (summary cards, findings, per-section tables with
  null bars and highlighted problem cells; light/dark, no network). The
  markdown report is still printed. Offer the HTML when the user wants
  something to open, share or keep; `reports/` is gitignored.
- Delta on MinIO: `s3a://lakehouse/<layer>/<table>` (needs MinIO up: `docker compose up -d minio`).
- Local files: pass the file path with `--format csv` (or parquet / json).
- Always pass `--key` with the column(s) that *should* be unique; without it
  key uniqueness is skipped.
- It prints a markdown report ending in a `Flags` list. Spark startup takes a
  minute on the first run; do not re-run it just to re-read the output.

Tables in this repo: `bronze/{orders_batch,orders_stream,products_catalog,customers}`,
`silver/{orders_batch,orders_stream,products_catalog,customers}`,
`gold/{fact_orders,dim_customers,dim_products}`.

## 2. What to check, and how to judge it

| Area | Question | Judge it by |
|---|---|---|
| Shape | Row count plausible? Column names and types as documented? | Compare to the layer above; counts should only shrink for a reason you can name |
| Missing | Nulls, blanks, `N/A`/`null`/`none` placeholder strings | Share of rows, and *which* columns - a null in a key is critical, in an optional field it is not |
| Duplicates | Exact duplicate rows vs repeated keys | Split them: **exact copies** are safe to drop; **conflicting** rows share a key but differ in values, so the key is wrong or they are different events. Never dedupe the second kind blindly |
| Outliers | Values outside the IQR fences (1.5x) | An outlier is a question, not an error. Check if it is plausible (a 9000 price in a catalog of 10s) before calling it bad |
| Impossible values | Negative quantity or price, dates in the future, zero where zero makes no sense | Domain rules, not statistics |
| Formats | Email, IPv4, id patterns, casing, whitespace | Regex per column; count failures and show examples |
| Types | Numbers or dates stored as strings | Whether casting loses data |
| Keys / joins | Does the fact table's foreign key exist in the dimension? | Left anti-join count; orphans silently drop rows or produce NULL dimensions |

The script covers everything except foreign-key orphans and domain rules; do
those by hand with a short Spark SQL query when the table has a relationship.

What the script deliberately does *not* flag, so you do not over-report:

- **Outliers on few-valued columns.** A column with 20 or fewer distinct values
  spread over many rows (a price list, a 1-5 quantity) gets no IQR check: its
  "extreme" values are real categories. In `silver/orders_stream`, `unit_price`
  has 5 distinct values and 803 rows beyond the IQR fence - all legitimate.
- **Format checks on hashed columns.** Names containing `hash`, `sha`, `md5`,
  `token` or `encrypted` are skipped; `email_hash` is not an email address.
- **Derived columns still get IQR flags** (e.g. `total_amount` = quantity x
  price). Expect noise there and judge it against the source columns.

Distinct counts are exact up to 5M rows and approximate beyond that (the
header says `~distinct` when approximate).

## 3. Read the results

- Report numbers, not adjectives: "33 of 14,000 rows (0.24%)", not "a few".
- For any repeated key, look at 2-3 actual rows before explaining it. The
  script reports exact-vs-conflicting counts; the *reason* (two customers on the
  same line id, a replayed message, a join fan-out) only comes from the rows.
- Separate **what the data shows** from **what you assume**. Say which is which.
- A clean profile on a sample proves little; say how many rows were examined.

## 4. Recommend the fix and where it belongs

Match the medallion layers and do not fix in the wrong place:

- **Bronze** - keep raw. Only add metadata. Report problems, do not repair.
- **Silver** - cleaning: dedupe, casting, trimming, hashing PII, renames,
  derived columns. Most findings belong here.
- **Gold** - business rules and joins. Flag orphan keys and metric definitions.
- **Quality gate (Great Expectations, `src/pipelines/quality.py`)** - turn each
  confirmed finding into a rule so it cannot regress: not-null keys,
  uniqueness, value ranges, regex formats, row-count sanity.

Every finding should end as: *problem -> evidence -> layer that should fix it
-> proposed rule*.

## 5. Output format

```
## Profile: <table> (<rows> rows)
### Verdict   (one line: safe to use / use with fixes / do not use)
### Findings  (most severe first: problem, count and %, example, likely cause)
### Fix by layer   (Bronze / Silver / Gold / Quality gate)
### Not checked   (what this profile did not cover)
```

## Lessons from this repo's earlier EDA (`src/notebooks/0*_explore_*.py`)

Check these first on the matching tables:

- `orders_batch`: `invoice_id` repeats in Bronze; one order has several lines,
  so `invoice_id` alone is not a key. Use `order_line_id` (invoice + line).
- `orders_stream`: `order_line_id` repeated across *different customers* -
  conflicting duplicates, not copies. The dedupe key must include
  `customer_id` (or the key must). The current Silver table profiles clean
  (0 repeated keys in 4,000 rows), so a regression here means the fix broke.
- `products_catalog`: key is `id`; has been unique.
- `customers`: check email contains `@` and a dot, IP matches IPv4, sign-up
  dates inside a sane range, and that `_source_system` has one expected value.
- Types arrive as strings in Bronze; Silver casts them.
- PII columns (`email`, `ip_address`, `customer_id`) are hashed in Silver; never
  print raw PII values in a report, show counts and masked examples.

## Guardrails

- Read-only: never write to, overwrite or delete a table while profiling.
- Do not print real customer values; summarize them.
- Keep Spark light: one session, aggregations over `.collect()` of rows, no
  full-table pulls to pandas.
