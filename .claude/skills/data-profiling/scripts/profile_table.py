"""Profile one table; print a markdown report and optionally write an HTML one.

Usage (from the repo root):
    python .claude/skills/data-profiling/scripts/profile_table.py \
        s3a://lakehouse/silver/orders_batch --key order_line_id \
        --html reports/data-profiling/silver_orders_batch.html

    python .claude/skills/data-profiling/scripts/profile_table.py \
        data/some_file.csv --format csv

Checks: row count and schema, nulls / blank strings / placeholder strings,
distinct counts, exact duplicate rows, duplicate keys (split into exact
copies vs conflicting rows that share a key), numeric ranges with negatives
and IQR outliers, date ranges, and email / IP format checks by column name.
Everything runs as Spark aggregations, so it stays light on RAM.

The HTML report is one self-contained file (inline CSS, no network access).
"""

from __future__ import annotations

import argparse
import html
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    DateType,
    NumericType,
    StringType,
    TimestampType,
)

EXACT_DISTINCT_LIMIT = 5_000_000
LOW_CARDINALITY = 20
LOW_CARDINALITY_RATIO = 0.1
NOT_PLAIN_TEXT_HINTS = ("hash", "sha", "md5", "token", "encrypted")
PLACEHOLDERS = ["n/a", "na", "null", "none", "nan", "unknown", "-", "?"]
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
IPV4_REGEX = r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$"
NEGATIVE_OK_HINTS = ("balance", "delta", "diff", "change", "profit")
# Columns where a value above zero is a finding, highlighted in the HTML report.
WARN_IF_POSITIVE = {"blank", "placeholder", "neg", "in future", "outliers"}


@dataclass
class Bar:
    """A cell drawn as a small bar in HTML (e.g. null share of a column)."""

    text: str
    fraction: float


@dataclass
class Section:
    title: str
    headers: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    # Plain notes. Prefix with two spaces to nest one level.
    notes: list[str] = field(default_factory=list)
    empty: str = ""  # shown when there is nothing to report


def find_repo_root() -> Path | None:
    for directory in (Path.cwd().resolve(), *Path.cwd().resolve().parents):
        if (directory / "config" / "spark_config.py").is_file():
            return directory
    return None


def get_spark(path: str, fmt: str) -> SparkSession:
    """Delta on MinIO needs the project's session (jars + s3a settings);
    plain local files use a bare local session."""
    if fmt == "delta" or path.startswith("s3a://"):
        root = find_repo_root()
        if root is None:
            sys.exit("Run from inside the repo: config/spark_config.py not found.")
        sys.path.insert(0, str(root))
        from config.spark_config import get_spark_session

        return get_spark_session("DataProfiling")
    return (
        SparkSession.builder.master("local[2]")
        .appName("DataProfiling")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def load(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    reader = spark.read
    if fmt == "csv":
        reader = reader.option("header", True).option("inferSchema", True)
    return reader.format(fmt).load(path)


def pct(part: int, whole: int) -> str:
    return f"{(100.0 * part / whole):.2f}%" if whole else "n/a"


def _num(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4g}" if isinstance(value, float) else str(value)


# --- checks: each returns a Section and appends findings to ``flags`` --------


def column_quality(
    df: DataFrame, total: int, flags: list[str]
) -> tuple[Section, dict[str, int]]:
    # Exact distinct counts up to a size limit; approximate beyond it (an
    # approximate count next to an exact duplicate check is confusing).
    exact = total <= EXACT_DISTINCT_LIMIT
    aggs: list[Any] = []
    for fld in df.schema.fields:
        col = f.col(f"`{fld.name}`")
        aggs.append(f.sum(col.isNull().cast("int")).alias(f"{fld.name}__nulls"))
        distinct_fn: Any = f.countDistinct if exact else f.approx_count_distinct
        aggs.append(distinct_fn(col).alias(f"{fld.name}__distinct"))
        if isinstance(fld.dataType, StringType):
            cleaned = f.lower(f.trim(col))
            aggs.append(f.sum((cleaned == "").cast("int")).alias(f"{fld.name}__blank"))
            aggs.append(
                f.sum(cleaned.isin(PLACEHOLDERS).cast("int")).alias(
                    f"{fld.name}__placeholder"
                )
            )
    row = df.agg(*aggs).first()
    assert row is not None

    out: list[list[Any]] = []
    distincts: dict[str, int] = {}
    for fld in df.schema.fields:
        name = fld.name
        nulls = row[f"{name}__nulls"] or 0
        blank = row[f"{name}__blank"] if f"{name}__blank" in row else 0
        placeholder = (
            row[f"{name}__placeholder"] if f"{name}__placeholder" in row else 0
        )
        distinct = row[f"{name}__distinct"]
        distincts[name] = distinct
        if nulls:
            flags.append(f"`{name}` has {nulls} nulls ({pct(nulls, total)})")
        if blank:
            flags.append(f"`{name}` has {blank} blank strings")
        if placeholder:
            flags.append(
                f"`{name}` has {placeholder} placeholder strings (N/A, null...)"
            )
        out.append(
            [
                name,
                fld.dataType.simpleString(),
                Bar(f"{nulls} ({pct(nulls, total)})", nulls / total),
                blank or 0,
                placeholder or 0,
                distinct,
            ]
        )
    label = "distinct" if exact else "~distinct"
    headers = ["column", "type", "nulls", "blank", "placeholder", label]
    return Section("Column quality", headers, out), distincts


def duplicates(df: DataFrame, keys: list[str], total: int, flags: list[str]) -> Section:
    notes: list[str] = []
    distinct_rows = df.dropDuplicates().count()
    exact = total - distinct_rows
    notes.append(f"Exact duplicate rows (every column equal): **{exact}**")
    if exact:
        flags.append(f"{exact} fully duplicated rows")

    if not keys:
        notes.append("No `--key` given, so key uniqueness was not checked.")
        return Section("Duplicates", notes=notes)

    dup_groups = df.groupBy(*keys).count().filter(f.col("count") > 1)
    n_groups = dup_groups.count()
    extra_rows = 0
    if n_groups:
        surplus = dup_groups.agg(f.sum(f.col("count") - 1)).first()
        extra_rows = (surplus[0] or 0) if surplus is not None else 0
    notes.append(
        f"Key `{', '.join(keys)}`: **{n_groups}** keys repeat, {extra_rows} surplus rows"
    )
    if n_groups:
        others = [c for c in df.columns if c not in keys]
        variants = (
            df.groupBy(*keys)
            .agg(
                f.count(f.lit(1)).alias("n"),
                f.countDistinct(f.struct(*others)).alias("variants"),
            )
            .filter(f.col("n") > 1)
        )
        conflicting = variants.filter(f.col("variants") > 1).count()
        identical = n_groups - conflicting
        notes.append(f"  {identical} are exact copies (safe to drop)")
        notes.append(
            f"  {conflicting} are CONFLICTING (same key, different values: "
            "the key is not a real key, or the rows are different events)"
        )
        flags.append(f"key {keys} repeats {n_groups} times ({conflicting} conflicting)")
        examples = [r[0] for r in dup_groups.select(keys[0]).limit(3).collect()]
        notes.append(f"  first repeated `{keys[0]}` values: {examples}")
    return Section("Duplicates", notes=notes)


def numeric_profile(
    df: DataFrame, total: int, distincts: dict[str, int], flags: list[str]
) -> Section:
    title = "Numeric ranges and outliers"
    numeric = [
        fld.name for fld in df.schema.fields if isinstance(fld.dataType, NumericType)
    ]
    if not numeric:
        return Section(title, empty="No numeric columns.")
    aggs: list[Any] = []
    for name in numeric:
        col = f.col(f"`{name}`")
        aggs += [
            f.min(col).alias(f"{name}__min"),
            f.max(col).alias(f"{name}__max"),
            f.avg(col).alias(f"{name}__mean"),
            f.stddev(col).alias(f"{name}__std"),
            f.sum((col < 0).cast("int")).alias(f"{name}__neg"),
            f.sum((col == 0).cast("int")).alias(f"{name}__zero"),
        ]
    row = df.agg(*aggs).first()
    assert row is not None

    rows: list[list[Any]] = []
    for name in numeric:
        q1, med, q3 = df.approxQuantile(name, [0.25, 0.5, 0.75], 0.01) or (None,) * 3
        outliers: int | str = "n/a"
        distinct = distincts.get(name, 0)
        if distinct <= LOW_CARDINALITY and distinct <= total * LOW_CARDINALITY_RATIO:
            # A handful of distinct values repeated over many rows (a price
            # list, a rating) is a category: values far from the middle are
            # legitimate, not outliers. The ratio keeps tiny tables checked.
            outliers = "n/a (few distinct values)"
        elif q1 is not None and q3 is not None:
            iqr = q3 - q1
            lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            col = f.col(f"`{name}`")
            outliers = df.filter((col < lo) | (col > hi)).count()
            if outliers:
                flags.append(
                    f"`{name}` has {outliers} IQR outliers (outside [{lo:.2f}, {hi:.2f}]): "
                    "judge whether they are errors or real extremes"
                )
        neg = row[f"{name}__neg"] or 0
        if neg and not any(h in name.lower() for h in NEGATIVE_OK_HINTS):
            flags.append(f"`{name}` has {neg} negative values")
        rows.append(
            [
                name,
                _num(row[f"{name}__min"]),
                _num(q1),
                _num(med),
                _num(q3),
                _num(row[f"{name}__max"]),
                _num(row[f"{name}__mean"]),
                _num(row[f"{name}__std"]),
                neg,
                row[f"{name}__zero"] or 0,
                outliers,
            ]
        )
    headers = [
        "column",
        "min",
        "q1",
        "median",
        "q3",
        "max",
        "mean",
        "std",
        "neg",
        "zero",
        "outliers",
    ]
    return Section(title, headers, rows)


def date_profile(df: DataFrame, flags: list[str]) -> Section:
    dates = [
        fld.name
        for fld in df.schema.fields
        if isinstance(fld.dataType, (DateType, TimestampType))
    ]
    if not dates:
        return Section("Dates", empty="No date/timestamp columns.")
    rows: list[list[Any]] = []
    for name in dates:
        col = f.col(f"`{name}`")
        res = df.agg(
            f.min(col),
            f.max(col),
            f.sum((col > f.current_timestamp()).cast("int")),
        ).first()
        assert res is not None
        future = res[2] or 0
        if future:
            flags.append(f"`{name}` has {future} dates in the future")
        rows.append([name, res[0], res[1], future])
    return Section("Dates", ["column", "min", "max", "in future"], rows)


def format_checks(df: DataFrame, flags: list[str]) -> Section:
    notes: list[str] = []
    for fld in df.schema.fields:
        if not isinstance(fld.dataType, StringType):
            continue
        name = fld.name.lower()
        # A hashed / tokenised column is not plain text, so a format check on it
        # would flag every row (e.g. email_hash is not an email address).
        if any(hint in name for hint in NOT_PLAIN_TEXT_HINTS):
            continue
        col = f.col(f"`{fld.name}`")
        regex = None
        if "email" in name:
            regex = EMAIL_REGEX
        elif name in ("ip", "ip_address") or name.endswith("_ip"):
            regex = IPV4_REGEX
        if regex is None:
            continue
        bad = df.filter(col.isNotNull() & ~col.rlike(regex)).count()
        notes.append(f"`{fld.name}`: {bad} values fail the expected format")
        if bad:
            flags.append(f"`{fld.name}` has {bad} malformed values")
    return Section(
        "Format checks", notes=notes, empty="No email / IP columns detected."
    )


# --- renderers --------------------------------------------------------------


def _cell_text(value: Any) -> str:
    return value.text if isinstance(value, Bar) else str(value)


def to_markdown(
    path: str, total: int, n_cols: int, sections: list[Section], flags: list[str]
) -> str:
    out = [f"# Profile: {path}\n", f"Rows: **{total}** | Columns: **{n_cols}**\n"]
    for sec in sections:
        out.append(f"## {sec.title}")
        if sec.headers:
            out.append("| " + " | ".join(sec.headers) + " |")
            out.append("|" + "|".join("---" for _ in sec.headers) + "|")
            for row in sec.rows:
                out.append("| " + " | ".join(_cell_text(c) for c in row) + " |")
        for note in sec.notes:
            nested = note.startswith("  ")
            out.append(("  - " if nested else "- ") + note.strip())
        if not sec.headers and not sec.notes and sec.empty:
            out.append(f"_{sec.empty}_")
        out.append("")
    out.append("## Flags")
    out.extend(f"- {x}" for x in flags) if flags else out.append("- None found.")
    return "\n".join(out)


def _inline(text: str) -> str:
    """Escape, then turn `code` and **bold** into tags (escape comes first so
    table contents can never inject markup)."""
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
    return re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)


def _is_number(text: str) -> bool:
    try:
        float(text.replace(",", "").rstrip("%"))
    except ValueError:
        return False
    return True


def _html_cell(header: str, value: Any) -> str:
    if isinstance(value, Bar):
        width = max(0.0, min(1.0, value.fraction)) * 100
        warn = " warn" if value.fraction > 0 else ""
        return (
            f'<td><span class="bar"><i class="{warn.strip()}" '
            f'style="width:{width:.1f}%"></i></span> {html.escape(value.text)}</td>'
        )
    text = str(value)
    classes = []
    if _is_number(text):
        classes.append("num")
    if header in WARN_IF_POSITIVE and _is_number(text) and float(text) > 0:
        classes.append("warn")
    cls = f' class="{" ".join(classes)}"' if classes else ""
    return f"<td{cls}>{html.escape(text)}</td>"


def _html_section(sec: Section) -> str:
    parts = [f"<section><h2>{html.escape(sec.title)}</h2>"]
    if sec.headers:
        head = "".join(f"<th>{html.escape(h)}</th>" for h in sec.headers)
        body = "".join(
            "<tr>"
            + "".join(_html_cell(h, c) for h, c in zip(sec.headers, row, strict=True))
            + "</tr>"
            for row in sec.rows
        )
        parts.append(
            f'<div class="wrap"><table><thead><tr>{head}</tr></thead>'
            f"<tbody>{body}</tbody></table></div>"
        )
    if sec.notes:
        items = "".join(
            f'<li class="{"nested" if n.startswith("  ") else ""}">{_inline(n.strip())}</li>'
            for n in sec.notes
        )
        parts.append(f"<ul>{items}</ul>")
    if not sec.headers and not sec.notes and sec.empty:
        parts.append(f'<p class="muted">{html.escape(sec.empty)}</p>')
    parts.append("</section>")
    return "".join(parts)


HTML_CSS = """
:root{--bg:#f6f7f9;--surface:#fff;--text:#1a1d23;--muted:#667085;--border:#e1e4ea;
--accent:#2f5bea;--ok:#14693a;--ok-bg:#e7f6ec;--warn:#9a5b00;--warn-bg:#fff3dc;--code:#f0f2f6}
@media (prefers-color-scheme:dark){:root{--bg:#12141a;--surface:#1b1e26;--text:#e7e9ee;
--muted:#98a0b3;--border:#2d313c;--accent:#6b8cff;--ok:#7fd8a1;--ok-bg:#12301f;
--warn:#ffc46b;--warn-bg:#3a2c10;--code:#232733}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 48px}
h1{font-size:20px;margin:0 0 4px;word-break:break-all}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin:0 0 10px}
.sub{color:var(--muted);font-size:13px;margin:0 0 18px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}
.card,section{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px}
section{margin-top:16px}
.card b{display:block;font-size:24px;line-height:1.2}
.card span{color:var(--muted);font-size:13px}
.card.ok b{color:var(--ok)}.card.warn b{color:var(--warn)}
.verdict{padding:10px 14px;border-radius:8px;font-weight:600;margin-bottom:10px}
.verdict.ok{background:var(--ok-bg);color:var(--ok)}
.verdict.warn{background:var(--warn-bg);color:var(--warn)}
.wrap{overflow-x:auto;border:1px solid var(--border);border-radius:8px}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{padding:7px 12px;text-align:left;border-bottom:1px solid var(--border);white-space:nowrap}
th{background:var(--bg);color:var(--muted);font-weight:600}
tr:last-child td{border-bottom:0}
td.num{text-align:right;font-variant-numeric:tabular-nums}
td.warn{background:var(--warn-bg);color:var(--warn);font-weight:600}
.bar{display:inline-block;width:60px;height:8px;background:var(--border);border-radius:4px;
vertical-align:middle;overflow:hidden;margin-right:6px}
.bar i{display:block;height:100%;background:var(--ok)}.bar i.warn{background:var(--warn)}
ul{margin:8px 0 0;padding-left:20px}li.nested{margin-left:18px;color:var(--muted)}
code{background:var(--code);border-radius:4px;padding:1px 5px;
font:13px ui-monospace,Consolas,monospace}
.muted{color:var(--muted)}
"""


def to_html(
    path: str, total: int, n_cols: int, sections: list[Section], flags: list[str]
) -> str:
    verdict_cls = "warn" if flags else "ok"
    verdict = (
        f"{len(flags)} finding{'s' if len(flags) != 1 else ''} to review"
        if flags
        else "No findings: every check passed"
    )
    flag_items = "".join(f"<li>{_inline(x)}</li>" for x in flags)
    flags_html = (
        f'<section><h2>Findings</h2><div class="verdict {verdict_cls}">'
        f"{html.escape(verdict)}</div>"
        + (f"<ul>{flag_items}</ul>" if flags else "")
        + "</section>"
    )
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>Data profile</title><style>{HTML_CSS}</style></head><body><main>"
        f"<h1>Data profile: {html.escape(path)}</h1>"
        f'<p class="sub">Generated {stamp} by the data-profiling skill</p>'
        '<div class="cards">'
        f'<div class="card"><b>{total:,}</b><span>rows</span></div>'
        f'<div class="card"><b>{n_cols}</b><span>columns</span></div>'
        f'<div class="card {verdict_cls}"><b>{len(flags)}</b><span>findings</span></div>'
        "</div>"
        + flags_html
        + "".join(_html_section(s) for s in sections)
        + "</main></body></html>"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("path", help="s3a:// Delta path, or a local file/folder")
    parser.add_argument(
        "--format", default="delta", choices=["delta", "parquet", "csv", "json"]
    )
    parser.add_argument("--key", default="", help="comma-separated key column(s)")
    parser.add_argument("--html", default="", help="also write an HTML report here")
    args = parser.parse_args()

    keys = [k.strip() for k in args.key.split(",") if k.strip()]
    spark = get_spark(args.path, args.format)
    spark.sparkContext.setLogLevel("ERROR")
    df = load(spark, args.path, args.format)
    total = df.count()
    if total == 0:
        print(f"# Profile: {args.path}\n\nThe table is empty.")
        return

    flags: list[str] = []
    quality, distincts = column_quality(df, total, flags)
    sections = [
        quality,
        duplicates(df, keys, total, flags),
        numeric_profile(df, total, distincts, flags),
        date_profile(df, flags),
        format_checks(df, flags),
    ]
    print(to_markdown(args.path, total, len(df.columns), sections, flags))

    if args.html:
        target = Path(args.html)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            to_html(args.path, total, len(df.columns), sections, flags),
            encoding="utf-8",
        )
        print(f"\nHTML report written to {target.resolve()}")
    spark.stop()


if __name__ == "__main__":
    main()
