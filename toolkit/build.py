"""Build the data engineering toolkit site.

    python -m toolkit.build                      # all pages -> reports/toolkit/
    python -m toolkit.build --refresh            # re-read live sources (MinIO, Spark, docker)
    python -m toolkit.build --only lineage catalog
    python -m toolkit.build --out some/dir

Static pages (LIVE = False: parsed from the repo) are collected on every build.
Live pages reuse their last snapshot (``<out>/_snapshots/<slug>.json``) unless
``--refresh`` is passed or no snapshot exists yet; if a live source is down the
last snapshot is used and the page says so.
"""

from __future__ import annotations

import argparse
import importlib
import json
import shutil
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

from toolkit.common import PAGE_SLUGS, REPO_ROOT, Raw, esc, repo_revision

ASSETS_DIR = Path(__file__).parent / "assets"
DEFAULT_OUT = REPO_ROOT / "reports" / "toolkit"

CDN_SCRIPTS = (
    "https://cdn.jsdelivr.net/npm/cytoscape@3.30.2/dist/cytoscape.min.js",
    "https://cdn.jsdelivr.net/npm/dagre@0.8.5/dist/dagre.min.js",
    "https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.js",
)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _nav(active: str, built: dict[str, str]) -> str:
    items = ['<a href="index.html"' + (' class="active"' if active == "index" else "")]
    items[0] += ">Overview</a>"
    for slug, label in built.items():
        cls = ' class="active"' if slug == active else ""
        items.append(f'<a href="{esc(slug)}.html"{cls}>{esc(label)}</a>')
    return "".join(items)


def page_shell(
    *, slug: str, title: str, body: str, nav: dict[str, str], footer: str
) -> str:
    scripts = "".join(f'<script src="{src}"></script>' for src in CDN_SCRIPTS)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)} · DE Toolkit</title>
<link rel="stylesheet" href="assets/style.css">
{scripts}
<script src="assets/graph.js" defer></script>
<script src="assets/site.js" defer></script>
</head>
<body data-page="{esc(slug)}">
<header class="topbar">
  <a class="brand" href="index.html">realtime-governance-engine <span>DE toolkit</span></a>
  <nav>{_nav(slug, nav)}</nav>
  <button type="button" class="theme-toggle" data-role="theme" aria-label="Toggle theme">◐</button>
</header>
<main>
<h1>{esc(title)}</h1>
{body}
</main>
<footer>{footer}</footer>
</body>
</html>
"""


def _load_module(name: str) -> ModuleType | None:
    try:
        return importlib.import_module(f"toolkit.pages.{name}")
    except ModuleNotFoundError as error:
        if error.name == f"toolkit.pages.{name}":
            return None
        raise


def _collect(
    module: ModuleType, snapshot: Path, refresh: bool
) -> tuple[dict[str, Any], str]:
    """Return (data, provenance note)."""
    live = bool(getattr(module, "LIVE", False))
    if live and not refresh and snapshot.exists():
        data = json.loads(snapshot.read_text(encoding="utf-8"))
        return data, f"live snapshot from {data.get('_collected_at', '?')}"
    try:
        data = module.collect()
    except Exception as error:
        if snapshot.exists():
            data = json.loads(snapshot.read_text(encoding="utf-8"))
            data["_stale_reason"] = f"{type(error).__name__}: {error}"
            return data, (
                f"STALE: live source failed ({type(error).__name__}); showing snapshot "
                f"from {data.get('_collected_at', '?')}"
            )
        raise
    data["_collected_at"] = _now()
    data["_revision"] = repo_revision()
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")
    kind = "live" if live else "parsed from the repo"
    return data, f"{kind}, collected {data['_collected_at']}"


def _index_body(cards: list[dict[str, Any]]) -> str:
    out = [
        '<p class="lead">Six views of this lakehouse, each generated from the code '
        "and the running stack rather than drawn by hand. Rebuild with "
        "<code>python -m toolkit.build --refresh</code>.</p>",
        '<div class="cards">',
    ]
    for card in cards:
        stats = "".join(
            f"<li><strong>{esc(v)}</strong> {esc(k)}</li>"
            for k, v in card["summary"].items()
        )
        status = (
            f'<p class="badge badge-fail">{esc(card["error"])}</p>'
            if card.get("error")
            else f'<p class="muted small">{esc(card["provenance"])}</p>'
        )
        out.append(
            f'<a class="card" href="{esc(card["slug"])}.html">'
            f'<h2>{esc(card["title"])}</h2><p>{esc(card["blurb"])}</p>'
            f'<ul class="mini-stats">{stats}</ul>{status}</a>'
        )
    out.append("</div>")
    return "".join(out)


def build(out: Path, refresh: bool, only: list[str] | None) -> int:
    out.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ASSETS_DIR, out / "assets", dirs_exist_ok=True)
    revision = repo_revision()

    modules: dict[str, ModuleType] = {}
    for name in PAGE_SLUGS:
        module = _load_module(name)
        if module is not None:
            modules[name] = module
    nav = {m.SLUG: m.NAV_LABEL for m in modules.values()}

    failures = 0
    cards: list[dict[str, Any]] = []
    for name, module in modules.items():
        slug = module.SLUG
        snapshot = out / "_snapshots" / f"{slug}.json"
        card: dict[str, Any] = {
            "slug": slug,
            "title": module.TITLE,
            "blurb": module.BLURB,
            "summary": {},
        }
        selected = only is None or slug in only or name in only
        try:
            if selected:
                data, provenance = _collect(module, snapshot, refresh)
                body = module.render(data)
                footer = Raw(
                    f"Generated from commit <code>{esc(data.get('_revision', revision))}</code>"
                    f" · {esc(provenance)}"
                )
                (out / f"{slug}.html").write_text(
                    page_shell(
                        slug=slug,
                        title=module.TITLE,
                        body=body,
                        nav=nav,
                        footer=footer,
                    ),
                    encoding="utf-8",
                )
                print(f"  built {slug}.html  ({provenance})")
            elif snapshot.exists():
                data = json.loads(snapshot.read_text(encoding="utf-8"))
                provenance = f"collected {data.get('_collected_at', '?')}"
            else:
                data, provenance = {}, "not built yet"
            card["summary"] = data.get("summary", {})
            card["provenance"] = provenance
        except Exception as error:
            failures += 1
            card["error"] = f"build failed: {type(error).__name__}: {error}"
            print(f"  FAILED {slug}: {error}", file=sys.stderr)
            traceback.print_exc()
        cards.append(card)

    (out / "index.html").write_text(
        page_shell(
            slug="index",
            title="Data engineering toolkit",
            body=_index_body(cards),
            nav=nav,
            footer=Raw(
                f"Generated from commit <code>{esc(revision)}</code> · {_now()}"
            ),
        ),
        encoding="utf-8",
    )
    print(f"Site written to {out}  (open {out / 'index.html'})")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--refresh", action="store_true", help="re-read live sources")
    parser.add_argument("--only", nargs="+", help="page slugs or module names")
    args = parser.parse_args(argv)
    return build(args.out.resolve(), args.refresh, args.only)


if __name__ == "__main__":
    raise SystemExit(main())
