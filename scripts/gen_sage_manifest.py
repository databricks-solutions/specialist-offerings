#!/usr/bin/env python3
"""Generate the Sage managed-source manifest (sage/assets.yaml).

Each SSA offering is registered in Sage as its OWN asset: one offering folder =
one manifest entry. We walk `<category>/<slug>/catalog-listing.yml`, read the
listing's structured metadata, and fan out to one asset per offering. The
listing is the source of truth for name, description, tags, quality, and owner
-- there is nothing to hand-maintain in this manifest.

Why one folder per offering (workaround): Sage's GitHub App crawler, given a
path, still indexes the whole *surrounding folder* rather than a single file.
The repo already stores one offering per folder, so each asset's `resource_url`
points at the offering folder and the folder-crawl is effectively single-asset.
When Sage ships single-file GitHub indexing, only `resource_url` changes
(folder -> file); the `external_ref`s stay stable, so there is no
delete/recreate churn.

Why `docs` (not `codebase`): this repo is PUBLIC, so an unauthenticated crawler
can reach it via the GitHub integration (verified: AI Assist scraped a folder
tree URL at ~85% confidence). fde-specs uses `codebase` only because it is
private and raw-github URLs 404 there. If Sage later exposes a dedicated
`offering` asset_type, change ASSET_TYPE in one place. (Open: confirm with
Pavlo -- AI Assist auto-classified these as "Reference Architecture".)

external_ref = ssa-offering-<folder-slug>, which is deliberately the SAME string
an SA types as the ASQ hashtag (`#ssa-offering #<slug>`). That shared identifier
is what lets a human, Isaac, and Sage all point at the same offering.

Dependency-free by design (no PyYAML): a catalog-listing.yml is a full YAML file
(it carries a multi-line `demo_description: |` block scalar and a `links:` map),
but every field this manifest needs is a TOP-LEVEL scalar. So we parse only
top-level `key: value` pairs and deterministically skip block scalars and nested
maps. This keeps the generator runnable on a stock `python3` with no install,
mirroring fde-specs' zero-dependency gate. (If listings ever need full YAML
semantics here, swap read_listing for `yaml.safe_load` and add PyYAML to CI.)

Usage:  python3 scripts/gen_sage_manifest.py [--check]
  (no args)  write sage/assets.yaml
  --check    exit non-zero if sage/assets.yaml is out of date (for CI)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "sage" / "assets.yaml"

# Offerings live at <category>/<slug>/catalog-listing.yml. This glob is two
# levels deep, so archived offerings (z-archive/<category>/<slug>/...) and the
# templates/ sample are naturally excluded; EXCLUDE_TOP is a belt-and-braces
# guard in case a non-offering dir ever grows a matching path.
LISTING_GLOB = "*/*/catalog-listing.yml"
LISTING_FILENAME = "catalog-listing.yml"
EXCLUDE_TOP = {"z-archive", "templates", "sage", ".git", ".github", "images"}

# Sage domain the source is bound to. Must match the slug in the UI.
DOMAIN = "ssa-offerings"

# Canonical public repo the GitHub App crawls.
REPO_URL = "https://github.com/databricks-solutions/specialist-offerings"
DEFAULT_BRANCH = "main"

# Public repo -> docs (see module docstring). One place to change if Sage adds
# a dedicated offering type.
ASSET_TYPE = "docs"

# Map the listing's `status` to a Sage quality tier. An offering may override
# with an explicit `quality_level:` in its listing. Tune this policy here.
STATUS_TO_QUALITY = {
    "complete": "silver",       # certified, shipped in real engagements
    "in progress": "bronze",    # being built / not yet certified
}
DEFAULT_QUALITY = "bronze"

# Block-scalar indicators: a top-level `key: |` (or `>`, with optional chomping
# `+`/`-`) starts an indented body we skip wholesale.
_BLOCK_INDICATORS = {"|", ">", "|-", "|+", ">-", ">+"}


def _parse_scalar(val: str) -> object:
    """Parse a top-level scalar value: quoted string, inline `[a, b]` list, or a
    bare token (with any trailing `# comment` stripped)."""
    if val.startswith('"') or val.startswith("'"):
        quote = val[0]
        end = val.find(quote, 1)
        return val[1:end] if end != -1 else val[1:]
    if val.startswith("[") and val.endswith("]"):
        return [t.strip() for t in val[1:-1].split(",") if t.strip()]
    # Bare scalar (e.g. a boolean): drop an inline comment.
    return val.split("#", 1)[0].strip()


def read_listing(path: Path) -> dict:
    """Return a catalog-listing's TOP-LEVEL fields as a flat dict.

    Only top-level `key: value` scalars (and inline lists) are captured. Block
    scalars (`demo_description: |`) and nested maps (`links:`) are detected by an
    empty or `|`/`>` value and their indented bodies are skipped, so the markdown
    body -- with its own colons and `#` headings -- never leaks into the dict.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    out: dict[str, object] = {}

    i = 0
    n = len(lines)
    while i < n:
        raw = lines[i]
        i += 1
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # Ignore anything indented at the top of the loop: a top-level key
        # never is, and indented lines belong to a block we skip below.
        if raw[0] in " \t":
            continue
        if ":" not in stripped:
            continue
        key, _, val = stripped.partition(":")
        key = key.strip()
        val = val.strip()

        if val == "" or val in _BLOCK_INDICATORS or val[0] in "|>":
            # Start of a block scalar or nested map: consume its indented body
            # (plus blank lines) until the next top-level line or EOF.
            while i < n:
                nxt = lines[i]
                if nxt.strip() == "":
                    i += 1
                    continue
                if nxt[0] not in " \t":  # back to column 0 -> block is over
                    break
                i += 1
            continue

        out[key] = _parse_scalar(val)

    return out


def external_ref(offering_dir: Path) -> str:
    """Stable external_ref from the offering's folder name. Equals the ASQ
    hashtag (`#ssa-offering #<slug>`). NEVER changes for an offering -- to Sage,
    renaming external_ref reads as delete-then-create."""
    return f"ssa-offering-{offering_dir.name}"


def quality_for(fm: dict) -> str:
    explicit = fm.get("quality_level")
    if explicit:
        return str(explicit)
    return STATUS_TO_QUALITY.get(str(fm.get("status", "")).lower(), DEFAULT_QUALITY)


def _kebab(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.strip().lower()).strip("-")


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it and it not in seen:
            seen.add(it)
            out.append(it)
    return out


def tags_for(fm: dict, slug: str, category: str) -> list[str]:
    """Prefer an explicit `tags:` list authored in the listing (fde-style
    pass-through). Otherwise derive deterministically from the structured
    fields, so tags never need hand-maintenance in the manifest."""
    explicit = fm.get("tags")
    if isinstance(explicit, list) and explicit:
        return _dedup([_kebab(t) for t in explicit])

    tags = ["ssa-offering", category]
    industry = str(fm.get("industry", "")).strip()
    if industry and industry.lower() != "ssa":  # "SSA" is redundant with the prefix
        tags.append(_kebab(industry))
    for field in ("product_lines", "products"):
        raw = str(fm.get(field, "")).strip()
        for part in raw.split(","):
            tags.append(_kebab(part))
    return _dedup(tags)


def yaml_escape(s: str) -> str:
    """Double-quote a scalar for YAML, escaping backslashes and quotes."""
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


def discover_offerings() -> list[Path]:
    paths = [
        p for p in REPO.glob(LISTING_GLOB)
        if p.relative_to(REPO).parts[0] not in EXCLUDE_TOP
    ]
    # Deterministic order by repo-relative path (category, then slug).
    return sorted(paths, key=lambda p: str(p.relative_to(REPO)))


def render_asset(listing: Path) -> list[str]:
    fm = read_listing(listing)
    offering_dir = listing.parent
    rel_dir = offering_dir.relative_to(REPO).as_posix()
    slug = offering_dir.name
    category = offering_dir.parent.name

    name = fm.get("title") or slug
    description = fm.get("business_outcome", "")
    owner = str(fm.get("demo_owner", "")).strip()
    resource_url = f"{REPO_URL}/tree/{DEFAULT_BRANCH}/{rel_dir}"

    return [
        f"  - external_ref: {external_ref(offering_dir)}",
        f"    name: {yaml_escape(str(name))}",
        f"    asset_type: {ASSET_TYPE}",
        f"    resource_url: {resource_url}",
        f"    description: {yaml_escape(str(description))}",
        f"    quality_level: {quality_for(fm)}",
        "    is_customer_facing: false",
        f"    maintained_by: {owner}".rstrip(),
        f"    tags: [{', '.join(tags_for(fm, slug, category))}]",
    ]


def render() -> str:
    offerings = discover_offerings()
    if not offerings:
        raise SystemExit(f"No offerings found under {LISTING_GLOB}")

    lines = [
        "# GENERATED by scripts/gen_sage_manifest.py -- do not edit by hand.",
        "# Edit each offering's catalog-listing.yml and regenerate. See CONTRIBUTING.md.",
        "version: 1",
        f"domain: {DOMAIN}",
        "assets:",
    ]
    for listing in offerings:
        lines.extend(render_asset(listing))
    return "\n".join(lines) + "\n"


def main() -> int:
    rendered = render()
    n = rendered.count("- external_ref:")
    if "--check" in sys.argv[1:]:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != rendered:
            print(
                "sage/assets.yaml is out of date -- run scripts/gen_sage_manifest.py",
                file=sys.stderr,
            )
            return 1
        print(f"sage/assets.yaml up to date ({n} assets)")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUT.relative_to(REPO)} ({n} assets)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
