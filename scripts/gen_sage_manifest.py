#!/usr/bin/env python3
"""Generate the Sage managed-source manifest (sage/assets.yaml).

Each SSA offering is registered in Sage as its OWN asset: one offering folder =
one manifest entry. We walk `<category>/<slug>/README.md`, read the offering
README's YAML frontmatter, and fan out to one asset per offering. The
frontmatter is the source of truth for name, description, tags, quality, and
owner -- there is nothing to hand-maintain in this manifest.

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

Dependency-free by design (no PyYAML): an offering README opens with a YAML
frontmatter block (delimited by `---`) holding flat metadata; the markdown body
after the closing fence is prose we ignore here. Every field this manifest needs
is a flat frontmatter scalar (or an inline `[a, b]` list), so we parse only
`key: value` pairs between the fences and stop at the closing `---`. This keeps
the generator runnable on a stock `python3` with no install, mirroring fde-specs'
zero-dependency gate. (If frontmatter ever needs full YAML semantics, swap
read_frontmatter for `yaml.safe_load` and add PyYAML to CI.)

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

# Offerings live at <category>/<slug>/README.md. This glob is two levels deep,
# so category-level indexes (<category>/README.md), the repo-root README, the
# templates/ sample, and archived offerings (z-archive/...) are all naturally
# excluded; EXCLUDE_TOP is a belt-and-braces guard in case a non-offering dir
# ever grows a matching path.
OFFERING_GLOB = "*/*/README.md"
OFFERING_FILENAME = "README.md"
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


def read_frontmatter(path: Path) -> dict:
    """Return an offering README's YAML frontmatter as a flat dict.

    Frontmatter is the block delimited by a leading `---` and the next `---`.
    Only flat `key: value` scalars (and inline `[a, b]` lists) between the fences
    are captured; parsing stops at the closing `---`, so the markdown body -- with
    its own colons and `#` headings -- never leaks into the dict. A nested map or
    block scalar in the frontmatter is not expected, but if one appears its
    indented body is skipped defensively. A file with no opening `---` returns {}.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    n = len(lines)
    out: dict[str, object] = {}

    # Frontmatter must open the file (allowing leading blank lines only).
    i = 0
    while i < n and lines[i].strip() == "":
        i += 1
    if i >= n or lines[i].strip() != "---":
        return out
    i += 1  # step past the opening fence

    while i < n:
        raw = lines[i]
        i += 1
        stripped = raw.strip()
        if stripped == "---":  # closing fence -> body starts here; stop.
            break
        if not stripped or stripped.startswith("#"):
            continue
        if raw[0] in " \t":  # indented -> part of a skipped nested structure
            continue
        if ":" not in stripped:
            continue
        key, _, val = stripped.partition(":")
        key = key.strip()
        val = val.strip()

        if val == "" or val in _BLOCK_INDICATORS or val[0] in "|>":
            # Unexpected nested map / block scalar: consume its indented body
            # (plus blank lines) until the next top-level key or the fence.
            while i < n and (lines[i].strip() == "" or lines[i][:1] in " \t"):
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
        p for p in REPO.glob(OFFERING_GLOB)
        if p.relative_to(REPO).parts[0] not in EXCLUDE_TOP
    ]
    # Deterministic order by repo-relative path (category, then slug).
    return sorted(paths, key=lambda p: str(p.relative_to(REPO)))


def render_asset(listing: Path) -> list[str]:
    fm = read_frontmatter(listing)
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
        raise SystemExit(f"No offerings found under {OFFERING_GLOB}")

    lines = [
        "# GENERATED by scripts/gen_sage_manifest.py -- do not edit by hand.",
        "# Edit each offering's README.md frontmatter and regenerate. See CONTRIBUTING.md.",
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
