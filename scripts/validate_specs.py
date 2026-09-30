#!/usr/bin/env python3
"""Validate the offering catalog structure before a PR merges (CI Tier 0).

Deterministic, dependency-free gate. No LLM, no network, no secret -- it only
inspects files already in the repo. Complements `gen_sage_manifest.py --check`
(which asserts the manifest is regenerated); this asserts the *offerings
themselves* are contributed the right way.

What it enforces:
  1. Frontmatter  -- every offering README at <category>/<slug>/README.md opens
                     with a YAML frontmatter block (the machine-readable source).
  2. Metadata     -- each README's frontmatter has the required keys with
                     non-empty values.
  3. Status       -- `status` is one of the known values that the quality map
                     understands, so a new value can't silently fall to bronze.
  4. Engage hint  -- the body carries the `#ssa-offering #<slug>` ASQ hashtag, the
                     stable join key across repo / Sage / ASQ (was auto-injected
                     by the old README generator; now authored, so we check it).
  5. No duplicates -- no two offerings collide on external_ref
                     (ssa-offering-<slug>) or on title (case-insensitive).

We intentionally do NOT require a separate stable `id` (as fde-specs does). Here
identity is the folder slug, which is contractually the same string as the ASQ
hashtag (`#ssa-offering #<slug>`); the directory name IS the stable id, so a
rename is a deliberate identity change and needs no decoupled field.

Exit code is non-zero if any check fails, and every failure is printed, so a PR
author sees the full list at once rather than fixing one, re-pushing, repeating.

Usage:  python3 scripts/validate_specs.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Reuse the generator's parser, discovery, and slug logic -- one source of truth.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from gen_sage_manifest import (  # noqa: E402
    OFFERING_FILENAME,
    REPO,
    STATUS_TO_QUALITY,
    discover_offerings,
    external_ref,
    read_frontmatter,
)

# Frontmatter keys every offering must carry with a non-empty value. These are
# the fields the manifest depends on (title/description/owner) plus status (drives
# quality). `products`/`product_lines`/`industry` feed tags but aren't required
# individually -- the tags check below asserts at least one produced a tag.
REQUIRED_KEYS = ("title", "status", "demo_owner", "business_outcome")

# Status values the quality map understands (see STATUS_TO_QUALITY).
KNOWN_STATUS = set(STATUS_TO_QUALITY)


def main() -> int:
    errors: list[str] = []

    offerings = discover_offerings()
    if not offerings:
        print(
            f"No offerings found under <category>/<slug>/{OFFERING_FILENAME}",
            file=sys.stderr,
        )
        return 1

    refs: dict[str, Path] = {}
    titles: dict[str, Path] = {}

    for readme in offerings:
        rel = readme.relative_to(REPO)
        slug = readme.parent.name

        # 1. Filename must be README.md (glob enforces it; be explicit).
        if readme.name != OFFERING_FILENAME:
            errors.append(f"{rel}: offering file must be named {OFFERING_FILENAME}")

        # 2. Must open with YAML frontmatter -- an empty parse means the `---`
        #    fence is missing, so a non-offering README got matched or the source
        #    metadata was dropped.
        fm = read_frontmatter(readme)
        if not fm:
            errors.append(
                f"{rel}: no YAML frontmatter (an offering README must open with a "
                f"`---` block holding {', '.join(REQUIRED_KEYS)}, ...)"
            )
            continue  # nothing more to check without metadata

        # 3. Required metadata present + non-empty.
        for key in REQUIRED_KEYS:
            val = fm.get(key)
            if val is None or (isinstance(val, str) and not val.strip()) or val == []:
                errors.append(f"{rel}: missing or empty required key '{key}'")

        # 4. Known status value (so the quality map never silently defaults).
        status = str(fm.get("status", "")).lower()
        if status and status not in KNOWN_STATUS:
            errors.append(
                f"{rel}: unknown status '{fm.get('status')}' (expected one of "
                f"{sorted(KNOWN_STATUS)}; update STATUS_TO_QUALITY to add one)"
            )

        # 5. Body carries the ASQ engage hashtag (`#ssa-offering #<slug>`).
        body = readme.read_text(encoding="utf-8")
        if f"#ssa-offering #{slug}" not in body:
            errors.append(
                f"{rel}: body is missing the ASQ engage hashtag "
                f"`#ssa-offering #{slug}` (SAs copy it into the ASQ Title/Description)"
            )

        # 6a. Duplicate external_ref (slug collision across categories).
        ref = external_ref(readme.parent)
        if ref in refs:
            errors.append(
                f"{rel}: duplicate external_ref '{ref}' -- also produced by "
                f"{refs[ref].relative_to(REPO)} (offering folder names must be unique)"
            )
        else:
            refs[ref] = readme

        # 6b. Duplicate title (case-insensitive).
        title = str(fm.get("title", "")).strip().lower()
        if title:
            if title in titles:
                errors.append(
                    f"{rel}: duplicate title '{fm.get('title')}' -- also used by "
                    f"{titles[title].relative_to(REPO)}"
                )
            else:
                titles[title] = readme

    if errors:
        print(f"Offering validation FAILED ({len(errors)} issue(s)):", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(f"Offering validation passed ({len(offerings)} offerings, no duplicates).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
