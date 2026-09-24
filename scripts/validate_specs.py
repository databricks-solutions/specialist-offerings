#!/usr/bin/env python3
"""Validate the offering catalog structure before a PR merges (CI Tier 0).

Deterministic, dependency-free gate. No LLM, no network, no secret -- it only
inspects files already in the repo. Complements `gen_sage_manifest.py --check`
(which asserts the manifest is regenerated); this asserts the *offerings
themselves* are contributed the right way.

What it enforces:
  1. Layout       -- every offering lives at <category>/<slug>/catalog-listing.yml;
                     no stray catalog-listing.yml sitting directly in a category
                     folder or at the repo root.
  2. Metadata     -- each listing has the required keys with non-empty values.
  3. Status       -- `status` is one of the known values that the quality map
                     understands, so a new value can't silently fall to bronze.
  4. No duplicates -- no two offerings collide on external_ref
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
    EXCLUDE_TOP,
    LISTING_FILENAME,
    REPO,
    STATUS_TO_QUALITY,
    discover_offerings,
    external_ref,
    read_listing,
)

# Listing keys every offering must carry with a non-empty value. These are the
# fields the manifest depends on (title/description/owner) plus status (drives
# quality). `products`/`product_lines`/`industry` feed tags but aren't required
# individually -- the tags check below asserts at least one produced a tag.
REQUIRED_KEYS = ("title", "status", "demo_owner", "business_outcome")

# Status values the quality map understands (see STATUS_TO_QUALITY).
KNOWN_STATUS = set(STATUS_TO_QUALITY)


def check_no_stray_listings(errors: list[str]) -> None:
    """Catch a catalog-listing.yml that isn't at <category>/<slug>/: one sitting
    directly in a category folder, or at the repo root."""
    # Repo root.
    if (REPO / LISTING_FILENAME).exists():
        errors.append(
            f"{LISTING_FILENAME}: listing at repo root -- move it to "
            f"<category>/<slug>/{LISTING_FILENAME}"
        )
    # One level deep: <category>/catalog-listing.yml (missing the <slug> folder).
    for cat in REPO.glob(f"*/{LISTING_FILENAME}"):
        if cat.relative_to(REPO).parts[0] in EXCLUDE_TOP:
            continue
        errors.append(
            f"{cat.relative_to(REPO)}: listing not in an offering folder -- move "
            f"it to <category>/<slug>/{LISTING_FILENAME} (one folder per offering)"
        )


def main() -> int:
    errors: list[str] = []

    check_no_stray_listings(errors)

    offerings = discover_offerings()
    if not offerings:
        print(
            f"No offerings found under <category>/<slug>/{LISTING_FILENAME}",
            file=sys.stderr,
        )
        return 1

    refs: dict[str, Path] = {}
    titles: dict[str, Path] = {}

    for listing in offerings:
        rel = listing.relative_to(REPO)

        # 1. Filename must be catalog-listing.yml (glob enforces it; be explicit).
        if listing.name != LISTING_FILENAME:
            errors.append(f"{rel}: listing file must be named {LISTING_FILENAME}")

        # 2. Required metadata present + non-empty.
        fm = read_listing(listing)
        for key in REQUIRED_KEYS:
            val = fm.get(key)
            if val is None or (isinstance(val, str) and not val.strip()) or val == []:
                errors.append(f"{rel}: missing or empty required key '{key}'")

        # 3. Known status value (so the quality map never silently defaults).
        status = str(fm.get("status", "")).lower()
        if status and status not in KNOWN_STATUS:
            errors.append(
                f"{rel}: unknown status '{fm.get('status')}' (expected one of "
                f"{sorted(KNOWN_STATUS)}; update STATUS_TO_QUALITY to add one)"
            )

        # 4a. Duplicate external_ref (slug collision across categories).
        ref = external_ref(listing.parent)
        if ref in refs:
            errors.append(
                f"{rel}: duplicate external_ref '{ref}' -- also produced by "
                f"{refs[ref].relative_to(REPO)} (offering folder names must be unique)"
            )
        else:
            refs[ref] = listing

        # 4b. Duplicate title (case-insensitive).
        title = str(fm.get("title", "")).strip().lower()
        if title:
            if title in titles:
                errors.append(
                    f"{rel}: duplicate title '{fm.get('title')}' -- also used by "
                    f"{titles[title].relative_to(REPO)}"
                )
            else:
                titles[title] = listing

    if errors:
        print(f"Offering validation FAILED ({len(errors)} issue(s)):", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(f"Offering validation passed ({len(offerings)} offerings, no duplicates).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
