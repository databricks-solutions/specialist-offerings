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
  3. Status       -- `status` is one of Active / Under Review / Inactive.
  3b. Scope       -- Global offerings leave `team` empty and carry the `global`
                     tag; local offerings (any other `region`) name their team
                     and don't carry `global`, so the generated display name
                     and tags match the Salesforce listing.
  3c. Certified   -- `last_certified`, when set, is a real YYYY-MM-DD date that
                     is not in the future (it records the last human update).
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
from datetime import date
from pathlib import Path

# Reuse the generator's parser, discovery, and slug logic -- one source of truth.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from gen_sage_manifest import (  # noqa: E402
    OFFERING_FILENAME,
    REPO,
    STATUSES,
    discover_offerings,
    external_ref,
    is_global,
    read_frontmatter,
)

# Frontmatter keys every offering must carry with a non-empty value. These are
# the fields the manifest depends on (title/description/owner), the health
# status, and the region that drives the display name and geo tag. `team` is
# checked separately because it must be empty for Global offerings.
REQUIRED_KEYS = ("title", "owner", "region", "status", "business_outcome")


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

        # 4. Known status value.
        status = str(fm.get("status", "")).strip()
        if status and status not in STATUSES:
            errors.append(
                f"{rel}: unknown status '{status}' (expected one of "
                f"{', '.join(STATUSES)})"
            )

        # 4b. Global offerings have no team; local offerings must name one.
        team = str(fm.get("team", "")).strip()
        if fm.get("region"):
            if is_global(fm) and team:
                errors.append(
                    f"{rel}: Global offerings must leave `team` empty (found '{team}')"
                )
            elif not is_global(fm) and not team:
                errors.append(
                    f"{rel}: local offering (region '{fm.get('region')}') must set `team` "
                    f"(e.g. FINS, DNB)"
                )
            tags = fm.get("tags") if isinstance(fm.get("tags"), list) else []
            has_global_tag = "global" in {str(t).strip().lower() for t in tags}
            if is_global(fm) and not has_global_tag:
                errors.append(f"{rel}: Global offerings must include `global` in `tags`")
            elif not is_global(fm) and has_global_tag:
                errors.append(
                    f"{rel}: `global` tag is only for Global offerings "
                    f"(region is '{fm.get('region')}')"
                )

        # 4c. last_certified is a real date, not in the future.
        certified = str(fm.get("last_certified", "")).strip()
        if certified:
            try:
                certified_on = date.fromisoformat(certified)
            except ValueError:
                errors.append(
                    f"{rel}: last_certified '{certified}' is not a YYYY-MM-DD date"
                )
            else:
                if certified_on > date.today():
                    errors.append(
                        f"{rel}: last_certified {certified} is in the future "
                        f"(use the date of the last human update)"
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
