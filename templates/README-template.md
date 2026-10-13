---
# ── Offering metadata (YAML frontmatter) ───────────────────────────────────────
# This block is the MACHINE-READABLE source of truth. `scripts/gen_sage_manifest.py`
# reads it to build sage/assets.yaml, and `scripts/validate_specs.py` checks it.
# Keep every value a flat scalar or an inline [a, b] list -- no nested maps here.
# The prose that humans read lives in the markdown body below the closing `---`.

# Required (validate_specs enforces these)
title: "Example Offering Title"                 # Offering name only -- no "Global", region, or team
owner: "john.doe@databricks.com"                # Primary owner
region: "Global"                                # Global | AMER | EMEA | ...
team: ""                                        # Empty for Global; otherwise FINS | DNB | ...
status: "Active"                                # Active | Under Review | Inactive
business_outcome: "Reduce customer churn by 20%"  # One-line outcome -> Sage asset description

# The Sage / Salesforce display name is generated from title + region + team:
#   Global -> "SSA | <title> | Global"     Local -> "SSA | <title> | <region> | <team>"

# Discovery tags -- curated keywords for human search + agent match, passed to
# Sage verbatim. Keep `ssa-offering` first. Global offerings MUST include
# `global`; local offerings must not (validate_specs checks this).
tags: [ssa-offering, global, data-warehousing, databricks-sql]

# Recommended
strategic_priority: "High"                      # High | Medium | Low
imperative: "Data Warehousing"
last_certified: "2027-01-15"                    # YYYY-MM-DD
products: "Databricks SQL, Delta Lake"          # Comma-separated
product_lines: "Data Warehousing"               # Comma-separated
---

## Example Offering Title

![Example Offering Title](images/thumbnail.png)

**Pitch deck:** [open in Google Slides](https://example.com/your-deck)

> **SSA offering — how to engage:** If you are interested in this SSA Offering, follow the ASQ process (go/gethelp): Product Help > Pilot/Production Advisory; then put the tag #ssa-offering #<offering-slug> in both the **Title** and **Description** fields.

<!-- The `#ssa-offering #<offering-slug>` hashtag MUST match this offering's folder name; validate_specs.py checks for it. -->

**GOAL:** One-sentence statement of the outcome this engagement drives.

### Overview
What this offering is and what a customer walks away with.

### GTM Signals — When to Engage
- Signal 1
- Signal 2

### Technical Readiness Criteria
- Criterion 1

### Engagement Process (N-Step Joint Execution Plan)

**Step 1 — …**
- Preparation: …
- Session: …
- Next Steps: …

### Customer Requirements
- Requirement 1

<!-- Images: put files under images/ next to this README and reference them inline, e.g. -->
<!-- ![architecture](images/architecture-diagram.png) -->
