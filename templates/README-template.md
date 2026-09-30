---
# ── Offering metadata (YAML frontmatter) ───────────────────────────────────────
# This block is the MACHINE-READABLE source of truth. `scripts/gen_sage_manifest.py`
# reads it to build sage/assets.yaml, and `scripts/validate_specs.py` checks it.
# Keep every value a flat scalar or an inline [a, b] list -- no nested maps here.
# The prose that humans read lives in the markdown body below the closing `---`.

# Required (validate_specs enforces these)
title: "Example Offering Title"                 # Max 500 chars
status: "Complete"                              # Complete | In Progress (drives Sage quality tier)
demo_owner: "john.doe@databricks.com"           # Primary owner
business_outcome: "Reduce customer churn by 20%"  # One-line outcome -> Sage asset description

# Discovery tags -- curated keywords for human search + agent match. Consumed
# verbatim by gen_sage_manifest.py; if omitted, tags are derived from the fields
# below. Always keep `ssa-offering` first.
tags: [ssa-offering, data-warehousing, databricks-sql]

# Industry / vertical scope:
#   - Global offering (default): OMIT `industry` entirely (stores NULL, surfaces everywhere).
#   - Vertical-specific: set ONE Field Catalog vertical code (FINS, CME, HLS, MFG, RCT, E&U, X-industry, PUBSEC).
# industry: "FINS"

# Recommended (carried through for humans / Field Catalog; optional for the manifest)
strategic_priority: "High"                      # High | Medium | Low
industry_lead: "jane.smith@databricks.com"
imperative: "Data Warehousing"
expected_delivery: "Q2 FY27"
last_certified: "2027-01-15"                    # YYYY-MM-DD
products: "Databricks SQL, Delta Lake"          # Comma-separated
product_lines: "Data Warehousing"               # Comma-separated
needs_fix: false
business_user_friendly: false
partner_developed: false
partner: ""
installable: false                              # true if there is a runnable install path
---

## Example Offering Title

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

### Links
- [pitch_deck](https://example.com/your-deck)

<!-- Images: put files under images/ next to this README and reference them inline, e.g. -->
<!-- ![architecture](images/architecture-diagram.png) -->
