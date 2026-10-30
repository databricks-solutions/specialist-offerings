"""`h2mi bridge`: emit Hive-side CTAS scripts for REDESIGN columns (LTZ, INTERVAL, UNION via bridges/union_to_struct).
Never a view: Spark re-resolves the base table and hits the read barrier again (TYP-08).

Not implemented in the scaffold; the bridge pattern is lab-proven (docs/requirements.md Appendix C, TYP-08)."""
