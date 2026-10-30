"""`h2mi bridge`: emit Hive-side CTAS scripts for REDESIGN columns (LTZ, INTERVAL, UNION via bridges/union_to_struct).
Never a view: Spark re-resolves the base table and hits the read barrier again (TYP-08).

Not implemented in the scaffold; the lab HQL that proves the bridges is in visa_utils/topics/01_datatypes/."""
