-- PHASE 2: apply UC column tags to bronze + silver columns.
-- Run AFTER the first pipeline execution (which creates the tables).
-- Idempotent: safe to re-run after any full refresh.

-- bronze: only structural rules (PK not-null). Shows the pattern at the raw layer.
ALTER TABLE alexn.sdp_demo.bronze_policies ALTER COLUMN policy_id SET TAGS ('not_null');
ALTER TABLE alexn.sdp_demo.bronze_claims   ALTER COLUMN claim_id  SET TAGS ('not_null');

-- silver_policies: full semantic validation.
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN policy_id      SET TAGS ('not_null');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN holder_name    SET TAGS ('not_null');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN holder_email   SET TAGS ('not_null', 'valid_email');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN property_zip   SET TAGS ('not_null', 'valid_zip');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN coverage_type  SET TAGS ('not_null', 'valid_coverage');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN premium_amount SET TAGS ('positive');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN effective_date SET TAGS ('not_null', 'valid_past_date');
ALTER TABLE alexn.sdp_demo.silver_policies ALTER COLUMN status         SET TAGS ('not_null', 'valid_policy_status');

-- silver_claims: full semantic validation.
ALTER TABLE alexn.sdp_demo.silver_claims ALTER COLUMN claim_id     SET TAGS ('not_null');
ALTER TABLE alexn.sdp_demo.silver_claims ALTER COLUMN policy_id    SET TAGS ('not_null');
ALTER TABLE alexn.sdp_demo.silver_claims ALTER COLUMN claim_date   SET TAGS ('not_null', 'valid_past_date');
ALTER TABLE alexn.sdp_demo.silver_claims ALTER COLUMN claim_amount SET TAGS ('positive');
ALTER TABLE alexn.sdp_demo.silver_claims ALTER COLUMN peril_type   SET TAGS ('not_null', 'valid_peril');
ALTER TABLE alexn.sdp_demo.silver_claims ALTER COLUMN status       SET TAGS ('not_null', 'valid_claim_status');
