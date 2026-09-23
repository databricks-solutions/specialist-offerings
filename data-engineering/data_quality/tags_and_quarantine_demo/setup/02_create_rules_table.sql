-- Rules table: each row is a generic DQ rule with a {column} placeholder.
-- Joined to system.information_schema.column_tags at pipeline definition time.

CREATE OR REPLACE TABLE alexn.sdp_demo.dq_rules (
    tag_name             STRING NOT NULL,
    rule_name            STRING NOT NULL,
    expression_template  STRING NOT NULL,
    description          STRING
) USING DELTA
COMMENT 'Generic DQ rules. {column} placeholder is filled per-column at definition time.';

INSERT INTO alexn.sdp_demo.dq_rules VALUES
  ('not_null',            'not_null_check',     '{column} IS NOT NULL',
     'Column must not be null'),
  ('positive',            'positive_check',     '{column} > 0',
     'Numeric value must be positive'),
  ('non_negative',        'non_negative_check', '{column} >= 0',
     'Numeric value must be non-negative'),
  ('valid_email',         'email_check',
     '{column} RLIKE "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$"',
     'Column must look like an email'),
  ('valid_past_date',     'past_date_check',    'try_cast({column} AS DATE) <= current_date()',
     'Date must not be in the future'),
  ('valid_zip',           'zip_check',
     '{column} RLIKE "^[0-9]{5}(-[0-9]{4})?$"',
     'US zip code, 5 or 9 digit'),
  ('valid_coverage',      'coverage_check',
     '{column} IN ("basic","standard","premium")',
     'Allowed values for coverage_type'),
  ('valid_policy_status', 'policy_status_check',
     '{column} IN ("active","lapsed","cancelled","pending")',
     'Allowed values for policy.status'),
  ('valid_peril',         'peril_check',
     '{column} IN ("fire","flood","wind","theft","liability","water_damage","other")',
     'Allowed values for claim.peril_type'),
  ('valid_claim_status',  'claim_status_check',
     '{column} IN ("open","approved","denied","paid","closed")',
     'Allowed values for claim.status');
