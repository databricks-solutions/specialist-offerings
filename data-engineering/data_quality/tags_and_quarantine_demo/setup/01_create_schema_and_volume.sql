-- Bootstrap: schema + volume under the alexn catalog.
-- Run with: databricks sql ... or paste into a SQL editor.

CREATE SCHEMA IF NOT EXISTS alexn.sdp_demo
  COMMENT 'SDP demo: tag-driven expectations from a rules table';

CREATE VOLUME IF NOT EXISTS alexn.sdp_demo.files
  COMMENT 'Landing volume for raw policies + claims JSON files';
