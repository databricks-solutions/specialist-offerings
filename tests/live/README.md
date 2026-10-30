# tests/live (opt-in)

The integration suites against a real UC sandbox catalog over IRC + the Statement API, run from an "on-prem" box with
HDFS, YARN and a Hive 3.1.3 metastore. Endpoints come from environment variables (`H2MI_ENV_YAML`, `H2MI_TABLES_DIR`);
secrets through references only (`vault:` / `aws-sm:` / `env:`).

The lab rig used so far is `visa_utils/setup/onprem_lab/` (EC2 simulation). Point this suite at it or at your own cluster.
Tests to close here: L-2, L-3, L-4, L-8 .. L-17 (design §11).
