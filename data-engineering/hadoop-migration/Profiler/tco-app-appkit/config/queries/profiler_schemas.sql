-- Schemas within a catalog for the profiler schema picker.
-- @param catalog STRING = profiler
SELECT schema_name
FROM system.information_schema.schemata
WHERE catalog_name = :catalog
  AND schema_name NOT IN ('information_schema')
ORDER BY schema_name;
