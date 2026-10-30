"""Raw HMS metadata via the JVM `HiveMetaStoreClient` over py4j (D4, §8.1): returns the raw type strings
(LTZ / UNIONTYPE / INTERVAL) that make Spark's catalog throw CANNOT_RECOGNIZE_HIVE_TYPE.

Not implemented in the scaffold."""
