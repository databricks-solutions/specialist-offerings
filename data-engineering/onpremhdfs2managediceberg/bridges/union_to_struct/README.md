# union_to_struct: Hive UDF for UNIONTYPE bridges (TYP-08)

Spark cannot read a Hive column typed `uniontype<...>` through the metastore (read barrier). The bridge materializes a
Spark-readable copy in Hive: `union_to_struct(col)` returns `struct<tag:int, f0:..., f1:...>`, and the table config then
points `source` at the CTAS copy. Never use a view: Spark re-resolves the base table and hits the barrier again.

```bash
./build.sh                       # needs JDK 8 and $HIVE_HOME/lib, $HADOOP_HOME on the box; writes out/union_to_struct.jar
hive --hivevar DB=<db> -e "ADD JAR out/union_to_struct.jar;
  CREATE TEMPORARY FUNCTION union_to_struct AS 'UnionToStruct';
  CREATE TABLE <db>.<t>_bridge STORED AS PARQUET AS SELECT ..., union_to_struct(u) AS u FROM <db>.<t>;"
```
`h2mi bridge` (scanner/bridges.py) will emit these scripts per table; lab evidence: `docs/requirements.md` Appendix C.
