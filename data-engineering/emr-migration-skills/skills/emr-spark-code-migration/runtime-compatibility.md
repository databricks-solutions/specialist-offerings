# Runtime Compatibility: EMR vs Databricks

This document provides version matrices and compatibility guidance for migrating Spark workloads from Amazon EMR to Databricks Runtime.

## EMR Release Version Matrix

| EMR Release | Spark Version | Scala Version | Java Version | Python Version | Delta Lake | Hadoop Version |
|---|---|---|---|---|---|---|
| EMR 6.15 | 3.4.1 | 2.12.17 | 8, 11 | 3.9 | Not bundled (install 2.4.x) | 3.3.6 |
| EMR 7.0 | 3.5.0 | 2.12.18 | 8, 11, 17 | 3.9 | Not bundled (install 3.1.x) | 3.3.6 |
| EMR 7.1 | 3.5.1 | 2.12.18 | 8, 11, 17 | 3.9 | Not bundled (install 3.1.x) | 3.3.6 |
| EMR 7.2 | 3.5.3 | 2.12.18 | 8, 11, 17 | 3.9 | Not bundled (install 3.2.x) | 3.4.0 |

**Notes:**
- Delta Lake on EMR requires manual installation via `--packages` or `--jars`. It is an add-on, not native.
- EMR 6.x series uses Amazon Linux 2; EMR 7.x uses Amazon Linux 2023.
- Java 17 support was added in EMR 7.0.
- Python version can be overridden via bootstrap actions, but 3.9 is the default.

## Databricks Runtime Version Matrix

| DBR Version | Spark Version | Scala Version | Java Version | Python Version | Delta Lake | Hadoop Version | End of Support |
|---|---|---|---|---|---|---|---|
| DBR 14.3 LTS | 3.5.0 | 2.12.15 | 8, 11 | 3.10 | 3.1.0 | 3.3.6 | ~March 2026 |
| DBR 15.4 LTS | 3.5.0 | 2.12.15 | 11, 17 | 3.11 | 3.2.0 | 3.3.6 | ~March 2027 |
| DBR 16.0 | 4.0.0 | 2.13.x | 17 | 3.12 | 4.0.0 | 3.4.x | Non-LTS |
| DBR 16.1 | 4.0.0 | 2.13.x | 17 | 3.12 | 4.0.0 | 3.4.x | Non-LTS |

**Notes:**
- Delta Lake is native to DBR and cannot be replaced or downgraded.
- LTS (Long Term Support) releases receive patches for approximately 2 years.
- Non-LTS releases are supported for approximately 6 months.
- Photon (native C++ engine) is available on all DBR versions and accelerates SQL/DataFrame operations.

## Recommended Migration Paths

### EMR 6.15 (Spark 3.4.1) → DBR 14.3 LTS

| Aspect | EMR 6.15 | DBR 14.3 LTS | Migration Notes |
|---|---|---|---|
| Spark | 3.4.1 | 3.5.0 | Minor version bump; review [Spark 3.5 migration guide](https://spark.apache.org/docs/3.5.0/migration-guide.html) |
| Scala | 2.12.17 | 2.12.15 | Compatible; no recompilation needed for JARs |
| Java | 8/11 | 8/11 | No change needed |
| Python | 3.9 | 3.10 | Test for deprecation warnings; `match` statements now available |
| Delta | Not bundled | 3.1.0 | Adopt Delta for new tables; convert existing Parquet tables |

**Spark 3.4 → 3.5 Key Changes:**
- `spark.sql.adaptive.enabled` is `true` by default (was `true` since 3.2, but some configs changed)
- New `IDENTIFIER` clause in SQL
- Arrow-optimized Python UDFs improved
- PySpark DataFrame `plot()` API added
- `spark.connect` (Spark Connect) available but optional

### EMR 7.0/7.1 (Spark 3.5.x) → DBR 15.4 LTS

| Aspect | EMR 7.0/7.1 | DBR 15.4 LTS | Migration Notes |
|---|---|---|---|
| Spark | 3.5.0/3.5.1 | 3.5.0 | Same major.minor; highly compatible |
| Scala | 2.12.18 | 2.12.15 | Compatible |
| Java | 8/11/17 | 11/17 | Java 8 not supported on DBR 15.4; upgrade if using Java 8 |
| Python | 3.9 | 3.11 | Significant jump; test for breaking changes |
| Delta | Not bundled | 3.2.0 | Native; major benefit of migration |

**Python 3.9 → 3.11 Key Changes:**
- `match`/`case` statements (3.10+)
- Better error messages with precise line numbers
- `tomllib` in standard library (3.11)
- `asyncio.TaskGroup` (3.11)
- Some removed deprecated modules: `aifc`, `audioop`, `cgi`, `cgitb`, `chunk`, `crypt`, `imghdr`, `mailcap`, `msilib`, `nis`, `nntplib`, `ossaudiodev`, `pipes`, `sndhdr`, `spwd`, `sunau`, `telnetlib`, `uu`, `xdrlib`
- `inspect.getargspec()` removed (use `inspect.getfullargspec()`)

### EMR 7.2 (Spark 3.5.3) → DBR 15.4 LTS

| Aspect | EMR 7.2 | DBR 15.4 LTS | Migration Notes |
|---|---|---|---|
| Spark | 3.5.3 | 3.5.0 | EMR has a slightly newer patch version; usually no issue |
| Hadoop | 3.4.0 | 3.3.6 | EMR has newer Hadoop; test any hadoop-specific features |
| Python | 3.9 | 3.11 | Same considerations as above |

### Any EMR → DBR 16.x (Spark 4.0) — Advanced Migration

| Aspect | EMR 7.x | DBR 16.x | Migration Notes |
|---|---|---|---|
| Spark | 3.5.x | 4.0.0 | **Major version change**; many deprecated APIs removed |
| Scala | 2.12 | 2.13 | **Binary incompatible**; all Scala JARs must be recompiled |
| Java | 8/11/17 | 17 only | Must upgrade to Java 17 |
| Python | 3.9 | 3.12 | Significant changes; test thoroughly |
| Delta | N/A | 4.0.0 | New Delta features; liquid clustering replaces Z-order |

**Only attempt this migration if you have strong test coverage.** The Spark 3.x → 4.0 transition involves breaking changes.

## Scala 2.12 vs 2.13 Considerations

This is the single most impactful change when migrating to DBR 16.x.

### Binary Incompatibility

Scala 2.12 and 2.13 are **binary incompatible**. Any JAR compiled for Scala 2.12 will NOT work on a Scala 2.13 runtime.

**What must be recompiled:**
- All custom Spark applications (JARs, uber-JARs)
- All custom UDF JARs
- All third-party libraries that are Scala-version-specific (look for `_2.12` or `_2.13` suffix in artifact name)

**What does NOT need recompilation:**
- Pure Java JARs (JDBC drivers, etc.)
- Python code
- SQL scripts

### How to Recompile for 2.13

```sbt
// build.sbt — change scalaVersion
scalaVersion := "2.13.12"  // was "2.12.18"

// Update Spark dependency
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided"
```

```xml
<!-- pom.xml — change scala.version and suffix -->
<properties>
    <scala.version>2.13.12</scala.version>
    <scala.binary.version>2.13</scala.binary.version>
    <spark.version>4.0.0</spark.version>
</properties>
```

### Common Scala 2.13 Migration Issues

| Issue | Solution |
|---|---|
| `scala.collection.JavaConverters` deprecated | Use `scala.jdk.CollectionConverters` |
| `Stream` deprecated | Use `LazyList` |
| `.toStream` deprecated | Use `.to(LazyList)` |
| `CanBuildFrom` removed | Use `iterableFactory` pattern |
| Parallel collections moved | Add `scala-parallel-collections` library |
| `Seq` is now `immutable.Seq` by default | Import `scala.collection.mutable.Seq` if needed |

## Java 8 Deprecation Timeline on Databricks

| DBR Version | Java 8 Support |
|---|---|
| DBR 13.3 LTS | Supported (default) |
| DBR 14.3 LTS | Supported (Java 11 recommended) |
| DBR 15.4 LTS | **Not supported** (Java 11 minimum) |
| DBR 16.x | **Not supported** (Java 17 only) |

### Impact of Java 8 Removal

If your Spark application or its dependencies use Java 8-specific APIs or rely on Java 8 behavior:

1. **Nashorn JavaScript engine**: Removed in Java 15. If you use `javax.script.ScriptEngine` with JavaScript, switch to GraalVM JS or another engine.
2. **`sun.misc.Unsafe`**: Still available but with warnings. Plan to replace with `java.lang.invoke.VarHandle`.
3. **Reflection access to JDK internals**: Requires `--add-opens` JVM flags on Java 11+. Add these to cluster Spark config:
   ```
   spark.driver.extraJavaOptions --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
   spark.executor.extraJavaOptions --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
   ```
4. **Removed APIs in Java 11**: `javax.xml.bind` (JAXB), `javax.activation`, `javax.annotation`, `javax.xml.ws` (JAX-WS), CORBA. Add replacements:
   ```xml
   <dependency>
       <groupId>jakarta.xml.bind</groupId>
       <artifactId>jakarta.xml.bind-api</artifactId>
       <version>4.0.0</version>
   </dependency>
   ```

## Photon Engine Considerations

Photon is a Databricks-proprietary C++ vectorized engine that accelerates Spark SQL and DataFrame operations. It is not available on EMR.

### What Photon Accelerates

- SQL queries (SELECT, JOIN, GROUP BY, WINDOW)
- DataFrame operations that compile to SQL
- Parquet and Delta Lake reads/writes
- Aggregations, filters, projections

### What Photon Does NOT Accelerate

- RDD operations
- Custom UDFs (Python, Scala, Java)
- Non-SQL data sources (custom data source V2 implementations)
- Spark Streaming micro-batches (the query planning is accelerated, not the streaming framework itself)

### Migration Impact

Photon is transparent — code does not need to change. Enable it on the cluster and observe performance improvements. If a query behaves differently with Photon enabled (rare), file a Databricks support ticket.

## Choosing the Right DBR Version

### Decision Tree

1. **Is your EMR code on Spark 3.4?** → Start with **DBR 14.3 LTS**
2. **Is your EMR code on Spark 3.5?** → Use **DBR 15.4 LTS** (preferred) or **DBR 14.3 LTS**
3. **Do you need Java 8?** → Use **DBR 14.3 LTS** (last version with Java 8 support)
4. **Do you have Scala 2.12 JARs you cannot recompile?** → Stay on **DBR 14.3 or 15.4 LTS** (both are Scala 2.12)
5. **Are you starting a greenfield project?** → Use **DBR 15.4 LTS** for stability or **DBR 16.x** for latest features
6. **Do you need the latest Delta Lake features (liquid clustering, UniForm)?** → Use **DBR 15.4 LTS** or **DBR 16.x**

### LTS vs Non-LTS

- **LTS**: Use for production workloads. Receives security patches and bug fixes for ~2 years.
- **Non-LTS**: Use for development, testing, or when you need the latest features. Supported for ~6 months.
