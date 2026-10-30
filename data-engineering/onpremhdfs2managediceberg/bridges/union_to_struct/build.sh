#!/usr/bin/env bash
# Build union_to_struct.jar with the Hive / Hadoop jars of the box on the classpath (JDK 8 for Hive 3.1.3).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${HIVE_HOME:?set HIVE_HOME}" "${HADOOP_HOME:?set HADOOP_HOME}"
JAVAC="${JAVA8_HOME:+$JAVA8_HOME/bin/}javac"
OUT="$HERE/out"; rm -rf "$OUT"; mkdir -p "$OUT/classes"
"$JAVAC" -cp "$HIVE_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*" -d "$OUT/classes" "$HERE/UnionToStruct.java"
jar cf "$OUT/union_to_struct.jar" -C "$OUT/classes" .
echo "built $OUT/union_to_struct.jar"
