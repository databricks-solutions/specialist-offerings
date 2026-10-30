// GenericUDF: Hive uniontype -> struct<tag:tinyint, field0:T0, field1:T1, ...>.
// Bridge for the metastore read barrier (type_matrix.yaml: UNIONTYPE). Spark cannot read a Hive union column at all,
// so a Hive CTAS through this UDF writes a Spark-readable struct copy (Parquet-friendly), and Spark reads the copy.
// A view does NOT work as a bridge: Spark re-resolves the base table's column types.
//
// Build on the lab box with JDK 8 against the Hive lib directory (step 01 does this):
//   javac -cp "$HIVE_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*" -d classes UnionToStruct.java
//   jar cf union_to_struct.jar -C classes .
// Register in Hive:
//   ADD JAR <path>/union_to_struct.jar;
//   CREATE TEMPORARY FUNCTION union_to_struct AS 'UnionToStruct';
//
// Generic across arity: the output struct has one fieldN per union member, and only the active member is filled.
// Lab-validated shape (2026-09-24): {tag:0, field0:42, field1:null} read by Spark 3.5.2 and 4.1.1.
// Source of the design: hive_type_addendum.md section 3b.

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;

public class UnionToStruct extends GenericUDF {
  private transient UnionObjectInspector unionOI;
  private transient List<ObjectInspector> memberOIs;
  private transient StandardStructObjectInspector outputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1 || arguments[0].getCategory() != Category.UNION) {
      throw new UDFArgumentException("union_to_struct expects exactly one uniontype argument");
    }
    unionOI = (UnionObjectInspector) arguments[0];
    memberOIs = new ArrayList<ObjectInspector>(unionOI.getObjectInspectors());

    List<String> names = new ArrayList<String>();
    List<ObjectInspector> fields = new ArrayList<ObjectInspector>();
    names.add("tag");
    fields.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
    for (int i = 0; i < memberOIs.size(); i++) {
      names.add("field" + i);
      // Standard OI of the member type, so the output is a plain Hive struct (Parquet-writable).
      fields.add(ObjectInspectorUtils.getStandardObjectInspector(memberOIs.get(i)));
    }
    outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, fields);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object union = arguments[0].get();
    if (union == null) {
      return null;
    }
    byte tag = unionOI.getTag(union);
    Object value = unionOI.getField(union);

    List<Object> row = new ArrayList<Object>(memberOIs.size() + 1);
    row.add(Byte.valueOf(tag));
    for (int i = 0; i < memberOIs.size(); i++) {
      if (i == tag && value != null) {
        row.add(ObjectInspectorUtils.copyToStandardObject(value, memberOIs.get(i)));
      } else {
        row.add(null);
      }
    }
    return row;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "union_to_struct(" + (children.length > 0 ? children[0] : "") + ")";
  }
}
