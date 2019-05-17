package com.luogh.learning.lab.hive;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class HiveConnector {

  public static void main(String[] args) throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set("hive.metastore.uris", "thrift://172.23.7.138:9083");
    Table table = Hive.get(hiveConf).getTable("cdp", "c24_order");

    System.out.println("input format : " + table.getTTable().getSd().getInputFormat());
    System.out.println("location : " + table.getTTable().getSd().getLocation());
    System.out.println("output format : " + table.getTTable().getSd().getOutputFormat());
    System.out.println("skewedColNames : " + table.getTTable().getSd().getSkewedInfo().isSetSkewedColNames());
    System.out.println("skewedColNames : " + String
        .join(",", table.getTTable().getSd().getSkewedInfo().getSkewedColNames()));

    List<FieldSchema> cols = table.getTTable().getSd().getCols();

    for (FieldSchema col : cols) {
      System.out.println("colName:" + col.getName() + ", colType:" + col.getType());
    }

    List<StructField> fields = table.getFields();
    for (StructField field : fields) {
      System.out.println("fieldName:" + field.getFieldName() + ", fieldId:" + field.getFieldID()
          + ", fieldTypeCatalog:" + field.getFieldObjectInspector().getCategory().name()
          + ", field:" + field.getFieldObjectInspector().getTypeName());
    }

    System.out.println(table.getSkewedColNames().stream().collect(Collectors.joining(",")));
  }

}
