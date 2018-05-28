package com.luogh.learning.lab.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class DmlClient {

  public static void main(String[] args) throws Exception {
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("hbase-client-%d").build();
    ExecutorService executorService = Executors.newCachedThreadPool(threadFactory);
    try (Connection conn = ConnectionFactory.createConnection(getConf(), executorService)) {
//      scanTable(conn);
      tableMetaData(conn, "people");
    }
  }


  private static Configuration getConf() {
    return HBaseConfiguration.create();
  }


  private static void tableMetaData(Connection conn, String tableName) throws Exception {
    try (Table hTable = conn.getTable(TableName.valueOf(tableName))) {
      HTableDescriptor descriptor = hTable.getTableDescriptor();
      HColumnDescriptor[] descriptors = descriptor.getColumnFamilies();
      System.out.println("============== column family info ==============");
      for (HColumnDescriptor desc : descriptors) {
        String info = new StringBuilder()
            .append("column family name:").append(Bytes.toString(desc.getName())).append("\n")
            .append("block size:").append(desc.getBlocksize()).append("\n")
            .append("bloom filter type:").append(desc.getBloomFilterType()).append("\n")
            .append("compaction compression:").append(desc.getCompactionCompression().getName()).append("\n")
            .append("compaction compression type:").append(desc.getCompactionCompressionType()).append("\n")
            .append("compression:").append(desc.getCompression().getName()).append("\n")
            .append("family max version:").append(desc.getMaxVersions()).append("\n")
            .toString();
        System.out.println(info);
      }
    }
  }

  private static void scanTable(Connection conn) throws Exception {
    try (Table hTable = conn.getTable(TableName.valueOf("people"))) {
      Scan scan = new Scan()
//          .addFamily(Bytes.toBytes("info"))
//          .setFilter(new PageFilter(2))
          .setBatch(5)
//          .addFamily(Bytes.toBytes("data"))
//          .setRowPrefixFilter(Bytes.toBytes("rk8"))
          .setFilter(new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info"))))
//          .setMaxResultSize(1)
//          .setReversed(true)
//          .setFilter(new KeyOnlyFilter())
          .setCacheBlocks(false);
      try (ResultScanner resultScanner = hTable.getScanner(scan)) {
        for (Result result : resultScanner) {
          Cell cell;
          String info;
          CellScanner cellscanner = result.cellScanner();
          while (cellscanner.advance()) {
            cell = cellscanner.current();
            info = new StringBuilder().append("family:")
                .append(Bytes.toString(CellUtil.cloneFamily(cell))).append("\n")
                .append("qualifier:").append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                .append("\n")
                .append("rowkey:").append(Bytes.toString(CellUtil.cloneRow(cell))).append("\n")
                .append("value:").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n")
                .append("timestamp:").append(cell.getTimestamp()).append("\n")
                .append("version:").append(cell.getSequenceId()).append("\n")
                .toString();
            System.out.println(info);
          }
        }
      }

    }
  }
}
