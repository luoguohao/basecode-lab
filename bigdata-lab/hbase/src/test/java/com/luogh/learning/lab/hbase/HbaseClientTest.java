package com.luogh.learning.lab.hbase;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.luogh.learning.lab.hbase.util.Utils;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class HbaseClientTest {

  private Configuration configuration;
  private ExecutorService executorService;
  private static final String TEST_PERF_SCHEMA_NAME = "perf_test_schema";
  private static final String TEST_PERF_TABLE_NAME = "perf_test_table_normal";
  private static final String TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION = "perf_test_table_normal_with_partition_key";
  private static final String READ_COLUMN_FAMILY = "r_cf";
  private static final String WRITE_COLUMN_FAMILY = "w_cf";


  @Before
  public void init() {
    configuration = HBaseConfiguration.create();
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("hbase-client-%d").build();
    executorService = Executors.newCachedThreadPool(threadFactory);
  }


  @Test
  public void testQuery() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService)) {
      try (Table table = conn.getTable(TableName.valueOf("crowd_bitmap"))) {
        FilterList filterList = new FilterList();
        Filter keyOnlyFilter = new KeyOnlyFilter();
        Filter keyPrefixFilter = new PrefixFilter(Bytes.toBytes("talking131_biz_tag"));
        Filter limitFilter = new PageFilter(100);
        filterList.addFilter(keyOnlyFilter);
        filterList.addFilter(limitFilter);
        filterList.addFilter(keyPrefixFilter);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("d"))
            .setFilter(filterList);

        try (ResultScanner resultScanner = table.getScanner(scan)) {
          for (Result result : resultScanner) {
            StringBuilder resultStr = new StringBuilder("【rowKey】").append(":")
                .append(Bytes.toString(result.getRow())).append("\n");
            CellScanner cellScanner = result.cellScanner();
            Cell cell;
            while (cellScanner.advance()) {
              cell = cellScanner.current();

              resultStr.append("【family】:").append(Bytes.toString(CellUtil.cloneFamily(cell)))
                  .append("\n")
                  .append("【qualifier】:").append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                  .append("\n")
                  .append("【value】:").append(Bytes.toString(CellUtil.cloneValue(cell)))
                  .append("\n\n");
            }
            System.out.println(resultStr.toString());
          }
        }
      }
    }
  }

  @Test
  public void testRowkeyQuery() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService)) {
      Stopwatch stopwatch = new Stopwatch();
      try (Table table = conn.getTable(TableName.valueOf("tag_bitmap"))) {
        String rowKey = Utils
            .formatRowkey("bestselleryszh930_m_attribute_ONLY/ALL/3_METAACCOUNT20171205001");
        Get getter = new Get(Bytes.toBytes(rowKey));
        getter.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bitmap"));
        Result result = table.get(getter);
        StringBuilder resultStr = new StringBuilder("【rowkey】").append(rowKey);
        CellScanner cellScanner = result.cellScanner();
        Cell cell;
        while (cellScanner.advance()) {
          cell = cellScanner.current();

          resultStr.append("【family】:").append(Bytes.toString(CellUtil.cloneFamily(cell)))
              .append("\n")
              .append("【qualifier】:").append(Bytes.toString(CellUtil.cloneQualifier(cell)))
              .append("\n")
              .append("【cell size】:").append(Utils.formatBytes(CellUtil.estimatedHeapSizeOf(cell)))
              .append("\n")
              .append("【value size】:").append(Utils.formatBytes(CellUtil.cloneValue(cell).length))
              .append("\n\n");
        }
        System.out.println("query total cost " + stopwatch.elapsedTime(TimeUnit.SECONDS) + " s");
        System.out.println(resultStr.toString());
      }
    }
  }


  @Test
  public void testRowPrefixPatternQuery() throws Exception {
    Stopwatch stopwatch = new Stopwatch();
    try (PrintWriter writer = new PrintWriter("rowkey_prefix_bitmap_size.csv",
        Charsets.UTF_8.displayName())) {
      try (Connection conn = ConnectionFactory.createConnection(configuration, executorService)) {
        try (Table table = conn.getTable(TableName.valueOf("tag_bitmap"))) {

          FilterList filterList = new FilterList();
          Filter rowkeyFilter = new PrefixFilter(Bytes.toBytes("bestselleryszh930_last_year_buy_category_attribute.+"));
//          Filter limitFilter = new PageFilter(1000);
          filterList.addFilter(rowkeyFilter);
//          filterList.addFilter(limitFilter);
          Scan scan = new Scan();
          scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bitmap"))
              .setFilter(filterList);

          try (ResultScanner resultScanner = table.getScanner(scan)) {
            for (Result result : resultScanner) {
              StringBuilder resultStr = new StringBuilder(Bytes.toString(result.getRow()))
                  .append(",");
              CellScanner cellScanner = result.cellScanner();
              Cell cell;
              while (cellScanner.advance()) {
                cell = cellScanner.current();

                resultStr.append(CellUtil.estimatedHeapSizeOf(cell)).append(",")
                    .append(CellUtil.cloneValue(cell).length);
              }
              writer.println(resultStr.toString());
            }
          }
        }
      }
      System.out.println("query total cost " + stopwatch.elapsedTime(TimeUnit.SECONDS) + " s");
      writer.flush();
    }
  }

  @Test
  public void testRowRegexPatternQuery() throws Exception {
    Stopwatch stopwatch = new Stopwatch();
    try (PrintWriter writer = new PrintWriter("rowkey_bitmap_size.csv",
        Charsets.UTF_8.displayName())) {
      try (Connection conn = ConnectionFactory.createConnection(configuration, executorService)) {
        try (Table table = conn.getTable(TableName.valueOf("tag_bitmap"))) {

          FilterList filterList = new FilterList();
          Filter rowkeyFilter = new RowFilter(CompareOp.EQUAL,
              new RegexStringComparator("talking131_.+_attribute_.+"));
//          Filter limitFilter = new PageFilter(1000);
          filterList.addFilter(rowkeyFilter);
//          filterList.addFilter(limitFilter);
          Scan scan = new Scan();
          scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bitmap"))
              .setFilter(filterList);

          try (ResultScanner resultScanner = table.getScanner(scan)) {
            for (Result result : resultScanner) {
              StringBuilder resultStr = new StringBuilder(Bytes.toString(result.getRow()))
                  .append(",");
              CellScanner cellScanner = result.cellScanner();
              Cell cell;
              while (cellScanner.advance()) {
                cell = cellScanner.current();

                resultStr.append(CellUtil.estimatedHeapSizeOf(cell)).append(",")
                    .append(CellUtil.cloneValue(cell).length);
              }
              writer.println(resultStr.toString());
            }
          }
        }
      }
      System.out.println("query total cost " + stopwatch.elapsedTime(TimeUnit.SECONDS) + " s");
      writer.flush();
    }
  }


  @Test
  public void createTableTest() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Admin admin = conn.getAdmin()) {
//      admin.createNamespace(NamespaceDescriptor.create(TEST_PERF_SCHEMA_NAME).build());
      if (!admin.tableExists(TableName.valueOf(TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME))) {
        HTableDescriptor descriptor = new HTableDescriptor(
            TableName.valueOf(TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME));
        descriptor
            .addFamily(new HColumnDescriptor(READ_COLUMN_FAMILY));  // read column family
        descriptor
            .addFamily(new HColumnDescriptor(WRITE_COLUMN_FAMILY));  // read column family
        admin.createTable(descriptor);
        log.info("table not exist, create new table {}.{} with two column {}, {}",
            new String[]{TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME, READ_COLUMN_FAMILY, WRITE_COLUMN_FAMILY});
      }
    }
  }

  @Test
  public void createTableWithPartitionKeyTest() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Admin admin = conn.getAdmin()) {
//      admin.createNamespace(NamespaceDescriptor.create(TEST_PERF_SCHEMA_NAME).build());
      if (!admin.tableExists(TableName.valueOf(TEST_PERF_SCHEMA_NAME,
          TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION))) {
        HTableDescriptor descriptor = new HTableDescriptor(
            TableName.valueOf(TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION));
        descriptor
            .addFamily(new HColumnDescriptor(READ_COLUMN_FAMILY));  // read column family
        descriptor
            .addFamily(new HColumnDescriptor(WRITE_COLUMN_FAMILY));  // read column family
        admin.createTable(descriptor);
        log.info("table not exist, create new table {}.{} with two column {}, {}",
            new String[]{TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME, READ_COLUMN_FAMILY, WRITE_COLUMN_FAMILY});
      }
    }
  }
}
