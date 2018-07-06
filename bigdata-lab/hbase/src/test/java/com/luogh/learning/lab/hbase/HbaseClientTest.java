package com.luogh.learning.lab.hbase;

import static com.luogh.learning.lab.hbase.constant.HbaseConstants.EXECUTOR_POOL_SIZE;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.luogh.learning.lab.hbase.constant.HbaseConstants;
import com.luogh.learning.lab.hbase.util.MD5Utils;
import com.luogh.learning.lab.hbase.util.Utils;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
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
  private static final String TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION = "perf_test_table_normal_with_partition_key_copy";
  private static final String READ_COLUMN_FAMILY = "r_cf";
  private static final String WRITE_COLUMN_FAMILY = "w_cf";


  @Before
  public void init() {
    configuration = HBaseConfiguration.create();
    configuration.setInt("hbase.client.scanner.timeout.period", 6000000);
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
      Stopwatch stopwatch = Stopwatch.createStarted();
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
        System.out.println("query total cost " + stopwatch.elapsed(TimeUnit.SECONDS) + " s");
        System.out.println(resultStr.toString());
      }
    }
  }


  @Test
  public void testRowPrefixPatternQuery() throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try (PrintWriter writer = new PrintWriter("rowkey_prefix_bitmap_size.csv",
        Charsets.UTF_8.displayName())) {
      try (Connection conn = ConnectionFactory.createConnection(configuration, executorService)) {
        try (Table table = conn.getTable(TableName.valueOf("tag_bitmap"))) {

          FilterList filterList = new FilterList();
          Filter rowkeyFilter = new PrefixFilter(
              Bytes.toBytes("bestselleryszh930_last_year_buy_category_attribute.+"));
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
      System.out.println("query total cost " + stopwatch.elapsed(TimeUnit.SECONDS) + " s");
      writer.flush();
    }
  }

  @Test
  public void testRowRegexPatternQuery() throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
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
      System.out
          .println("query total cost " + stopwatch.stop().elapsed(TimeUnit.SECONDS) + " s");
      writer.flush();
    }
  }


  @Test
  public void testPut() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Random rand = new Random();

    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService)) {
      /** a callback invoked when an asynchronous write fails. */
      final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
        for (int i = 0; i < e.getNumExceptions(); i++) {
          log.info("Failed to sent put " + e.getRow(i) + ".");
        }
      };

      BufferedMutatorParams params = new BufferedMutatorParams(
          TableName.valueOf(TEST_PERF_SCHEMA_NAME,
              TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION))
          .writeBufferSize(4 * 1024 * 1024)
          .listener(listener);

      try (final BufferedMutator mutator = conn.getBufferedMutator(params)) {
        /** worker pool that operates on BufferedTable instances */
        final ExecutorService workerPool = Executors.newFixedThreadPool(EXECUTOR_POOL_SIZE);
        List<Future<Void>> futures = new ArrayList<>(100000);

        for (int i = 0; i < 1000000; i++) {
          futures.add(workerPool.submit(() -> {
            //
            // step 2: each worker sends edits to the shared BufferedMutator instance. They all use
            // the same backing buffer, call-back "listener", and RPC executor pool.
            //
            Put put = new Put(Bytes.toBytes(MD5Utils.encodeByMD5(UUID.randomUUID().toString())));
            put.addColumn(Bytes.toBytes(WRITE_COLUMN_FAMILY),
                Bytes.toBytes(HbaseConstants.COLUMN_NAME), Bytes.toBytes(
                    UUID.randomUUID().toString()));
            put.addColumn(Bytes.toBytes(WRITE_COLUMN_FAMILY),
                Bytes.toBytes(HbaseConstants.COLUMN_NAME), Bytes.toBytes(
                    UUID.randomUUID().toString()));

            mutator.mutate(put);
            // do work... maybe you want to call mutator.flush() after many edits to ensure any of
            // this worker's edits are sent before exiting the Callable
            return null;
          }));
        }

        //
        // step 3: clean up the worker pool, shut down.
        //
        for (Future<Void> f : futures) {
          f.get(5, TimeUnit.MINUTES);
        }
        workerPool.shutdown();
      }
    } catch (Exception e) {
      // exception while creating/destroying Connection or BufferedMutator
      log.info("exception while creating/destroying Connection or BufferedMutator", e);
    } // BufferedMutator.close() ensures all work is flushed. Could be the custom listener is
    // invoked from here.
    System.out.println("query total cost " + stopwatch.stop().elapsed(TimeUnit.SECONDS) + " s");
  }


  @Test
  public void testBatchQuery() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Random rand = new Random();
    int matchCount = 0;
    try (
        Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Table table = conn.getTable(TableName.valueOf(TEST_PERF_SCHEMA_NAME,
            TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION))
    ) {
//      table.setOperationTimeout((int) TimeUnit.MINUTES.toMillis(5));
      List<Get> gets = Lists.newArrayListWithCapacity(100000);
      for (int i = 0; i < 100000; i++) {
        Get get = new Get(Bytes
            .toBytes(MD5Utils.encodeByMD5(String.format("%16x", rand.nextInt(Integer.MAX_VALUE)))));
        get.setCacheBlocks(true);
        gets.add(get);
      }
      Cell cell;
      CellScanner cellScanner;
      for (Result re : table.get(gets)) {
        cellScanner = re.cellScanner();
        while (cellScanner.advance()) {
          matchCount++;
          cell = cellScanner.current();
          System.out.println(
              "cell name:" + new String(CellUtil.cloneQualifier(cell)) + ", cell value:"
                  + new String(CellUtil.cloneValue(cell)));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(
        "query total cost " + stopwatch.stop().elapsed(TimeUnit.SECONDS) + " s, match count:"
            + matchCount);
  }


  @Test
  public void testBlockCache() {

    int matchCount = 0;
    try (
        Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Table table = conn.getTable(TableName.valueOf(TEST_PERF_SCHEMA_NAME,
            TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION))
    ) {
      Scan scan = new Scan();
//      scan.setConsistency(Consistency.TIMELINE);
      scan.setStartRow(Bytes.toBytes("33333333333333333333333333322220"));
      scan.setStopRow(Bytes.toBytes("3333333333333333333333333333333F"));
      scan.setMaxResultSize(100);
      scan.setCaching(1000);
      scan.setBatch(10000);
      scan.setFilter(new KeyOnlyFilter());
      scan.addFamily(Bytes.toBytes(HbaseConstants.WRITE_COLUMN_FAMILY));

      List<Get> gets = Lists.newArrayList();
      int resultCnt = 0;
      for (Result result : table.getScanner(scan)) {
        resultCnt++;
        Get get = new Get(result.getRow());
        get.setCacheBlocks(true);
        gets.add(get);
        if (resultCnt % 1000 == 0) {
          System.out.println("bucket with count : " + resultCnt);
        }
      }

      System.out.println("scan result count:" + resultCnt);

      int tryTime = 10;
      for (int i = 0; i < tryTime; i++) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Cell cell;
        CellScanner cellScanner;
        for (Result re : table.get(gets)) {
          cellScanner = re.cellScanner();
          while (cellScanner.advance()) {
            matchCount++;
            cell = cellScanner.current();
          }
        }
        System.out.println("query total cost " + stopwatch.stop().elapsed(TimeUnit.SECONDS)
            + " s, match count:" + matchCount);
        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Test
  public void testMultiQuery() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Random rand = new Random();
    int matchCount = 0;
    try (
        Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Table table = conn.getTable(TableName.valueOf(TEST_PERF_SCHEMA_NAME,
            TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION))
    ) {
//      table.setOperationTimeout((int) TimeUnit.MINUTES.toMillis(5));
      for (int i = 0; i < 10000000; i++) {
        Get get = new Get(Bytes
            .toBytes(MD5Utils.encodeByMD5(String.format("%16x", rand.nextInt(Integer.MAX_VALUE)))));
        get.setCacheBlocks(true);
        Cell cell;
        CellScanner cellScanner;
        Result re = table.get(get);
        cellScanner = re.cellScanner();
        while (cellScanner.advance()) {
          matchCount++;
          cell = cellScanner.current();
          System.out.println(
              "cell name:" + new String(CellUtil.cloneQualifier(cell)) + ", cell value:"
                  + new String(CellUtil.cloneValue(cell)));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println(
        "query total cost " + stopwatch.stop().elapsed(TimeUnit.SECONDS) + " s, match count:"
            + matchCount);
  } // 结果很慢
}
