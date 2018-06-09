package com.luogh.learning.lab.hbase;

import static com.luogh.learning.lab.hbase.constant.HbaseConstants.HIGHEST_MD5_KEY;
import static com.luogh.learning.lab.hbase.constant.HbaseConstants.LOWEST_MD5_KEY;
import static com.luogh.learning.lab.hbase.constant.HbaseConstants.READ_COLUMN_FAMILY;
import static com.luogh.learning.lab.hbase.constant.HbaseConstants.TEST_PERF_SCHEMA_NAME;
import static com.luogh.learning.lab.hbase.constant.HbaseConstants.TEST_PERF_TABLE_NAME;
import static com.luogh.learning.lab.hbase.constant.HbaseConstants.TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION;
import static com.luogh.learning.lab.hbase.constant.HbaseConstants.WRITE_COLUMN_FAMILY;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.luogh.learning.lab.hbase.util.MD5Utils;
import java.math.BigInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class HbaseAdminTest {

  private Configuration configuration;
  private ExecutorService executorService;


  @Before
  public void init() {
    configuration = HBaseConfiguration.create();
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("hbase-client-%d").build();
    executorService = Executors.newCachedThreadPool(threadFactory);
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

  /**
   * Hbase预分区
   * @throws Exception
   */
  @Test
  public void createTableWithPartitionKeyTest() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Admin admin = conn.getAdmin()) {
//      admin.createNamespace(NamespaceDescriptor.create(TEST_PERF_SCHEMA_NAME).build());
      TableName tableName = TableName.valueOf(TEST_PERF_SCHEMA_NAME,
          TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION);
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }

      HTableDescriptor descriptor = new HTableDescriptor(
          TableName.valueOf(TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION)
      );
      descriptor
          .addFamily(new HColumnDescriptor(READ_COLUMN_FAMILY));  // read column family
      descriptor
          .addFamily(new HColumnDescriptor(WRITE_COLUMN_FAMILY));  // read column family
      admin.createTable(descriptor,  getHexSplits(LOWEST_MD5_KEY, HIGHEST_MD5_KEY, 10));
      log.info("table not exist, create new table {}.{} with two column {}, {}",
          new String[]{TEST_PERF_SCHEMA_NAME, TEST_PERF_TABLE_NAME, READ_COLUMN_FAMILY, WRITE_COLUMN_FAMILY});
    }
  }


  /**
   * rowkey进行md5加密后取大写，生成32位的字符
   * region split key 根据region个数来将md5的值平均划分
   * @param startKey 有效rowkey的最小可能值（此处默认为32位md5的最小值：00000000000000000000000000000000）
   * @param endKey   有效rowkey的最大可能值（此处默认为32位md5的最小值：FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF）
   * @param numRegions
   * @return
   */
  private static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
    byte[][] splits = new byte[numRegions - 1][];
    BigInteger lowestKey = new BigInteger(startKey, 16);
    BigInteger highestKey = new BigInteger(endKey, 16);
    BigInteger range = highestKey.subtract(lowestKey);
    BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
    lowestKey = lowestKey.add(regionIncrement);
    for (int i = 0; i < numRegions - 1; i++) {
      BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
      String splitKey = String.format("%16x", key).toUpperCase();
      System.out.println(splitKey);
      splits[i] = splitKey.getBytes();
    }
    return splits;
  }

  @Test
  public void testHexSplitKey() {
    System.out.println(MD5Utils.encodeByMD5("test"));
    getHexSplits("00000000000000000000000000000000", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 3);
  }
}
