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
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.LoadBalancerProtos;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
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
   * Hbase预分区
   * @throws Exception
   */
  @Test
  public void createTableWithPartitionKeyTest1() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Admin admin = conn.getAdmin()) {
//      admin.createNamespace(NamespaceDescriptor.create(TEST_PERF_SCHEMA_NAME).build());
      TableName tableName = TableName.valueOf("wiki-event");
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }

      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor
          .addFamily(new HColumnDescriptor("default"));
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


  @Test
  public void testClusterState() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(configuration, executorService);
        Admin admin = conn.getAdmin()) {
      ClusterStatus status = admin.getClusterStatus();
      boolean isBalanceOn = status.isBalancerOn();
      for ( ServerName serverName : status.getServers()) {
        ServerLoad load = status.getLoad(serverName);
        System.out.println("server name:" + serverName + " with load:" + load.getLoad());
      }
      System.out.println("status:" + status);
    }
  }


  @Test
  public void testReadZkNode() throws Exception {
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    ZooKeeperWatcher zooKeeper = new ZooKeeperWatcher(HBaseConfiguration.create(), "zk-node-reader", abortable, false);
    byte[] data = ZKUtil.getData(zooKeeper, "/hbase/balancer");
    LoadBalancerProtos.LoadBalancerState state = parseFrom(data);
    System.out.println(state.getBalancerOn());
  }


  @Test
  public void testReadMetaRegionZkNode() throws Exception {
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    ZooKeeperWatcher zooKeeper = new ZooKeeperWatcher(HBaseConfiguration.create(), "zk-node-reader", abortable, false);
//    byte[] data = ZKUtil.getData(zooKeeper, "/hbase/master");
    byte[] data = ZKUtil.getData(zooKeeper, "/hbase/rs/sz-pg-smce-devhadoop-009.tendcloud.com,16020,1530707987453");
    System.out.println(ServerName.parseFrom(data).toString());
  }


  @Test
  public void testListZkNodeWithWatcher() throws Exception {
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    ZooKeeperWatcher zooKeeper = new ZooKeeperWatcher(HBaseConfiguration.create(), "zk-node-reader", abortable, false);
    zooKeeper.registerListenerFirst(new ZooKeeperListener(zooKeeper) {
      @Override
      public void nodeCreated(String path) {
        try{
          log.info("nodeCreated path:{}", path);
          ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(path));
          log.info("nodeCreated servername:{}", sn);
        } catch (Exception e) {
          log.error("with error:{}", e);
        }
      }

      @Override
      public void nodeDeleted(String path) {
        try {
          log.info("nodeDeleted path:{}", path);
          ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(path));
          log.info("nodeDeleted servername:{}", sn);
        } catch (Exception e) {
          log.error("with error:{}", e);
        }
      }

      @Override
      public void nodeDataChanged(String path) {
        try{
          log.info("nodeDataChanged path:{}", path);
          ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(path));
          log.info("nodeDataChanged servername:{}", sn);
        } catch (Exception e) {
          log.error("with error:{}", e);
        }
      }

      @Override
      public void nodeChildrenChanged(String path) {
       log.info("node childrenChanged:{}", path);
        try {
          ZKUtil.listChildrenAndWatchThem(zooKeeper, path);
        } catch (KeeperException e) {
          e.printStackTrace();
        }
      }
    });
    ZKUtil.listChildrenAndWatchThem(zooKeeper, "/hbase/rs");
    Thread.currentThread().join();
  }

  @Test
  public void testListZkNode() throws Exception {
    Abortable abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    ZooKeeperWatcher zooKeeper = new ZooKeeperWatcher(HBaseConfiguration.create(), "zk-node-reader", abortable, false);
    List<String> servers = ZKUtil.listChildrenNoWatch(zooKeeper, "/hbase/rs");
    for (String n: servers) {
      ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(n));
      log.info("servername:{}", sn);
      RegionServerInfo.Builder rsInfoBuilder = RegionServerInfo.newBuilder();
      try {
        String nodePath = ZKUtil.joinZNode("/hbase/rs", n);
        byte[] data = ZKUtil.getData(zooKeeper, nodePath);
        if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
          int magicLen = ProtobufUtil.lengthOfPBMagic();
          ProtobufUtil.mergeFrom(rsInfoBuilder, data, magicLen, data.length - magicLen);
        }
        log.debug("Added tracking of RS " + nodePath);
      } catch (KeeperException e) {
        log.warn("Get Rs info port from ephemeral node", e);
      } catch (IOException e) {
        log.warn("Illegal data from ephemeral node", e);
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
      log.info("resinfo:{}", rsInfoBuilder.build());
    }
  }


  private LoadBalancerProtos.LoadBalancerState parseFrom(byte [] pbBytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(pbBytes);
    LoadBalancerProtos.LoadBalancerState.Builder builder =
        LoadBalancerProtos.LoadBalancerState.newBuilder();
    try {
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ProtobufUtil.mergeFrom(builder, pbBytes, magicLen, pbBytes.length - magicLen);
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return builder.build();
  }


  @Test
  public void testDNS() throws Exception {
    String[] hosts = org.apache.hadoop.net.DNS.getIPs("en0");
    System.out.println("--> " + Arrays.toString(hosts));

    String host = DNS.getDefaultHost("en0", "172.20.0.1");
    InetSocketAddress initialIsa = new InetSocketAddress(host, 1012);
    System.out.println(initialIsa.getHostName());
  }
}
