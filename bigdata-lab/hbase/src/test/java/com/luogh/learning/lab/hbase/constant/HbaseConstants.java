package com.luogh.learning.lab.hbase.constant;

public interface HbaseConstants {

  String TEST_PERF_SCHEMA_NAME = "perf_test_schema";
  String TEST_PERF_TABLE_NAME = "perf_test_table_normal";
  String TEST_PERF_TABLE_NAME_WITH_PRE_PARTITION = "perf_test_table_normal_with_partition_key";
  String READ_COLUMN_FAMILY = "r_cf";
  String WRITE_COLUMN_FAMILY = "w_cf";
  String COLUMN_NAME = "value";
  String LOWEST_MD5_KEY = "00000000000000000000000000000000";
  String HIGHEST_MD5_KEY = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

  int EXECUTOR_POOL_SIZE = 100;
}
