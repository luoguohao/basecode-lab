package com.luogh.learning.lab.hbase;

import org.apache.hadoop.hbase.util.CompressionTest;

public class CompressionClient {

  public static void main(String[] args) throws Exception {
    System.getenv().forEach((k , v ) -> System.out.println("k:" + k + ", v:" + v));
    args = new String[]{"/Users/luogh/Code_Repository/talkingdata/logs/dmp-web.log", "snappy"};
    new CompressionTest().main(args);
  }
}
