package com.luogh.learning.lab.bitmap;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;

public class BloomFilterBitmapTest {

  public static void main(String[] args) throws Exception {

    BitmapStore.clearBitmap("123");
    BloomFilterBitmap bloomFilterBitmap = new BloomFilterBitmap("123");
    List<String> keyList = new ArrayList<>();
    Integer count = 0;
    for (int i = 0; i < 10000000; i++) {
      double seed = Math.random();
      String key = DigestUtils.sha256Hex(String.valueOf(seed));
      keyList.add(key);
      boolean result = bloomFilterBitmap.checkExists(key);
      if (result) {
        System.out.println(String.format("first check key = %s ,result = %b", key, result));
      }
      bloomFilterBitmap.checkedAdd(key);
    }
    BitmapStore.storeAllToFile(true);
    bloomFilterBitmap = new BloomFilterBitmap("123");
    System.out.println("check ...");
//    for (String key : keyList) {
//      boolean result = bloomFilterBitmap.checkExists(key);
//      if (!result) {
//        count++;
//      }
//    }
//    System.out.println(String.format(" double check result false  count is %d", count));
  }
}
