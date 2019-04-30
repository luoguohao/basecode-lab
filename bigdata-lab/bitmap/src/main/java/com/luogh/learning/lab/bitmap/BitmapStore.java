package com.luogh.learning.lab.bitmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
public class BitmapStore {

  private static String localPath = "/tmp";
  private static Map<String, RoaringBitmap> bitmapStore = new ConcurrentHashMap<>();
  private static String logGroup = "bitmap-store";
  private static Long lastSaveTime = 0l;

  public static RoaringBitmap getBitmap(String key) {
    if (bitmapStore.get(key) == null) {
      log.info("BloomFilterBitmap for {} create", key);
      String filePath = String.format("%s/push_bitmap/%s", localPath, key);
      RoaringBitmap bitmap = new RoaringBitmap();
      File file = new File(filePath);
      if (file.exists()) {
        log.info("load bitmap file file {}", filePath);
        try {
          FileInputStream in = new FileInputStream(file);
          bitmap.deserialize(new DataInputStream(in));
          in.close();
        } catch (IOException e) {
          bitmap.clear();
          log.error("getBitmap err", e);
        }
      }

      bitmapStore.put(key, bitmap);
    }
    return bitmapStore.get(key);
  }

  private static Boolean saveBitmapToFile(String key) {
    Boolean result = false;
    RoaringBitmap bitmap = bitmapStore.get(key);
    if (bitmap != null) {
      try {
        log.info("save {} bitmap", key);
        String filePath = String.format("%s/push_bitmap/%s", localPath, key);
        File file = new File(filePath);
        File directory = new File(file.getParent());
        if (!directory.exists()) {
          directory.mkdir();
        }
        if (!file.exists()) {
          file.createNewFile();
        }
        FileOutputStream out = new FileOutputStream(file);

        bitmap.serialize(new DataOutputStream(out));
        result = true;
        out.close();
      } catch (IOException e) {
        log.error("saveBitmapToFile err {}", e);
      }
    }
    return result;
  }

  public static Boolean storeAllToFile() {
    return BitmapStore.storeAllToFile(false);
  }

  public static Boolean storeAllToFile(Boolean force) {
    Long currentTime = System.currentTimeMillis();
    if (currentTime - lastSaveTime > 1000 * 60 * 30 || force) {
      //小于半个小时，不保存
      log.info("storeAllToFile");
      lastSaveTime = currentTime;
      for (String key : bitmapStore.keySet()) {
        saveBitmapToFile(key);
      }
      //清除下内存。 每次都清理，可以防止没有用的数据被重复写入。写入必将比读更加消耗资源
      for (String key : bitmapStore.keySet()) {
        RoaringBitmap bitmap = bitmapStore.get(key);
        bitmap.clear();
      }
      bitmapStore.clear();
      return true;
    }
    return false;

  }

  public static void clearBitmap(String key) {
    RoaringBitmap bitmap = bitmapStore.get(key);
    if (bitmap != null) {
      bitmap.clear();
      bitmapStore.remove(key);
    }
    String filePath = String.format("%s/push_bitmap/%s", localPath, key);
    File file = new File(filePath);
    try {
      if (file.exists()) {
        file.delete();
      } else {
        log.info("bitmap for key:{} not exists", key);
      }
    } catch (Exception e) {
      log.error("clear bitmap error for key {} exception is {}", key,
          e);
    }
    file = null;

  }
}
