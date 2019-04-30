package com.luogh.learning.lab.bitmap;


import java.lang.reflect.Method;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
public class BloomFilterBitmap {

  private static String logGroup = "bloom-filter-redis";
  private static String[] hashFuncs = {"rsHash", "jsHash", "pjwHash", "elfHash", "bkdrHash",
      "sdbmHash", "djdHash", "dekHash"};   //用到的hash函数
  //    private Jedis redisClient;
  private RoaringBitmap bitmap;
  private String hashKey = "BloomFilterBitmap_default";

  public BloomFilterBitmap(String key) {
    this.hashKey = String.format("bloom-filter-bitmap_%s", key);
    this.bitmap = BitmapStore.getBitmap(this.hashKey);
    Objects.requireNonNull(bitmap, "BloomFilterBitmap 使用了null bitmap");
  }

  public void cleanFilter() {
    BitmapStore.clearBitmap(this.hashKey);
  }

  // 常见哈希函数http://www.partow.net/programming/hashfunctions/index.html
  public Integer rsHash(String key) {
    Integer a = 378551;
    Integer b = 63689;
    Integer hashValue = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = hashValue * a + Integer.valueOf(key.charAt(i));
      a = a * b;
    }
    return hashValue;
  }

  public Integer jsHash(String key) {
    Integer hashValue = 1315423911;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue ^= ((hashValue << 5) + Integer.valueOf(key.charAt(i)) + (hashValue >> 2));
    }
    return hashValue;
  }

  public Integer pjwHash(String key) {
    Integer bitsSize = 4 * 8;
    Integer threeQuarters = (bitsSize * 3) / 4;
    Integer oneEight = bitsSize / 4;
    Integer highBits = 0xFFFFFFFF << bitsSize - oneEight;
    Integer hashValue = 0;
    Integer test = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = (hashValue << oneEight) + Integer.valueOf(key.charAt(i));
      test = hashValue & highBits;
    }
    if (test != 0) {
      hashValue = ((hashValue ^ (test >> threeQuarters)) & (~highBits));
    }
    return hashValue;
  }

  public Integer elfHash(String key) {
    Integer hashValue = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = (hashValue << 4) + Integer.valueOf(key.charAt(i));
      Integer x = hashValue & 0xF0000000;
      if (x != 0) {
        hashValue ^= (x >> 24);
      }
      hashValue &= ~x;
    }
    return hashValue;
  }

  public Integer bkdrHash(String key) {
    Integer seed = 131; // 31 131 1313 13131 131313 etc..
    Integer hashValue = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = (hashValue * seed) + Integer.valueOf(key.charAt(i));
    }

    return hashValue;
  }

  public Integer sdbmHash(String key) {
    Integer hashValue = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = Integer.valueOf(key.charAt(i)) + (hashValue << 6) + (hashValue << 16);
    }
    return hashValue;
  }

  public Integer djdHash(String key) {
    Integer hashValue = 5381;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = ((hashValue << 5) + hashValue) + Integer.valueOf(key.charAt(i));
    }
    return hashValue;
  }

  public Integer dekHash(String key) {
    Integer hashValue = key.length();
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = ((hashValue) << 5) ^ (hashValue >> 27) ^ Integer.valueOf(key.charAt(i));
    }
    return hashValue;
  }

  public Integer bpHash(String key) {
    Integer hashValue = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue = hashValue << 7 ^ Integer.valueOf(key.charAt(i));
    }
    return hashValue;
  }

  public Integer fnvHash(String key) {
    Integer fnvPrime = 0x811C9DC5;
    Integer hashValue = 0;
    for (Integer i = 0; i < key.length(); i++) {
      hashValue *= fnvPrime;
      hashValue ^= Integer.valueOf(key.charAt(i));
    }
    return hashValue;
  }

  public Integer apHash(String key) {
    Integer hashValue = 0xAAAAAAAA;
    for (Integer i = 0; i < key.length(); i++) {
      if ((i & 1) == 0) {
        hashValue ^= ((hashValue << 7) ^ Integer.valueOf(key.charAt(i)) * (hashValue >> 3));
      } else {
        hashValue ^= (~((hashValue << 11) + Integer.valueOf(key.charAt(i)) ^ (hashValue >> 5)));
      }
    }

    return hashValue;
  }


  public Boolean checkedAdd(String key) {
    Boolean result = false;
    Objects.requireNonNull(this.bitmap, "BloomFilterBitmap bitmap 不能为Null");
    for (String funcName : hashFuncs) {
      try {
        Method method = this.getClass().getMethod(funcName, String.class);
        Integer hash = (Integer) method.invoke(this, key);
        Integer offset = hash % (1 << 31);
        if (this.bitmap.checkedAdd(offset)) {
          result = true;
        }
      } catch (Exception e) {
        log.error("error", e);
      }
    }
    return result;
  }

  //检查是否存在，存在返回true
  public Boolean checkExists(String key) throws Exception {
    Boolean result = true;
    //check key。并做过期
    Objects.requireNonNull(this.bitmap, "BloomFilterBitmap bitmap 不能为Null");
    for (String funcName : hashFuncs) {
      try {
        Method method = this.getClass().getMethod(funcName, String.class);
        Integer hash = (Integer) method.invoke(this, key);
        Integer offset = hash % (1 << 31);
        if (!this.bitmap.contains(offset)) {
          result = false;
          break;
        }
      } catch (Exception e) {
        log.error("err", e);
      }
    }
    return result;
  }
}
