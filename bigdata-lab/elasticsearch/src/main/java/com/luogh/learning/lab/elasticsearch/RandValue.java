package com.luogh.learning.lab.elasticsearch;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class RandValue {

  private final ThreadLocalRandom random = ThreadLocalRandom.current();

  public static void main(String[] args) {
    RandValue randValue = new RandValue();
    int minDimension = 1;
    int maxDimension = 1000;
    IntStream.range(0, 1000).parallel().forEach(id -> {
      int a = Math.min(minDimension +  (maxDimension - minDimension) / 500, maxDimension); // 大部分是稀疏的数据
      int b = 500;
      long currentDimension = Math.abs(Math.round(Math.sqrt(b) * randValue.random.nextGaussian() + a)); // 均值为a，方差为b的随机数
      System.out.println("mean: " + a + " -> " + currentDimension);
    });

  }

}
