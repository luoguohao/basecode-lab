package com.luogh.learning.lab.leetcode;

public class Utils {


  public static void printArray(int[] array) {
    StringBuilder stringBuilder = new StringBuilder("array result:[");
    for (int a : array) {
      stringBuilder.append(a).append(",");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1).append("]");
    System.out.println(stringBuilder.toString());
  }
}
