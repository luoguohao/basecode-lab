package com.luogh.learning.lab.leetcode;

import java.util.List;

public class Utils {


  public static void printArray(int[] array) {
    StringBuilder stringBuilder = new StringBuilder("array result:[");
    for (int a : array) {
      stringBuilder.append(a).append(",");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1).append("]");
    System.out.println(stringBuilder.toString());
  }

  public static void printArray(char[][] arrays) {
    StringBuilder stringBuilder = new StringBuilder("array result:");
    stringBuilder.append("\n[\n");
    for (char[] array : arrays) {
      stringBuilder.append("[");
      for (char c : array) {
        stringBuilder.append(c).append(",");
      }
      stringBuilder.deleteCharAt(stringBuilder.length() - 1).append("]").append(",").append("\n");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 2).append("]");
    System.out.println(stringBuilder.toString());
  }

  public static <T> void printList(List<List<T>> arrays) {
    StringBuilder stringBuilder = new StringBuilder("array result:");
    stringBuilder.append("\n[\n");
    for (List<T> array : arrays) {
      stringBuilder.append("[");
      for (T c : array) {
        stringBuilder.append(c).append(",");
      }
      stringBuilder.deleteCharAt(stringBuilder.length() - 1).append("]").append(",").append("\n");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 2).append("]");
    System.out.println(stringBuilder.toString());
  }
}
