package com.luogh.learning.lab.leetcode;

/**
 * leetcode-28: 实现子字符串匹配
 */
public class SubStr {

  public static void main(String[] args) {
    int index = new SubStr().strStr("mississippi", "issi");
    System.out.println(index);
  }
  public int strStr(String haystack, String needle) {
    return strStr(haystack, needle, 0);
  }

  private int strStr(String haystack, String needle, int position) {
    if (haystack.length() < needle.length()) {
      return -1;
    } else if (!haystack.startsWith(needle)) {
      return strStr(haystack.substring(1), needle, position + 1);
    } else {
      return position;
    }
  }
}
