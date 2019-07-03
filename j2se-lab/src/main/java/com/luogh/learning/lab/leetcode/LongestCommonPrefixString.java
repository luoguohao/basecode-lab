package com.luogh.learning.lab.leetcode;

/**
 * leetcode-14: 最长公共前缀
 */
public class LongestCommonPrefixString {

  public String longestCommonPrefix(String[] strs) {
    if (strs.length == 0) {
      return "";
    }
    String prefix = "";

    for (int i = 0; ; i++) {
      Character temp = null;
      for (String str : strs) {
        if (i < str.length()) {
          if (temp == null) {
            temp = str.charAt(i);
          } else {
            if (temp != str.charAt(i)) {
              return prefix;
            }
          }
        } else {
          return prefix;
        }
      }
      prefix += temp;
    }

  }
}
