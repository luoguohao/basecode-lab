package com.luogh.learning.lab.leetcode;

/**
 * leetcode-38: 报数
 */
public class CountAndSay {

  public static void main(String[] args) {
    String result = new CountAndSay().countAndSay(10);
    System.out.println("result:" + result);
  }

  public String countAndSay(int n) {
    String str = "1";
    for (int i = 0; i < n - 1; i++) {
      char prev = 0;
      int cnt = 0;
      StringBuilder newStr = new StringBuilder();
      for (int j = 0; j < str.length(); j++) {
        if (prev == 0) {
          prev = str.charAt(j);
          cnt++;
        } else if (prev == str.charAt(j)) {
          cnt++;
        } else {
          newStr.append(cnt).append(prev);
          prev = str.charAt(j);
          cnt = 1;
        }

        if (j == str.length() - 1) {
          newStr.append(cnt).append(prev);
        }
      }
      str = newStr.toString();
    }

    return str;
  }
}
