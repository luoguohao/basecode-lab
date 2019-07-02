package com.luogh.learning.lab.leetcode;

/**
 * leetcode-8: 字符串转换整数 (atoi)
 */
public class StringToNumber {

  public static void main(String[] args) {
    System.out.println(new StringToNumber().myAtoi("   +0 123"));
  }

  public int myAtoi(String str) {
    int genNum = 0;
    boolean inNumberProgress = false;
    int flag = 1;

    for (int i = 0; i < str.length(); i++) {
      char charAt = str.charAt(i);
      if (charAt == 32) {

        if (inNumberProgress) {
          break; // 如果已经在存在数字了，依然遇到空格，则需要退出了
        } else {
          continue; // 空格 过滤
        }
      }

      if (charAt >= 48 && charAt <= 57) { // 48～57为0到9十个阿拉伯数字
        inNumberProgress = true;
        int number = charAt - 48;
        if (flag == 1 && (Integer.MAX_VALUE - number) / 10 < genNum) {
          return Integer.MAX_VALUE;
        } else if (flag == -1 && (Integer.MIN_VALUE + number) / (flag * 10) < genNum) {
          return Integer.MIN_VALUE;
        } else {
          genNum = genNum * 10 + number;
        }
      } else if (charAt == '-' || charAt == '+') {
        if (!inNumberProgress) {
          flag = charAt == '-' ? -1 : 1;
          inNumberProgress = true;
        } else {
          break;
        }
      } else {
        break; // 其他非数字
      }
    }

    return genNum * flag;
  }
}
