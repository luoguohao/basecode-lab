package com.luogh.learning.lab.leetcode;

/**
 * leetcode-9: 回文数
 */
public class PalindromicNumber {

  public static void main(String[] args) {
    System.out.println(new PalindromicNumber().isPalindrome(121));
  }

  public boolean isPalindrome(int x) {
    if (x < 0) {
      return false;
    }

    int origin = x;
    int num = 0;
    while (x != 0) {
      int digit = x % 10;
      x /= 10;
      if ((Integer.MAX_VALUE - digit) / 10 < num) {
        return false;
      } else {
        num = num * 10 + digit;
      }
    }

    return num == origin;
  }
}
