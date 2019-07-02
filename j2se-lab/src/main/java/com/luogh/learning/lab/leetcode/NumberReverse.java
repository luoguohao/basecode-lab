package com.luogh.learning.lab.leetcode;

/**
 * leetcode-7: 数字反转
 */
public class NumberReverse {

  public static void main(String[] args) {
    System.out.println(new NumberReverse().reverse(-901000));
  }

  public int reverse(int x) {
    int reversedNumber = 0;

    while (x != 0) {
      int digit = x % 10;
      x /= 10;

      if (reversedNumber > 0 && reversedNumber > (Integer.MAX_VALUE - digit) / 10) {
        return 0;
      } else if (reversedNumber < 0 && reversedNumber < (Integer.MIN_VALUE - digit) / 10) {
        return 0;
      } else {
        reversedNumber = reversedNumber * 10 + digit;
      }
    }

    return reversedNumber;
  }
}
