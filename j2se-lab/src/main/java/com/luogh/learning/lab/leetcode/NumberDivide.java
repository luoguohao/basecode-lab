package com.luogh.learning.lab.leetcode;

/**
 * leetcode-29:  两数相除
 *
 * 给定两个整数，被除数 dividend 和除数 divisor。将两数相除，要求不使用乘法、除法和 mod 运算符。 返回被除数 dividend 除以除数 divisor 得到的商。
 *
 * 输入: dividend = 10, divisor = 3 输出: 3
 *
 * 说明: 被除数和除数均为 32 位有符号整数。 除数不为 0。 假设我们的环境只能存储 32 位有符号整数，其数值范围是 [−2^31,  2^31 − 1]。本题中，如果除法结果溢出，则返回
 * 231 − 1。
 */
public class NumberDivide {

  public static void main(String[] args) {
    System.out.println(new NumberDivide().divide(1100540749, -1090366779));
  }

  /**
   * 如果只是单纯的使用除数不断累加的操作，那么在被除数远大于除数的情况如：Integer.MAX_VALUE / 1 ，此时效率太慢。
   * 因此为了增加计算效率，可以对除数进行倍增操作，加快收敛速度，思想类似于二分查找（折半查找）。 同时需要考虑特殊情况：即Integer.MIN_VALUE /
   * -1，此时的值已经超过了int的范围，当前算法要求返回Integer.MAX_VALUE.
   */
  public int divide(int dividend, int divisor) {
    if (dividend == Integer.MIN_VALUE && divisor == -1) {
      return Integer.MAX_VALUE;
    }

    boolean neg = (dividend ^ divisor) < 0;
    // 全部转为负数，保证相同符号，并且负数的范围比整数的范围大
    int dived = dividend > 0 ? 0 - dividend : dividend;
    int div = divisor > 0 ? 0 - divisor : divisor;
    int i = 0;
    int multiplex = 1;
    while (div >= dived) {
      i += multiplex;
      dived -= div;
      if ((Integer.MIN_VALUE - div <= div) && div << 1 >= dived) { // 判断数字是否超出int范围
        multiplex <<= 1;
        div <<= 1;
      } else if (multiplex != 1) { // reset
        div = divisor > 0 ? 0 - divisor : divisor;
        multiplex = 1;
      }
    }
    return neg ? (0 - i) : i;
  }
}
