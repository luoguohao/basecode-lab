package com.luogh.learning.lab.leetcode;

/**
 * leetcode-12/leetcode-13: 整数转罗马数字&罗马数字转整数
 */
public class RomanNumber {

  /**
   * 罗马数字转整数
   * @param s
   * @return
   */
  public int romanToInt(String s) {
    int number = 0;
    int preNumber = 0;
    for (int i = s.length() -1; i >= 0; i--) {
      switch(s.charAt(i)) {
        case 'I':
          if (preNumber > 1) {
            number -= 1;
          } else {
            number += 1;
          }
          preNumber = 1;
          break;
        case 'V':
          number += 5;
          preNumber = 5;
          break;
        case 'X':
          if (preNumber > 10) {
            number -= 10;
          } else {
            number += 10;
          }
          preNumber = 10;
          break;
        case 'L':
          number += 50;
          preNumber = 50;
          break;
        case 'C':
          if (preNumber > 100) {
            number -= 100;
          } else {
            number += 100;
          }
          preNumber = 100;
          break;
        case 'D':
          number += 500;
          preNumber = 500;
          break;
        case 'M':
          number += 1000;
          preNumber = 1000;
          break;
      }
    }

    return number;
  }

  public String intToRoman(int num) {
    if (num >= 1000) {
      return 'M' + intToRoman(num - 1000);
    } else if (num >= 900) {
      return "CM" + intToRoman(num - 900);
    } else if (num >= 500) {
      return 'D' + intToRoman(num - 500);
    } else if (num >= 400) {
      return "CD" + intToRoman(num - 400);
    } else if (num >= 100) {
      return 'C' + intToRoman(num - 100);
    } else if (num >= 90) {
      return "XC" + intToRoman(num - 90);
    } else if (num >= 50) {
      return 'L' + intToRoman(num - 50);
    } else if (num >= 40) {
      return "XL" + intToRoman(num - 40);
    } else if (num >= 10) {
      return 'X' + intToRoman(num - 10);
    } else if (num >= 9) {
      return "IX" + intToRoman(num - 9);
    } else if (num >= 5) {
      return 'V' + intToRoman(num - 5);
    } else if (num >= 4) {
      return "IV" + intToRoman(num - 4);
    } else if (num >= 1) {
      return 'I' + intToRoman(num - 1);
    } else {
      return "";
    }
  }

  public String intToRoman2(int num) {
    //用数组定义字典
    int[] values = {1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1};
    String[] strs = {"M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"};

    StringBuilder res = new StringBuilder();

    for (int i = 0; i < values.length; i++) {
      int digitAt = num / values[i];
      if (digitAt == 0) {
        continue;
      }
      for (int j = digitAt; j > 0; j--) {
        res.append(strs[i]);
      }
      num -= (digitAt * values[i]);
      if (num == 0) {
        break;
      }
    }
    return res.toString();
  }
}
