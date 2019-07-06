package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.List;

/**
 * leetcode-22: 括号生成
 *
 * 给出 n 代表生成括号的对数，请你写出一个函数，使其能够生成所有可能的并且有效的括号组合。
 *
 * 例如，给出 n = 3，生成结果为：
 *
 * [ "((()))", "(()())", "(())()", "()(())", "()()()" ]
 *
 *
 * 回溯法实现：回溯法就是沿着某种可能的方向一直前进，当此方向不通时，就退回到上一状态，换一个可能方向继续向前探索。 回溯法的关键：找到递归的出口；
 * 本题的解法：只要在回溯的过程中满足左括号的数量一直大于等于右括号数量即可
 */
public class GenerateParenthesis {

  public static void main(String[] args) {
    new GenerateParenthesis().generateParenthesis(2).forEach(System.out::println);
  }

  public List<String> generateParenthesis(int n) {
    List<String> result = new ArrayList<>();
    if (n > 0) {
      dfs("", 0, 0, n, result);
    }
    return result;
  }

  private void dfs(String chars, int left, int right, int total, List<String> collector) {
    if (chars.length() == 2 * total) {
      collector.add(chars);
      return;
    }
    if (left < total) {
      dfs(chars + "(", left + 1, right, total, collector);
    }
    if (right < left) {
      dfs(chars + ")", left, right + 1, total, collector);
    }
  }
}
