package com.luogh.learning.lab.leetcode;

/**
 * leetcode-10: 正则表达式匹配
 *
 * 重叠子问题，可以使用动态规划算法
 * 对于本问题，我们先抽出算法框架：
 * def dp(i, j):
 *     dp(i, j + 2)     #1
 *     dp(i + 1, j)     #2
 *     dp(i + 1, j + 1) #3
 *
 * 提出类似的问题，请问如何从原问题 dp(i, j) 触达子问题 dp(i + 2, j + 2) ？
 *
 * 至少有两种路径，
 *    一是 dp(i, j) -> #3 -> #3，
 *    二是 dp(i, j) -> #1 -> #2 -> #2。
 *
 * 因此，本问题一定存在重叠子问题，一定需要动态规划的优化技巧来处理。
 */
public class RegexPatternMatch {

  /**
   * 暴力破解法
   * @param text
   * @param pattern
   * @return
   */
  public boolean isMatch(String text, String pattern) {
    if (pattern.isEmpty()) {
      return text.isEmpty();
    }

    boolean isMatched =
        !text.isEmpty() && (text.charAt(0) == pattern.charAt(0) || pattern.charAt(0) == '.');

    if (pattern.length() >= 2 && pattern.charAt(1) == '*') {
      return isMatch(text, pattern.substring(2)) || (isMatched && isMatch(text.substring(1),
          pattern));
    } else {
      return isMatched && isMatch(text.substring(1), pattern.substring(1));
    }
  }

  enum Result {
    TRUE, FALSE
  }


  // 自顶向下的动态规划方法
  public boolean isMatchUsingDpWithUpToBottom(String text, String pattern) {
    return dp(0, 0, text, pattern, new Result[text.length() + 1][pattern.length() + 1]);
  }

  public boolean dp(int i, int j, String text, String pattern, Result[][] memo) {
    if (memo[i][j] != null) {
      return memo[i][j] == Result.TRUE;
    }
    boolean ans;
    if (j == pattern.length()) {
      ans = i == text.length();
    } else {
      boolean first_match = (i < text.length() &&
          (pattern.charAt(j) == text.charAt(i) ||
              pattern.charAt(j) == '.'));

      if (j + 1 < pattern.length() && pattern.charAt(j + 1) == '*') {
        ans = (dp(i, j + 2, text, pattern, memo) ||
            first_match && dp(i + 1, j, text, pattern, memo));
      } else {
        ans = first_match && dp(i + 1, j + 1, text, pattern, memo);
      }
    }
    memo[i][j] = (ans ? Result.TRUE : Result.FALSE);
    return ans;
  }

  // 自底向上的动态规划方法
  public boolean isMatchUsingDpWithBottomToUp(String text, String pattern) {
    boolean[][] dp = new boolean[text.length() + 1][pattern.length() + 1];

    dp[text.length()][pattern.length()] = true;

    for (int i = text.length(); i >= 0; i--) {
      for (int j = pattern.length() - 1; j >= 0; j--) {

        boolean first_match = (i < text.length() &&
            (pattern.charAt(j) == text.charAt(i) ||
                pattern.charAt(j) == '.'));

        if (j + 1 < pattern.length() && pattern.charAt(j + 1) == '*') {
          dp[i][j] = dp[i][j + 2] || first_match && dp[i + 1][j];
        } else {
          dp[i][j] = first_match && dp[i + 1][j + 1];
        }
      }
    }

    return dp[0][0];

  }
}
