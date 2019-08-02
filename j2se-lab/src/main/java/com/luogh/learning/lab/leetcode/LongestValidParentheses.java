package com.luogh.learning.lab.leetcode;

/**
 * leetcode-32: 最长有效括号
 */
public class LongestValidParentheses {

  public static void main(String[] args) {
    int result = new LongestValidParentheses().longestValidParentheses(")()())");
    System.out.println(result);

    result = new LongestValidParentheses().longestValidParentheses("(()");
    System.out.println(result);

    result = new LongestValidParentheses().longestValidParentheses("((()()))");
    System.out.println(result);

    result = new LongestValidParentheses().longestValidParentheses(")(()()))()");
    System.out.println(result);

    result = new LongestValidParentheses().longestValidParentheses("()(()");
    System.out.println(result);

    result = new LongestValidParentheses().longestValidParentheses("((((((((");
    System.out.println(result);
  }

  /**
   * 动态规划：见https://leetcode-cn.com/problems/longest-valid-parentheses/solution/zui-chang-you-xiao-gua-hao-by-leetcode/
   */
  public int longestValidParentheses(String s) {
    int maxans = 0;
    int dp[] = new int[s.length()];
    for (int i = 1; i < s.length(); i++) {
      if (s.charAt(i) == ')') {
        if (s.charAt(i - 1) == '(') {
          dp[i] = (i >= 2 ? dp[i - 2] : 0) + 2;
        } else if (i - dp[i - 1] > 0 && s.charAt(i - dp[i - 1] - 1) == '(') {
          dp[i] = dp[i - 1] + ((i - dp[i - 1]) >= 2 ? dp[i - dp[i - 1] - 2] : 0) + 2;
        }
        maxans = Math.max(maxans, dp[i]);
      }
    }
    return maxans;
  }

  public int longestValidParentheses3(String s) {
    int left = 0, right = 0, max = 0;
    for (int j = 0; j < s.length(); j++) {
      if (s.charAt(j) == '(') {
        left++;
      } else {
        right++;
      }

      if (left == right) {
        max = Math.max(max, 2 * left);
      } else if (left < right) {
        left = right = 0;
      }
    }

    // 倒序
    left = right = 0;
    for (int j = s.length() - 1; j >= 0; j--) {
      if (s.charAt(j) == '(') {
        left++;
      } else {
        right++;
      }

      if (left == right) {
        max = Math.max(max, 2 * left);
      } else if (left > right) {
        left = right = 0;
      }
    }
    return max;
  }

  /**
   * 时间复杂度高
   */
  public int longestValidParentheses2(String s) {
    int max = 0;
    for (int i = 0; i < s.length(); i++) {
      int left = 0, right = 0, cnt = 0;
      for (int j = i; j < s.length(); j++) {
        if (s.charAt(j) == '(') {
          left++;
        } else {
          if (left == ++right) {
            cnt += 2 * left;
            max = Math.max(max, cnt);
            left = 0;
            right = 0;
          } else if (left < right) {
            max = Math.max(max, cnt);
            break;
          }
        }
      }
    }
    return max;
  }

  /**
   * 超时
   */
  public int longestValidParentheses1(String s) {
    return dfs(s, 0, 0, 0, 0, 0);
  }

  private int dfs(String s, int index, int leftCnt, int rightCnt, int cnt,
      int maxCnt) {
    if (s.length() <= index) {
      return Math.max(cnt, maxCnt);
    }
    char c = s.charAt(index);
    if (c == '(') {
      return dfs(s, index + 1, leftCnt + 1, rightCnt, cnt, maxCnt);
    } else {
      rightCnt++;
      if (leftCnt == rightCnt) {
        cnt += 2 * leftCnt;
        return dfs(s, index + 1, 0, 0, cnt, maxCnt);
      } else if (leftCnt < rightCnt) {
        return dfs(s, index + 1, 0, 0, 0, Math.max(maxCnt, cnt));
      } else {
        return dfs(s, index + 1, leftCnt, rightCnt, cnt,
            Math.max(maxCnt, cnt));
      }
    }
  }
}
