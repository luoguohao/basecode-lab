package com.luogh.learning.lab.leetcode;

/**
 * leetcode-11: 盛最多水的容器 输入: [1,8,6,2,5,4,8,3,7] 输出: 49
 *
 * Max{min(x, y) * slop(x, y)}
 */
public class MaxWaterArea {

  public int maxArea(int[] height) {
    int max = 0;
    for (int i = 0; i < height.length; i++) {
      for (int j = i + 1; j < height.length; j++) {
        max = Math.max(max, Math.min(height[i], height[j]) * (j - i));
      }
    }
    return max;
  }
}
