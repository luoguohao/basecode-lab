package com.luogh.learning.lab.leetcode;

import java.util.Arrays;

/**
 * leetcode-16: 最接近的三数之和
 */
public class ThreeSumClosest {

  public static void main(String[] args) {
    int result = new ThreeSumClosest().threeSumClosest(new int[]{-1, 2, 1, -4}, 1);
    System.out.println(result);

    int result1 = new ThreeSumClosest().threeSumClosest(new int[]{0, 0, 0}, 1);
    System.out.println(result1);

    int result2 = new ThreeSumClosest().threeSumClosest(new int[]{0, 2, 1, -3}, 1);
    System.out.println(result2);
  }

  public int threeSumClosest(int[] nums, int target) {
    int closest = Integer.MAX_VALUE;
    Arrays.sort(nums);
    for (int i = 0; i < nums.length - 2; i++) {
      int m = i + 1, k = nums.length - 1;
      while (m < k) {
        int s = nums[i] + nums[m] + nums[k];
        if (s < target) {
          m++;
        } else {
          k--;
        }

        closest = Math.abs(closest) > Math.abs(target - s) ? target - s : closest;
      }
    }
    return target - closest;
  }
}
