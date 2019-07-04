package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * leetcode-18: 四数之和
 */
public class FourSum {

  public static void main(String[] args) {

    int[] ar = new int[] { 7,1,7,-7,10,3,-5,-3,-9,6,-8,-6,-10,10,-3,4,2 };
    Arrays.sort(ar);
    for (int a : ar) {
      System.out.print(a + ",");
    }


    System.out.println("....");


    new FourSum().fourSum(new int[]{1, 0, -1, 0, -2, 2}, 0).forEach(System.out::println);
    System.out.println("-----------");
    new FourSum().fourSum(new int[]{0, 1, 5, 0, 1, 5, 5, -4}, 11).forEach(System.out::println);
  }

  public List<List<Integer>> fourSum(int[] nums, int target) {
    if (nums.length < 4) {
      return new ArrayList<>();
    }

    Arrays.sort(nums);

    List<List<Integer>> result = new ArrayList<>();
    for (int i = 0; i < nums.length - 3; i++) {
      if (i > 0 && nums[i] == nums[i - 1]) {
        continue;
      }
      for (int j = i + 1; j < nums.length - 2; j++) {
        if (j > i + 1 && nums[j] == nums[j - 1]) {
          continue;
        }
        int sum = nums[i] + nums[j];
        int m = j + 1, k = nums.length - 1;

        while (m < k) {
          if ((k <= nums.length - 2 && nums[k] == nums[k + 1]) || sum + nums[m] + nums[k] > target) {
            k--;
          } else if ((m > j + 2 && nums[m] == nums[m - 1]) || sum + nums[m] + nums[k] < target) {
            m++;
          } else {
            result.add(Arrays.asList(nums[i], nums[j], nums[m++], nums[k--]));
          }
        }
      }
    }
    return result;
  }
}
