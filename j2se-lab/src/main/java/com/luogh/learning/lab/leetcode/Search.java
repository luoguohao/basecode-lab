package com.luogh.learning.lab.leetcode;

/**
 * leetcode-33: 搜索旋转排序数组
 */
public class Search {

  public static void main(String[] args) {
    int result = new Search().search(new int[]{4, 5, 6, 7, 0, 1, 2}, 0);
    System.out.println("result:" + result);

    result = new Search().search(new int[]{4, 5, 6, 7, 0, 1, 2}, 3);
    System.out.println("result:" + result);

    result = new Search().search(new int[]{4, 5, 6, 7, 0, 1, 2}, 6);
    System.out.println("result:" + result);

    result = new Search().search(new int[]{1, 3}, 3);
    System.out.println("result:" + result);

    result = new Search().search(new int[]{3, 1}, 3);
    System.out.println("result:" + result);
  }

  public int search(int[] nums, int target) {
    int i = 0, j = nums.length - 1;
    while (i <= j) {
      if (nums[i] > target) {
        if (nums[j] > target) {
          i++;
          j--;
        } else if (nums[j] == target) {
          return j;
        } else {
          return -1;
        }
      } else if (nums[i] == target) {
        return i;
      } else {
        i++;
      }
    }

    return -1;
  }

}
