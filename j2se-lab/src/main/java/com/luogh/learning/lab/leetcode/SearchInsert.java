package com.luogh.learning.lab.leetcode;

/**
 * leetcode-35: 搜索插入位置
 *
 * 给定一个排序数组和一个目标值，在数组中找到目标值，并返回其索引。如果目标值不存在于数组中，返回它将会被按顺序插入的位置。 你可以假设数组中无重复元素。 示例 1: 输入: [1,3,5,6],
 * 5 输出: 2
 *
 * 示例 2: 输入: [1,3,5,6], 2 输出: 1
 *
 * 示例 3: 输入: [1,3,5,6], 7 输出: 4
 *
 * 示例 4: 输入: [1,3,5,6], 0 输出: 0
 */
public class SearchInsert {

  public static void main(String[] args) {
    int result = new SearchInsert().searchInsert(new int[]{1, 3, 5, 6, 9}, 0);
    System.out.println("result:" + result);
  }

  public int searchInsert(int[] nums, int target) {
    return secSerach(nums, 0, nums.length - 1, target);
  }

  /**
   * 二分查找
   * @param nums
   * @param start
   * @param end
   * @param target
   * @return
   */
  private int secSerach(int[] nums, int start, int end, int target) {
    if (start > end) {
      return start;
    }
    int mid = start + (end - start) / 2;
    if (nums[mid] == target) {
      return mid;
    } else if (nums[mid] > target) {
      return secSerach(nums, start, mid - 1, target);
    } else {
      return secSerach(nums, mid + 1, end, target);
    }
  }
}
