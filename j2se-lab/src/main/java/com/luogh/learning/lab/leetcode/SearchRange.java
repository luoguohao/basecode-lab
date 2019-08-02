package com.luogh.learning.lab.leetcode;

/**
 * leetcode-34: 在排序数组中查找元素的第一个和最后一个位置
 */
public class SearchRange {

  public static void main(String[] args) {
    int[] result = new SearchRange().searchRange(new int[]{5, 7, 7, 8, 8, 10}, 8);
    Utils.printArray(result);

    result = new SearchRange().searchRange(new int[]{5, 7, 7, 8, 8, 10}, 6);
    Utils.printArray(result);
  }

  public int[] searchRange(int[] nums, int target) {
    int startIndex = nums.length, endIndex = -1;
    int i = 0, j = nums.length - 1;
    while (i <= j) {
      if (nums[i] < target) {
        i++;
        if (nums[j] > target) {
          j--;
        } else if (nums[j] == target) {
          startIndex = Math.min(startIndex, j);
          endIndex = Math.max(endIndex, j);
          j--;
        }
      } else if (nums[i] == target) {
        startIndex = Math.min(startIndex, i);
        endIndex = Math.max(endIndex, i);
        i++;
      } else {
        break;
      }
    }

    return new int[]{startIndex == nums.length ? -1: startIndex, endIndex};
  }
}
