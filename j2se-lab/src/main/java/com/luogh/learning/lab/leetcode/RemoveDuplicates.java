package com.luogh.learning.lab.leetcode;

import java.util.Arrays;

/**
 * leetcode-26: 删除排序数组中的重复项
 *
 * 给定nums = [0,0,1,1,1,2,2,3,3,4], 函数应该返回新的长度5, 并且原数组nums的前五个元素被修改为 0, 1, 2, 3, 4。
 * 你不需要考虑数组中超出新长度后面的元素。
 */
public class RemoveDuplicates {

  public static void main(String[] args) {
    int[] arr = new int[]{0,0,1,1,1,2,2,3,3,4 };
    int length = new RemoveDuplicates().removeDuplicates2(arr);
    System.out.println("length:" + length);
    System.out.println("result: " + Arrays.toString(arr));
  }

  public int removeDuplicates2(int[] nums) {
    int i = 0;
    for (int j = 1; j < nums.length; j++) {
      if (nums[i] != nums[j]) {
        nums[++i] = nums[j];
      }
    }
    return i + 1;
  }

  public int removeDuplicates(int[] nums) {
    int endIndex = nums.length;
    int index = 0;
    while (index < endIndex - 1) {
      while (nums[index] == nums[index + 1] && index < endIndex - 1) {
        for (int k = index + 1; k < nums.length - 1; k++) {
          int tmp = nums[k];
          nums[k] = nums[k + 1];
          nums[k + 1] = tmp;
        }

        endIndex--;
      }
      index++;
    }

    return endIndex;
  }
}
