package com.luogh.learning.lab.leetcode;

import java.util.Arrays;

/**
 * leetcode-31: 下一个排列
 *
 * 实现获取下一个排列的函数，算法需要将给定数字序列重新排列成字典序中下一个更大的排列。
 * 如果不存在下一个更大的排列，则将数字重新排列成最小的排列（即升序排列）。
 * 必须原地修改，只允许使用额外常数空间。
 * 以下是一些例子，输入位于左侧列，其相应输出位于右侧列。
 * 1,2,3 → 1,3,2
 * 3,2,1 → 1,2,3
 * 1,1,5 → 1,5,1
 */
public class NextPermutation {

  public static void main(String[] args) {
    int[] arr1 = new int[]{1, 2,3};
    new NextPermutation().nextPermutation(arr1);
    Utils.printArray(arr1);

    int[] arr2 = new int[]{3, 2 , 1};
    new NextPermutation().nextPermutation(arr2);
    Utils.printArray(arr2);


    int[] arr3 = new int[]{1,1,5};
    new NextPermutation().nextPermutation(arr3);
    Utils.printArray(arr3);

    int[] arr4 = new int[]{1, 3, 2};
    new NextPermutation().nextPermutation(arr4);
    Utils.printArray(arr4);

    int[] arr5 = new int[]{1, 2,3};
    new NextPermutation().nextPermutation(arr5);
    Utils.printArray(arr5);
  }


  public void nextPermutation(int[] nums) {
   for (int i = nums.length - 1; i > 0; i--) {
     if (nums[i] > nums[i-1]) {
       for (int j = nums.length -1; j >= i; j--) {
         if (nums[j] > nums[i-1]) {
           int tmp = nums[i-1];
           nums[i-1] = nums[j];
           nums[j] = tmp;
           Arrays.sort(nums, i, nums.length);
           return;
         }
       }
     }
   }
   Arrays.sort(nums);
  }
}
