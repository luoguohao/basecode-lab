package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * leetcode-15: 三数之和
 */
public class ThreeSum {

  public static void main(String[] args) {
    List<List<Integer>> result = new ThreeSum().threeSum2(new int[]{
        -1, 0, 1, 2, -1, -4
    });

    List<List<Integer>> result1 = new ThreeSum().threeSum2(new int[]{
        0, 0, 0
    });

    result.forEach(System.out::println);
    System.out.println("=========");
    result1.forEach(System.out::println);
    System.out.println("=========");
  }


  /**
   * 排序 + 双指针
   */
  public List<List<Integer>> threeSum2(int[] nums) {
    if (nums.length < 3) {
      return new ArrayList<>();
    }
    Arrays.sort(nums);
    List<List<Integer>> lists = new ArrayList<>();
    for (int i = 0; i < nums.length - 2; i++) {
      if (nums[i] > 0) {
        break;
      }
      if (i > 0 && nums[i] == nums[i - 1]) {
        continue; // 去除重复
      }

      int m = i + 1, k = nums.length - 1;

      while (m < k) {
        if (nums[i] + nums[m] <= 0) {

          if ((k > m + 1 && nums[k] == nums[k - 1])/*考虑重复的问题*/ || nums[i] + nums[m] + nums[k] > 0) {
            k--;
          } else if ((m < k - 1 && nums[m] == nums[m + 1])/*考虑重复的问题*/
              || nums[i] + nums[m] + nums[k] < 0) {
            m++;
          } else {
            lists.add(Arrays.asList(nums[i], nums[m++], nums[k--]));
          }
        } else {
          break;
        }
      }
    }
    return lists;
  }

  /**
   * 超出时间限额
   */
  public List<List<Integer>> threeSum(int[] nums) {
    if (nums.length < 3) {
      return new ArrayList<>();
    }

    List<List<Integer>> lists = new ArrayList<>();
    Arrays.sort(nums);

    int max = nums[nums.length - 1];
    if (max < 0) {
      return new ArrayList<>();
    }

    Set<String> exists = new HashSet<>(3 * nums.length - 3);

    for (int i = 0; i < nums.length - 2; i++) {
      if (nums[i] <= 0) {
        for (int j = i + 1; j < nums.length - 1; j++) {
          if (nums[i] + nums[j] <= 0) {
            for (int k = nums.length - 1; k >= j + 1 && nums[k] >= 0; k--) {
              if (nums[i] + nums[j] + nums[k] == 0) {
                int[] sorted = new int[]{nums[i], nums[j], nums[k]};
                Arrays.sort(sorted);
                String key = "" + sorted[0] + sorted[1] + sorted[2];
                if (!exists.contains(key)) {
                  List<Integer> list = new ArrayList<>();
                  list.add(sorted[0]);
                  list.add(sorted[1]);
                  list.add(sorted[2]);
                  lists.add(list);
                  exists.add(key);
                }
              } else if (nums[i] + nums[j] + nums[k] < 0) {
                break;
              }
            }
          } else {
            break;
          }
        }
      } else {
        break;
      }
    }
    return lists;
  }
}
