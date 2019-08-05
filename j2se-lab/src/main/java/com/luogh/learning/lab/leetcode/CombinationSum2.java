package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * leetcode-40: 组合总和 II
 */
public class CombinationSum2 {

  public static void main(String[] args) {
    List<List<Integer>> result = new CombinationSum2()
        .combinationSum2(new int[]{10, 1, 2, 7, 6, 1 , 5}, 8);
    Utils.printList(result);
  }

  /**
   * @param candidates 包含重复项
   */
  public List<List<Integer>> combinationSum2(int[] candidates, int target) {
    Arrays.sort(candidates);

    List<List<Integer>> result = new ArrayList<>();
    int prev = -1;
    for (int i = 0; i < candidates.length; i++) {
      if (prev != candidates[i]) {
        prev = candidates[i];
        combine(candidates, i, target, new ArrayList<>(), result);
      }
    }

    return result;
  }

  private void combine(int[] candidates, int i, int remaining, List<Integer> list,
      List<List<Integer>> result) {
    if (i >= candidates.length) {
      return;
    }

    int r = remaining - candidates[i];
    if (r == 0) {
      list.add(candidates[i]);
      result.add(list);
    } else if (r > 0) {
      list.add(candidates[i]);
      int prev = -1;
      for (int k = i + 1; k < candidates.length; k++) {
        if (r - candidates[k] >= 0 && prev != candidates[k]) {
          prev = candidates[k];
          List<Integer> cList = new ArrayList<>(list);
          combine(candidates, k, r, cList, result);
        }
      }
    }
  }
}
