package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * leetcode-39: 组合总和
 */
public class CombinationSum {

  public static void main(String[] args) {
    List<List<Integer>> result = new CombinationSum().combinationSum(new int[]{2, 3, 6,7}, 7);
    Utils.printList(result);
  }

  /**
   * @param candidates 无重复项
   * @param target
   * @return
   */
  public List<List<Integer>> combinationSum(int[] candidates, int target) {
    Arrays.sort(candidates);

    List<List<Integer>> result = new ArrayList<>();
    for (int i = 0; i < candidates.length; i++) {
      combine(candidates, i, target, new ArrayList<>(), result);
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
      for (int k = i; k < candidates.length; k++) {
        if (r - candidates[k] >= 0) {
          List<Integer> cList = new ArrayList<>(list);
          combine(candidates, k, r, cList, result);
        }
      }
    }
  }
}
