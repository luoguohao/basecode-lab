package com.luogh.learning.lab.leetcode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Main {

  public static void main(String[] args) {
    int max = lengthOfLongestSubstring("acab");
    System.out.println("max:" + max);
    int[] nums = new int[]{3, 3};
    int result = removeElement(nums, 5);
    for (int i = 0; i < result; i++) {
      System.out.println(nums[i]);
    }
    System.out.println("result:" + result);

  }


  public static int lengthOfLongestSubstring(String s) {
    int cursor = 0;
    int startIndex = 0;
    int maxLength = 0;
    Map<Character, Integer> chars = new HashMap<>(s.length());

    while (cursor < s.length()) {
      char c = s.charAt(cursor);
      Integer oldIndex = chars.get(c);
      if (oldIndex != null && oldIndex >= startIndex) {
        maxLength = Math.max(maxLength, cursor - startIndex);
        startIndex = chars.get(c) + 1;
      }
      chars.put(c, cursor);
      cursor++;
    }
    maxLength = Math.max(maxLength, cursor - startIndex);
    return maxLength;
  }

  public static int shortestSubarray(int[] A, int K) {
    int[] tmp = new int[A.length];
    int minLength = 0;
    int preSum = 0;
    Map<Integer, Integer> preIndex = new HashMap<>(A.length);

    for (int i = 0; i < A.length; i++) {
      int newValue = A[i];
      if (newValue >= K) {
        minLength = Math.min(minLength, 1);
      } else {
        preIndex.put(i, newValue);
        preSum += newValue;
        if (preSum < K) {

        } else {

        }
      }
    }

    return -1;
  }

  public static int removeElement(int[] nums, int val) {
    int result = 0;
    for (int i = 0; i < nums.length; i++) {
      boolean isChanged = false;
      if (nums[i] == val) {
        for (int j = i + 1; j < nums.length; j++) {
          if (nums[j] != val) {
            int tmp = nums[i];
            nums[i] = nums[j];
            nums[j] = tmp;
            isChanged = true;
            continue;
          }
        }
        if (isChanged) {
          result++;
        }
      } else {
        result++;
      }
    }
    return result;
  }

  public static int strStr(String haystack, String needle) {
    if (haystack.contains(needle) && needle.length() > 0) {
      return haystack.split(needle, -1)[0].length();
    } else {
      return -1;
    }
  }

  public static int strStrInViolence(String haystack, String needle) {
    if (haystack.equals(needle) || needle.length() == 0) {
      return 0;
    }

    if (haystack.length() < needle.length()) {
      return -1;
    }

    int i = 0;
    int j = 0;
    while (i < haystack.length() && j < needle.length()) {
      if (haystack.charAt(i) == needle.charAt(j)) {
        i++;
        j++;
      } else {
        i = i - j + 1;
        j = 0;
      }
    }

    return j >= needle.length() ? i - j : -1;
  }

  public static int strStrInKMP(String haystack, String needle) {
    if (haystack.equals(needle) || needle.length() == 0) {
      return 0;
    }

    if (haystack.length() < needle.length()) {
      return -1;
    }

    int i = 0;
    int j = 0;
    while (i < haystack.length() && j < needle.length()) {
      if (haystack.charAt(i) == needle.charAt(j)) {
        i++;
        j++;
      } else {
        // find the next right postion.
        i = i - j + 1;
        j = 0;
      }
    }

    return j >= needle.length() ? i - j : -1;
  }

  public List<List<Integer>> combinationSum2(int[] candidates, int target) {
    List<List<Integer>> result = new ArrayList<>();
    Arrays.sort(candidates);

    for (Integer cand : candidates) {

    }
    return result;
  }
}